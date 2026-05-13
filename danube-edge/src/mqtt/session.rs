//! Per-connection MQTT session state machine.
//!
//! Handles the lifecycle of a single MQTT device connection:
//! - CONNECT → CONNACK handshake
//! - PINGREQ → PINGRESP keep-alive
//! - PUBLISH → topic mapping + ingestion
//! - SUBSCRIBE → rejected (telemetry-only ingestion)
//! - DISCONNECT → graceful teardown
//!
//! Production features:
//! - **Keep-alive timeout**: session closes if no packet arrives within
//!   1.5× the client's `keep_alive` value (per MQTT spec §3.1.2.10).
//! - **Max payload size**: oversized packets rejected at codec level.
//! - **Metrics**: connection/message counters instrumented via `metrics.rs`.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use mqtt_frame::packet::{ConnAck, PubAck};
use mqtt_frame::{MqttCodec, MqttPacket, ProtocolLevel};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tracing::{debug, info, warn};

use crate::mqtt::bridge::{self, TopicRouter};
use crate::mqtt::ingester::{IngestError, MqttIngester};
use crate::mqtt::metrics;

/// Default keep-alive timeout when the client sends `keep_alive: 0`.
/// Per MQTT spec, 0 means "no keep-alive" but we still enforce a server-side
/// timeout to prevent zombie sessions.
const DEFAULT_KEEP_ALIVE_SECS: u64 = 300; // 5 minutes

/// Run the session loop for a single MQTT connection.
///
/// This function owns the TCP stream for its lifetime. It:
/// 1. Waits for CONNECT (must be the first packet)
/// 2. Replies with CONNACK
/// 3. Enters the main packet loop (PUBLISH, PINGREQ, etc.)
/// 4. Returns when the client disconnects, times out, or an error occurs
pub async fn run_session(
    socket: TcpStream,
    peer: std::net::SocketAddr,
    router: Arc<TopicRouter>,
    ingester: Arc<MqttIngester>,
    max_payload_size: usize,
) {
    let mut framed = Framed::new(socket, MqttCodec::with_max_packet_size(max_payload_size));

    // ---- Step 1: CONNECT handshake ----
    let (client_id, protocol_level, keep_alive) = match framed.next().await {
        Some(Ok(MqttPacket::Connect(connect))) => {
            debug!(
                peer = %peer,
                client_id = %connect.client_id,
                protocol = ?connect.protocol_level,
                keep_alive = connect.keep_alive,
                "MQTT CONNECT received"
            );
            (
                connect.client_id.clone(),
                connect.protocol_level,
                connect.keep_alive,
            )
        }
        Some(Ok(other)) => {
            warn!(peer = %peer, packet = ?other, "expected CONNECT as first packet, closing");
            return;
        }
        Some(Err(e)) => {
            warn!(peer = %peer, error = %e, "error reading first packet, closing");
            return;
        }
        None => {
            debug!(peer = %peer, "connection closed before CONNECT");
            return;
        }
    };

    // ---- Step 2: CONNACK ----
    if let Err(e) = framed
        .send(MqttPacket::ConnAck(ConnAck {
            session_present: false,
            return_code: 0, // Connection Accepted
        }))
        .await
    {
        warn!(peer = %peer, error = %e, "failed to send CONNACK");
        return;
    }

    // Calculate keep-alive timeout: 1.5× client value (per MQTT spec §3.1.2.10).
    // If client sends 0, use a server-side default to prevent zombie sessions.
    let timeout_secs = if keep_alive == 0 {
        DEFAULT_KEEP_ALIVE_SECS
    } else {
        (keep_alive as u64 * 3) / 2
    };
    let session_timeout = Duration::from_secs(timeout_secs);

    info!(
        peer = %peer,
        client_id = %client_id,
        keep_alive_secs = keep_alive,
        timeout_secs,
        "MQTT session established"
    );

    metrics::connection_opened();

    // ---- Step 3: Main packet loop (with keep-alive timeout) ----
    loop {
        // Wait for the next packet, or time out if the device goes silent.
        let result = tokio::time::timeout(session_timeout, framed.next()).await;

        let packet = match result {
            // Timeout: device missed keep-alive
            Err(_) => {
                warn!(
                    peer = %peer,
                    client_id = %client_id,
                    timeout_secs,
                    "keep-alive timeout, closing session"
                );
                metrics::message_dropped("_session", "keep_alive_timeout");
                break;
            }
            // Stream ended (TCP closed)
            Ok(None) => {
                debug!(peer = %peer, client_id = %client_id, "connection closed by peer");
                break;
            }
            // Decode error
            Ok(Some(Err(e))) => {
                warn!(
                    peer = %peer,
                    client_id = %client_id,
                    error = %e,
                    "MQTT decode error, closing session"
                );
                break;
            }
            // Got a packet
            Ok(Some(Ok(p))) => p,
        };

        match packet {
            MqttPacket::Publish(publish) => {
                let qos = publish.qos;
                let packet_id = publish.packet_id;
                let mqtt_topic = publish.topic.clone();

                metrics::message_received(&mqtt_topic);

                let mut validation_failed = false;

                // Route the MQTT topic to a Danube topic
                match router.resolve(&mqtt_topic) {
                    Some(match_result) => {
                        // Merge MQTT v5 properties into attributes
                        let mut attributes = match_result.attributes;
                        for prop in &publish.properties {
                            match prop {
                                mqtt_frame::Property::ContentType(v) => {
                                    attributes
                                        .insert("mqtt.content_type".to_string(), v.clone());
                                }
                                mqtt_frame::Property::ResponseTopic(v) => {
                                    attributes
                                        .insert("mqtt.response_topic".to_string(), v.clone());
                                }
                                mqtt_frame::Property::UserProperty(k, v) => {
                                    attributes
                                        .insert(format!("mqtt.user.{}", k), v.clone());
                                }
                                _ => {} // Other properties are metadata, not user attributes
                            }
                        }

                        // Build StreamMessage
                        let stream_msg = bridge::build_stream_message(
                            &match_result.danube_topic,
                            Bytes::copy_from_slice(&publish.payload),
                            attributes,
                        );

                        // Ingest (batched)
                        match ingester.ingest(&match_result.danube_topic, stream_msg).await {
                            Ok(()) => {
                                // Success — fall through to send PUBACK
                            }
                            Err(IngestError::ValidationFailed(reason)) => {
                                // Permanent error: payload will never be valid.
                                // - MQTT v5: send PUBACK with reason_code 0x99
                                //   (Payload format invalid) so the device knows why.
                                // - MQTT v3.1.1: send normal PUBACK to stop retries
                                //   (v3.1.1 has no rejection mechanism).
                                // In both cases, the message is dropped (not ingested).
                                warn!(
                                    peer = %peer,
                                    client_id = %client_id,
                                    mqtt_topic = %mqtt_topic,
                                    reason = %reason,
                                    protocol = ?protocol_level,
                                    "payload validation failed, acknowledging to stop retries"
                                );
                                validation_failed = true;
                                metrics::message_dropped(&mqtt_topic, "validation_failed");
                                // Fall through to PUBACK (with reason_code for v5)
                            }
                            Err(e) => {
                                // Transient error (not ready, WAL failure, etc.).
                                // Withhold PUBACK so QoS 1 clients retry later.
                                warn!(
                                    peer = %peer,
                                    client_id = %client_id,
                                    mqtt_topic = %mqtt_topic,
                                    error = %e,
                                    "ingestion failed (transient), withholding PUBACK"
                                );
                                metrics::message_dropped(&mqtt_topic, "transient_error");
                                continue;
                            }
                        }
                    }
                    None => {
                        debug!(
                            peer = %peer,
                            mqtt_topic = %mqtt_topic,
                            "no topic mapping found, dropping message"
                        );
                        metrics::message_dropped(&mqtt_topic, "no_mapping");
                        // Still send PUBACK so the client doesn't retry forever
                    }
                }

                // QoS 1: acknowledge
                if qos == 1 {
                    if let Some(pid) = packet_id {
                        // For MQTT v5 validation failures, set reason_code 0x99
                        // (Payload format invalid). For v3.1.1 or success, None.
                        let reason_code = if validation_failed
                            && protocol_level == ProtocolLevel::V5
                        {
                            Some(0x99) // Payload format invalid
                        } else {
                            None // Success (or v3.1.1 accept-but-drop)
                        };

                        if let Err(e) = framed
                            .send(MqttPacket::PubAck(PubAck {
                                packet_id: pid,
                                reason_code,
                            }))
                            .await
                        {
                            warn!(
                                peer = %peer,
                                error = %e,
                                "failed to send PUBACK"
                            );
                            break;
                        }
                    }
                }
            }

            MqttPacket::PingReq => {
                if let Err(e) = framed.send(MqttPacket::PingResp).await {
                    warn!(peer = %peer, error = %e, "failed to send PINGRESP");
                    break;
                }
            }

            MqttPacket::Subscribe(sub) => {
                // Reject subscriptions — this is a telemetry-only ingestion gateway.
                let return_codes = vec![0x80u8; sub.filters.len()]; // 0x80 = Failure
                if let Err(e) = framed
                    .send(MqttPacket::SubAck(mqtt_frame::packet::SubAck {
                        packet_id: sub.packet_id,
                        return_codes,
                    }))
                    .await
                {
                    warn!(peer = %peer, error = %e, "failed to send SUBACK rejection");
                    break;
                }
                debug!(
                    peer = %peer,
                    client_id = %client_id,
                    "SUBSCRIBE rejected (telemetry-only mode)"
                );
            }

            MqttPacket::Disconnect => {
                info!(
                    peer = %peer,
                    client_id = %client_id,
                    "MQTT client disconnected gracefully"
                );
                break;
            }

            other => {
                debug!(
                    peer = %peer,
                    packet = ?other,
                    "ignoring unsupported packet type"
                );
            }
        }
    }

    metrics::connection_closed();
    debug!(
        peer = %peer,
        client_id = %client_id,
        "MQTT session ended"
    );
}
