//! Per-connection MQTT session state machine.
//!
//! Handles the lifecycle of a single MQTT device connection:
//! - CONNECT → CONNACK handshake
//! - PINGREQ → PINGRESP keep-alive
//! - PUBLISH → topic mapping + ingestion
//! - SUBSCRIBE → rejected (telemetry-only ingestion)
//! - DISCONNECT → graceful teardown

use std::sync::Arc;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use mqtt_frame::packet::{ConnAck, PubAck};
use mqtt_frame::{MqttCodec, MqttPacket, ProtocolLevel};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tracing::{debug, info, warn};

use crate::mqtt::bridge::{self, TopicRouter};
use crate::mqtt::ingester::{IngestError, MqttIngester};

/// Run the session loop for a single MQTT connection.
///
/// This function owns the TCP stream for its lifetime. It:
/// 1. Waits for CONNECT (must be the first packet)
/// 2. Replies with CONNACK
/// 3. Enters the main packet loop (PUBLISH, PINGREQ, etc.)
/// 4. Returns when the client disconnects or an error occurs
pub async fn run_session(
    socket: TcpStream,
    peer: std::net::SocketAddr,
    router: Arc<TopicRouter>,
    ingester: Arc<MqttIngester>,
) {
    let mut framed = Framed::new(socket, MqttCodec::new());

    // ---- Step 1: CONNECT handshake ----
    let (client_id, protocol_level) = match framed.next().await {
        Some(Ok(MqttPacket::Connect(connect))) => {
            debug!(
                peer = %peer,
                client_id = %connect.client_id,
                protocol = ?connect.protocol_level,
                "MQTT CONNECT received"
            );
            (connect.client_id.clone(), connect.protocol_level)
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

    info!(
        peer = %peer,
        client_id = %client_id,
        "MQTT session established"
    );

    // ---- Step 3: Main packet loop ----
    while let Some(result) = framed.next().await {
        let packet = match result {
            Ok(p) => p,
            Err(e) => {
                warn!(
                    peer = %peer,
                    client_id = %client_id,
                    error = %e,
                    "MQTT decode error, closing session"
                );
                break;
            }
        };

        match packet {
            MqttPacket::Publish(publish) => {
                let qos = publish.qos;
                let packet_id = publish.packet_id;
                let mqtt_topic = publish.topic.clone();

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

    debug!(
        peer = %peer,
        client_id = %client_id,
        "MQTT session ended"
    );
}
