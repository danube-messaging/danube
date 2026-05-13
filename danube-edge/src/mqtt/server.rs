//! MQTT TCP server.
//!
//! Binds to the configured address (default `0.0.0.0:1883`) and accepts
//! incoming MQTT connections. Each connection is handed off to a dedicated
//! `session::run_session` task.
//!
//! Production features:
//! - **Max connections**: `Arc<Semaphore>` enforces a global connection limit.
//!   When full, new connections are accepted and immediately closed (avoids
//!   TCP backlog buildup).
//! - **Graceful shutdown**: Respects a `CancellationToken` — stops accepting
//!   new connections when signalled.

use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::{watch, Semaphore};
use tracing::{error, info, warn};

use crate::mqtt::bridge::TopicRouter;
use crate::mqtt::ingester::MqttIngester;
use crate::mqtt::metrics;

/// Start the MQTT TCP listener.
///
/// Runs until the `shutdown` token is cancelled, accepting connections
/// and spawning session tasks (up to `max_connections` concurrently).
pub async fn start_listener(
    addr: &str,
    router: Arc<TopicRouter>,
    ingester: Arc<MqttIngester>,
    max_payload_size: usize,
    max_connections: usize,
    mut shutdown: watch::Receiver<bool>,
) {
    let listener = match TcpListener::bind(addr).await {
        Ok(l) => {
            info!(
                addr = %addr,
                max_connections,
                max_payload_size,
                "MQTT server listening"
            );
            l
        }
        Err(e) => {
            error!(addr = %addr, error = %e, "failed to bind MQTT listener");
            return;
        }
    };

    let semaphore = Arc::new(Semaphore::new(max_connections));

    loop {
        // Check shutdown before blocking on accept
        tokio::select! {
            biased;

            _ = shutdown.changed() => {
                info!("MQTT server: shutdown signal received, stopping accept loop");
                break;
            }

            result = listener.accept() => {
                match result {
                    Ok((socket, peer)) => {
                        // Try to acquire a connection permit
                        let permit = match semaphore.clone().try_acquire_owned() {
                            Ok(p) => p,
                            Err(_) => {
                                warn!(
                                    peer = %peer,
                                    max_connections,
                                    "max MQTT connections reached, rejecting"
                                );
                                metrics::connection_rejected();
                                // Accept + immediately drop to avoid TCP backlog
                                drop(socket);
                                continue;
                            }
                        };

                        let router = router.clone();
                        let ingester = ingester.clone();
                        tokio::spawn(async move {
                            super::session::run_session(
                                socket, peer, router, ingester, max_payload_size,
                            )
                            .await;
                            // Permit drops here → frees the semaphore slot
                            drop(permit);
                        });
                    }
                    Err(e) => {
                        warn!(error = %e, "failed to accept MQTT connection");
                    }
                }
            }
        }
    }
}
