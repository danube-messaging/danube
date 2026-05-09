//! MQTT TCP server.
//!
//! Binds to the configured address (default `0.0.0.0:1883`) and accepts
//! incoming MQTT connections. Each connection is handed off to a dedicated
//! `session::run_session` task.

use std::sync::Arc;

use tokio::net::TcpListener;
use tracing::{error, info, warn};

use crate::mqtt::bridge::TopicRouter;
use crate::mqtt::ingester::MqttIngester;

/// Start the MQTT TCP listener.
///
/// This function runs forever, accepting connections and spawning session tasks.
/// It should be called via `tokio::spawn()` from the edge bootstrap.
pub async fn start_listener(
    addr: &str,
    router: Arc<TopicRouter>,
    ingester: Arc<MqttIngester>,
) {
    let listener = match TcpListener::bind(addr).await {
        Ok(l) => {
            info!(addr = %addr, "MQTT server listening");
            l
        }
        Err(e) => {
            error!(addr = %addr, error = %e, "failed to bind MQTT listener");
            return;
        }
    };

    loop {
        match listener.accept().await {
            Ok((socket, peer)) => {
                let router = router.clone();
                let ingester = ingester.clone();
                tokio::spawn(async move {
                    super::session::run_session(socket, peer, router, ingester).await;
                });
            }
            Err(e) => {
                warn!(error = %e, "failed to accept MQTT connection");
            }
        }
    }
}
