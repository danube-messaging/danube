/// Valkey rotation listener — cleans up `active_segment` offsets on WAL rotation.
///
/// Spawns a background task that listens for [`RotationEvent`]s from the WAL
/// writer and removes the rotated offset range from the Valkey `active_segment`
/// hash. This keeps Valkey in sync with what the WAL considers "active".
///
/// When the WAL rotates, the sealed file's offsets are no longer in the active
/// file. Removing them from Valkey means `active_segment` always mirrors only
/// the current active WAL file — regardless of storage mode (Local, S3, etc.)
/// and regardless of whether a durable exporter is running.

use super::client::ValkeyClient;
use super::segment_lifecycle;
use crate::wal::RotationEvent;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::debug;

/// Spawn a background task that listens for WAL rotation events and cleans
/// up the corresponding offsets from the Valkey `active_segment` hash.
///
/// The task runs until the `rotation_rx` channel is closed (which happens
/// when the WAL writer shuts down and drops the sender).
///
/// # Arguments
/// * `valkey`       — Shared Valkey client connection
/// * `topic_path`   — e.g. `"default/my-topic"`
/// * `rotation_rx`  — Receiver end of the WAL rotation event channel
pub fn spawn_rotation_listener(
    valkey: Arc<ValkeyClient>,
    topic_path: String,
    mut rotation_rx: mpsc::UnboundedReceiver<RotationEvent>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        debug!(
            target = "valkey_rotation",
            topic = %topic_path,
            "rotation listener started"
        );

        while let Some(event) = rotation_rx.recv().await {
            debug!(
                target = "valkey_rotation",
                topic = %topic_path,
                start = event.start_offset,
                end = event.end_offset,
                "WAL rotation event — cleaning up Valkey active_segment"
            );

            segment_lifecycle::cleanup_exported_offsets(
                &valkey,
                &topic_path,
                event.start_offset,
                event.end_offset,
            )
            .await;
        }

        debug!(
            target = "valkey_rotation",
            topic = %topic_path,
            "rotation listener stopped (WAL channel closed)"
        );
    })
}
