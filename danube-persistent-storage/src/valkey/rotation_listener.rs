/// Valkey rotation listener — cleans up old segments from the stream on WAL rotation.
///
/// This background task listens for `RotationEvent` messages from the WAL
/// writer. When warm caching is enabled, it keeps the last `max_cached` segments
/// in the stream by delaying the trim operation. When the cache limit is exceeded,
/// it trims the oldest segment from the stream using `XTRIM MINID`.

use super::client::ValkeyClient;
use crate::valkey::config::WriteBufferConfig;
use crate::wal::RotationEvent;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, warn};
use crate::metadata::StorageMetadata;

/// Spawn a background task that listens for WAL rotation events and trims
/// old segments from the Valkey stream.
///
/// The task runs until the `rotation_rx` channel is closed.
pub fn spawn_rotation_listener(
    valkey: Arc<ValkeyClient>,
    topic_path: String,
    config: WriteBufferConfig,
    mut rotation_rx: mpsc::UnboundedReceiver<RotationEvent>,
    metadata: StorageMetadata,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        debug!(
            target = "valkey_rotation",
            topic = %topic_path,
            "rotation listener started"
        );

        let stream_key = format!("/topics/{}/stream", topic_path);
        let mut recent_rotations: VecDeque<RotationEvent> = VecDeque::new();
        let max_cached = config.max_cached_closed_segments as usize;

        // On broker restart, populate our cached rotations list using the storage metadata.
        if let Ok(descriptors) = metadata.get_segment_descriptors(&topic_path).await {
            let skip_count = descriptors.len().saturating_sub(max_cached);
            for desc in descriptors.into_iter().skip(skip_count) {
                recent_rotations.push_back(RotationEvent {
                    start_offset: desc.start_offset,
                    end_offset: desc.end_offset,
                });
            }
        }

        while let Some(event) = rotation_rx.recv().await {
            debug!(
                target = "valkey_rotation",
                topic = %topic_path,
                start = event.start_offset,
                end = event.end_offset,
                "WAL rotation event received"
            );

            recent_rotations.push_back(event);

            // If we have more cached segments than allowed, trim the oldest one
            if recent_rotations.len() > max_cached {
                if let Some(oldest) = recent_rotations.pop_front() {
                    // We want to trim everything up to and including oldest.end_offset.
                    // XTRIM MINID drops entries with IDs lower than the specified MINID.
                    // So we set MINID to oldest.end_offset + 1.
                    let min_id = format!("{}-1", oldest.end_offset + 1);
                    
                    debug!(
                        target = "valkey_rotation",
                        topic = %topic_path,
                        min_id = %min_id,
                        "trimming old segment from Valkey stream"
                    );

                    if let Err(e) = valkey.xtrim_minid(&stream_key, &min_id).await {
                        warn!(
                            target = "valkey_rotation",
                            topic = %topic_path,
                            min_id = %min_id,
                            error = %e,
                            "failed to XTRIM Valkey stream"
                        );
                    }
                }
            }
        }

        debug!(
            target = "valkey_rotation",
            topic = %topic_path,
            "rotation listener stopped (channel closed)"
        );
    })
}
