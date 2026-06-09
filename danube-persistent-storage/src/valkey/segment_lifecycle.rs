/// Segment lifecycle management in Valkey.
///
/// Handles the lifecycle of topic segments in the Valkey write buffer:
///
/// - **Cleanup after export**: Remove exported offsets from `active_segment` so
///   Valkey memory doesn't grow unbounded. Once a segment is safely in durable
///   storage, those offsets no longer need the Valkey safety buffer.
///
/// - **Segment promotion**: When a WAL segment rotates and is exported, rename
///   the `active_segment` hash to a named closed-segment key for warm caching.
///
/// - **Segment eviction**: When the number of cached closed segments exceeds
///   `max_cached_closed_segments`, evict the oldest to free Valkey memory.

use super::client::ValkeyClient;
use super::config::WriteBufferConfig;
use tracing::{debug, info, warn};

/// Remove exported offsets from the Valkey `active_segment` hash.
///
/// After a WAL segment has been successfully exported to durable storage
/// (S3 / shared FS), the corresponding offsets in Valkey are no longer needed
/// for crash recovery. This function removes them to prevent unbounded growth.
///
/// Only the specified offset range is removed — any newer offsets written to
/// `active_segment` since the segment rotation are preserved.
///
/// Returns the number of fields actually removed from Valkey.
pub async fn cleanup_exported_offsets(
    client: &ValkeyClient,
    topic_path: &str,
    start_offset: u64,
    end_offset: u64,
) -> u64 {
    let active_key = format!("/topics/{}/active_segment", topic_path);
    let fields: Vec<String> = (start_offset..=end_offset)
        .map(|o| o.to_string())
        .collect();
    let field_count = fields.len();

    match client.hdel_multi(&active_key, &fields).await {
        Ok(removed) => {
            info!(
                target = "valkey_lifecycle",
                topic = %topic_path,
                start_offset = start_offset,
                end_offset = end_offset,
                requested = field_count,
                removed = removed,
                "cleaned up exported offsets from active_segment"
            );
            removed
        }
        Err(e) => {
            warn!(
                target = "valkey_lifecycle",
                topic = %topic_path,
                start_offset = start_offset,
                end_offset = end_offset,
                error = %e,
                "HDEL failed during exported offset cleanup"
            );
            0
        }
    }
}

/// Promote the Valkey `active_segment` hash to a named closed-segment key
/// and evict the oldest cached segments if the count exceeds the configured
/// limit.
///
/// **Important**: This is ONLY called when `max_cached_closed_segments > 0`
/// (warm caching is enabled). When warm caching is disabled (the default),
/// `cleanup_exported_offsets` already removed the exported fields and the
/// remaining un-exported fields in `active_segment` must stay there for
/// crash recovery.
///
/// This is best-effort: Valkey errors are logged but don't fail the export.
pub async fn promote_segment(
    client: &ValkeyClient,
    config: &WriteBufferConfig,
    topic_path: &str,
    segment_id: &str,
) {
    // When warm caching is disabled, skip promotion entirely.
    // The active_segment hash may still contain un-exported offsets from the
    // next WAL file — RENAME would move them out of active_segment, breaking
    // crash recovery which only looks at active_segment.
    if config.max_cached_closed_segments == 0 {
        debug!(
            target = "valkey_lifecycle",
            topic = %topic_path,
            segment = %segment_id,
            "skipping segment promotion (max_cached_closed_segments=0)"
        );
        return;
    }

    let active_key = format!("/topics/{}/active_segment", topic_path);
    let segment_key = format!("/topics/{}/segments/{}", topic_path, segment_id);
    let cached_list_key = format!("/topics/{}/cached_segments", topic_path);

    // RENAME active_segment → segments/{id}
    if let Err(e) = client.rename(&active_key, &segment_key).await {
        // RENAME fails if active_key doesn't exist (e.g., no writes since last rotation)
        debug!(
            target = "valkey_lifecycle",
            topic = %topic_path,
            segment = %segment_id,
            error = %e,
            "RENAME active_segment skipped (likely empty)"
        );
        return;
    }

    // Track in the cached segments list
    if let Err(e) = client.rpush(&cached_list_key, segment_id).await {
        warn!(
            target = "valkey_lifecycle",
            topic = %topic_path,
            segment = %segment_id,
            error = %e,
            "RPUSH cached_segments failed"
        );
    }

    // Evict oldest cached segments if over the limit
    evict_oldest_segments(client, topic_path, &cached_list_key, config.max_cached_closed_segments as u64).await;
}

/// Evict the oldest cached segments from Valkey when the count exceeds the
/// configured maximum. Each eviction pops the oldest segment ID from the
/// cached list and deletes the corresponding hash key.
async fn evict_oldest_segments(
    client: &ValkeyClient,
    topic_path: &str,
    cached_list_key: &str,
    max_cached: u64,
) {
    match client.llen(cached_list_key).await {
        Ok(len) if len > max_cached => {
            let to_evict = len - max_cached;
            for _ in 0..to_evict {
                match client.lpop(cached_list_key).await {
                    Ok(Some(old_segment_id)) => {
                        let old_key =
                            format!("/topics/{}/segments/{}", topic_path, old_segment_id);
                        if let Err(e) = client.del(&old_key).await {
                            warn!(
                                target = "valkey_lifecycle",
                                topic = %topic_path,
                                segment = %old_segment_id,
                                error = %e,
                                "DEL evicted segment failed"
                            );
                        } else {
                            debug!(
                                target = "valkey_lifecycle",
                                topic = %topic_path,
                                segment = %old_segment_id,
                                "evicted cached segment from Valkey"
                            );
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        warn!(
                            target = "valkey_lifecycle",
                            topic = %topic_path,
                            error = %e,
                            "LPOP cached_segments failed during eviction"
                        );
                        break;
                    }
                }
            }
        }
        _ => {} // under limit or error — skip
    }
}
