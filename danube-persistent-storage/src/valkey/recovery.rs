/// Valkey crash-recovery helpers.
///
/// Provides the logic to download buffered messages from the Valkey
/// active_segment hash so they can be replayed into a fresh WAL after a
/// broker crash or disk loss.

use super::client::ValkeyClient;
use crate::buffered_storage::BufferedStorage;
use danube_core::message::StreamMessage;
use tracing::{error, info, warn};

/// Download all messages from the Valkey `active_segment` hash for a topic.
///
/// Returns `Some((min_offset, sorted_messages))` if Valkey has buffered data,
/// `None` if the key is empty or unreachable.
///
/// Messages are sorted by offset ascending so they can be replayed into
/// the WAL in the correct order via `Wal::append_batch()`.
pub async fn download_active_segment(
    client: &ValkeyClient,
    topic_path: &str,
) -> Option<(u64, Vec<StreamMessage>)> {
    let active_key = format!("/topics/{}/active_segment", topic_path);
    match client.hgetall(&active_key).await {
        Ok(entries) if !entries.is_empty() => {
            // Parse offset→bytes entries and sort by offset
            let mut offset_messages: Vec<(u64, Vec<u8>)> = entries
                .into_iter()
                .filter_map(|(field, value)| {
                    let offset = field.parse::<u64>().ok()?;
                    Some((offset, value))
                })
                .collect();
            offset_messages.sort_by_key(|(offset, _)| *offset);

            if offset_messages.is_empty() {
                return None;
            }

            let min_offset = offset_messages[0].0;
            let max_offset = offset_messages.last().unwrap().0;
            let total = offset_messages.len();

            // Deserialize each message from bincode
            let mut messages = Vec::with_capacity(total);
            let mut deserialize_errors = 0usize;
            for (offset, bytes) in &offset_messages {
                match BufferedStorage::deserialize_message(bytes) {
                    Ok(msg) => messages.push(msg),
                    Err(e) => {
                        deserialize_errors += 1;
                        warn!(
                            target = "valkey_recovery",
                            topic = %topic_path,
                            offset = offset,
                            error = %e,
                            "failed to deserialize message — skipping"
                        );
                    }
                }
            }

            if messages.is_empty() {
                error!(
                    target = "valkey_recovery",
                    topic = %topic_path,
                    total_entries = total,
                    deserialize_errors = deserialize_errors,
                    "all messages failed to deserialize"
                );
                return None;
            }

            info!(
                target = "valkey_recovery",
                topic = %topic_path,
                min_offset = min_offset,
                max_offset = max_offset,
                message_count = messages.len(),
                deserialize_errors = deserialize_errors,
                "downloaded active_segment for WAL replay"
            );

            Some((min_offset, messages))
        }
        Ok(_) => {
            // Empty active_segment — no buffered data
            None
        }
        Err(e) => {
            warn!(
                target = "valkey_recovery",
                topic = %topic_path,
                error = %e,
                "HGETALL active_segment failed"
            );
            None
        }
    }
}
