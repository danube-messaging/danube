/// Valkey crash-recovery helpers.
///
/// Provides the logic to download buffered messages from the Valkey
/// stream so they can be replayed into a fresh WAL after a broker crash
/// or disk loss.
use super::client::ValkeyClient;
use crate::tiered_storage::TieredStorage;
use danube_core::message::StreamMessage;
use tracing::{error, info, warn};

/// Download un-exported messages from the Valkey stream for a topic.
///
/// Returns `Some((min_offset, messages))` if Valkey has buffered data,
/// `None` if the stream is empty or unreachable.
///
/// Messages are retrieved sequentially from `start_offset` using `XRANGE`
/// so they can be replayed into the WAL in the correct order.
pub async fn download_unexported_stream(
    client: &ValkeyClient,
    topic_path: &str,
    start_offset: u64,
) -> Option<(u64, Vec<StreamMessage>)> {
    let stream_key = format!("/topics/{}/stream", topic_path);
    let start_id = format!("{}-1", start_offset);

    match client.xrange(&stream_key, &start_id, "+").await {
        Ok(entries) if !entries.is_empty() => {
            let total = entries.len();
            let mut messages = Vec::with_capacity(total);
            let mut deserialize_errors = 0usize;
            let mut min_parsed_offset = None;
            let mut max_parsed_offset = 0;

            for (stream_id, bytes) in entries {
                // Parse offset from `<offset>-0`
                let offset_str = stream_id.split('-').next().unwrap_or("");
                let offset = match offset_str.parse::<u64>() {
                    Ok(o) => o,
                    Err(_) => {
                        warn!(
                            target = "valkey_recovery",
                            topic = %topic_path,
                            stream_id = %stream_id,
                            "failed to parse offset from stream ID — skipping"
                        );
                        continue;
                    }
                };

                if min_parsed_offset.is_none() {
                    min_parsed_offset = Some(offset);
                }
                max_parsed_offset = offset;

                match TieredStorage::deserialize_message(&bytes) {
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

            let min_offset = min_parsed_offset.unwrap_or(0);
            info!(
                target = "valkey_recovery",
                topic = %topic_path,
                start_query = start_offset,
                min_offset = min_offset,
                max_offset = max_parsed_offset,
                message_count = messages.len(),
                deserialize_errors = deserialize_errors,
                "downloaded stream for WAL replay"
            );

            Some((min_offset, messages))
        }
        Ok(_) => {
            // Empty stream — no buffered data
            None
        }
        Err(e) => {
            warn!(
                target = "valkey_recovery",
                topic = %topic_path,
                start_query = start_offset,
                error = %e,
                "XRANGE stream failed"
            );
            None
        }
    }
}
