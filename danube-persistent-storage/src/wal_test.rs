#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;

    use crate::wal::{Wal, WalConfig};
    use danube_core::message::{MessageID, StreamMessage};
    use danube_core::storage::PersistentStorageError;
    use tempfile::TempDir;
    use tokio::time::{timeout, Duration};
    use tokio_stream::StreamExt;

    fn make_message(i: u64) -> StreamMessage {
        StreamMessage {
            request_id: i,
            msg_id: MessageID {
                producer_id: 1,
                topic_name: "t".to_string(),
                broker_addr: "b".to_string(),
                topic_offset: 0,
            },
            payload: format!("msg-{i}").into_bytes(),
            publish_time: i,
            producer_name: "p".to_string(),
            subscription_name: None,
            attributes: HashMap::new(),
        }
    }

    /// Test: Latest live-only delivers only messages appended after subscription
    ///
    /// Purpose
    /// - Validate the live fast-path semantics for `StartPosition::Latest` mapped to `(current_offset, live = true)`.
    /// - Ensure that only messages appended after subscription are delivered.
    ///
    /// Flow
    /// - Create WAL, subscribe with `current_offset` and `live = true`.
    /// - Append three messages after subscribing.
    ///
    /// Expected
    /// - The reader yields exactly the three appended payloads (msg-0, msg-1, msg-2) and nothing from before.
    #[tokio::test]
    async fn test_latest_live_only() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;
        let dir = tmp.path().to_path_buf();
        let wal = make_wal_with_dir(&dir, None).await?;

        // Subscribe with Latest semantics (from now)
        let current = wal.current_offset();
        let mut stream = wal.tail_reader(current, true).await?;

        // Append three messages after subscription
        for i in 0..3u64 {
            wal.append(&make_message(i)).await?;
        }

        // Expect to receive only the three messages appended after subscribing
        let mut got = Vec::new();
        while got.len() < 3 {
            let next_item = timeout(Duration::from_secs(5), stream.next())
                .await
                .map_err(|_| "timeout waiting for next message")?;
            if let Some(Ok(msg)) = next_item {
                got.push(msg.payload);
            }
        }

        assert_eq!(
            got,
            vec![b"msg-0".to_vec(), b"msg-1".to_vec(), b"msg-2".to_vec()]
        );
        Ok(())
    }

    /// Test: Cache streaming in batches (bounded lock time) from within cache range
    ///
    /// Purpose
    /// - Validate the batched cache streaming logic implemented in `wal/cache.rs::build_cache_stream`.
    /// - Ensure short lock hold time and correct ordering without materializing full snapshots.
    ///
    /// Flow
    /// - Append 1500 messages to populate cache.
    /// - Start reading from `n-100` to ensure we stream entirely from cache.
    ///
    /// Expected
    /// - We receive exactly 100 messages, offsets `[n-100 .. n)`, in order, with correct `topic_offset`.
    #[tokio::test]
    async fn test_cache_streaming_batches() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;
        let dir = tmp.path().to_path_buf();
        let wal = make_wal_with_dir(&dir, None).await?;

        // Append enough messages to span multiple cache batches (batch=512 in impl)
        let n = 1500u64;
        for i in 0..n {
            wal.append(&make_message(i)).await?;
        }

        // Start from within cache to force cache path
        let start = n - 100;
        let mut stream = wal.tail_reader(start, false).await?;
        let mut got_offs = Vec::new();
        while let Some(item) = timeout(Duration::from_secs(10), stream.next())
            .await
            .map_err(|_| "timeout cache batches")?
        {
            let msg = item?;
            got_offs.push(msg.msg_id.topic_offset);
            if got_offs.len() == 100 {
                break;
            }
        }

        assert_eq!(got_offs.len(), 100);
        for (i, off) in got_offs.into_iter().enumerate() {
            assert_eq!(off, start + i as u64);
        }
        Ok(())
    }

    /// Test: Stateful transitions Files → Cache → Live in a single WAL instance
    ///
    /// Purpose
    /// - Validate dynamic phase transitions coordinated by `StatefulReader`.
    /// - Ensure replay from Files, then Cache, then transition to Live and receive newly appended items.
    ///
    /// Flow
    /// - Configure small rotation threshold and append 100 messages (triggering file replay + cache fill).
    /// - Read from 0 and consume 100 messages (Files→Cache).
    /// - Append three new messages and ensure they are delivered via the Live phase.
    ///
    /// Expected
    /// - First 100 offsets `[0..100)` delivered in order.
    /// - Then offsets `100, 101, 102` delivered via live broadcast.
    #[tokio::test]
    async fn test_stateful_transitions_files_cache_live() -> Result<(), Box<dyn std::error::Error>>
    {
        let tmp = TempDir::new()?;
        let dir = tmp.path().to_path_buf();
        // Force rotation to ensure file replay exists
        let wal = make_wal_with_dir(&dir, Some(1024)).await?;

        // Append enough to cause rotation and fill cache
        let n = 100u64;
        for i in 0..n {
            wal.append(&make_message(i)).await?;
        }

        // Start from 0 to trigger Files phase, then Cache, then Live
        let mut stream = wal.tail_reader(0, false).await?;

        // Consume first n messages (Files + Cache)
        let mut offs = Vec::new();
        while let Some(item) = timeout(Duration::from_secs(20), stream.next())
            .await
            .map_err(|_| "timeout initial replay")?
        {
            let msg = item?;
            offs.push(msg.msg_id.topic_offset);
            if offs.len() == n as usize {
                break;
            }
        }
        assert_eq!(offs.len(), n as usize);
        for (i, off) in offs.into_iter().enumerate() {
            assert_eq!(off, i as u64);
        }

        // Now append a few more to validate transition to Live
        for i in n..n + 3 {
            wal.append(&make_message(i)).await?;
        }

        let mut live_offs = Vec::new();
        while let Some(item) = timeout(Duration::from_secs(10), stream.next())
            .await
            .map_err(|_| "timeout live replay")?
        {
            let msg = item?;
            live_offs.push(msg.msg_id.topic_offset);
            if live_offs.len() == 3 {
                break;
            }
        }
        assert_eq!(live_offs, vec![n, n + 1, n + 2]);
        Ok(())
    }

    /// Test: Cache refill before live
    ///
    /// Purpose
    /// - Validate that when the cache appears exhausted, the reader first attempts a quick
    ///   cache refill from `last_yielded + 1` before transitioning to live, avoiding gaps.
    ///
    /// Flow
    /// - Create a WAL with a small cache capacity to narrow the cache window.
    /// - Append 5 messages [0..5) and start a reader from 0 (cache path).
    /// - Shortly after streaming starts, append another 5 messages [5..10).
    ///
    /// Expected
    /// - The stream yields offsets [0..10) in order with no gaps or duplicates, regardless of whether
    ///   the last items come from a cache refill or live.
    #[tokio::test]
    async fn test_cache_refill_before_live() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;
        let dir = tmp.path().to_path_buf();
        // Use a small cache to increase likelihood of hitting the boundary
        let cfg = WalConfig {
            dir: Some(dir.clone()),
            file_name: Some("wal.log".to_string()),
            cache_capacity: Some(16),
            fsync_interval_ms: Some(1),
            fsync_max_batch_bytes: Some(1024),
            rotate_max_bytes: None,
            rotate_max_seconds: None,
        };
        let wal = Wal::with_config(cfg).await?;

        // Prime cache with 5 messages
        for i in 0..5u64 {
            wal.append(&make_message(i)).await?;
        }

        // Start reader from 0 (cache path)
        let mut stream = wal.tail_reader(0, false).await?;

        // Append more soon after starting to read
        let wal_clone = wal.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(25)).await;
            for i in 5..10u64 {
                let _ = wal_clone.append(&make_message(i)).await;
            }
        });

        // Collect first 10 offsets and assert continuity
        let mut offs = Vec::new();
        while let Some(item) = timeout(Duration::from_secs(10), stream.next())
            .await
            .map_err(|_| "timeout collecting refill boundary")?
        {
            let msg = item?;
            offs.push(msg.msg_id.topic_offset);
            if offs.len() == 10 {
                break;
            }
        }

        assert_eq!(offs.len(), 10, "expected 10 messages");
        for (i, off) in offs.into_iter().enumerate() {
            assert_eq!(off, i as u64);
        }
        Ok(())
    }

    async fn make_wal_with_dir(
        dir: &PathBuf,
        rotate_max_bytes: Option<u64>,
    ) -> Result<Wal, PersistentStorageError> {
        let cfg = WalConfig {
            dir: Some(dir.clone()),
            file_name: Some("wal.log".to_string()),
            cache_capacity: Some(1024),
            fsync_interval_ms: Some(1),
            fsync_max_batch_bytes: Some(1024),
            rotate_max_bytes,
            rotate_max_seconds: None,
        };
        Wal::with_config(cfg).await
    }

    /// Test: WAL file replay (fresh reader over existing WAL directory)
    ///
    /// Purpose
    /// - Validate that frames appended and flushed by a writer can be replayed by a fresh
    ///   WAL instance pointing to the same directory.
    /// - Ensures correctness of persisted on-disk format and the reader replay path.
    ///
    /// Flow
    /// - Create a writer WAL, append 3 messages, shutdown (flush to disk).
    /// - Create a new reader WAL over the same directory and call `tail_reader(0)`.
    ///
    /// Expected
    /// - The reader yields exactly the 3 payloads in order: msg-0, msg-1, msg-2, then tails live.
    #[tokio::test]
    async fn test_wal_file_replay_all() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;
        let dir = tmp.path().to_path_buf();
        let writer = make_wal_with_dir(&dir, None).await?;
        for i in 0..3u64 {
            writer.append(&make_message(i)).await?;
        }
        writer.shutdown().await;

        // New reader instance
        let reader = make_wal_with_dir(&dir, None).await?;
        let mut stream = reader.tail_reader(0, false).await?;
        let mut got = Vec::new();
        while got.len() < 3 {
            let next_item = timeout(Duration::from_secs(5), stream.next())
                .await
                .map_err(|_| "timeout waiting for next message")?;
            match next_item {
                Some(item) => {
                    let msg = item?;
                    got.push(msg.payload);
                }
                None => break,
            }
        }
        assert_eq!(
            got,
            vec![b"msg-0".to_vec(), b"msg-1".to_vec(), b"msg-2".to_vec()]
        );
        Ok(())
    }

    /// Test: WAL file replay from an offset
    ///
    /// Purpose
    /// - Validate that `tail_reader(from_offset)` replays only persisted/cached messages with
    ///   offsets >= `from_offset` before switching to live tailing.
    ///
    /// Flow
    /// - Create writer WAL, append 5 messages, shutdown (flush).
    /// - Create reader WAL over same directory, call `tail_reader(3)`.
    ///
    /// Expected
    /// - The first two items from the stream are payloads for offsets 3 and 4: msg-3, msg-4.
    ///   Afterwards the stream would switch to live (not asserted here).
    #[tokio::test]
    async fn test_wal_file_replay_from_offset() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;
        let dir = tmp.path().to_path_buf();
        let writer = make_wal_with_dir(&dir, None).await?;
        for i in 0..5u64 {
            writer.append(&make_message(i)).await?;
        }
        writer.shutdown().await;

        let reader = make_wal_with_dir(&dir, None).await?;
        let mut stream = reader.tail_reader(3, false).await?;
        let first = timeout(Duration::from_secs(5), stream.next())
            .await
            .map_err(|_| "timeout waiting for first message")?
            .unwrap()?;
        let second = timeout(Duration::from_secs(5), stream.next())
            .await
            .map_err(|_| "timeout waiting for second message")?
            .unwrap()?;
        assert_eq!(first.payload, b"msg-3".to_vec());
        assert_eq!(second.payload, b"msg-4".to_vec());
        Ok(())
    }

    /// Test: Append N messages and tail from offset 0
    ///
    /// Purpose
    /// - Validate end-to-end in-memory cache + file path by appending N messages
    ///   and reading them in order from offset 0.
    ///
    /// Flow
    /// - Create a WAL with a temp dir, append 20 messages, shutdown (flush),
    ///   then `tail_reader(0)` and collect exactly 20 messages.
    ///
    /// Expected
    /// - We receive 20 messages; each message payload is msg-i, and the injected
    ///   `msg_id.topic_offset` equals i (the WAL offset assigned during append).
    #[tokio::test]
    async fn test_append_and_tail_from_zero() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;
        let dir = tmp.path().to_path_buf();
        let wal = make_wal_with_dir(&dir, None).await?;

        let n = 20u64;
        for i in 0..n {
            let m = make_message(i);
            wal.append(&m).await?;
        }

        // Ensure data is flushed
        wal.shutdown().await;

        // Read from offset 0 and collect all
        let mut stream = wal.tail_reader(0, false).await?;
        let mut got: Vec<StreamMessage> = Vec::new();
        while let Some(item) = stream.next().await {
            let msg = item?;
            got.push(msg);
            if got.len() == n as usize {
                break;
            }
        }

        assert_eq!(got.len(), n as usize);
        for (i, msg) in got.into_iter().enumerate() {
            assert_eq!(msg.payload, format!("msg-{}", i).into_bytes());
            // topic_offset is stamped by WAL append and should equal the WAL offset
            assert_eq!(msg.msg_id.topic_offset, i as u64);
        }
        Ok(())
    }

    /// Test: Rotation + ordered reads from a specific offset
    ///
    /// Purpose
    /// - Validate that when rotation occurs (size threshold), `tail_reader(from)` correctly
    ///   stitches replay from file + cache and yields ordered messages starting at `from`.
    ///
    /// Flow
    /// - Configure a small rotate threshold, append 50 messages (trigger rotation), shutdown.
    /// - Call `tail_reader(10)` and collect messages until offset < 25.
    ///
    /// Expected
    /// - We receive exactly 15 messages for offsets [10..25). Payloads match msg-<offset>,
    ///   and `msg_id.topic_offset` equals each expected offset.
    #[tokio::test]
    async fn test_rotation_and_tail_from_offset() -> Result<(), Box<dyn std::error::Error>> {
        let tmp = TempDir::new()?;
        let dir = tmp.path().to_path_buf();
        // Force rotation after small number of bytes
        let wal = make_wal_with_dir(&dir, Some(512)).await?;

        let n = 50u64;
        for i in 0..n {
            let m = make_message(i);
            wal.append(&m).await?;
        }

        wal.shutdown().await;

        // Read from offset 10 and collect next 15
        let mut stream = wal.tail_reader(10, false).await?;
        let mut got: Vec<StreamMessage> = Vec::new();
        while let Some(item) = stream.next().await {
            let msg = item?;
            if msg.msg_id.topic_offset >= 10 && msg.msg_id.topic_offset < 25 {
                got.push(msg);
            }
            if got.len() == 15 {
                break;
            }
        }

        assert_eq!(got.len(), 15);
        for (idx, msg) in got.into_iter().enumerate() {
            let expected_off = 10 + idx as u64;
            assert_eq!(msg.payload, format!("msg-{}", expected_off).into_bytes());
            assert_eq!(msg.msg_id.topic_offset, expected_off);
        }

        Ok(())
    }
}
