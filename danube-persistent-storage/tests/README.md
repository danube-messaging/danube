# danube-persistent-storage tests

This directory contains integration and unit tests covering the WAL + Cloud + Metadata stack.
Each test focuses on a specific behavior. Use this map to quickly identify coverage areas.

## Test Map

- wal_file_replay.rs
  - Purpose: Validate WAL file-backed replay and replay-from-offset behavior.
  - Coverage: `Wal::with_config`, on-disk frames, `tail_reader(from)` replays persisted records then tails live.

- wal_rotation_checkpoint.rs
  - Purpose: Validate WAL rotation (size/time) and checkpoint creation.
  - Coverage: rotation thresholds, `wal.ckpt` content, sequential file naming.

- cloud_reader_memory.rs
  - Purpose: Validate `CloudReader` range reads and DNB1 object parsing using an in-memory cloud.
  - Coverage: ETCD descriptor scanning, object framing/decoding, range filtering and ordering.

- chaining_stream_handoff.rs
  - Purpose: Validate Cloud→WAL handoff chaining logic directly (without factory).
  - Coverage: Historical cloud data followed by `wal.tail_reader(h)`, correct handoff watermark computation.

- uploader_memory.rs
  - Purpose: Validate `Uploader` writes objects and ETCD descriptors with in-memory cloud backend.
  - Coverage: Object key conventions `storage/topics/<topic>/objects/<object_id>`, descriptor contents and `completed` flag.

- uploader_fs.rs
  - Purpose: Same as above, but using filesystem cloud backend to exercise file I/O paths.
  - Coverage: Filesystem-backed cloud operations under a root prefix.

- uploader_memory_resume.rs
  - Purpose: Validate uploader resume from checkpoint and duplication avoidance.
  - Coverage: `UploaderCheckpoint` read/write, resuming from last committed offset.

- factory_multi_topic_isolation.rs
  - Purpose: Validate multi-topic isolation via `WalStorageFactory`.
  - Coverage: Per-topic WAL directories under `<wal_root>/<ns>/<topic>/`, reader isolation per topic, per-topic uploader namespaces.

- factory_cloud_wal_handoff.rs
  - Purpose: Validate Cloud→WAL handoff using factory-produced `WalStorage` per topic.
  - Coverage: Pre-populated cloud objects + ETCD descriptors, reader starts at offset 0, reads historical, then tails live WAL.

## Notes

- Tests that interact with `WalStorageFactory` use `#[tokio::test(flavor = "multi_thread")]` because the factory creates per-topic WALs via `block_in_place`.
- Topic paths:
  - Broker-facing API uses `"/ns/topic"` (leading slash).
  - Cloud/ETCD paths use `"ns/topic"` (no leading slash).
- Cloud object framing (DNB1):
  - Header: `"DNB1"` magic, version `1u8`, record count `u32` (LE)
  - Records: `[u64 offset][u32 len][bytes bincode(StreamMessage)]`
