# Danube Efficient Uploader Architecture

This document outlines a revised architecture for the `Uploader` component in `danube-persistent-storage`. The goal is to create a highly efficient, zero-copy pipeline for moving data from the local Write-Ahead Log (WAL) to cloud storage, replacing the previous inefficient deserialize/re-serialize process.

This design uses the `UploaderCheckpoint` to track the precise read-head of the uploader, making it fully self-contained and eliminating the need for complex indexing within the WAL itself.

## 1. Core Principles

- **Zero-Copy Reading**: The uploader must read raw, serialized message frames from the WAL files and upload them directly to the cloud without deserializing the content.
- **Self-Contained State**: The uploader's state, including its exact read position, should be stored exclusively within its own `UploaderCheckpoint`.
- **Resilience**: The uploader must be able to gracefully handle WAL file rotations and resume from its last known position after a restart.

## 2. Architectural Changes

### 2.1. Enhanced `UploaderCheckpoint`

The `UploaderCheckpoint` will be the single source of truth for the uploader's progress. It will be extended to store not just the last committed message offset, but also the physical location of the uploader's read-head.

**File**: `danube-persistent-storage/src/checkpoint.rs`

```rust
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct UploaderCheckpoint {
    /// The last message offset that has been successfully uploaded and committed.
    pub last_committed_offset: u64,

    /// The sequence number of the WAL file the uploader was last reading from.
    pub last_read_file_seq: u64,

    /// The byte position within that file where the next read should begin.
    pub last_read_byte_position: u64,

    /// The unique identifier of the last cloud object that was written.
    pub last_object_id: Option<String>,

    /// The timestamp when this checkpoint was last updated.
    pub updated_at: u64,
}
```

### 2.2. Uploader Logic Flow

The main upload loop (`run_once`) will follow a new, more direct logic:

1.  **Load State**: On startup or at the beginning of each cycle, the uploader loads two pieces of information:
    - Its own `UploaderCheckpoint` to get `last_read_file_seq` and `last_read_byte_position`.
    - The current `WalCheckpoint` to get the list of all available WAL files (`rotated_files` and the active file).

2.  **Locate Starting Point**: The uploader finds the correct WAL file to start reading from by matching its `last_read_file_seq` against the list of available files. It then seeks directly to `last_read_byte_position` within that file.

3.  **Perform Bulk Read**: 
    - Starting from that exact position, it reads raw, complete message frames into a buffer, up to the configured `max_batch_bytes`.
    - This process is frame-aware, ensuring no partial messages are ever read into the buffer.
    - If the read reaches the end of one WAL file, it seamlessly continues with the next file in the sequence.

4.  **Upload and Update Checkpoint**:
    - The raw buffer is uploaded directly to cloud storage.
    - Upon successful upload, the uploader creates a new `UploaderCheckpoint`. It updates `last_committed_offset` based on the last message in the batch, and critically, it updates `last_read_file_seq` and `last_read_byte_position` to point to the start of the next frame to be read.
    - This new checkpoint is then persisted to disk.

## 3. Benefits of this Architecture

- **Simplicity**: It removes the need for a complex sparse index in the `WalCheckpoint`, simplifying the WAL writer's logic.
- **Efficiency**: It achieves the zero-copy goal by reading and uploading raw bytes, drastically reducing CPU and memory usage.
- **Decoupling**: It properly decouples the uploader's internal state from the WAL's state. The `WalCheckpoint` is only concerned with the state of the written log, while the `UploaderCheckpoint` is concerned with the state of the upload process.
- **Robustness**: By storing the exact byte position, the uploader can resume with precision, eliminating the need for scanning and making the process more resilient to restarts.
