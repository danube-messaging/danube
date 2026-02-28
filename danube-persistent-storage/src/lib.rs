mod errors;

pub mod wal;
pub use wal::Wal;

// Unified checkpoints (WAL + uploader)
pub mod checkpoint;
pub use checkpoint::{CheckPoint, UploaderCheckpoint, WalCheckpoint};

mod wal_storage;
pub use wal_storage::WalStorage;

// New flat cloud module entry
pub mod cloud;
pub use cloud::{
    BackendConfig, CloudBackend, CloudRangeReader, CloudReader, CloudStore, CloudWriter,
    LocalBackend, Uploader, UploaderBaseConfig, UploaderConfig,
};

// Backward-compatibility shims for legacy module paths used by tests/consumers.
// These re-export from the new cloud entry so existing `use crate::cloud_store::*` keeps working.
pub mod cloud_store {
    pub use crate::cloud::{
        BackendConfig, CloudBackend, CloudRangeReader, CloudStore, CloudWriter, LocalBackend,
    };
}
pub mod cloud_reader {
    pub use crate::cloud::CloudReader;
}
pub mod uploader {
    pub use crate::cloud::{Uploader, UploaderBaseConfig, UploaderConfig};
}
pub mod uploader_stream {
    pub use crate::cloud::uploader_stream::*;
}

// Shared frame utilities (header size, CRC-checked scanning)
mod frames;

mod etcd_metadata;
pub use etcd_metadata::{ObjectDescriptor, StorageMetadata};

// WalStorageFactory: facade to create per-topic WalStorage and manage per-topic uploaders
mod wal_factory;
pub use wal_factory::WalStorageFactory;

mod persistent_metrics;

// Unit tests
#[cfg(test)]
mod checkpoints_test;
#[cfg(test)]
mod wal_test;
