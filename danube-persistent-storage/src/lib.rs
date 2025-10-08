mod errors;

pub mod wal;
pub use wal::Wal;

// Unified checkpoints (WAL + uploader)
pub mod checkpoint;
pub use checkpoint::{CheckPoint, UploaderCheckpoint, WalCheckpoint};

mod wal_storage;
pub use wal_storage::WalStorage;

mod cloud_store;
pub use cloud_store::{BackendConfig, CloudBackend, CloudStore, LocalBackend};
pub use cloud_store::{CloudRangeReader, CloudWriter};

// Shared frame utilities (header size, CRC-checked scanning)
mod frames;

mod uploader;
pub use uploader::{Uploader, UploaderBaseConfig, UploaderConfig};

// Uploader streaming internals
mod uploader_stream;

mod etcd_metadata;
pub use etcd_metadata::{EtcdMetadata, ObjectDescriptor};

mod cloud_reader;
pub use cloud_reader::CloudReader;

// WalStorageFactory: facade to create per-topic WalStorage and manage per-topic uploaders
mod wal_factory;
pub use wal_factory::WalStorageFactory;

// Unit tests
#[cfg(test)]
mod checkpoints_test;
#[cfg(test)]
mod cloud_reader_test;
#[cfg(test)]
mod cloud_store_test;
#[cfg(test)]
mod uploader_test;
#[cfg(test)]
mod wal_test;
