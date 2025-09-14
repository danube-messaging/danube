//! # Danube Iceberg Storage
//!
//! Apache Iceberg-based storage backend for Danube messaging platform.
//!
//! This crate provides a cloud-native storage solution using Apache Iceberg
//! table format with Write-Ahead Log (WAL) for low-latency producer acknowledgments
//! and asynchronous background processing to Iceberg tables on object storage.

pub mod catalog;
pub mod config;
pub mod errors;
pub mod iceberg_storage;
pub mod topic_reader;
pub mod topic_writer;
pub mod wal;

pub use catalog::{create_catalog, create_danube_schema};
pub use config::IcebergConfig;
pub use errors::{IcebergStorageError, Result};
pub use iceberg_storage::IcebergStorage;
pub use topic_reader::TopicReader;
pub use topic_writer::TopicWriter;
pub use wal::{WalReader, WriteAheadLog};

pub use danube_core::message::StreamMessage;
/// Re-export commonly used types
pub use danube_core::storage::PersistentStorage;
