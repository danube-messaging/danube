use danube_core::storage::{StorageBackend, StorageConfig};
use std::sync::Arc;

mod in_memory;
pub use in_memory::InMemoryStorage;
//mod disk;
//pub use disk::DiskStorage;
//mod aws_s3;
//pub use aws_s3::S3Storage;

pub fn create_message_storage(storage_config: &StorageConfig) -> Arc<dyn StorageBackend> {
    match storage_config {
        StorageConfig::InMemory => Arc::new(InMemoryStorage::new()),
        StorageConfig::Disk(_disk_config) => {
            todo!()
        }
        StorageConfig::S3(_s3_config) => {
            todo!()
        }
    }
}
