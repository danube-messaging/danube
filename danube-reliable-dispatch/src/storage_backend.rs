use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;

use danube_core::storage::StorageConfig;

use crate::{errors::Result, topic_storage::Segment};

mod in_memory;
pub use in_memory::InMemoryStorage;
mod disk;
pub use disk::DiskStorage;
mod aws_s3;
pub use aws_s3::S3Storage;

#[async_trait]
pub trait StorageBackend: Send + Sync + std::fmt::Debug + 'static {
    async fn get_segment(&self, id: usize) -> Result<Option<Arc<RwLock<Segment>>>>;
    async fn put_segment(&self, id: usize, segment: Arc<RwLock<Segment>>) -> Result<()>;
    async fn remove_segment(&self, id: usize) -> Result<()>;
}

pub fn create_message_storage(storage_config: &StorageConfig) -> Arc<dyn StorageBackend> {
    match storage_config {
        StorageConfig::InMemory => Arc::new(InMemoryStorage::new()),
        StorageConfig::Disk(disk_config) => Arc::new(DiskStorage::new(&disk_config.path)),
        StorageConfig::S3(s3_config) => {
            Arc::new(S3Storage::new(&s3_config.bucket, &s3_config.region))
        }
    }
}
