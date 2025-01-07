use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::RwLock;

use danube_client::StorageType;

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

pub(crate) fn create_backend(storage_type: &StorageType) -> Arc<dyn StorageBackend> {
    match storage_type {
        StorageType::InMemory => Arc::new(InMemoryStorage::new()),
        StorageType::Disk(path) => Arc::new(DiskStorage::new(path)),
        StorageType::S3(bucket) => Arc::new(S3Storage::new(bucket)),
    }
}
