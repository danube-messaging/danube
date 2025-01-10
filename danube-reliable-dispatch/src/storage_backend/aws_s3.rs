use async_trait::async_trait;
use aws_config::meta::region::RegionProviderChain;
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use danube_core::storage::Segment;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::{
    errors::{ReliableDispatchError, Result},
    storage_backend::StorageBackend,
};

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct S3Storage {
    client: Option<Client>,
    bucket_name: String,
    region: String,
}

impl S3Storage {
    pub fn new(bucket: &str, region: &str) -> Self {
        S3Storage {
            client: None,
            bucket_name: bucket.into(),
            region: region.into(),
        }
    }

    #[allow(dead_code)]
    pub async fn init(&mut self) -> Result<()> {
        let region_provider = RegionProviderChain::default_provider();
        let sdk_config = aws_config::defaults(BehaviorVersion::latest())
            .region(region_provider)
            .load()
            .await;
        let config = aws_sdk_s3::config::Builder::from(&sdk_config).build();
        self.client = Some(Client::from_conf(config));
        Ok(())
    }

    fn segment_key(&self, id: usize) -> String {
        format!("segment_{}.bin", id)
    }

    fn get_client(&self) -> Result<&Client> {
        self.client.as_ref().ok_or_else(|| {
            ReliableDispatchError::UnknownAWSClient("Client not initialized".to_string())
        })
    }
}

#[async_trait]
impl StorageBackend for S3Storage {
    async fn get_segment(&self, id: usize) -> Result<Option<Arc<RwLock<Segment>>>> {
        let client = self.get_client()?;
        let key = self.segment_key(id);

        let response = client
            .get_object()
            .bucket(&self.bucket_name)
            .key(&key)
            .send()
            .await;

        match response {
            Ok(output) => {
                let bytes = output.body.collect().await?.into_bytes();
                let segment: Segment = bincode::deserialize(&bytes)?;
                Ok(Some(Arc::new(RwLock::new(segment))))
            }
            Err(err) => {
                if err.to_string().contains("NoSuchKey") {
                    Ok(None)
                } else {
                    Err(err.into())
                }
            }
        }
    }

    async fn put_segment(&self, id: usize, segment: Arc<RwLock<Segment>>) -> Result<()> {
        let key = self.segment_key(id);
        let segment_data = segment.read().await;
        let bytes = bincode::serialize(&*segment_data)?;

        let client = self.get_client()?;

        client
            .put_object()
            .bucket(&self.bucket_name)
            .key(&key)
            .body(bytes.into())
            .send()
            .await?;

        Ok(())
    }

    async fn remove_segment(&self, id: usize) -> Result<()> {
        let key = self.segment_key(id);

        let client = self.get_client()?;

        client
            .delete_object()
            .bucket(&self.bucket_name)
            .key(&key)
            .send()
            .await?;

        Ok(())
    }
}
