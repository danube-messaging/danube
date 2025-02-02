mod errors;
use crate::errors::{Result, S3Error};

use async_trait::async_trait;
use aws_config::{meta::region::RegionProviderChain, BehaviorVersion};
use aws_sdk_s3::Client;
use danube_core::storage::{Segment, StorageBackend, StorageBackendError};
use std::sync::Arc;
use tokio::sync::RwLock;

// S3Storage is a storage backend that stores segments on S3.
// This creates a bucket structure like:
// bucket/
//     topic1/
//         segment_0.bin
//         segment_1.bin
//     topic2/
//         segment_0.bin
//         segment_1.bin

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

    fn segment_key(&self, topic_name: &str, id: usize) -> String {
        format!("{}/segment_{}.bin", topic_name, id)
    }

    fn get_client(&self) -> Result<&Client> {
        self.client
            .as_ref()
            .ok_or_else(|| S3Error::AWSClient("Client not initialized".to_string()))
    }
}

#[async_trait]
impl StorageBackend for S3Storage {
    async fn get_segment(
        &self,
        topic_name: &str,
        id: usize,
    ) -> std::result::Result<Option<Arc<RwLock<Segment>>>, StorageBackendError> {
        let client = self.get_client().map_err(S3Error::from)?;
        let key = self.segment_key(topic_name, id);

        match client
            .get_object()
            .bucket(&self.bucket_name)
            .key(&key)
            .send()
            .await
        {
            Ok(output) => {
                let bytes = output
                    .body
                    .collect()
                    .await
                    .map_err(S3Error::from)?
                    .into_bytes();
                let segment: Segment = bincode::deserialize(&bytes).map_err(S3Error::from)?;
                Ok(Some(Arc::new(RwLock::new(segment))))
            }
            Err(err) => {
                if err.to_string().contains("NoSuchKey") {
                    Ok(None)
                } else {
                    Err(S3Error::AWS(err.into()).into())
                }
            }
        }
    }

    async fn put_segment(
        &self,
        topic_name: &str,
        id: usize,
        segment: Arc<RwLock<Segment>>,
    ) -> std::result::Result<(), StorageBackendError> {
        let client = self.get_client().map_err(S3Error::from)?;
        let key = self.segment_key(topic_name, id);
        let segment_data = segment.read().await;
        let bytes = bincode::serialize(&*segment_data).map_err(S3Error::from)?;

        client
            .put_object()
            .bucket(&self.bucket_name)
            .key(&key)
            .body(bytes.into())
            .send()
            .await
            .map_err(|e| S3Error::AWSOther(e.to_string()))?;

        Ok(())
    }

    async fn remove_segment(
        &self,
        topic_name: &str,
        id: usize,
    ) -> std::result::Result<(), StorageBackendError> {
        let client = self.get_client().map_err(S3Error::from)?;
        let key = self.segment_key(topic_name, id);

        client
            .delete_object()
            .bucket(&self.bucket_name)
            .key(&key)
            .send()
            .await
            .map_err(|e| S3Error::AWSOther(e.to_string()))?;

        Ok(())
    }
}
