use async_trait::async_trait;
use danube_core::storage::{ManagedConfig, Segment, StorageBackend, StorageBackendError};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Uri};

use crate::{
    connection::{new_rpc_connection, ConnectionOptions, RpcConnection},
    errors::PersistentStorageError,
};

// Generated gRPC client code
use danube_core::managed_storage_proto::{
    managed_storage_client::ManagedStorageClient, GetSegmentRequest, PutSegmentRequest,
    RemoveSegmentRequest,
};

#[derive(Debug, Clone)]
pub struct ManagedStorage {
    client: Arc<Mutex<ManagedStorageClient<Channel>>>,
}

impl ManagedStorage {
    pub async fn new(config: &ManagedConfig) -> Result<Self, PersistentStorageError> {
        let tls_config = if config.use_tls {
            let ca_cert = std::fs::read(&config.ca_file)?;
            Some(ClientTlsConfig::new().ca_certificate(Certificate::from_pem(ca_cert)))
        } else {
            None
        };

        // Create connection options
        let cnx_options = ConnectionOptions {
            tls_config: tls_config,
            use_tls: config.use_tls,
        };

        // Parse endpoint into Uri
        let uri: Uri = config
            .endpoint
            .parse()
            .expect("Failed to parse endpoint URI");

        // Establish RPC connection using connection.rs
        let RpcConnection { grpc_cnx } = new_rpc_connection(&cnx_options, &uri)
            .await
            .expect("Failed to establish RPC connection");

        // Create client with the established connection
        let client = ManagedStorageClient::new(grpc_cnx);

        Ok(Self {
            client: Arc::new(Mutex::new(client)),
        })
    }
}

#[async_trait]
impl StorageBackend for ManagedStorage {
    async fn get_segment(
        &self,
        topic_name: &str,
        id: usize,
    ) -> Result<Option<Arc<RwLock<Segment>>>, StorageBackendError> {
        let mut client = self.client.lock().await;
        let request = GetSegmentRequest {
            topic_name: topic_name.to_string(),
            segment_id: id as u64,
        };

        match client.get_segment(request).await {
            Ok(response) => {
                let segment_data = response.into_inner().segment_data;
                let segment: Segment = bincode::deserialize(&segment_data)
                    .map_err(|e| StorageBackendError::Managed(e.to_string()))?;
                Ok(Some(Arc::new(RwLock::new(segment))))
            }
            Err(e) => Err(StorageBackendError::Managed(e.to_string())),
        }
    }

    async fn put_segment(
        &self,
        topic_name: &str,
        id: usize,
        segment: Arc<RwLock<Segment>>,
    ) -> Result<(), StorageBackendError> {
        let mut client = self.client.lock().await;
        let segment_data = segment.read().await;
        let serialized = bincode::serialize(&*segment_data)
            .map_err(|e| StorageBackendError::Managed(e.to_string()))?;

        let request = PutSegmentRequest {
            topic_name: topic_name.to_string(),
            segment_id: id as u64,
            segment_data: serialized,
        };

        match client.put_segment(request).await {
            Ok(_) => Ok(()),
            Err(e) => Err(StorageBackendError::Managed(e.to_string())),
        }
    }

    async fn remove_segment(&self, topic_name: &str, id: usize) -> Result<(), StorageBackendError> {
        let mut client = self.client.lock().await;
        let request = RemoveSegmentRequest {
            topic_name: topic_name.to_string(),
            segment_id: id as u64,
        };

        match client.remove_segment(request).await {
            Ok(_) => Ok(()),
            Err(e) => Err(StorageBackendError::Managed(e.to_string())),
        }
    }
}
