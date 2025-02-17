use async_trait::async_trait;
use danube_core::storage::{RemoteStorageConfig, Segment, StorageBackend, StorageBackendError};
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
pub struct RemoteStorage {
    client: Arc<Mutex<Option<ManagedStorageClient<Channel>>>>,
    connection_config: RemoteStorageConfig,
}

impl RemoteStorage {
    pub fn new(config: RemoteStorageConfig) -> Self {
        Self {
            client: Arc::new(Mutex::new(None)),
            connection_config: config,
        }
    }

    async fn ensure_connected(&self) -> Result<(), PersistentStorageError> {
        let mut client_guard = self.client.lock().await;
        if client_guard.is_none() {
            let tls_config = if self.connection_config.use_tls {
                let ca_cert = std::fs::read(&self.connection_config.ca_file)?;
                Some(ClientTlsConfig::new().ca_certificate(Certificate::from_pem(ca_cert)))
            } else {
                None
            };

            let cnx_options = ConnectionOptions {
                tls_config,
                use_tls: self.connection_config.use_tls,
            };

            let uri: Uri = self.connection_config.endpoint.parse()?;
            let RpcConnection { grpc_cnx } = new_rpc_connection(&cnx_options, &uri).await?;
            *client_guard = Some(ManagedStorageClient::new(grpc_cnx));
        }
        Ok(())
    }
}

#[async_trait]
impl StorageBackend for RemoteStorage {
    async fn get_segment(
        &self,
        topic_name: &str,
        id: usize,
    ) -> Result<Option<Arc<RwLock<Segment>>>, StorageBackendError> {
        self.ensure_connected().await?;

        let mut client_guard = self.client.lock().await;
        let client = client_guard.as_mut().unwrap();

        let request = GetSegmentRequest {
            topic_name: topic_name.to_string(),
            segment_id: id as u64,
        };

        dbg!("Sending GET request...");

        match client.get_segment(request).await {
            Ok(response) => {
                let segment_data = response.into_inner().segment_data;
                if segment_data.is_empty() {
                    Ok(None)
                } else {
                    let segment: Segment = bincode::deserialize(&segment_data)
                        .map_err(|e| StorageBackendError::Managed(e.to_string()))?;
                    Ok(Some(Arc::new(RwLock::new(segment))))
                }
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
        self.ensure_connected().await?;

        let mut client_guard = self.client.lock().await;
        let client = client_guard.as_mut().unwrap();

        let segment_data = segment.read().await;
        let serialized = bincode::serialize(&*segment_data)
            .map_err(|e| StorageBackendError::Managed(e.to_string()))?;

        let request = PutSegmentRequest {
            topic_name: topic_name.to_string(),
            segment_id: id as u64,
            segment_data: serialized,
        };

        dbg!("Sending PUT request...");

        match client.put_segment(request).await {
            Ok(_) => Ok(()),
            Err(e) => Err(StorageBackendError::Managed(e.to_string())),
        }
    }

    async fn remove_segment(&self, topic_name: &str, id: usize) -> Result<(), StorageBackendError> {
        self.ensure_connected().await?;

        let mut client_guard = self.client.lock().await;
        let client = client_guard.as_mut().unwrap();

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
