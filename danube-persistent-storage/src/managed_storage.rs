use async_trait::async_trait;
use bytes::Bytes;
use danube_core::storage::{RemoteStorageConfig, Segment, StorageBackend, StorageBackendError};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Uri};

use crate::{
    connection::{new_rpc_connection, ConnectionOptions, RpcConnection},
    errors::PersistentStorageError,
};

// Generated gRPC client code
use danube_core::managed_storage_proto::{
    managed_storage_client::ManagedStorageClient, GetSegmentRequest, RemoveSegmentRequest,
    SegmentChunk,
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

        let response = client
            .get_segment(request)
            .await
            .map_err(|e| StorageBackendError::Managed(e.to_string()))?;
        let mut stream = response.into_inner();
        let mut segment_data = Vec::new();

        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| StorageBackendError::Managed(e.to_string()))?;
            segment_data.extend_from_slice(&chunk.chunk_data);
        }

        if segment_data.is_empty() {
            return Ok(None);
        }

        let segment: Segment = bincode::deserialize(&segment_data)
            .map_err(|e| StorageBackendError::Managed(e.to_string()))?;
        Ok(Some(Arc::new(RwLock::new(segment))))
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

        const CHUNK_SIZE: usize = 2_097_152; // 2MB chunks
        let (tx, rx) = mpsc::channel(10);

        let segment_data = segment.read().await;
        let serialized = bincode::serialize(&*segment_data)
            .map_err(|e| StorageBackendError::Managed(e.to_string()))?;

        let topic_name = topic_name.to_string();
        let total_chunks = (serialized.len() + CHUNK_SIZE - 1) / CHUNK_SIZE;

        tokio::spawn(async move {
            for (chunk_index, chunk) in serialized.chunks(CHUNK_SIZE).enumerate() {
                let is_last_chunk = chunk_index == total_chunks - 1;

                let chunk_request = SegmentChunk {
                    topic_name: topic_name.clone(),
                    segment_id: id as u64,
                    chunk_data: Bytes::copy_from_slice(chunk).to_vec(),
                    chunk_index: chunk_index as u64,
                    is_last_chunk,
                };

                if tx.send(chunk_request).await.is_err() {
                    break;
                }
            }
        });

        let response = client
            .put_segment(ReceiverStream::new(rx))
            .await
            .map_err(|e| StorageBackendError::Managed(e.to_string()))?;

        if response.get_ref().total_chunks_received as usize != total_chunks {
            return Err(StorageBackendError::Managed(
                "Incomplete segment transmission".to_string(),
            ));
        }

        Ok(())
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
