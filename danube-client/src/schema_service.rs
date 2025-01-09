use crate::{
    connection_manager::ConnectionManager,
    errors::{DanubeError, Result},
    Schema,
};

use danube_core::proto::{
    discovery_client::DiscoveryClient, Schema as ProtoSchema, SchemaRequest, SchemaResponse,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tonic::transport::Uri;
use tonic::{Response, Status};

#[derive(Debug, Clone)]
pub(crate) struct SchemaService {
    cnx_manager: Arc<ConnectionManager>,
    // unique identifier for every request sent by LookupService
    request_id: Arc<AtomicU64>,
}

impl SchemaService {
    pub fn new(cnx_manager: Arc<ConnectionManager>) -> Self {
        SchemaService {
            cnx_manager,
            request_id: Arc::new(AtomicU64::new(0)),
        }
    }
    pub(crate) async fn get_schema(&self, addr: &Uri, topic: impl Into<String>) -> Result<Schema> {
        let grpc_cnx = self.cnx_manager.get_connection(addr, addr).await?;

        let mut client = DiscoveryClient::new(grpc_cnx.grpc_cnx.clone());

        let schema_request = SchemaRequest {
            request_id: self.request_id.fetch_add(1, Ordering::SeqCst),
            topic: topic.into(),
        };

        let request = tonic::Request::new(schema_request);

        let response: std::result::Result<Response<SchemaResponse>, Status> =
            client.get_schema(request).await;

        let schema: Schema;

        match response {
            Ok(resp) => {
                let schema_resp = resp.into_inner();
                let proto_schema: ProtoSchema = schema_resp
                    .schema
                    .expect("can't get a response without a valid schema");

                schema = proto_schema.into();
            }

            Err(status) => {
                return Err(DanubeError::FromStatus(status, None));
            }
        };

        Ok(schema)
    }
}
