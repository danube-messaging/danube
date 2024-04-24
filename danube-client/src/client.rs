use crate::lookup_service;
use crate::producer::ProducerBuilder;
use crate::{
    connection_manager::ConnectionManager,
    errors::Result,
    lookup_service::{LookupResult, LookupService},
};
use proto::danube_client;

use std::sync::Arc;

pub mod proto {
    include!("../../proto/danube.rs");
}

#[derive(Debug)]
pub struct DanubeClient {
    url: String,
    cnx_manager: Arc<ConnectionManager>,
    lookup_service: LookupService,
}

impl DanubeClient {
    pub(crate) async fn new(url: impl Into<String>) -> Result<Self> {
        let cnx_manager = ConnectionManager::new().await?;
        let cnx_manager = Arc::new(cnx_manager);

        let lookup_service = LookupService::new(cnx_manager.clone());

        Ok(DanubeClient {
            url: Default::default(),
            cnx_manager,
            lookup_service,
        })
    }

    //creates a Client Builder
    pub fn builder() -> DanubeClientBuilder {
        DanubeClientBuilder::default()
    }

    /// creates a Producer Builder
    pub fn new_producer(&self) -> ProducerBuilder {
        ProducerBuilder::new()
    }

    /// gets the address of a broker handling the topic
    pub async fn lookup_topic(&self, topic: impl Into<String>) -> Result<LookupResult> {
        self.lookup_service.lookup_topic(topic).await
    }

    pub async fn connect(&self) -> Result<()> {
        let mut client = danube_client::DanubeClient::connect(String::from(&self.url)).await?;

        let req = proto::ProducerRequest {
            request_id: 1,
            producer_id: 2,
            producer_name: "hello_producer".to_string(),
            topic: "hello_topic".to_string(),
            schema: Some(proto::Schema {
                name: "schema_name".to_string(),
                schema_data: "1".as_bytes().to_vec(),
                type_schema: 0,
            }),
        };

        let request = tonic::Request::new(req);
        let response = client.create_producer(request).await?;

        println!("Response: {:?}", response.get_ref().request_id);

        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct DanubeClientBuilder {
    url: String,
}

impl DanubeClientBuilder {
    pub fn service_url(mut self, url: impl Into<String>) -> Self {
        self.url = url.into();

        self
    }
    pub async fn build(self) -> Result<DanubeClient> {
        Ok(DanubeClient::new(self.url).await?)
    }
}
