use std::sync::Arc;
use tonic::transport::Uri;

use crate::{
    auth_service::AuthService,
    connection_manager::{ConnectionManager, ConnectionOptions},
    consumer::ConsumerBuilder,
    errors::Result,
    health_check::HealthCheckService,
    lookup_service::{LookupResult, LookupService},
    producer::ProducerBuilder,
    schema::Schema,
    schema_service::SchemaService,
};

/// The main client for interacting with the Danube messaging system.
///
/// The `DanubeClient` struct is designed to facilitate communication with the Danube messaging system.
/// It provides various methods for managing producers and consumers, performing topic lookups, and retrieving schema information. This client acts as the central interface for interacting with the messaging system and managing connections and services.
#[derive(Debug, Clone)]
pub struct DanubeClient {
    pub(crate) uri: Uri,
    pub(crate) cnx_manager: Arc<ConnectionManager>,
    pub(crate) lookup_service: LookupService,
    pub(crate) schema_service: SchemaService,
    pub(crate) health_check_service: HealthCheckService,
    pub(crate) auth_service: AuthService,
}

impl DanubeClient {
    fn new_client(builder: DanubeClientBuilder, uri: Uri) -> Self {
        let cnx_manager = ConnectionManager::new(builder.connection_options);
        let cnx_manager = Arc::new(cnx_manager);

        let auth_service = AuthService::new(cnx_manager.clone());

        let lookup_service = LookupService::new(cnx_manager.clone());

        let schema_service = SchemaService::new(cnx_manager.clone(), auth_service.clone());

        let health_check_service =
            HealthCheckService::new(cnx_manager.clone(), auth_service.clone());

        DanubeClient {
            uri: uri,
            cnx_manager,
            lookup_service,
            schema_service,
            health_check_service,
            auth_service,
        }
    }

    /// Initializes a new `DanubeClientBuilder` instance.
    ///
    /// The builder pattern allows for configuring and constructing a `DanubeClient` instance with optional settings and options.
    /// Using the builder, you can customize various aspects of the `DanubeClient`, such as connection settings, timeouts, and other configurations before creating the final `DanubeClient` instance.
    pub fn builder() -> DanubeClientBuilder {
        DanubeClientBuilder::default()
    }

    /// Returns a new `ProducerBuilder` for configuring and creating a `Producer` instance.
    ///
    /// This method initializes a `ProducerBuilder`, which is used to set up various options and settings for a `Producer`.
    /// The builder pattern allows you to specify details such as the topic, producer name, partitions, schema, and other configurations before creating the final `Producer` instance.
    pub fn new_producer(&self) -> ProducerBuilder {
        ProducerBuilder::new(self)
    }

    /// Returns a new `ConsumerBuilder` for configuring and creating a `Consumer` instance.
    ///
    /// This method initializes a `ConsumerBuilder`, which is used to set up various options and settings for a `Consumer`.
    /// The builder pattern allows you to specify details such as the topic, consumer name, subscription, subscription type, and other configurations before creating the final `Consumer` instance.
    pub fn new_consumer(&self) -> ConsumerBuilder {
        ConsumerBuilder::new(self)
    }

    /// Returns a reference to the `AuthService`.
    ///
    /// This method provides access to the `AuthService` instance used by the `DanubeClient`.
    pub fn auth_service(&self) -> &AuthService {
        &self.auth_service
    }

    /// Retrieves the address of the broker responsible for a specified topic.
    ///
    /// This asynchronous method performs a lookup to find the broker that is responsible for the given topic. The `addr` parameter specifies the address of the broker to connect to for performing the lookup. The method returns information about the broker handling the topic.
    ///
    /// # Parameters
    ///
    /// - `addr`: The address of the broker to connect to for the lookup. This is provided as a `&Uri`, which specifies where the request should be sent.
    /// - `topic`: The name of the topic for which to look up the broker.
    ///
    /// # Returns
    ///
    /// - `Ok(LookupResult)`: Contains the result of the lookup operation, including the broker address.
    /// - `Err(e)`: An error if the lookup fails or if there are issues during the operation. This could include connectivity problems, invalid topic names, or other errors related to the lookup process.
    pub async fn lookup_topic(&self, addr: &Uri, topic: impl Into<String>) -> Result<LookupResult> {
        self.lookup_service.lookup_topic(addr, topic).await
    }

    /// Retrieves the schema associated with a specified topic from the schema service.
    ///
    /// This asynchronous method fetches the schema for the given topic from the schema service. The schema describes the structure and format of the messages for the specified topic. The method returns the schema details or an error if the retrieval fails.
    ///
    /// # Parameters
    ///
    /// - `topic`: The name of the topic for which the schema is to be retrieved.
    ///
    /// # Returns
    ///
    /// - `Ok(Schema)`: The schema associated with the specified topic. This includes information about the schema type and its definition, if available.
    /// - `Err(e)`: An error if the schema retrieval fails or if there are issues during the operation. This could include errors such as non-existent topics, connectivity issues, or internal service errors.
    pub async fn get_schema(&self, topic: impl Into<String>) -> Result<Schema> {
        self.schema_service.get_schema(&self.uri, topic).await
    }
}

/// A builder for configuring and creating a `DanubeClient` instance.
///
/// The `DanubeClientBuilder` struct provides methods for setting various options needed to construct a `DanubeClient`. This includes configuring the base URI for the Danube service, connection settings.
///
/// # Fields
///
/// - `uri`: The base URI for the Danube service. This is a required field and specifies the address of the service that the client will connect to. It is essential for constructing the `DanubeClient`.
/// - `connection_options`: Optional connection settings that define how the grpc client connects to the Danube service. These settings can include parameters such as timeouts, retries, and other connection-related configurations.
#[derive(Debug, Clone, Default)]
pub struct DanubeClientBuilder {
    uri: String,
    connection_options: ConnectionOptions,
    api_key: Option<String>,
}

impl DanubeClientBuilder {
    /// Sets the base URI for the Danube service in the builder.
    ///
    /// This method configures the base URI that the `DanubeClient` will use to connect to the Danube service. The base URI is a required parameter for establishing a connection and interacting with the service.
    ///
    /// # Parameters
    ///
    /// - `url`: The base URI to use for connecting to the Danube service. The URI should include the protocol and address of the Danube service.
    pub fn service_url(mut self, url: impl Into<String>) -> Self {
        self.uri = url.into();
        self
    }

    /// Sets optional connection settings for the client in the builder.
    ///
    /// This method allows you to configure various connection settings for the `DanubeClient` through the builder. These settings determine how the client connects to the grpc Danube service and can be tailored to meet specific requirements.
    ///
    /// # Parameters
    ///
    /// - `connection_options`: A `ConnectionOptions` instance that includes various settings for configuring the client's connection. This may include parameters such as connection timeouts, keep alive interval.
    pub fn with_connection_options(mut self, connection_options: ConnectionOptions) -> Self {
        self.connection_options = connection_options;
        self
    }

    /// Sets the API key for the client in the builder.
    pub fn with_api_key(mut self, api_key: impl Into<String>) -> Self {
        self.api_key = Some(api_key.into());
        self
    }

    /// Sets the connection to be insecure (without TLS).
    pub fn insecure_connection(mut self) -> Self {
        self.connection_options.insecure = true;
        self
    }

    /// Constructs and returns a `DanubeClient` instance based on the configuration specified in the builder.
    ///
    /// This method finalizes the configuration and creates a new `DanubeClient` instance. It uses the settings and options that were configured using the `DanubeClientBuilder` methods.
    ///
    /// # Returns
    ///
    /// - `Ok(DanubeClient)`: A new instance of `DanubeClient` configured with the specified options.
    /// - `Err(e)`: An error if the configuration is invalid or incomplete.
    pub async fn build(mut self) -> Result<DanubeClient> {
        let uri = self.uri.parse::<Uri>()?;
        let cnx_manager = Arc::new(ConnectionManager::new(self.connection_options.clone()));
        let auth_service = AuthService::new(cnx_manager.clone());

        if let Some(api_key) = self.api_key.clone() {
            self.connection_options.api_key = Some(api_key.clone());
            let token = auth_service.authenticate_client(&uri, &api_key).await?;
            self.connection_options.jwt_token = Some(token);
        }

        Ok(DanubeClient::new_client(self, uri))
    }
}
