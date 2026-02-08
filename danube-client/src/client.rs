use std::path::Path;
use std::sync::Arc;
use tonic::transport::{Certificate, ClientTlsConfig, Identity, Uri};

use crate::{
    auth_service::AuthService,
    connection_manager::{ConnectionManager, ConnectionOptions},
    consumer::ConsumerBuilder,
    errors::Result,
    health_check::HealthCheckService,
    lookup_service::{LookupResult, LookupService},
    producer::ProducerBuilder,
    schema_registry_client::SchemaRegistryClient,
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
    pub(crate) health_check_service: HealthCheckService,
    pub(crate) auth_service: AuthService,
}

impl DanubeClient {
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

    /// Returns a `SchemaRegistryClient` for schema registry operations.
    ///
    /// The returned client shares the same connection manager and auth service as the `DanubeClient`
    pub fn schema(&self) -> SchemaRegistryClient {
        SchemaRegistryClient::new(
            self.cnx_manager.clone(),
            self.auth_service.clone(),
            self.uri.clone(),
        )
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

    /// Sets the TLS configuration for the client in the builder.
    pub fn with_tls(mut self, ca_cert: impl AsRef<Path>) -> Result<Self> {
        let tls_config =
            ClientTlsConfig::new().ca_certificate(Certificate::from_pem(std::fs::read(ca_cert)?));
        self.connection_options.tls_config = Some(tls_config);
        self.connection_options.use_tls = true;
        Ok(self)
    }

    /// Sets the mutual TLS configuration for the client in the builder.
    pub fn with_mtls(
        mut self,
        ca_cert: impl AsRef<Path>,
        client_cert: impl AsRef<Path>,
        client_key: impl AsRef<Path>,
    ) -> Result<Self> {
        let ca_data = std::fs::read(ca_cert)?;
        let cert_data = std::fs::read(client_cert)?;
        let key_data = std::fs::read(client_key)?;

        let tls_config = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(ca_data))
            .identity(Identity::from_pem(cert_data, key_data));

        self.connection_options.tls_config = Some(tls_config);
        self.connection_options.use_tls = true;
        Ok(self)
    }

    /// Sets the API key for the client in the builder.
    ///
    /// Automatically enables TLS. If no TLS config has been set via `with_tls()` or
    /// `with_mtls()`, a default TLS config using system root certificates is applied.
    pub fn with_api_key(mut self, api_key: impl Into<String>) -> Self {
        self.api_key = Some(api_key.into());
        if self.connection_options.tls_config.is_none() {
            self.connection_options.tls_config = Some(ClientTlsConfig::new());
        }
        self.connection_options.use_tls = true;
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

        // Set api_key on connection_options before creating the single ConnectionManager
        if let Some(ref api_key) = self.api_key {
            self.connection_options.api_key = Some(api_key.clone());
        }

        let cnx_manager = Arc::new(ConnectionManager::new(self.connection_options));
        let auth_service = AuthService::new(cnx_manager.clone());

        // Authenticate if api_key is present; token is cached inside AuthService
        if let Some(ref api_key) = self.api_key {
            auth_service.authenticate_client(&uri, api_key).await?;
        }

        let lookup_service = LookupService::new(cnx_manager.clone(), auth_service.clone());
        let health_check_service =
            HealthCheckService::new(cnx_manager.clone(), auth_service.clone());

        Ok(DanubeClient {
            uri,
            cnx_manager,
            lookup_service,
            health_check_service,
            auth_service,
        })
    }
}
