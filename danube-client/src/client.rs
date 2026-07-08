use std::path::Path;
use std::sync::Arc;
use tonic::transport::{Certificate, ClientTlsConfig, Identity, Uri};

use crate::{
    auth_service::AuthService,
    connection_manager::{BrokerAddress, ConnectionManager, ConnectionOptions},
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

    /// Resolve which broker owns a topic in the cluster.
    ///
    /// Performs a topic lookup via the Discovery service and follows
    /// redirects to find the owning broker. Returns the broker address
    /// that the caller should connect to for this topic.
    ///
    /// This is the recommended way for edge brokers and external services
    /// to discover topic-to-broker assignment without needing direct access
    /// to internal `LookupService` or `ConnectionManager`.
    pub async fn resolve_topic_broker(&self, topic: &str) -> Result<BrokerAddress> {
        self.lookup_service.handle_lookup(&self.uri, topic).await
    }

    /// Get a gRPC channel to a specific broker.
    ///
    /// Uses the client's internal connection pool with TLS/mTLS already
    /// configured (from `DanubeClientBuilder`). Connections are cached —
    /// repeated calls for the same broker return the same channel.
    ///
    /// The returned `Channel` can be used to construct any gRPC service
    /// client (e.g., `EdgeReplicatorServiceClient::new(channel)`).
    pub async fn get_broker_channel(
        &self,
        broker: &BrokerAddress,
    ) -> Result<tonic::transport::Channel> {
        let cnx = self
            .cnx_manager
            .get_connection(&broker.broker_url, &broker.connect_url)
            .await?;
        Ok(cnx.grpc_cnx.clone())
    }

    /// Get a gRPC channel to the service URL configured in the builder.
    ///
    /// Convenience wrapper around [`get_broker_channel`] for external tools
    /// (e.g., `danube-iceberg`) that connect directly to the configured broker
    /// without topic-based lookup or proxy routing.
    ///
    /// Uses the same connection pool and TLS/mTLS configuration.
    pub async fn get_service_channel(&self) -> Result<tonic::transport::Channel> {
        let cnx = self
            .cnx_manager
            .get_connection(&self.uri, &self.uri)
            .await?;
        Ok(cnx.grpc_cnx.clone())
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
    token: Option<String>,
    internal_broker: Option<String>,
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

    /// Sets the authentication token (JWT) for the client.
    ///
    /// Use `danube-admin security tokens create` to generate a token.
    /// Automatically enables TLS. If no TLS config has been set via `with_tls()` or
    /// `with_mtls()`, a default TLS config using system root certificates is applied.
    ///
    /// For tokens that expire, consider [`with_token_supplier`](Self::with_token_supplier)
    /// instead, which allows runtime token refresh.
    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        if self.connection_options.tls_config.is_none() {
            self.connection_options.tls_config = Some(ClientTlsConfig::new());
        }
        self.connection_options.use_tls = true;
        self
    }

    /// Sets a dynamic token supplier for the client.
    ///
    /// The supplier function is called on **every gRPC request** to obtain the current
    /// token, enabling runtime token refresh without restarting the client. This is
    /// useful for:
    ///
    /// - **File-based tokens**: Read from a file that is updated by infrastructure
    ///   (e.g., K8s projected volumes, sidecar token refreshers)
    /// - **Environment-based tokens**: Read from an environment variable
    /// - **Custom refresh logic**: Implement your own token rotation
    ///
    /// Automatically enables TLS (same as `with_token`).
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// // Read token from a file on each request
    /// let client = DanubeClient::builder()
    ///     .service_url("https://broker:6650")
    ///     .with_token_supplier(|| {
    ///         std::fs::read_to_string("/var/run/secrets/danube-token")
    ///             .unwrap_or_default()
    ///             .trim()
    ///             .to_string()
    ///     })
    ///     .build()
    ///     .await?;
    /// ```
    pub fn with_token_supplier(mut self, supplier: impl Fn() -> String + Send + Sync + 'static) -> Self {
        self.connection_options.token_supplier = Some(Arc::new(supplier));
        if self.connection_options.tls_config.is_none() {
            self.connection_options.tls_config = Some(ClientTlsConfig::new());
        }
        self.connection_options.use_tls = true;
        self
    }

    /// Deprecated: Use `with_token()` instead. This method treats the input as a JWT token.
    #[deprecated(
        since = "0.11.0",
        note = "Use with_token() — Danube now uses JWT tokens instead of API keys"
    )]
    pub fn with_api_key(self, api_key: impl Into<String>) -> Self {
        self.with_token(api_key)
    }

    /// Sets the internal broker identity for the client in the builder.
    pub fn with_internal_broker(mut self, broker_name: impl Into<String>) -> Self {
        self.internal_broker = Some(broker_name.into());
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

        // Set token on connection_options before creating the single ConnectionManager
        if let Some(ref token) = self.token {
            self.connection_options.token = Some(token.clone());
        }

        if let Some(ref internal_broker) = self.internal_broker {
            self.connection_options.internal_broker = Some(internal_broker.clone());
        }

        let cnx_manager = Arc::new(ConnectionManager::new(self.connection_options));
        let auth_service = AuthService::new(cnx_manager.clone());

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
