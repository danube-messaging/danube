/// Configuration for the admin gRPC client.
///
/// Built from the CLI connection flags/env (`--endpoint`/`DANUBE_ENDPOINT`,
/// `--token`/`DANUBE_TOKEN`, `--tls*`) via `Connection::grpc_config()`; this type
/// itself does not read any environment variables.
#[derive(Clone, Debug)]
pub struct GrpcClientConfig {
    pub endpoint: String,
    pub request_timeout_ms: u64,
    pub enable_tls: Option<bool>,
    pub domain: Option<String>,
    pub ca_path: Option<String>,
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
    /// JWT token for admin authentication.
    pub token: Option<String>,
}

impl Default for GrpcClientConfig {
    fn default() -> Self {
        Self {
            endpoint: "http://127.0.0.1:6650".to_string(),
            request_timeout_ms: 5000,
            enable_tls: None,
            domain: None,
            ca_path: None,
            cert_path: None,
            key_path: None,
            token: None,
        }
    }
}
