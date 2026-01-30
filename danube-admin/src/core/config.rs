/// Configuration for the admin gRPC client
#[derive(Clone, Debug)]
pub struct GrpcClientConfig {
    pub endpoint: String,
    pub request_timeout_ms: u64,
    pub enable_tls: Option<bool>,
    pub domain: Option<String>,
    pub ca_path: Option<String>,
    pub cert_path: Option<String>,
    pub key_path: Option<String>,
}

impl Default for GrpcClientConfig {
    fn default() -> Self {
        Self {
            endpoint: std::env::var("DANUBE_ADMIN_ENDPOINT")
                .unwrap_or_else(|_| "http://127.0.0.1:50051".to_string()),
            request_timeout_ms: 5000,
            enable_tls: None,
            domain: None,
            ca_path: None,
            cert_path: None,
            key_path: None,
        }
    }
}
