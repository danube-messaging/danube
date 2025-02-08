use std::sync::Arc;
use tonic::transport::{Channel, ClientTlsConfig, Uri};
use tracing::info;

use crate::errors::PersistentStorageError;

#[derive(Debug, Clone)]
#[allow(dead_code)]
enum ConnectionStatus {
    Connected(Arc<RpcConnection>),
    Disconnected,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ConnectionOptions {
    pub(crate) tls_config: Option<ClientTlsConfig>,
    pub(crate) use_tls: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct RpcConnection {
    pub(crate) grpc_cnx: Channel,
}

pub(crate) async fn new_rpc_connection(
    cnx_options: &ConnectionOptions,
    connect_url: &Uri,
) -> Result<RpcConnection, PersistentStorageError> {
    info!("Establishing new RPC connection to {}", connect_url);

    let channel = match cnx_options.use_tls {
        false => {
            // Plain TCP connection
            Channel::from_shared(connect_url.to_string())?
                .connect()
                .await?
        }
        true => {
            // TLS is enabled, tls_config must be present
            let tls_config = cnx_options
                .tls_config
                .as_ref()
                .expect("TLS config must be present when TLS is enabled");

            Channel::from_shared(connect_url.to_string())?
                .tls_config(tls_config.clone())?
                .connect()
                .await?
        }
    };

    Ok(RpcConnection { grpc_cnx: channel })
}
