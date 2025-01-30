use crate::errors::Result;

use std::{
    collections::{hash_map::Entry, HashMap},
    sync::Arc,
};
use tokio::sync::Mutex;
use tonic::transport::{Channel, ClientTlsConfig, Uri};
use tracing::info;

/// holds connection information for a broker
#[derive(Debug, Clone, Eq, Hash, PartialEq)]
pub struct BrokerAddress {
    /// URL we're using for connection (can be the proxy's URL)
    pub connect_url: Uri,
    /// Danube URL for the broker we're actually contacting
    pub broker_url: Uri,
    /// true if the connection is through a proxy
    pub proxy: bool,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
enum ConnectionStatus {
    Connected(Arc<RpcConnection>),
    Disconnected,
}

#[derive(Debug, Clone, Default)]
pub struct ConnectionOptions {
    pub(crate) tls_config: Option<ClientTlsConfig>,
    pub(crate) api_key: Option<String>,
    pub(crate) jwt_token: Option<String>,
    // indicate the the connection doesn't use TLS
    pub(crate) insecure: bool,
}

#[derive(Debug, Clone)]
pub struct ConnectionManager {
    connections: Arc<Mutex<HashMap<BrokerAddress, ConnectionStatus>>>,
    pub(crate) connection_options: ConnectionOptions,
}

impl ConnectionManager {
    pub fn new(connection_options: ConnectionOptions) -> Self {
        ConnectionManager {
            connections: Arc::new(Mutex::new(HashMap::new())),
            connection_options,
        }
    }

    pub(crate) async fn get_connection(
        &self,
        broker_url: &Uri,
        connect_url: &Uri,
    ) -> Result<Arc<RpcConnection>> {
        let mut proxy = false;
        if broker_url == connect_url {
            proxy = true;
        }
        let broker = BrokerAddress {
            connect_url: connect_url.clone(),
            broker_url: broker_url.clone(),
            proxy,
        };

        let mut cnx = self.connections.lock().await;

        match cnx.entry(broker) {
            Entry::Occupied(mut occupied_entry) => match occupied_entry.get() {
                ConnectionStatus::Connected(rpc_cnx) => Ok(rpc_cnx.clone()),
                ConnectionStatus::Disconnected => {
                    let new_rpc_cnx =
                        new_rpc_connection(&self.connection_options, connect_url).await?;
                    let rpc_cnx = Arc::new(new_rpc_cnx);
                    *occupied_entry.get_mut() = ConnectionStatus::Connected(rpc_cnx.clone());
                    Ok(rpc_cnx)
                }
            },
            Entry::Vacant(vacant_entry) => {
                let new_rpc_cnx = new_rpc_connection(&self.connection_options, connect_url).await?;
                let rpc_cnx = Arc::new(new_rpc_cnx);
                vacant_entry.insert(ConnectionStatus::Connected(rpc_cnx.clone()));
                Ok(rpc_cnx)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct RpcConnection {
    pub(crate) grpc_cnx: Channel,
}

pub(crate) async fn new_rpc_connection(
    cnx_options: &ConnectionOptions,
    connect_url: &Uri,
) -> Result<RpcConnection> {
    info!(
        "Attempting to establish a new RPC connection to {}",
        connect_url
    );
    let channel = if cnx_options.insecure {
        Channel::from_shared(connect_url.to_string())?
            .connect()
            .await?
    } else if let Some(tls_config) = &cnx_options.tls_config {
        // mTLS configuration
        Channel::from_shared(connect_url.to_string())?
            .tls_config(tls_config.clone())?
            .connect()
            .await?
    } else {
        // Normal TLS configuration
        let default_tls_config = ClientTlsConfig::new();
        Channel::from_shared(connect_url.to_string())?
            .tls_config(default_tls_config)?
            .connect()
            .await?
    };

    Ok(RpcConnection { grpc_cnx: channel })
}
