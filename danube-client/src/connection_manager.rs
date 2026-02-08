use crate::errors::{DanubeError, Result};

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
pub(crate) struct ConnectionOptions {
    pub(crate) tls_config: Option<ClientTlsConfig>,
    pub(crate) api_key: Option<String>,
    pub(crate) use_tls: bool,
}

#[derive(Debug, Clone)]
pub struct ConnectionManager {
    connections: Arc<Mutex<HashMap<BrokerAddress, ConnectionStatus>>>,
    pub(crate) connection_options: ConnectionOptions,
}

impl ConnectionManager {
    pub(crate) fn new(connection_options: ConnectionOptions) -> Self {
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
        let proxy = broker_url != connect_url;
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
            let tls_config = cnx_options.tls_config.as_ref().ok_or_else(|| {
                DanubeError::Unrecoverable(
                    "TLS is enabled but no TLS config provided. Use with_tls() or with_mtls() before enabling TLS".to_string(),
                )
            })?;

            Channel::from_shared(connect_url.to_string())?
                .tls_config(tls_config.clone())?
                .connect()
                .await?
        }
    };

    Ok(RpcConnection { grpc_cnx: channel })
}
