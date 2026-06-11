/// Async Valkey/Redis client supporting standalone and cluster topologies.
///
/// Provides a thin async API over the `redis` crate, supporting both
/// standalone and cluster modes. All operations are async and Tokio-compatible.
use super::config::WriteBufferConfig;
use redis::aio::MultiplexedConnection;
use redis::cluster::ClusterClient;
use redis::cluster_async::ClusterConnection;
use redis::{Client, RedisError};
use std::fmt;
use tracing::{debug, warn};

/// Result of a `WAIT` command: how many replicas confirmed.
#[derive(Debug)]
pub struct WaitResult {
    pub confirmed_replicas: u32,
    pub timed_out: bool,
}

/// Async Valkey/Redis client supporting standalone and cluster topologies.
pub struct ValkeyClient {
    inner: ConnectionKind,
}

enum ConnectionKind {
    Standalone(MultiplexedConnection),
    Cluster(ClusterConnection),
}

impl fmt::Debug for ValkeyClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.inner {
            ConnectionKind::Standalone(_) => f.debug_struct("ValkeyClient(Standalone)").finish(),
            ConnectionKind::Cluster(_) => f.debug_struct("ValkeyClient(Cluster)").finish(),
        }
    }
}

impl ValkeyClient {
    /// Connect to Valkey/Redis. Uses cluster mode if multiple endpoints are provided,
    /// standalone mode for a single endpoint.
    pub async fn connect(config: &WriteBufferConfig) -> Result<Self, RedisError> {
        if config.endpoints.len() > 1 {
            let client = ClusterClient::new(config.endpoints.clone())?;
            let conn = client.get_async_connection().await?;
            debug!(
                endpoints = ?config.endpoints,
                "connected to Valkey cluster"
            );
            Ok(Self {
                inner: ConnectionKind::Cluster(conn),
            })
        } else {
            let endpoint = config
                .endpoints
                .first()
                .expect("at least one endpoint required");
            let client = Client::open(endpoint.as_str())?;
            let conn = client.get_multiplexed_async_connection().await?;
            debug!(
                endpoint = %endpoint,
                "connected to standalone Valkey"
            );
            Ok(Self {
                inner: ConnectionKind::Standalone(conn),
            })
        }
    }

    /// WAIT: block until the specified number of replicas confirm the last write.
    async fn wait(
        &self,
        replicas: u32,
        timeout_ms: u64,
    ) -> Result<WaitResult, RedisError> {
        let confirmed: u32 = match &self.inner {
            ConnectionKind::Standalone(conn) => {
                let mut conn = conn.clone();
                redis::cmd("WAIT")
                    .arg(replicas)
                    .arg(timeout_ms)
                    .query_async(&mut conn)
                    .await?
            }
            ConnectionKind::Cluster(conn) => {
                let mut conn = conn.clone();
                redis::cmd("WAIT")
                    .arg(replicas)
                    .arg(timeout_ms)
                    .query_async(&mut conn)
                    .await?
            }
        };
        let timed_out = confirmed < replicas;
        if timed_out {
            warn!(
                requested = replicas,
                confirmed = confirmed,
                timeout_ms = timeout_ms,
                "WAIT timed out with fewer replicas than requested"
            );
        }
        Ok(WaitResult {
            confirmed_replicas: confirmed,
            timed_out,
        })
    }

    /// XADD: Append a message to a stream.
    pub async fn xadd(
        &self,
        key: &str,
        id: &str,
        field: &str,
        value: &[u8],
    ) -> Result<(), RedisError> {
        match &self.inner {
            ConnectionKind::Standalone(conn) => {
                let mut conn = conn.clone();
                redis::cmd("XADD")
                    .arg(key)
                    .arg(id)
                    .arg(field)
                    .arg(value)
                    .query_async(&mut conn)
                    .await
            }
            ConnectionKind::Cluster(conn) => {
                let mut conn = conn.clone();
                redis::cmd("XADD")
                    .arg(key)
                    .arg(id)
                    .arg(field)
                    .arg(value)
                    .query_async(&mut conn)
                    .await
            }
        }
    }

    /// XADD followed by WAIT: write to stream and wait for replica confirmation.
    pub async fn xadd_and_wait(
        &self,
        key: &str,
        id: &str,
        field: &str,
        value: &[u8],
        wait_replicas: u32,
        wait_timeout_ms: u64,
    ) -> Result<WaitResult, RedisError> {
        self.xadd(key, id, field, value).await?;
        self.wait(wait_replicas, wait_timeout_ms).await
    }

    /// Pipeline multiple XADD commands followed by a single WAIT.
    ///
    /// More efficient than individual xadd_and_wait calls for batch writes.
    pub async fn xadd_batch_and_wait(
        &self,
        key: &str,
        entries: &[(String, String, Vec<u8>)], // (id, field, value)
        wait_replicas: u32,
        wait_timeout_ms: u64,
    ) -> Result<WaitResult, RedisError> {
        let mut pipe = redis::pipe();
        for (id, field, value) in entries {
            pipe.cmd("XADD")
                .arg(key)
                .arg(id.as_str())
                .arg(field.as_str())
                .arg(value.as_slice())
                .ignore();
        }

        match &self.inner {
            ConnectionKind::Standalone(conn) => {
                let mut conn = conn.clone();
                pipe.query_async::<()>(&mut conn).await?;
            }
            ConnectionKind::Cluster(conn) => {
                let mut conn = conn.clone();
                pipe.query_async::<()>(&mut conn).await?;
            }
        }

        self.wait(wait_replicas, wait_timeout_ms).await
    }

    /// XRANGE: read messages from a stream between start and end IDs.
    /// Returns a list of (StreamID, Vec<(Field, Value)>)
    /// We only care about retrieving the binary payload values.
    pub async fn xrange(
        &self,
        key: &str,
        start: &str,
        end: &str,
    ) -> Result<Vec<(String, Vec<u8>)>, RedisError> {
        // Return type for XRANGE is: Vec<(String, Vec<(String, Vec<u8>)>)>
        let result: Vec<(String, Vec<(String, Vec<u8>)>)> = match &self.inner {
            ConnectionKind::Standalone(conn) => {
                let mut conn = conn.clone();
                redis::cmd("XRANGE")
                    .arg(key)
                    .arg(start)
                    .arg(end)
                    .query_async(&mut conn)
                    .await?
            }
            ConnectionKind::Cluster(conn) => {
                let mut conn = conn.clone();
                redis::cmd("XRANGE")
                    .arg(key)
                    .arg(start)
                    .arg(end)
                    .query_async(&mut conn)
                    .await?
            }
        };

        // Flatten the nested field-value pairs (assuming we only set 1 field per stream entry)
        let mut flattened = Vec::with_capacity(result.len());
        for (stream_id, mut fields) in result {
            if let Some((_, value)) = fields.pop() {
                flattened.push((stream_id, value));
            }
        }
        Ok(flattened)
    }

    /// XTRIM MINID: drop all entries from a stream with IDs lower than the specified MINID.
    /// Used to purge safely exported segments while preserving the warm cache.
    pub async fn xtrim_minid(
        &self,
        key: &str,
        min_id: &str,
    ) -> Result<u64, RedisError> {
        match &self.inner {
            ConnectionKind::Standalone(conn) => {
                let mut conn = conn.clone();
                redis::cmd("XTRIM")
                    .arg(key)
                    .arg("MINID")
                    .arg(min_id)
                    .query_async(&mut conn)
                    .await
            }
            ConnectionKind::Cluster(conn) => {
                let mut conn = conn.clone();
                redis::cmd("XTRIM")
                    .arg(key)
                    .arg("MINID")
                    .arg(min_id)
                    .query_async(&mut conn)
                    .await
            }
        }
    }

    /// DEL: delete a key entirely.
    pub async fn del(&self, key: &str) -> Result<(), RedisError> {
        match &self.inner {
            ConnectionKind::Standalone(conn) => {
                let mut conn = conn.clone();
                redis::cmd("DEL").arg(key).query_async(&mut conn).await
            }
            ConnectionKind::Cluster(conn) => {
                let mut conn = conn.clone();
                redis::cmd("DEL").arg(key).query_async(&mut conn).await
            }
        }
    }
}
