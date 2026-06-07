/// Valkey/Redis client wrapper for the write buffer.
///
/// Provides a thin async API over the `redis` crate, supporting both
/// standalone and cluster modes. All operations are async and Tokio-compatible.
pub mod config;

use config::WriteBufferConfig;
use redis::aio::MultiplexedConnection;
use redis::cluster::ClusterClient;
use redis::cluster_async::ClusterConnection;
use redis::{AsyncCommands, Client, RedisError};
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

    /// HSET: write a single field into a hash.
    pub async fn hset(
        &self,
        key: &str,
        field: &str,
        value: &[u8],
    ) -> Result<(), RedisError> {
        match &self.inner {
            ConnectionKind::Standalone(conn) => {
                let mut conn = conn.clone();
                conn.hset(key, field, value).await
            }
            ConnectionKind::Cluster(conn) => {
                let mut conn = conn.clone();
                conn.hset(key, field, value).await
            }
        }
    }

    /// HSET followed by WAIT: write a field and then wait for replica confirmation.
    ///
    /// Returns `WaitResult` indicating how many replicas confirmed and whether
    /// the timeout was reached.
    pub async fn hset_and_wait(
        &self,
        key: &str,
        field: &str,
        value: &[u8],
        wait_replicas: u32,
        wait_timeout_ms: u64,
    ) -> Result<WaitResult, RedisError> {
        self.hset(key, field, value).await?;
        self.wait(wait_replicas, wait_timeout_ms).await
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

    /// Pipeline multiple HSET commands followed by a single WAIT.
    ///
    /// More efficient than individual hset_and_wait calls for batch writes.
    pub async fn hset_batch_and_wait(
        &self,
        key: &str,
        fields: &[(String, Vec<u8>)],
        wait_replicas: u32,
        wait_timeout_ms: u64,
    ) -> Result<WaitResult, RedisError> {
        // Build and execute pipeline of HSETs
        let mut pipe = redis::pipe();
        for (field, value) in fields {
            pipe.hset(key, field.as_str(), value.as_slice()).ignore();
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

    /// HGETALL: read all fields from a hash. Returns (field, value) pairs.
    pub async fn hgetall(&self, key: &str) -> Result<Vec<(String, Vec<u8>)>, RedisError> {
        match &self.inner {
            ConnectionKind::Standalone(conn) => {
                let mut conn = conn.clone();
                conn.hgetall(key).await
            }
            ConnectionKind::Cluster(conn) => {
                let mut conn = conn.clone();
                conn.hgetall(key).await
            }
        }
    }

    /// RENAME: atomically rename a key (used for segment promotion).
    pub async fn rename(&self, old_key: &str, new_key: &str) -> Result<(), RedisError> {
        match &self.inner {
            ConnectionKind::Standalone(conn) => {
                let mut conn = conn.clone();
                redis::cmd("RENAME")
                    .arg(old_key)
                    .arg(new_key)
                    .query_async(&mut conn)
                    .await
            }
            ConnectionKind::Cluster(conn) => {
                let mut conn = conn.clone();
                redis::cmd("RENAME")
                    .arg(old_key)
                    .arg(new_key)
                    .query_async(&mut conn)
                    .await
            }
        }
    }

    /// RPUSH: append a value to a list (used for cached segment tracking).
    pub async fn rpush(&self, key: &str, value: &str) -> Result<(), RedisError> {
        match &self.inner {
            ConnectionKind::Standalone(conn) => {
                let mut conn = conn.clone();
                conn.rpush(key, value).await
            }
            ConnectionKind::Cluster(conn) => {
                let mut conn = conn.clone();
                conn.rpush(key, value).await
            }
        }
    }

    /// LPOP: pop the oldest value from a list (used for segment eviction).
    pub async fn lpop(&self, key: &str) -> Result<Option<String>, RedisError> {
        match &self.inner {
            ConnectionKind::Standalone(conn) => {
                let mut conn = conn.clone();
                conn.lpop(key, None).await
            }
            ConnectionKind::Cluster(conn) => {
                let mut conn = conn.clone();
                conn.lpop(key, None).await
            }
        }
    }

    /// LLEN: return the length of a list.
    pub async fn llen(&self, key: &str) -> Result<u64, RedisError> {
        match &self.inner {
            ConnectionKind::Standalone(conn) => {
                let mut conn = conn.clone();
                conn.llen(key).await
            }
            ConnectionKind::Cluster(conn) => {
                let mut conn = conn.clone();
                conn.llen(key).await
            }
        }
    }

    /// DEL: delete a key entirely (used for segment eviction).
    pub async fn del(&self, key: &str) -> Result<(), RedisError> {
        match &self.inner {
            ConnectionKind::Standalone(conn) => {
                let mut conn = conn.clone();
                conn.del(key).await
            }
            ConnectionKind::Cluster(conn) => {
                let mut conn = conn.clone();
                conn.del(key).await
            }
        }
    }

    /// EXISTS: check if a key exists.
    pub async fn exists(&self, key: &str) -> Result<bool, RedisError> {
        match &self.inner {
            ConnectionKind::Standalone(conn) => {
                let mut conn = conn.clone();
                conn.exists(key).await
            }
            ConnectionKind::Cluster(conn) => {
                let mut conn = conn.clone();
                conn.exists(key).await
            }
        }
    }
}
