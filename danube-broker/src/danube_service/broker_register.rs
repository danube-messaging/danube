use anyhow::Result;
use danube_metadata_store::MetadataStorage;
use tokio::time::{sleep, Duration};
use tracing::{error, info};

use crate::resources::BASE_REGISTER_PATH;
use crate::utils::join_path;

pub(crate) async fn register_broker(
    store: MetadataStorage,
    broker_id: &str,
    broker_addr: &str,
    admin_addr: &str,
    metrics_addr: Option<&str>,
    ttl: i64,
    is_secure: bool,
) -> Result<()> {
    match store {
        MetadataStorage::Etcd(_) => {
            // Create a lease with a TTL (time to live)
            let lease = store.create_lease(ttl).await?;

            let lease_id = lease.id();
            let path = join_path(&[BASE_REGISTER_PATH, broker_id]);
            let scheme = if is_secure { "https" } else { "http" };
            let broker_uri = format!("{}://{}", scheme, broker_addr);
            let admin_uri = format!("{}://{}", scheme, admin_addr);
            let payload = if let Some(m) = metrics_addr {
                serde_json::json!({
                    "broker_addr": broker_uri,
                    "advertised_addr": broker_addr,
                    "admin_addr": admin_uri,
                    "prom_exporter": m,
                })
            } else {
                serde_json::json!({
                    "broker_addr": broker_uri,
                    "advertised_addr": broker_addr,
                    "admin_addr": admin_uri,
                })
            };

            store.put_with_lease(&path, payload, lease_id).await?;
            info!(
                broker_id = %broker_id,
                broker_addr = %broker_addr,
                "broker registered in the cluster"
            );

            // Lease management is ETCD-specific
            let broker_id_owned = broker_id.to_string();
            tokio::spawn(async move {
                loop {
                    match store.keep_lease_alive(lease_id, "Broker Register").await {
                        Ok(_) => sleep(Duration::from_secs((ttl as u64) / 3)).await,
                        Err(e) => {
                            error!(
                                broker_id = %broker_id_owned,
                                error = %e,
                                "Failed to keep broker registration lease alive"
                            );
                            break;
                        }
                    }
                }
            });
        }
        _ => return Err(anyhow::anyhow!("Unsupported storage backend")),
    }

    Ok(())
}
