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
    ttl: i64,
) -> Result<()> {
    match store {
        MetadataStorage::Etcd(_) => {
            // Create a lease with a TTL (time to live)
            let lease = store.create_lease(ttl).await?;

            let lease_id = lease.id();
            let path = join_path(&[BASE_REGISTER_PATH, broker_id]);
            let broker_uri = format!("http://{}", broker_addr);
            let payload = serde_json::Value::String(broker_uri);

            store.put_with_lease(&path, payload, lease_id).await?;
            info!("Broker {} registered in the cluster", broker_id);

            // Lease management is ETCD-specific
            tokio::spawn(async move {
                loop {
                    match store.keep_lease_alive(lease_id, "Broker Register").await {
                        Ok(_) => sleep(Duration::from_secs((ttl as u64) / 3)).await,
                        Err(e) => {
                            error!("Failed to keep lease alive: {}", e);
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
