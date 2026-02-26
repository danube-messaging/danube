use crate::metadata_storage::MetadataStorage;
use anyhow::Result;
use danube_core::metadata::{MetadataStore};
use tokio::time::{sleep, Duration};
use tracing::{error, info};

use crate::resources::BASE_REGISTER_PATH;
use crate::utils::join_path;

pub(crate) async fn register_broker(
    store: MetadataStorage,
    broker_id: &str,
    broker_url: &str,
    connect_url: &str,
    admin_addr: &str,
    metrics_addr: Option<&str>,
    ttl: i64,
    is_secure: bool,
) -> Result<()> {
    let path = join_path(&[BASE_REGISTER_PATH, broker_id]);
    let scheme = if is_secure { "https" } else { "http" };
    let admin_uri = format!("{}://{}", scheme, admin_addr);
    let payload = if let Some(m) = metrics_addr {
        serde_json::json!({
            "broker_url": broker_url,
            "connect_url": connect_url,
            "admin_addr": admin_uri,
            "prom_exporter": m,
        })
    } else {
        serde_json::json!({
            "broker_url": broker_url,
            "connect_url": connect_url,
            "admin_addr": admin_uri,
        })
    };

    let ttl_duration = Duration::from_secs(ttl as u64);

    // Initial registration with TTL
    store
        .put_with_ttl(&path, payload.clone(), ttl_duration)
        .await?;
    info!(
        broker_id = %broker_id,
        broker_url = %broker_url,
        connect_url = %connect_url,
        "broker registered in the cluster"
    );

    // Background task renews the registration periodically via put_with_ttl.
    // No ETCD-specific lease management — works with any backend.
    let broker_id_owned = broker_id.to_string();
    let renew_interval = Duration::from_secs((ttl as u64) / 3);
    tokio::spawn(async move {
        loop {
            sleep(renew_interval).await;
            match store
                .put_with_ttl(&path, payload.clone(), ttl_duration)
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    error!(
                        broker_id = %broker_id_owned,
                        error = %e,
                        "Failed to renew broker registration"
                    );
                    break;
                }
            }
        }
    });

    Ok(())
}
