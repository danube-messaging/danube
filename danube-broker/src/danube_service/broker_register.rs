use crate::metadata_storage::MetadataStorage;
use anyhow::Result;
use danube_core::metadata::{MetaOptions, MetadataStore};
use tokio::time::{sleep, Duration};
use tracing::{error, info, warn};

use crate::resources::{BASE_BROKER_PATH, BASE_REGISTER_PATH};
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
    // Resilient: retries with backoff on failure instead of breaking.
    // If failures exceed TTL duration, marks the broker as drained on recovery.
    let broker_id_owned = broker_id.to_string();
    let renew_interval = Duration::from_secs((ttl as u64) / 3);
    let ttl_ms = (ttl as u64) * 1000;

    tokio::spawn(async move {
        let backoff_base = Duration::from_secs(1);
        let backoff_max = Duration::from_secs(5);
        let mut current_backoff = backoff_base;
        let mut consecutive_failure_ms: u64 = 0;
        let mut registration_lost = false;

        loop {
            sleep(renew_interval).await;

            match store
                .put_with_ttl(&path, payload.clone(), ttl_duration)
                .await
            {
                Ok(_) => {
                    if registration_lost {
                        // Recovered after being presumed dead.
                        // Registration renewed, but set state to drained for safety.
                        warn!(
                            broker_id = %broker_id_owned,
                            consecutive_failure_ms,
                            "registration recovered after expiry — setting broker to drained"
                        );
                        let state_path = join_path(&[BASE_BROKER_PATH, &broker_id_owned, "state"]);
                        let drained = serde_json::json!({
                            "mode": "drained",
                            "reason": "registration_expired"
                        });
                        if let Err(e) = store.put(&state_path, drained, MetaOptions::None).await {
                            error!(
                                broker_id = %broker_id_owned,
                                error = %e,
                                "failed to set broker state to drained after recovery"
                            );
                        } else {
                            info!(
                                broker_id = %broker_id_owned,
                                "broker set to drained after registration recovery. \
                                 Use `danube-admin brokers activate` to resume."
                            );
                        }
                        registration_lost = false;
                    }
                    // Reset failure tracking on success
                    consecutive_failure_ms = 0;
                    current_backoff = backoff_base;
                }
                Err(e) => {
                    consecutive_failure_ms += renew_interval.as_millis() as u64;
                    error!(
                        broker_id = %broker_id_owned,
                        consecutive_failure_ms,
                        error = %e,
                        "failed to renew broker registration"
                    );

                    if consecutive_failure_ms > ttl_ms && !registration_lost {
                        warn!(
                            broker_id = %broker_id_owned,
                            ttl_ms,
                            consecutive_failure_ms,
                            "registration likely expired (failures > TTL). \
                             Will register as drained on recovery."
                        );
                        registration_lost = true;
                    }

                    // Retry with exponential backoff (capped)
                    sleep(current_backoff).await;
                    current_backoff = (current_backoff * 2).min(backoff_max);
                    continue;
                }
            }
        }
    });

    Ok(())
}
