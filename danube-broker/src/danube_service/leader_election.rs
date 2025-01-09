use anyhow::Result;
use danube_metadata_store::{MetaOptions, MetadataStore, MetadataStorage};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration, Interval};
use tracing::{debug, error, warn};

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum LeaderElectionState {
    NoLeader,
    Leading,
    Following,
}

// Leader Election service is needed for critical tasks such as topic assignment to brokers and partitioning.
// Load Manager is using this service, as only one broker is selected to make the load usage calculations and post the results.
// Should be selected one broker leader per cluster, who takes the decissions.
#[derive(Debug, Clone)]
pub(crate) struct LeaderElection {
    path: String,
    broker_id: u64,
    store: MetadataStorage,
    state: Arc<Mutex<LeaderElectionState>>,
}

impl LeaderElection {
    pub fn new(store: MetadataStorage, path: &str, broker_id: u64) -> Self {
        Self {
            path: path.to_owned(),
            broker_id,
            store,
            state: Arc::new(Mutex::new(LeaderElectionState::NoLeader)),
        }
    }

    pub async fn start(&mut self, mut leader_check_interval: Interval) {
        //      self.elect().await;
        loop {
            let _ = self.check_leader().await;
            leader_check_interval.tick().await;
        }
    }

    pub async fn get_state(&self) -> LeaderElectionState {
        let state = self.state.lock().await;
        state.clone()
    }

    async fn set_state(&self, new_state: LeaderElectionState) {
        let mut state = self.state.lock().await;
        if *state != new_state {
            *state = new_state;
        }
    }

    async fn elect(&mut self) {
        debug!("Broker {} attempting to become the leader", self.broker_id);
        match self.try_to_become_leader().await {
            Ok(is_leader) => {
                if is_leader {
                    self.set_state(LeaderElectionState::Leading).await;
                } else {
                    self.set_state(LeaderElectionState::Following).await;
                }
            }
            Err(e) => {
                warn!("Election error: {}", e);
            }
        }
    }

    async fn try_to_become_leader(&mut self) -> Result<bool> {
        match self.store {
            MetadataStorage::Etcd(_) => {
                //TODO! make this user configurable, this should be half of the broker register TTL
                // we want the broker to become the leader first,
                // before another broker unregister in order for leader broker to alocate/ delete resources
                let ttl = 14;

                // Create lease
                let lease = self.store.create_lease(ttl).await?;
                let lease_id = lease.id();

                // Prepare payload
                let payload = serde_json::Value::Number(serde_json::Number::from(self.broker_id));

                // Try to become leader by putting value with lease
                match self
                    .store
                    .put_with_lease(&self.path, payload, lease_id)
                    .await
                {
                    Ok(_) => {
                        // Start lease keepalive in background
                        tokio::spawn({
                            let store = self.store.clone();
                            async move {
                                loop {
                                    match store.keep_lease_alive(lease_id, "Leader Election").await
                                    {
                                        Ok(_) => {
                                            sleep(Duration::from_secs(ttl as u64 / 2)).await;
                                        }
                                        Err(e) => {
                                            error!("Failed to keep leader lease alive: {}", e);
                                            break;
                                        }
                                    }
                                }
                            }
                        });
                        Ok(true)
                    }
                    Err(e) => Err(e.into()),
                }
            }
            _ => return Err(anyhow::anyhow!("Unsupported storage backend")),
        }
    }

    async fn check_leader(&mut self) -> Result<()> {
        match self.store.get(self.path.as_str(), MetaOptions::None).await {
            Ok(response) => {
                if response.is_none() {
                    self.elect().await;
                } else {
                    let leader_id: u64 = response
                        .expect("checked aboved that the value is present")
                        .as_u64()
                        .expect("Broker Id should be a valid u64");
                    if leader_id == self.broker_id {
                        self.set_state(LeaderElectionState::Leading).await;
                        debug!("Broker {} is the leader", self.broker_id);
                    } else {
                        self.set_state(LeaderElectionState::Following).await;
                        debug!("Broker {} is a follower", self.broker_id);
                    }
                }
            }
            Err(e) => {
                warn!("Failed to check leader: {}", e);
            }
        }
        Ok(())
    }
}
