//! Raft node lifecycle — create, start, and bootstrap a Raft cluster node.
//!
//! This module is the main entry point for consumers of `danube-raft`.
//! It creates all the necessary components (state machine, log store, network,
//! gRPC server) and returns a `RaftMetadataStore` ready for use by the broker.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use rand::Rng;

use openraft::{BasicNode, Config, Raft};
use tonic::transport::{Endpoint, Server};
use tracing::{info, warn};

use danube_core::raft_proto::raft_transport_client::RaftTransportClient;

use danube_core::raft_proto::raft_transport_server::RaftTransportServer;

use crate::leadership::LeadershipHandle;
use crate::log_store::RedbLogStore;
use crate::network::DanubeNetworkFactory;
use crate::raft_store::RaftMetadataStore;
use crate::server::RaftTransportHandler;
use crate::state_machine::DanubeStateMachine;
use crate::ttl_worker;
use crate::typ::TypeConfig;

/// Configuration for starting a Raft node.
pub struct RaftNodeConfig {
    /// Directory for redb log store and node metadata.
    /// A stable `node_id` file is auto-generated here on first boot.
    pub data_dir: PathBuf,
    /// Address this node listens on for Raft gRPC transport.
    pub raft_addr: SocketAddr,
    /// Advertised address other nodes use to reach this node's Raft transport.
    /// If `None`, falls back to `raft_addr` (works on localhost, NOT in Docker).
    pub advertised_addr: Option<String>,
    /// TTL expiration check interval.
    pub ttl_check_interval: Duration,
}

/// A running Raft node with all background tasks.
pub struct RaftNode {
    /// The `MetadataStore` implementation backed by this Raft node.
    pub store: RaftMetadataStore,
    /// Handle to the Raft instance (for admin operations like add_learner, change_membership).
    pub raft: Raft<TypeConfig>,
    /// Auto-generated stable node identity (persisted in `{data_dir}/node_id`).
    pub node_id: u64,
    /// The address this node advertises to peers (used in cluster membership).
    pub advertised_addr: String,
    /// gRPC server join handle.
    _grpc_handle: tokio::task::JoinHandle<()>,
    /// TTL worker join handle.
    _ttl_handle: tokio::task::JoinHandle<()>,
}

impl RaftNode {
    /// Read or generate the stable node identity.
    ///
    /// On first boot a random `u64` is generated and written to `{data_dir}/node_id`.
    /// On subsequent boots the persisted value is read back, giving the node a
    /// stable identity across restarts — identical to the pattern used by
    /// CockroachDB, TiKV, and Consul.
    fn resolve_node_id(data_dir: &PathBuf) -> anyhow::Result<u64> {
        let id_path = data_dir.join("node_id");
        if id_path.exists() {
            let contents = fs::read_to_string(&id_path)?;
            let id: u64 = contents
                .trim()
                .parse()
                .map_err(|e| anyhow::anyhow!("invalid node_id file: {}", e))?;
            Ok(id)
        } else {
            let id: u64 = rand::rng().random();
            fs::create_dir_all(data_dir)?;
            fs::write(&id_path, id.to_string())?;
            info!(node_id = id, path = %id_path.display(), "generated new stable node_id");
            Ok(id)
        }
    }

    /// Create and start a new Raft node.
    ///
    /// The node ID is auto-resolved from `{data_dir}/node_id` (generated on first boot).
    /// This does NOT bootstrap the cluster — call `init_cluster` on the first node.
    pub async fn start(cfg: RaftNodeConfig) -> anyhow::Result<Self> {
        // 0. Resolve stable node identity
        fs::create_dir_all(&cfg.data_dir)?;
        let node_id = Self::resolve_node_id(&cfg.data_dir)?;

        // 1. Create openraft config
        let raft_config = Config {
            heartbeat_interval: 500,
            election_timeout_min: 1500,
            election_timeout_max: 3000,
            install_snapshot_timeout: 5000,
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(1000),
            max_in_snapshot_log_to_keep: 100,
            ..Config::default()
        };
        let raft_config = Arc::new(raft_config.validate()?);

        // 2. Create state machine and get shared data handle
        let sm = DanubeStateMachine::new();
        let shared_data = sm.shared_data();

        // 3. Create persistent log store
        let db_path = cfg.data_dir.join("raft-log.redb");
        let log_store = RedbLogStore::new(&db_path)?;

        // 4. Create Raft instance (takes ownership of SM)
        let raft = Raft::new(node_id, raft_config, DanubeNetworkFactory, log_store, sm).await?;

        // 5. Start gRPC server for Raft transport
        let advertised_addr = cfg
            .advertised_addr
            .clone()
            .unwrap_or_else(|| cfg.raft_addr.to_string());
        let handler = RaftTransportHandler::new(raft.clone(), node_id, advertised_addr.clone());
        let grpc_addr = cfg.raft_addr;
        let grpc_handle = tokio::spawn(async move {
            info!(%grpc_addr, "starting Raft gRPC transport");
            if let Err(e) = Server::builder()
                .add_service(RaftTransportServer::new(handler))
                .serve(grpc_addr)
                .await
            {
                warn!(?e, "Raft gRPC transport exited with error");
            }
        });

        // 6. Start TTL expiration worker
        let ttl_handle =
            ttl_worker::spawn_ttl_worker(raft.clone(), shared_data.clone(), cfg.ttl_check_interval);

        // 7. Build the MetadataStore wrapper
        let store = RaftMetadataStore::new(raft.clone(), shared_data);

        info!(node_id, "Raft node started");

        Ok(Self {
            store,
            raft,
            node_id,
            advertised_addr,
            _grpc_handle: grpc_handle,
            _ttl_handle: ttl_handle,
        })
    }

    /// Bootstrap a single-node cluster. Call this only on the **first** node
    /// during initial cluster creation. After this, the node becomes the leader.
    pub async fn init_cluster(&self, addr: &str) -> anyhow::Result<()> {
        let mut members = BTreeMap::new();
        members.insert(
            self.node_id,
            BasicNode {
                addr: addr.to_string(),
            },
        );

        self.raft.initialize(members).await?;
        info!(
            node_id = self.node_id,
            "Raft cluster initialized (single-node)"
        );
        Ok(())
    }

    /// Add a learner node to the cluster. Must be called on the leader.
    pub async fn add_learner(&self, node_id: u64, addr: &str) -> anyhow::Result<()> {
        self.raft
            .add_learner(
                node_id,
                BasicNode {
                    addr: addr.to_string(),
                },
                true,
            )
            .await?;
        info!(node_id, "learner added");
        Ok(())
    }

    /// Promote learners to voters by changing membership. Must be called on the leader.
    pub async fn change_membership(&self, member_ids: Vec<u64>) -> anyhow::Result<()> {
        let members: BTreeSet<u64> = member_ids.into_iter().collect();
        self.raft.change_membership(members, false).await?;
        info!("membership changed");
        Ok(())
    }

    /// Bootstrap the Raft cluster based on seed node configuration.
    ///
    /// - **Empty `seed_nodes`**: single-node auto-init (development mode).
    /// - **Non-empty `seed_nodes`**: multi-node mode. Contacts each seed peer
    ///   via `GetNodeInfo` RPC to discover `(node_id, raft_addr)` pairs.
    ///   The peer with the **lowest `node_id`** calls `raft.initialize()`.
    ///   Others simply wait — they receive membership via Raft replication.
    ///
    /// Idempotent: if the cluster is already initialized (persisted state), returns Ok.
    pub async fn bootstrap_cluster(
        &self,
        self_addr: &str,
        seed_nodes: &[String],
    ) -> anyhow::Result<()> {
        // Check if already initialized (persisted membership from a previous run).
        let metrics = self.raft.metrics().borrow().clone();
        if let Some(membership) = metrics
            .membership_config
            .membership()
            .get_joint_config()
            .first()
        {
            if !membership.is_empty() {
                info!(
                    node_id = self.node_id,
                    "cluster already initialized (from persisted state)"
                );
                return Ok(());
            }
        }

        if seed_nodes.is_empty() {
            // Single-node auto-init
            info!(
                node_id = self.node_id,
                "no seed_nodes configured — auto-initializing single-node cluster"
            );
            self.init_cluster(self_addr).await?;
            return Ok(());
        }

        // Multi-node: discover all seed peers
        info!(
            node_id = self.node_id,
            seed_count = seed_nodes.len(),
            "discovering seed peers for cluster formation..."
        );

        let mut members: BTreeMap<u64, BasicNode> = BTreeMap::new();

        // Add self
        members.insert(
            self.node_id,
            BasicNode {
                addr: self_addr.to_string(),
            },
        );

        // Poll all seed peers until they respond
        for seed_addr in seed_nodes {
            // Skip self
            if seed_addr == self_addr {
                continue;
            }

            let endpoint_url = format!("http://{}", seed_addr);
            let mut attempts = 0u32;
            loop {
                attempts += 1;
                match Self::discover_peer(&endpoint_url).await {
                    Ok((peer_id, peer_addr)) => {
                        info!(
                            peer_node_id = peer_id,
                            peer_addr = %peer_addr,
                            seed = %seed_addr,
                            "discovered seed peer"
                        );
                        members.insert(peer_id, BasicNode { addr: peer_addr });
                        break;
                    }
                    Err(e) => {
                        if attempts % 10 == 1 {
                            warn!(
                                seed = %seed_addr,
                                attempt = attempts,
                                error = %e,
                                "waiting for seed peer to become reachable..."
                            );
                        }
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                }
            }
        }

        info!(
            node_id = self.node_id,
            member_count = members.len(),
            "all seed peers discovered"
        );

        // The node with the lowest node_id performs the initialization
        let lowest_id = *members.keys().next().unwrap();
        if self.node_id == lowest_id {
            info!(
                node_id = self.node_id,
                "this node has the lowest ID — initializing cluster"
            );
            self.raft.initialize(members).await?;
            info!(
                node_id = self.node_id,
                "multi-node cluster initialized, waiting for leader election..."
            );
            // Fall through to the leader-wait loop below
        } else {
            info!(
                node_id = self.node_id,
                initializer = lowest_id,
                "waiting for node {} to initialize the cluster...",
                lowest_id
            );
        }

        // Wait until a leader is established (applies to both initializer and followers)
        loop {
            let m = self.raft.metrics().borrow().clone();
            if m.current_leader.is_some() {
                info!(
                    node_id = self.node_id,
                    leader = ?m.current_leader,
                    "cluster leader established"
                );
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// Contact a peer's Raft transport to get its node_id and raft_addr.
    async fn discover_peer(endpoint_url: &str) -> anyhow::Result<(u64, String)> {
        let channel = Endpoint::from_shared(endpoint_url.to_string())?
            .connect_timeout(Duration::from_secs(3))
            .connect()
            .await?;
        let mut client = RaftTransportClient::new(channel);
        let resp = client
            .get_node_info(danube_core::raft_proto::Empty {})
            .await?;
        let info = resp.into_inner();
        Ok((info.node_id, info.raft_addr))
    }

    /// Create a lightweight handle for querying leadership status.
    pub fn leadership_handle(&self) -> LeadershipHandle {
        LeadershipHandle::new(self.raft.clone(), self.node_id)
    }
}
