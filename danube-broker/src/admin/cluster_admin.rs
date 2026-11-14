use crate::admin::desired_state::{
    build_object_store_config, compute_plan, DesiredObjectStoreSpec, DesiredSharedFsSpec,
    DesiredStateManifest,
};
use crate::admin::topics_admin::resolve_intent_and_strategy;
use crate::admin::DanubeAdminImpl;
use crate::danube_service::load_manager::config::{
    AssignmentStrategy, LoadManagerConfig, RebalancingAggressiveness,
};
use crate::danube_service::{ClusterWalRetentionSpec, ClusterWriteBufferSpec};
use crate::security::authn::get_security_context;
use crate::security::authz::{enforce_authorization, Permission, Resource};
use crate::service_configuration::RecoveryConfig;
use crate::storage_configuration::{
    LocalRetentionNode, ObjectStoreNode, SharedFsDurableNode, StorageConfig, WalFlushNode, WalNode,
    WalRotationNode, WriteBufferNode,
};
use danube_core::admin_proto::{
    cluster_admin_server::ClusterAdmin, AddNodeRequest, AddNodeResponse, ClusterStatusResponse,
    ConfigureClusterStorageRequest, ConfigureClusterStorageResponse, ConfigureLoadManagerRequest,
    ConfigureLoadManagerResponse, ConfigureObjectStoreRequest, ConfigureObjectStoreResponse,
    ConfigureWalRetentionRequest, ConfigureWalRetentionResponse, ConfigureWriteBufferRequest,
    ConfigureWriteBufferResponse, DesiredStateApplyResponse, DesiredStatePlanResponse,
    DesiredStateRequest, Empty, ExportClusterConfigRequest, ExportClusterConfigResponse,
    JoinClusterRequest, JoinClusterResponse, ObjectStoreConfigResponse, PlannedAction,
    PromoteNodeRequest, PromoteNodeResponse, RemoveNodeRequest, RemoveNodeResponse,
};
use danube_core::metadata::{MetaOptions, MetadataStore};
use danube_core::proto::{
    schema_reference::VersionRef, DispatchStrategy as CoreDispatchStrategy, SchemaReference,
};
use danube_persistent_storage::valkey::config::WaitTimeoutPolicy;
use danube_persistent_storage::{
    ObjectStoreBackend, ObjectStoreConfig, SharedFsConfig, StorageFactory, StorageMode,
};
use danube_raft::BasicNode;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

#[tonic::async_trait]
impl ClusterAdmin for DanubeAdminImpl {
    /// Return current Raft cluster state.
    async fn cluster_status(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<ClusterStatusResponse>, Status> {
        let security_context = get_security_context(&request)?;
        enforce_authorization(&security_context, &Resource::Cluster, Permission::ManageCluster, &self.resources.security).await?;

        let cluster = self.cluster.as_ref()
            .ok_or_else(|| Status::unimplemented(
                "cluster operations not available in standalone/edge mode"
            ))?;

        let metrics = cluster.raft.metrics().borrow().clone();

        let membership = metrics.membership_config.membership();
        let voters: Vec<u64> = membership
            .get_joint_config()
            .first()
            .map(|s| s.iter().copied().collect())
            .unwrap_or_default();

        let all_nodes: std::collections::BTreeSet<u64> =
            membership.nodes().map(|(id, _)| *id).collect();
        let voter_set: std::collections::BTreeSet<u64> = voters.iter().copied().collect();
        let learners: Vec<u64> = all_nodes.difference(&voter_set).copied().collect();

        let last_applied = metrics.last_applied.map(|lid| lid.index).unwrap_or(0);

        Ok(Response::new(ClusterStatusResponse {
            leader_id: metrics.current_leader.unwrap_or(0),
            current_term: metrics.current_term,
            last_applied,
            voters,
            learners,
            self_node_id: cluster.leadership.node_id(),
            raft_addr: cluster.raft_addr.to_string(),
        }))
    }

    /// Add a node as a Raft learner (non-voting).
    async fn add_node(
        &self,
        request: Request<AddNodeRequest>,
    ) -> Result<Response<AddNodeResponse>, Status> {
        let security_context = get_security_context(&request)?;
        let req = request.into_inner();

        enforce_authorization(&security_context, &Resource::Cluster, Permission::ManageCluster, &self.resources.security).await?;

        let cluster = self.cluster.as_ref()
            .ok_or_else(|| Status::unimplemented(
                "cluster operations not available in standalone/edge mode"
            ))?;

        if req.addr.is_empty() {
            return Err(Status::invalid_argument("addr is required"));
        }
        if req.node_id == 0 {
            return Err(Status::invalid_argument(
                "node_id is required (discovered from the target node's {data_dir}/node_id)",
            ));
        }

        info!(node_id = req.node_id, addr = %req.addr, "adding learner node");

        cluster.raft
            .add_learner(
                req.node_id,
                BasicNode {
                    addr: req.addr.clone(),
                },
                true,
            )
            .await
            .map_err(|e| Status::internal(format!("failed to add learner: {}", e)))?;

        info!(node_id = req.node_id, "learner added successfully");

        let _ = crate::audit::record_audit_event(
            &self.resources.store,
            security_context.principal.principal_name(),
            "add_node",
            &req.node_id.to_string(),
            serde_json::json!({ "addr": req.addr }),
        )
        .await;

        Ok(Response::new(AddNodeResponse {
            success: true,
            node_id: req.node_id,
            message: format!("Node {} added as learner at {}", req.node_id, req.addr),
        }))
    }

    /// Promote a learner to a full voting member.
    async fn promote_node(
        &self,
        request: Request<PromoteNodeRequest>,
    ) -> Result<Response<PromoteNodeResponse>, Status> {
        let security_context = get_security_context(&request)?;
        let req = request.into_inner();

        enforce_authorization(&security_context, &Resource::Cluster, Permission::ManageCluster, &self.resources.security).await?;

        let cluster = self.cluster.as_ref()
            .ok_or_else(|| Status::unimplemented(
                "cluster operations not available in standalone/edge mode"
            ))?;

        if req.node_id == 0 {
            return Err(Status::invalid_argument("node_id is required"));
        }

        info!(node_id = req.node_id, "promoting learner to voter");

        // Get current voters and add the new one.
        let metrics = cluster.raft.metrics().borrow().clone();
        let mut voters: std::collections::BTreeSet<u64> = metrics
            .membership_config
            .membership()
            .get_joint_config()
            .first()
            .map(|s| s.clone())
            .unwrap_or_default();

        if voters.contains(&req.node_id) {
            return Ok(Response::new(PromoteNodeResponse {
                success: true,
                message: format!("Node {} is already a voter", req.node_id),
            }));
        }

        let target_cap = self.target_voter_count(0).await;
        if voters.len() >= target_cap {
            return Err(Status::failed_precondition(format!(
                "voter cap ({}) reached; node {} will remain a non-voting broker",
                target_cap, req.node_id
            )));
        }

        voters.insert(req.node_id);

        cluster.raft
            .change_membership(voters, false)
            .await
            .map_err(|e| Status::internal(format!("failed to promote node: {}", e)))?;

        info!(node_id = req.node_id, "node promoted to voter");

        let _ = crate::audit::record_audit_event(
            &self.resources.store,
            security_context.principal.principal_name(),
            "promote_node",
            &req.node_id.to_string(),
            serde_json::json!({}),
        )
        .await;

        Ok(Response::new(PromoteNodeResponse {
            success: true,
            message: format!("Node {} promoted to voter", req.node_id),
        }))
    }

    /// Remove a node from the Raft cluster.
    async fn remove_node(
        &self,
        request: Request<RemoveNodeRequest>,
    ) -> Result<Response<RemoveNodeResponse>, Status> {
        let security_context = get_security_context(&request)?;
        let req = request.into_inner();

        enforce_authorization(&security_context, &Resource::Cluster, Permission::ManageCluster, &self.resources.security).await?;

        let cluster = self.cluster.as_ref()
            .ok_or_else(|| Status::unimplemented(
                "cluster operations not available in standalone/edge mode"
            ))?;

        if req.node_id == 0 {
            return Err(Status::invalid_argument("node_id is required"));
        }

        info!(node_id = req.node_id, "removing node from cluster");

        // Get current voters and remove the target.
        let metrics = cluster.raft.metrics().borrow().clone();
        let mut voters: std::collections::BTreeSet<u64> = metrics
            .membership_config
            .membership()
            .get_joint_config()
            .first()
            .map(|s| s.clone())
            .unwrap_or_default();

        if !voters.remove(&req.node_id) {
            warn!(
                node_id = req.node_id,
                "node is not a voter — may be a learner or unknown"
            );
        }

        if voters.is_empty() {
            return Err(Status::failed_precondition(
                "cannot remove the last voter from the cluster",
            ));
        }

        cluster.raft
            .change_membership(voters, false)
            .await
            .map_err(|e| Status::internal(format!("failed to remove node: {}", e)))?;

        info!(node_id = req.node_id, "node removed from cluster");

        let _ = crate::audit::record_audit_event(
            &self.resources.store,
            security_context.principal.principal_name(),
            "remove_node",
            &req.node_id.to_string(),
            serde_json::json!({}),
        )
        .await;

        Ok(Response::new(RemoveNodeResponse {
            success: true,
            message: format!("Node {} removed from cluster", req.node_id),
        }))
    }

    async fn configure_object_store(
        &self,
        request: Request<ConfigureObjectStoreRequest>,
    ) -> Result<Response<ConfigureObjectStoreResponse>, Status> {
        let security_context = get_security_context(&request)?;
        let req = request.into_inner();
        enforce_authorization(
            &security_context,
            &Resource::Cluster,
            Permission::ManageCluster,
            &self.resources.security,
        )
        .await?;

        self.apply_object_store_spec(&DesiredObjectStoreSpec {
            backend: req.backend.clone(),
            root: req.root.clone(),
            options: req.options.clone(),
            force: req.force,
        })
        .await?;

        let _ = crate::audit::record_audit_event(
            &self.resources.store,
            security_context.principal.principal_name(),
            "configure_object_store",
            &req.root,
            serde_json::json!({ "backend": req.backend, "force": req.force }),
        )
        .await;

        Ok(Response::new(ConfigureObjectStoreResponse {
            success: true,
            message: format!("Configured {} object store at {}", req.backend, req.root),
        }))
    }

    async fn get_object_store_config(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<ObjectStoreConfigResponse>, Status> {
        let security_context = get_security_context(&request)?;
        enforce_authorization(
            &security_context,
            &Resource::Cluster,
            Permission::ManageCluster,
            &self.resources.security,
        )
        .await?;

        let factory = self.broker_service.storage_factory();
        let cloud_tiered_topics = factory.cloud_tiered_topic_count() as u32;
        match factory.current_object_store_config() {
            Some(ObjectStoreConfig::ObjectStore {
                backend,
                root,
                options,
            }) => {
                let backend_str = match backend {
                    ObjectStoreBackend::S3 => "s3",
                    ObjectStoreBackend::Gcs => "gcs",
                    ObjectStoreBackend::Azblob => "azblob",
                }
                .to_string();
                Ok(Response::new(ObjectStoreConfigResponse {
                    configured: true,
                    backend: backend_str,
                    root,
                    options,
                    cloud_tiered_topics,
                }))
            }
            Some(ObjectStoreConfig::TestFilesystem { root }) => {
                Ok(Response::new(ObjectStoreConfigResponse {
                    configured: true,
                    backend: "filesystem".to_string(),
                    root,
                    options: Default::default(),
                    cloud_tiered_topics,
                }))
            }
            None => Ok(Response::new(ObjectStoreConfigResponse {
                configured: false,
                backend: String::new(),
                root: String::new(),
                options: Default::default(),
                cloud_tiered_topics,
            })),
        }
    }

    async fn plan_desired_state(
        &self,
        request: Request<DesiredStateRequest>,
    ) -> Result<Response<DesiredStatePlanResponse>, Status> {
        let security_context = get_security_context(&request)?;
        let req = request.into_inner();
        enforce_authorization(
            &security_context,
            &Resource::Cluster,
            Permission::ManageCluster,
            &self.resources.security,
        )
        .await?;

        let manifest = DesiredStateManifest::parse(&req.manifest)?;
        let actions = self.build_desired_state_plan(&manifest, req.prune).await;
        let has_destructive_changes = actions.iter().any(|a| a.kind == "delete_topic");

        Ok(Response::new(DesiredStatePlanResponse {
            actions,
            has_destructive_changes,
        }))
    }

    async fn apply_desired_state(
        &self,
        request: Request<DesiredStateRequest>,
    ) -> Result<Response<DesiredStateApplyResponse>, Status> {
        let security_context = get_security_context(&request)?;
        let req = request.into_inner();
        enforce_authorization(
            &security_context,
            &Resource::Cluster,
            Permission::ManageCluster,
            &self.resources.security,
        )
        .await?;

        let manifest = DesiredStateManifest::parse(&req.manifest)?;
        let actions = self.build_desired_state_plan(&manifest, req.prune).await;

        // Pre-flight guard before executing destructive deletes when --prune is requested.
        if req.prune && !req.force_delete_non_empty {
            for action in actions.iter().filter(|a| a.kind == "delete_topic") {
                let has_producers = !self
                    .resources
                    .topic
                    .get_topic_producers(&action.resource)
                    .await
                    .is_empty();
                let has_subscriptions = !self
                    .resources
                    .topic
                    .get_subscription_for_topic(&action.resource)
                    .await
                    .is_empty();
                if has_producers || has_subscriptions {
                    return Err(Status::failed_precondition(format!(
                        "refusing to prune active or non-empty topic '{}' without force=true",
                        action.resource
                    )));
                }
            }
        }

        let mut applied: Vec<PlannedAction> = Vec::new();

        if let Some(ref shared_fs) = manifest.shared_fs {
            self.apply_shared_fs_spec(shared_fs).await?;
            applied.push(PlannedAction {
                kind: "update_config".to_string(),
                resource: "cluster/shared_fs".to_string(),
                details: format!("root={}", shared_fs.root),
            });
        }

        if let Some(ref obj) = manifest.object_store {
            self.apply_object_store_spec(obj).await?;
            applied.push(PlannedAction {
                kind: "update_config".to_string(),
                resource: "cluster/object_store".to_string(),
                details: format!("backend={} root={}", obj.backend, obj.root),
            });
        }

        if let Some(ref wb) = manifest.write_buffer {
            self.apply_write_buffer_spec(wb).await?;
            applied.push(PlannedAction {
                kind: "update_config".to_string(),
                resource: "cluster/write_buffer".to_string(),
                details: format!("enabled={} endpoints={:?}", wb.enabled, wb.endpoints),
            });
        }

        if let Some(ref lm) = manifest.load_manager {
            self.apply_load_manager_config(lm).await?;
            applied.push(PlannedAction {
                kind: "update_config".to_string(),
                resource: "cluster/load_manager".to_string(),
                details: format!(
                    "strategy={:?} rebalancing_enabled={}",
                    lm.assignment_strategy, lm.rebalancing.enabled
                ),
            });
        }

        for action in &actions {
            match action.kind.as_str() {
                "create_namespace" => {
                    self.resources
                        .namespace
                        .create_namespace(&action.resource, None)
                        .await
                        .map_err(|e| {
                            Status::internal(format!(
                                "failed to create namespace '{}': {}",
                                action.resource, e
                            ))
                        })?;
                    applied.push(action.clone());
                }
                "create_topic" => {
                    if let Some(spec) = manifest
                        .topics
                        .iter()
                        .find(|t| t.name == action.resource)
                    {
                        self.apply_create_topic_spec(spec).await?;
                        applied.push(action.clone());
                    }
                }
                "update_config" if action.resource != "cluster/object_store" => {
                    if let Some(spec) = manifest
                        .topics
                        .iter()
                        .find(|t| t.name == action.resource)
                    {
                        if let Some(ref mode) = spec.storage_mode {
                            if matches!(
                                mode.trim().to_ascii_lowercase().as_str(),
                                "cloud_tiered" | "cloud-tiered" | "object_store"
                            ) {
                                self.broker_service
                                    .storage_factory()
                                    .enable_cloud_tiering_for_topic(&spec.name)
                                    .await
                                    .map_err(|e| {
                                        Status::failed_precondition(format!(
                                            "failed to enable cloud tiering on '{}': {}",
                                            spec.name, e
                                        ))
                                    })?;
                                self.resources
                                    .topic
                                    .set_topic_storage_mode(&spec.name, "cloud_tiered")
                                    .await
                                    .map_err(|e| {
                                        Status::internal(format!(
                                            "failed to persist storage mode for '{}': {}",
                                            spec.name, e
                                        ))
                                    })?;
                                applied.push(action.clone());
                            }
                        }
                    }
                }
                "delete_topic" => {
                    let _ = crate::audit::snapshot_topic_cursors(
                        &self.resources.topic,
                        &self.resources.store,
                        &action.resource,
                        "apply_desired_state_prune",
                    )
                    .await;
                    self.broker_service
                        .delete_topic(&action.resource)
                        .await
                        .map_err(|e| {
                            Status::internal(format!(
                                "failed to prune topic '{}': {}",
                                action.resource, e
                            ))
                        })?;
                    applied.push(action.clone());
                }
                _ => {}
            }
        }

        let _ = crate::audit::record_audit_event(
            &self.resources.store,
            security_context.principal.principal_name(),
            "apply_desired_state",
            "cluster",
            serde_json::json!({
                "applied_count": applied.len(),
                "prune": req.prune,
            }),
        )
        .await;

        let applied_count = applied.len();
        Ok(Response::new(DesiredStateApplyResponse {
            success: true,
            applied,
            message: format!("Applied {} actions cleanly", applied_count),
        }))
    }

    async fn join_cluster(
        &self,
        request: Request<JoinClusterRequest>,
    ) -> Result<Response<JoinClusterResponse>, Status> {
        let security_context = get_security_context(&request)?;
        let req = request.into_inner();
        enforce_authorization(
            &security_context,
            &Resource::Cluster,
            Permission::ManageCluster,
            &self.resources.security,
        )
        .await?;

        let cluster = self.cluster.as_ref().ok_or_else(|| {
            Status::unimplemented("cluster operations not available in standalone/edge mode")
        })?;

        if req.node_id == 0 || req.raft_addr.trim().is_empty() {
            return Err(Status::invalid_argument(
                "node_id and raft_addr are required",
            ));
        }

        let metrics = cluster.raft.metrics().borrow().clone();
        let mut voters: std::collections::BTreeSet<u64> = metrics
            .membership_config
            .membership()
            .get_joint_config()
            .first()
            .cloned()
            .unwrap_or_default();

        if voters.contains(&req.node_id) {
            return Ok(Response::new(JoinClusterResponse {
                success: true,
                promoted_to_voter: true,
                role: "voter".to_string(),
                message: format!("Node {} is already a voter", req.node_id),
            }));
        }

        cluster
            .raft
            .add_learner(
                req.node_id,
                BasicNode {
                    addr: req.raft_addr.clone(),
                },
                true,
            )
            .await
            .map_err(|e| Status::internal(format!("failed to add learner {}: {}", req.node_id, e)))?;

        let target_cap = self.target_voter_count(req.target_voter_count).await;
        if voters.len() < target_cap {
            voters.insert(req.node_id);
            cluster
                .raft
                .change_membership(voters, false)
                .await
                .map_err(|e| Status::internal(format!("failed to promote voter: {}", e)))?;
            let _ = crate::audit::record_audit_event(
                &self.resources.store,
                security_context.principal.principal_name(),
                "join_cluster",
                &req.node_id.to_string(),
                serde_json::json!({ "raft_addr": req.raft_addr, "role": "voter" }),
            )
            .await;
            Ok(Response::new(JoinClusterResponse {
                success: true,
                promoted_to_voter: true,
                role: "voter".to_string(),
                message: format!("Node {} joined as voter (voter cap={})", req.node_id, target_cap),
            }))
        } else {
            let _ = crate::audit::record_audit_event(
                &self.resources.store,
                security_context.principal.principal_name(),
                "join_cluster",
                &req.node_id.to_string(),
                serde_json::json!({ "raft_addr": req.raft_addr, "role": "learner" }),
            )
            .await;
            Ok(Response::new(JoinClusterResponse {
                success: true,
                promoted_to_voter: false,
                role: "learner".to_string(),
                message: format!(
                    "Node {} joined as non-voting learner (voter cap {} reached)",
                    req.node_id, target_cap
                ),
            }))
        }
    }

    async fn configure_cluster_storage(
        &self,
        request: Request<ConfigureClusterStorageRequest>,
    ) -> Result<Response<ConfigureClusterStorageResponse>, Status> {
        let security_context = get_security_context(&request)?;
        let req = request.into_inner();
        enforce_authorization(
            &security_context,
            &Resource::Cluster,
            Permission::ManageCluster,
            &self.resources.security,
        )
        .await?;

        let mode_norm = req.mode.trim().to_ascii_lowercase();
        let (active_mode, message) = match mode_norm.as_str() {
            "local" => {
                self.broker_service
                    .storage_factory()
                    .configure_local_storage(req.force)
                    .map_err(|e| Status::failed_precondition(format!("local storage config failed: {}", e)))?;
                let stored_doc = serde_json::json!({ "mode": "local" });
                self.resources
                    .cluster
                    .create("/cluster/config/storage", stored_doc)
                    .await
                    .map_err(|e| {
                        Status::internal(format!("failed to persist /cluster/config/storage: {}", e))
                    })?;
                ("local".to_string(), "Configured cluster storage mode: local".to_string())
            }
            "shared_fs" | "shared-fs" => {
                let spec = DesiredSharedFsSpec {
                    root: req.root.clone(),
                    force: req.force,
                };
                self.apply_shared_fs_spec(&spec).await?;
                (
                    "shared_fs".to_string(),
                    format!("Configured shared_fs storage at {}", req.root),
                )
            }
            "object_store" | "object-store" => {
                let spec = DesiredObjectStoreSpec {
                    backend: req.backend.clone(),
                    root: req.root.clone(),
                    options: req.options.clone(),
                    force: req.force,
                };
                self.apply_object_store_spec(&spec).await?;
                (
                    "object_store".to_string(),
                    format!("Configured {} object store at {}", req.backend, req.root),
                )
            }
            other => {
                return Err(Status::invalid_argument(format!(
                    "unsupported storage mode '{}'; expected 'local', 'shared_fs', or 'object_store'",
                    other
                )));
            }
        };

        let _ = crate::audit::record_audit_event(
            &self.resources.store,
            security_context.principal.principal_name(),
            "configure_cluster_storage",
            &active_mode,
            serde_json::json!({ "mode": active_mode, "backend": req.backend, "root": req.root, "force": req.force }),
        )
        .await;

        Ok(Response::new(ConfigureClusterStorageResponse {
            success: true,
            message,
            active_mode,
        }))
    }

    async fn configure_write_buffer(
        &self,
        request: Request<ConfigureWriteBufferRequest>,
    ) -> Result<Response<ConfigureWriteBufferResponse>, Status> {
        let security_context = get_security_context(&request)?;
        let req = request.into_inner();
        enforce_authorization(
            &security_context,
            &Resource::Cluster,
            Permission::ManageCluster,
            &self.resources.security,
        )
        .await?;

        let mut spec = if req.enabled {
            let mut base = if let Ok(Some(val)) = self
                .resources
                .store
                .get("/cluster/config/write_buffer", MetaOptions::None)
                .await
            {
                serde_json::from_value::<ClusterWriteBufferSpec>(val).unwrap_or_default()
            } else {
                ClusterWriteBufferSpec::default()
            };
            base.enabled = true;
            if !req.endpoints.is_empty() {
                base.endpoints = req.endpoints.clone();
            } else if base.endpoints.is_empty() {
                if let Some(cur) = self
                    .broker_service
                    .storage_factory()
                    .current_write_buffer_config()
                {
                    base.endpoints = cur.endpoints;
                }
            }
            if let Some(ref profile) = req.profile {
                base.apply_profile(profile)
                    .map_err(Status::invalid_argument)?;
            }
            if let Some(wait_replicas) = req.wait_replicas {
                base.wait_replicas = Some(wait_replicas);
            }
            if let Some(wait_timeout_ms) = req.wait_timeout_ms {
                base.wait_timeout_ms = Some(wait_timeout_ms);
            }
            if let Some(on_wait_timeout) = req.on_wait_timeout {
                base.on_wait_timeout = Some(on_wait_timeout);
            }
            if let Some(max_cached) = req.max_cached_closed_segments {
                base.max_cached_closed_segments = Some(max_cached);
            }
            base
        } else {
            ClusterWriteBufferSpec {
                enabled: false,
                ..Default::default()
            }
        };

        self.apply_write_buffer_spec(&spec).await?;

        let effective_profile = spec
            .profile
            .take()
            .unwrap_or_else(|| if spec.enabled { "custom".to_string() } else { "disabled".to_string() });

        let _ = crate::audit::record_audit_event(
            &self.resources.store,
            security_context.principal.principal_name(),
            "configure_write_buffer",
            "cluster",
            serde_json::json!({ "enabled": req.enabled, "profile": effective_profile, "endpoints": spec.endpoints }),
        )
        .await;

        Ok(Response::new(ConfigureWriteBufferResponse {
            success: true,
            message: if req.enabled {
                format!("Write buffer enabled (profile={})", effective_profile)
            } else {
                "Write buffer disabled".to_string()
            },
            enabled: req.enabled,
            effective_profile,
        }))
    }

    async fn configure_wal_retention(
        &self,
        request: Request<ConfigureWalRetentionRequest>,
    ) -> Result<Response<ConfigureWalRetentionResponse>, Status> {
        let security_context = get_security_context(&request)?;
        let req = request.into_inner();
        enforce_authorization(
            &security_context,
            &Resource::Cluster,
            Permission::ManageCluster,
            &self.resources.security,
        )
        .await?;

        let factory = self.broker_service.storage_factory();
        let mut spec = if let Ok(Some(val)) = self
            .resources
            .store
            .get("/cluster/config/wal", MetaOptions::None)
            .await
        {
            serde_json::from_value::<ClusterWalRetentionSpec>(val).unwrap_or_default()
        } else {
            let wal = factory.current_wal_config();
            let del = factory.current_deleter_config();
            ClusterWalRetentionSpec {
                retention_time_minutes: del.as_ref().and_then(|d| d.retention_time_minutes),
                retention_size_mb: del.as_ref().and_then(|d| d.retention_size_mb),
                check_interval_minutes: del.as_ref().map(|d| d.check_interval_minutes),
                rotate_max_bytes: wal.rotate_max_bytes,
                rotate_max_hours: wal.rotate_max_seconds.map(|s| s / 3600),
                cache_capacity: wal.cache_capacity,
                flush_interval_ms: wal.flush_interval_ms,
                flush_max_batch_bytes: wal.flush_max_batch_bytes,
            }
        };

        if req.clear_retention {
            spec.retention_time_minutes = None;
            spec.retention_size_mb = None;
            spec.check_interval_minutes = None;
        } else {
            if req.retention_time_minutes.is_some() {
                spec.retention_time_minutes = req.retention_time_minutes;
            }
            if req.retention_size_mb.is_some() {
                spec.retention_size_mb = req.retention_size_mb;
            }
            if req.check_interval_minutes.is_some() {
                spec.check_interval_minutes = req.check_interval_minutes;
            }
        }
        if req.rotate_max_bytes.is_some() {
            spec.rotate_max_bytes = req.rotate_max_bytes;
        }
        if req.rotate_max_hours.is_some() {
            spec.rotate_max_hours = req.rotate_max_hours;
        }
        if let Some(cap) = req.cache_capacity {
            spec.cache_capacity = Some(cap as usize);
        }
        if req.flush_interval_ms.is_some() {
            spec.flush_interval_ms = req.flush_interval_ms;
        }
        if let Some(batch) = req.flush_max_batch_bytes {
            spec.flush_max_batch_bytes = Some(batch as usize);
        }

        let stored_doc = serde_json::to_value(&spec).map_err(|e| {
            Status::internal(format!("failed to serialize WAL config: {}", e))
        })?;
        self.resources
            .cluster
            .create("/cluster/config/wal", stored_doc.clone())
            .await
            .map_err(|e| Status::internal(format!("failed to persist /cluster/config/wal: {}", e)))?;

        spec.apply_to_factory(&factory);

        let _ = crate::audit::record_audit_event(
            &self.resources.store,
            security_context.principal.principal_name(),
            "configure_wal_retention",
            "cluster",
            stored_doc,
        )
        .await;

        Ok(Response::new(ConfigureWalRetentionResponse {
            success: true,
            message: "Updated cluster WAL and retention configuration".to_string(),
        }))
    }

    async fn configure_load_manager(
        &self,
        request: Request<ConfigureLoadManagerRequest>,
    ) -> Result<Response<ConfigureLoadManagerResponse>, Status> {
        let security_context = get_security_context(&request)?;
        let req = request.into_inner();
        enforce_authorization(
            &security_context,
            &Resource::Cluster,
            Permission::ManageCluster,
            &self.resources.security,
        )
        .await?;

        let cluster = self.cluster.as_ref().ok_or_else(|| {
            Status::unimplemented("load manager configuration is only available in cluster mode")
        })?;

        let mut cfg = cluster.load_manager.current_config();

        if let Some(ref strategy_str) = req.assignment_strategy {
            cfg.assignment_strategy =
                AssignmentStrategy::from_str_name(strategy_str).map_err(Status::invalid_argument)?;
        }
        if let Some(interval) = req.load_report_interval_seconds {
            if interval == 0 {
                return Err(Status::invalid_argument(
                    "load_report_interval_seconds must be greater than 0",
                ));
            }
            cfg.load_report_interval_seconds = interval;
        }
        if let Some(enabled) = req.rebalancing_enabled {
            cfg.rebalancing.enabled = enabled;
        }
        if let Some(ref profile_str) = req.rebalancing_profile {
            let agg = RebalancingAggressiveness::from_str_name(profile_str)
                .map_err(Status::invalid_argument)?;
            cfg.rebalancing.apply_profile(agg);
        }
        if let Some(interval) = req.check_interval_seconds {
            if interval == 0 {
                return Err(Status::invalid_argument(
                    "check_interval_seconds must be greater than 0",
                ));
            }
            cfg.rebalancing.check_interval_seconds = interval;
        }
        if let Some(max_moves) = req.max_moves_per_hour {
            cfg.rebalancing.max_moves_per_hour = max_moves as usize;
        }
        if let Some(cooldown) = req.cooldown_seconds {
            cfg.rebalancing.cooldown_seconds = cooldown;
        }
        if let Some(min_brokers) = req.min_brokers_for_rebalance {
            cfg.rebalancing.min_brokers_for_rebalance = min_brokers as usize;
        }
        if let Some(min_age) = req.min_topic_age_seconds {
            cfg.rebalancing.min_topic_age_seconds = min_age;
        }
        if req.replace_blacklist || !req.blacklist_topics.is_empty() {
            cfg.rebalancing.blacklist_topics = req.blacklist_topics.clone();
        }

        self.apply_load_manager_config(&cfg).await?;

        let _ = crate::audit::record_audit_event(
            &self.resources.store,
            security_context.principal.principal_name(),
            "configure_load_manager",
            "cluster",
            serde_json::to_value(&cfg).unwrap_or_default(),
        )
        .await;

        Ok(Response::new(ConfigureLoadManagerResponse {
            success: true,
            message: format!(
                "Updated LoadManager (strategy={:?}, rebalancing={}, profile={:?})",
                cfg.assignment_strategy, cfg.rebalancing.enabled, cfg.rebalancing.aggressiveness
            ),
        }))
    }

    async fn export_cluster_config(
        &self,
        request: Request<ExportClusterConfigRequest>,
    ) -> Result<Response<ExportClusterConfigResponse>, Status> {
        let security_context = get_security_context(&request)?;
        let req = request.into_inner();
        enforce_authorization(
            &security_context,
            &Resource::Cluster,
            Permission::ManageCluster,
            &self.resources.security,
        )
        .await?;

        let factory = self.broker_service.storage_factory();
        let mut recovery = RecoveryConfig {
            super_admins: if self.auth.super_admins.is_empty() {
                None
            } else {
                Some(self.auth.super_admins.clone())
            },
            auto_create_topics: Some(self.broker_service.auto_create_topics),
            storage: Some(build_export_storage_config(&factory)),
            load_manager: self
                .cluster
                .as_ref()
                .map(|c| c.load_manager.current_config()),
            auth: Some(self.auth.clone()),
            ..Default::default()
        };

        if let Some(broker_id) = req.broker_id.filter(|&id| id > 0) {
            if let Some(info) = self
                .resources
                .cluster
                .get_broker_register_info(&broker_id.to_string())
                .await
            {
                if let Some(broker_url) = info.get("broker_url").and_then(|v| v.as_str()) {
                    let stripped = strip_uri_scheme(broker_url);
                    if !stripped.is_empty() {
                        recovery.broker_addr = Some(stripped);
                    }
                }
                if let Some(admin_addr) = info.get("admin_addr").and_then(|v| v.as_str()) {
                    let stripped = strip_uri_scheme(admin_addr);
                    if !stripped.is_empty() {
                        recovery.admin_addr = Some(stripped);
                    }
                }
            }
        }

        let format_norm = req.format.trim().to_ascii_lowercase();
        let (fmt, content) = match format_norm.as_str() {
            "json" => (
                "json".to_string(),
                serde_json::to_string_pretty(&recovery).map_err(|e| {
                    Status::internal(format!("failed to serialize recovery config to JSON: {}", e))
                })?,
            ),
            "" | "yaml" | "yml" => (
                "yaml".to_string(),
                serde_yaml::to_string(&recovery).map_err(|e| {
                    Status::internal(format!("failed to serialize recovery config to YAML: {}", e))
                })?,
            ),
            other => {
                return Err(Status::invalid_argument(format!(
                    "unsupported export format '{}'; expected 'yaml' or 'json'",
                    other
                )));
            }
        };

        Ok(Response::new(ExportClusterConfigResponse {
            success: true,
            format: fmt,
            content,
        }))
    }
}

impl DanubeAdminImpl {
    async fn target_voter_count(&self, explicit: u32) -> usize {
        if explicit > 0 {
            return explicit as usize;
        }
        if let Ok(Some(val)) = danube_core::metadata::MetadataStore::get(
            &self.resources.store,
            "/cluster/config/target_voter_count",
            danube_core::metadata::MetaOptions::None,
        )
        .await
        {
            if let Some(n) = val.as_u64() {
                if n > 0 {
                    return n as usize;
                }
            }
        }
        5
    }
    async fn apply_shared_fs_spec(&self, spec: &DesiredSharedFsSpec) -> Result<(), Status> {
        let cfg = SharedFsConfig {
            root: spec.root.clone(),
        };
        self.broker_service
            .storage_factory()
            .configure_shared_fs(cfg, spec.force)
            .map_err(|e| Status::failed_precondition(format!("shared_fs config failed: {}", e)))?;

        let stored_doc = serde_json::json!({
            "mode": "shared_fs",
            "root": spec.root,
        });
        self.resources
            .cluster
            .create("/cluster/config/storage", stored_doc)
            .await
            .map_err(|e| {
                Status::internal(format!("failed to persist /cluster/config/storage: {}", e))
            })?;
        Ok(())
    }

    async fn apply_write_buffer_spec(&self, spec: &ClusterWriteBufferSpec) -> Result<(), Status> {
        let mut effective = spec.clone();
        if effective.enabled {
            if let Some(ref profile) = effective.profile.clone() {
                // Only apply profile defaults for unspecified fields if coming from declarative manifest
                let mut profiled = ClusterWriteBufferSpec::default();
                profiled
                    .apply_profile(profile)
                    .map_err(Status::invalid_argument)?;
                if effective.wait_replicas.is_none() {
                    effective.wait_replicas = profiled.wait_replicas;
                }
                if effective.wait_timeout_ms.is_none() {
                    effective.wait_timeout_ms = profiled.wait_timeout_ms;
                }
                if effective.on_wait_timeout.is_none() {
                    effective.on_wait_timeout = profiled.on_wait_timeout;
                }
                if effective.max_cached_closed_segments.is_none() {
                    effective.max_cached_closed_segments = profiled.max_cached_closed_segments;
                }
            }
        }

        let wb_cfg = effective
            .to_write_buffer_config()
            .map_err(Status::invalid_argument)?;
        self.broker_service
            .storage_factory()
            .configure_write_buffer(wb_cfg)
            .await
            .map_err(|e| Status::failed_precondition(format!("write_buffer config failed: {}", e)))?;

        let stored_doc = serde_json::to_value(&effective).map_err(|e| {
            Status::internal(format!("failed to serialize write_buffer config: {}", e))
        })?;
        self.resources
            .cluster
            .create("/cluster/config/write_buffer", stored_doc)
            .await
            .map_err(|e| {
                Status::internal(format!(
                    "failed to persist /cluster/config/write_buffer: {}",
                    e
                ))
            })?;
        Ok(())
    }

    async fn apply_load_manager_config(&self, cfg: &LoadManagerConfig) -> Result<(), Status> {
        if let Some(ref cluster) = self.cluster {
            cluster.load_manager.update_config(cfg.clone());
        }
        let stored_doc = serde_json::to_value(cfg).map_err(|e| {
            Status::internal(format!("failed to serialize load_manager config: {}", e))
        })?;
        self.resources
            .cluster
            .create("/cluster/config/load_manager", stored_doc)
            .await
            .map_err(|e| {
                Status::internal(format!(
                    "failed to persist /cluster/config/load_manager: {}",
                    e
                ))
            })?;
        Ok(())
    }

    async fn apply_object_store_spec(&self, spec: &DesiredObjectStoreSpec) -> Result<(), Status> {
        let cfg = build_object_store_config(&spec.backend, spec.root.clone(), spec.options.clone())?;
        self.broker_service
            .storage_factory()
            .configure_object_store(cfg, spec.force)
            .await
            .map_err(|e| Status::failed_precondition(format!("object store config failed: {}", e)))?;

        let stored_doc = serde_json::json!({
            "mode": "object_store",
            "backend": spec.backend,
            "root": spec.root,
            "options": spec.options,
        });
        self.resources
            .cluster
            .create("/cluster/config/storage", stored_doc)
            .await
            .map_err(|e| {
                Status::internal(format!("failed to persist /cluster/config/storage: {}", e))
            })?;
        Ok(())
    }

    async fn build_desired_state_plan(
        &self,
        manifest: &DesiredStateManifest,
        prune: bool,
    ) -> Vec<PlannedAction> {
        let existing_namespaces = self.resources.cluster.get_namespaces().await;
        let mut existing_topics = Vec::new();
        for ns in &existing_namespaces {
            let topics = self.resources.namespace.get_topics_for_namespace(ns).await;
            existing_topics.extend(topics);
        }
        compute_plan(manifest, &existing_namespaces, &existing_topics, prune)
    }

    async fn apply_create_topic_spec(
        &self,
        spec: &crate::admin::desired_state::DesiredTopicSpec,
    ) -> Result<(), Status> {
        let requested_strategy = match spec
            .dispatch_strategy
            .as_deref()
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            Some("reliable") => CoreDispatchStrategy::Reliable,
            _ => CoreDispatchStrategy::NonReliable,
        };
        let (strategy, expanded_intent) =
            resolve_intent_and_strategy(spec.intent.as_deref(), requested_strategy)?;
        let schema_ref = spec.schema_subject.as_ref().map(|subject| SchemaReference {
            subject: subject.clone(),
            version_ref: Some(VersionRef::UseLatest(true)),
        });

        let partitions = spec.partitions.unwrap_or(0);
        if partitions > 0 {
            for part in 0..partitions {
                let concrete = format!("{}-part-{}", spec.name, part);
                self.broker_service
                    .create_topic(&concrete, Some(strategy), schema_ref.clone())
                    .await
                    .map_err(|e| {
                        Status::internal(format!("failed to create topic '{}': {}", concrete, e))
                    })?;
                if let Some(ref settings) = expanded_intent {
                    self.persist_expanded_intent(&concrete, settings).await?;
                }
            }
        } else {
            self.broker_service
                .create_topic(&spec.name, Some(strategy), schema_ref)
                .await
                .map_err(|e| {
                    Status::internal(format!("failed to create topic '{}': {}", spec.name, e))
                })?;
            if let Some(ref settings) = expanded_intent {
                self.persist_expanded_intent(&spec.name, settings).await?;
            }
        }
        Ok(())
    }
}

fn strip_uri_scheme(raw: &str) -> String {
    raw.trim()
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .trim_end_matches('/')
        .to_string()
}

fn build_export_storage_config(factory: &StorageFactory) -> StorageConfig {
    let wal_cfg = factory.current_wal_config();
    let deleter_cfg = factory.current_deleter_config();
    let wb_cfg = factory.current_write_buffer_config();

    let wal_node = WalNode {
        dir: wal_cfg
            .dir
            .as_ref()
            .map(|p| p.to_string_lossy().into_owned()),
        file_name: wal_cfg.file_name,
        rotation: if wal_cfg.rotate_max_bytes.is_some() || wal_cfg.rotate_max_seconds.is_some() {
            Some(WalRotationNode {
                max_bytes: wal_cfg.rotate_max_bytes,
                max_hours: wal_cfg.rotate_max_seconds.map(|s| s / 3600),
            })
        } else {
            None
        },
        advanced: None,
        cache_capacity: wal_cfg.cache_capacity,
        file_sync: if wal_cfg.flush_interval_ms.is_some()
            || wal_cfg.flush_max_batch_bytes.is_some()
        {
            Some(WalFlushNode {
                interval_ms: wal_cfg.flush_interval_ms,
                max_batch_bytes: wal_cfg.flush_max_batch_bytes,
            })
        } else {
            None
        },
    };

    let local_retention = deleter_cfg.map(|d| LocalRetentionNode {
        time_minutes: d.retention_time_minutes,
        size_mb: d.retention_size_mb,
        check_interval_minutes: Some(d.check_interval_minutes),
    });

    let write_buffer = wb_cfg.map(|wb| WriteBufferNode {
        backend: "valkey".to_string(),
        endpoints: wb.endpoints,
        wait_replicas: Some(wb.wait_replicas),
        wait_timeout_ms: Some(wb.wait_timeout_ms),
        on_wait_timeout: Some(match wb.on_wait_timeout {
            WaitTimeoutPolicy::Ack => "ack".to_string(),
            WaitTimeoutPolicy::Fail => "fail".to_string(),
        }),
        max_cached_closed_segments: Some(wb.max_cached_closed_segments),
    });

    let metadata_prefix = Some("/danube".to_string());
    let opt_wal_root = wal_cfg
        .dir
        .as_ref()
        .map(|p| p.to_string_lossy().into_owned());

    match factory.current_mode() {
        StorageMode::Local => StorageConfig::Local {
            local_wal_root: opt_wal_root.unwrap_or_else(|| "./danube-data/wal".to_string()),
            metadata_prefix,
            local_retention,
            wal: wal_node,
            write_buffer,
        },
        StorageMode::SharedFs(sfs) => StorageConfig::SharedFs {
            local_wal_root: opt_wal_root,
            metadata_prefix,
            durable: Some(SharedFsDurableNode { root: sfs.root }),
            local_retention,
            wal: wal_node,
            write_buffer,
        },
        StorageMode::ObjectStore(ObjectStoreConfig::ObjectStore {
            backend,
            root,
            options,
        }) => {
            let durable = match backend {
                ObjectStoreBackend::S3 => ObjectStoreNode::S3 {
                    root,
                    region: options.get("region").cloned(),
                    endpoint: options.get("endpoint").cloned(),
                    access_key: options.get("access_key").cloned(),
                    secret_key: options.get("secret_key").cloned(),
                    profile: options.get("profile").cloned(),
                    role_arn: options.get("role_arn").cloned(),
                    session_token: options.get("session_token").cloned(),
                    anonymous: options.get("anonymous").and_then(|v| v.parse().ok()),
                    virtual_host_style: options
                        .get("virtual_host_style")
                        .and_then(|v| v.parse().ok()),
                },
                ObjectStoreBackend::Gcs => ObjectStoreNode::Gcs {
                    root,
                    project: options.get("project").cloned(),
                    credentials_json: options.get("credentials_json").cloned(),
                    credentials_path: options.get("credentials_path").cloned(),
                },
                ObjectStoreBackend::Azblob => ObjectStoreNode::Azblob {
                    root,
                    endpoint: options.get("endpoint").cloned(),
                    account_name: options.get("account_name").cloned(),
                    account_key: options.get("account_key").cloned(),
                },
            };
            StorageConfig::ObjectStore {
                local_wal_root: opt_wal_root,
                metadata_prefix,
                durable: Some(durable),
                local_retention,
                wal: wal_node,
                write_buffer,
            }
        }
        StorageMode::ObjectStore(ObjectStoreConfig::TestFilesystem { root }) => {
            StorageConfig::SharedFs {
                local_wal_root: opt_wal_root,
                metadata_prefix,
                durable: Some(SharedFsDurableNode { root }),
                local_retention,
                wal: wal_node,
                write_buffer,
            }
        }
    }
}
