use crate::admin::desired_state::{
    build_object_store_config, compute_plan, DesiredObjectStoreSpec, DesiredStateManifest,
};
use crate::admin::topics_admin::resolve_intent_and_strategy;
use crate::admin::DanubeAdminImpl;
use crate::security::authz::{enforce_authorization, Permission, Resource};
use crate::security::authn::get_security_context;
use danube_core::admin_proto::{
    cluster_admin_server::ClusterAdmin, AddNodeRequest, AddNodeResponse, ClusterStatusResponse,
    ConfigureObjectStoreRequest, ConfigureObjectStoreResponse, DesiredStateApplyResponse,
    DesiredStatePlanResponse, DesiredStateRequest, Empty, JoinClusterRequest, JoinClusterResponse,
    ObjectStoreConfigResponse, PlannedAction, PromoteNodeRequest, PromoteNodeResponse,
    RemoveNodeRequest, RemoveNodeResponse,
};
use danube_core::proto::{
    schema_reference::VersionRef, DispatchStrategy as CoreDispatchStrategy, SchemaReference,
};
use danube_persistent_storage::{ObjectStoreBackend, ObjectStoreConfig};
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

        if let Some(ref obj) = manifest.object_store {
            self.apply_object_store_spec(obj).await?;
            applied.push(PlannedAction {
                kind: "update_config".to_string(),
                resource: "cluster/object_store".to_string(),
                details: format!("backend={} root={}", obj.backend, obj.root),
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
            Ok(Response::new(JoinClusterResponse {
                success: true,
                promoted_to_voter: true,
                role: "voter".to_string(),
                message: format!("Node {} joined as voter (voter cap={})", req.node_id, target_cap),
            }))
        } else {
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
    async fn apply_object_store_spec(&self, spec: &DesiredObjectStoreSpec) -> Result<(), Status> {
        let cfg = build_object_store_config(&spec.backend, spec.root.clone(), spec.options.clone())?;
        self.broker_service
            .storage_factory()
            .configure_object_store(cfg, spec.force)
            .await
            .map_err(|e| Status::failed_precondition(format!("object store config failed: {}", e)))?;

        let stored_doc = serde_json::json!({
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
                    let val = serde_json::to_value(settings).map_err(|e| {
                        Status::internal(format!("failed to serialize intent: {}", e))
                    })?;
                    self.resources
                        .topic
                        .set_topic_intent(&concrete, val)
                        .await
                        .map_err(|e| Status::internal(e.to_string()))?;
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
                let val = serde_json::to_value(settings)
                    .map_err(|e| Status::internal(format!("failed to serialize intent: {}", e)))?;
                self.resources
                    .topic
                    .set_topic_intent(&spec.name, val)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;
            }
        }
        Ok(())
    }
}
