use crate::admin::DanubeAdminImpl;
use crate::stream_intent::{ExpandedStreamSettings, StreamIntent};
use crate::subscription::{
    SubscriptionBackoffStrategy as BrokerSubscriptionBackoffStrategy,
    SubscriptionFailurePolicy as BrokerSubscriptionFailurePolicy,
    SubscriptionPoisonPolicy as BrokerSubscriptionPoisonPolicy,
};
use crate::topic::TopicStore;
use danube_core::admin_proto::{
    topic_admin_server::TopicAdmin, BranchTopicRequest, BranchTopicResponse, BrokerRequest,
    DescribeTopicRequest, DescribeTopicResponse, ExplainConsumerStallRequest,
    ExplainConsumerStallResponse, GetSubscriptionDispatchConfigRequest,
    GetSubscriptionDispatchConfigResponse, GetSubscriptionFailurePolicyRequest,
    GetSubscriptionFailurePolicyResponse, GetTopicTimeRangeResponse, InferTopicSchemaRequest,
    InferTopicSchemaResponse, NamespaceRequest, NewTopicRequest, PartitionedTopicRequest,
    PeekMessagesRequest, PeekMessagesResponse, PeekedMessage, ReplayDlqRequest,
    ReplayDlqResponse, SampleTopicJsonRequest, SampleTopicJsonResponse, SeekSubscriptionRequest,
    SeekSubscriptionResponse, SetSubscriptionDispatchConfigRequest,
    SetSubscriptionFailurePolicyRequest,
    SubscriptionBackoffStrategy as AdminSubscriptionBackoffStrategy,
    SubscriptionDispatchConfig as AdminSubscriptionDispatchConfig,
    SubscriptionFailurePolicy as AdminSubscriptionFailurePolicy, SubscriptionListResponse,
    SubscriptionPoisonPolicy as AdminSubscriptionPoisonPolicy, SubscriptionRequest,
    SubscriptionResponse, TopicInfo, TopicInfoListResponse, TopicRequest, TopicResponse,
    UpdateTopicStorageModeRequest,
};
use danube_core::dispatch_strategy::ConfigDispatchStrategy;
use danube_core::message::StreamMessage;
use danube_core::proto::{
    schema_reference::VersionRef, DispatchStrategy as CoreDispatchStrategy, SchemaReference,
};
use danube_core::storage::StartPosition;
use futures::StreamExt;
use std::collections::{BTreeMap, BTreeSet};

use crate::security::authz::{enforce_authorization, Permission, Resource};
use crate::security::authn::get_security_context;
use tonic::{Request, Response, Status};
use tracing::{trace, Level};

#[tonic::async_trait]
impl TopicAdmin for DanubeAdminImpl {
    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn list_namespace_topics(
        &self,
        request: Request<NamespaceRequest>,
    ) -> std::result::Result<Response<TopicInfoListResponse>, tonic::Status> {
        let security_context = get_security_context(&request)?;
        let req = request.into_inner();

        enforce_authorization(
            &security_context,
            &Resource::Namespace(req.name.clone()),
            Permission::ManageTopic,
            &self.resources.security,
        ).await?;

        trace!("list topics with broker_id for a namespace");

        let names = self
            .resources
            .namespace
            .get_topics_for_namespace(&req.name)
            .await;

        let mut topics: Vec<TopicInfo> = Vec::with_capacity(names.len());
        for name in names.into_iter() {
            let normalized = if name.starts_with('/') {
                name.clone()
            } else {
                format!("/{}", name)
            };
            let broker_id = self
                .resources
                .cluster
                .get_broker_for_topic(&normalized)
                .await
                .unwrap_or_default();
            let lookup = normalized.trim_start_matches('/');
            let delivery = match self.resources.topic.get_dispatch_strategy(lookup).await {
                Some(ConfigDispatchStrategy::Reliable) => "Reliable".to_string(),
                Some(ConfigDispatchStrategy::NonReliable) => "NonReliable".to_string(),
                None => "NonReliable".to_string(),
            };
            topics.push(TopicInfo {
                name: normalized,
                broker_id,
                delivery,
            });
        }

        Ok(Response::new(TopicInfoListResponse { topics }))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn list_broker_topics(
        &self,
        request: Request<BrokerRequest>,
    ) -> std::result::Result<Response<TopicInfoListResponse>, tonic::Status> {
        let security_context = get_security_context(&request)?;
        let req = request.into_inner();

        enforce_authorization(
            &security_context,
            &Resource::Broker(req.broker_id.clone()),
            Permission::ManageTopic,
            &self.resources.security,
        ).await?;

        trace!("list topics hosted by a broker");

        let names = self
            .resources
            .cluster
            .get_topics_for_broker(&req.broker_id)
            .await;

        let mut topics: Vec<TopicInfo> = Vec::with_capacity(names.len());
        for name in names.into_iter() {
            let lookup = name.trim_start_matches('/');
            let delivery = match self.resources.topic.get_dispatch_strategy(lookup).await {
                Some(ConfigDispatchStrategy::Reliable) => "Reliable".to_string(),
                Some(ConfigDispatchStrategy::NonReliable) => "NonReliable".to_string(),
                None => "NonReliable".to_string(),
            };
            topics.push(TopicInfo {
                name,
                broker_id: req.broker_id.clone(),
                delivery,
            });
        }

        Ok(Response::new(TopicInfoListResponse { topics }))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn create_topic(
        &self,
        request: Request<NewTopicRequest>,
    ) -> std::result::Result<Response<TopicResponse>, tonic::Status> {
        let security_context = get_security_context(&request)?;
        let req = request.into_inner();

        enforce_authorization(
            &security_context,
            &Resource::Topic(req.name.clone()),
            Permission::ManageTopic,
            &self.resources.security,
        ).await?;

        trace!(topic = %req.name, "creates a non-partitioned topic");

        // Convert schema_subject to SchemaReference if provided
        let schema_ref = req.schema_subject.map(|subject| SchemaReference {
            subject,
            version_ref: Some(VersionRef::UseLatest(true)),
        });

        // Map admin NewTopicRequest.dispatch_strategy (enum i32) into core proto DispatchStrategy
        let requested_strategy =
            <CoreDispatchStrategy as core::convert::TryFrom<i32>>::try_from(req.dispatch_strategy)
                .map_err(|_| Status::invalid_argument("invalid dispatch_strategy value"))?;
        let (dispatch_strategy, expanded_intent) =
            resolve_intent_and_strategy(req.intent.as_deref(), requested_strategy)?;

        let service = self.broker_service.as_ref();

        // Unified create_topic dispatches based on BrokerMode
        match service
            .create_topic(&req.name, Some(dispatch_strategy), schema_ref)
            .await
        {
            Ok(_) => {
                if let Some(ref settings) = expanded_intent {
                    self.persist_expanded_intent(&req.name, settings).await?;
                }
                let _ = crate::audit::record_audit_event(
                    &self.resources.store,
                    security_context.principal.principal_name(),
                    "create_topic",
                    &req.name,
                    serde_json::json!({ "intent": req.intent }),
                )
                .await;
                Ok(tonic::Response::new(TopicResponse { success: true }))
            }
            Err(err) => {
                let status = Status::not_found(format!(
                    "Unable to create the topic {} due to {}",
                    req.name, err
                ));
                Err(status)
            }
        }
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn create_partitioned_topic(
        &self,
        request: Request<PartitionedTopicRequest>,
    ) -> std::result::Result<Response<TopicResponse>, tonic::Status> {
        let security_context = get_security_context(&request)?;
        let req = request.into_inner();

        enforce_authorization(
            &security_context,
            &Resource::Topic(req.base_name.clone()),
            Permission::ManageTopic,
            &self.resources.security,
        ).await?;

        trace!(
            base_name = %req.base_name,
            partitions = %req.partitions,
            "creates a partitioned topic"
        );

        // Convert schema_subject to SchemaReference if provided
        let schema_ref = req.schema_subject.map(|subject| SchemaReference {
            subject,
            version_ref: Some(VersionRef::UseLatest(true)),
        });

        // Dispatch mapping: admin enum (i32) -> core proto enum
        let requested_strategy =
            <CoreDispatchStrategy as core::convert::TryFrom<i32>>::try_from(req.dispatch_strategy)
                .map_err(|_| Status::invalid_argument("invalid dispatch_strategy value"))?;
        let (dispatch_strategy, expanded_intent) =
            resolve_intent_and_strategy(req.intent.as_deref(), requested_strategy)?;

        let service = self.broker_service.as_ref();

        // Create all partitions; fail fast on error
        for partition_id in 0..req.partitions {
            let topic = format!("{}-part-{}", req.base_name, partition_id);
            // Unified create_topic dispatches based on BrokerMode
            if let Err(err) = service
                .create_topic(&topic, Some(dispatch_strategy), schema_ref.clone())
                .await
            {
                let status = Status::not_found(format!(
                    "Unable to create the topic {} due to {}",
                    topic, err
                ));
                return Err(status);
            }
            if let Some(ref settings) = expanded_intent {
                self.persist_expanded_intent(&topic, settings).await?;
            }
        }

        let _ = crate::audit::record_audit_event(
            &self.resources.store,
            security_context.principal.principal_name(),
            "create_partitioned_topic",
            &req.base_name,
            serde_json::json!({ "partitions": req.partitions, "intent": req.intent }),
        )
        .await;

        Ok(tonic::Response::new(TopicResponse { success: true }))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn delete_topic(
        &self,
        request: Request<TopicRequest>,
    ) -> std::result::Result<Response<TopicResponse>, tonic::Status> {
        let security_context = get_security_context(&request)?;
        let req = request.into_inner();

        enforce_authorization(
            &security_context,
            &Resource::Topic(req.name.clone()),
            Permission::ManageTopic,
            &self.resources.security,
        ).await?;

        trace!(topic = %req.name, "delete the topic");

        let _ = crate::audit::snapshot_topic_cursors(
            &self.resources.topic,
            &self.resources.store,
            &req.name,
            "before_delete_topic",
        )
        .await;

        let service = self.broker_service.as_ref();

        // Unified delete_topic dispatches based on BrokerMode
        let success = match service.delete_topic(&req.name).await {
            Ok(()) => {
                let _ = crate::audit::record_audit_event(
                    &self.resources.store,
                    security_context.principal.principal_name(),
                    "delete_topic",
                    &req.name,
                    serde_json::json!({}),
                )
                .await;
                true
            }
            Err(err) => {
                let status = Status::not_found(format!(
                    "Unable to delete the topic {} due to {}",
                    req.name, err
                ));
                return Err(status);
            }
        };

        let response = TopicResponse { success };
        Ok(tonic::Response::new(response))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn list_subscriptions(
        &self,
        request: Request<TopicRequest>,
    ) -> std::result::Result<Response<SubscriptionListResponse>, tonic::Status> {
        let security_context = get_security_context(&request)?;
        let req = request.into_inner();

        enforce_authorization(
            &security_context,
            &Resource::Topic(req.name.clone()),
            Permission::ManageTopic,
            &self.resources.security,
        ).await?;

        trace!(
            topic = %req.name,
            "get the list of subscriptions on the topic"
        );

        let subscriptions = self
            .resources
            .topic
            .get_subscription_for_topic(&req.name)
            .await;

        let response = SubscriptionListResponse { subscriptions };
        Ok(tonic::Response::new(response))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn unsubscribe(
        &self,
        request: Request<SubscriptionRequest>,
    ) -> std::result::Result<Response<SubscriptionResponse>, tonic::Status> {
        let security_context = get_security_context(&request)?;
        let req = request.into_inner();

        enforce_authorization(
            &security_context,
            &Resource::Topic(req.topic.clone()),
            Permission::ManageTopic,
            &self.resources.security,
        ).await?;

        trace!(
            subscription = %req.subscription,
            topic = %req.topic,
            "unsubscribe subscription from topic"
        );

        let service = self.broker_service.as_ref();

        let success = match service.unsubscribe(&req.subscription, &req.topic).await {
            Ok(()) => {
                let _ = crate::audit::record_audit_event(
                    &self.resources.store,
                    security_context.principal.principal_name(),
                    "unsubscribe",
                    &req.topic,
                    serde_json::json!({ "subscription": req.subscription }),
                )
                .await;
                true
            }
            Err(err) => {
                let status = Status::not_found(format!(
                    "Unable to unsubscribe the subscription {} due to error: {}",
                    req.subscription, err
                ));
                return Err(status);
            }
        };

        let response = SubscriptionResponse { success };
        Ok(tonic::Response::new(response))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn set_subscription_failure_policy(
        &self,
        request: Request<SetSubscriptionFailurePolicyRequest>,
    ) -> std::result::Result<Response<SubscriptionResponse>, tonic::Status> {
        let security_context = get_security_context(&request)?;
        let req = request.into_inner();

        enforce_authorization(
            &security_context,
            &Resource::Topic(req.topic.clone()),
            Permission::ManageTopic,
            &self.resources.security,
        ).await?;

        trace!(
            topic = %req.topic,
            subscription = %req.subscription,
            "set subscription failure policy"
        );

        let failure_policy = req
            .failure_policy
            .ok_or_else(|| Status::invalid_argument("failure_policy must be provided"))?;

        let lookup = req.topic.trim_start_matches('/');
        let mut dispatch_strategy = self.resources.topic.get_dispatch_strategy(lookup).await;

        if dispatch_strategy.is_none() {
            if let Some(topic) = self
                .broker_service
                .topic_registry
                .get_topic(&req.topic)
                .or_else(|| self.broker_service.topic_registry.get_topic(lookup))
            {
                dispatch_strategy = match topic.dispatch_strategy {
                    crate::dispatcher::DispatchStrategy::Reliable => {
                        Some(ConfigDispatchStrategy::Reliable)
                    }
                    crate::dispatcher::DispatchStrategy::NonReliable => {
                        Some(ConfigDispatchStrategy::NonReliable)
                    }
                };
            }
        }

        if dispatch_strategy.is_none() {
            // Allow Raft metadata replication to catch up on followers when topic was just created
            for _ in 0..10 {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                dispatch_strategy = self.resources.topic.get_dispatch_strategy(lookup).await;
                if dispatch_strategy.is_some() {
                    break;
                }
            }
        }

        if !matches!(dispatch_strategy, Some(ConfigDispatchStrategy::Reliable)) {
            return Err(Status::failed_precondition(
                "subscription failure policy can only be configured for reliable topics",
            ));
        }

        let failure_policy = admin_failure_policy_to_broker(failure_policy)?;

        self.resources
            .topic
            .set_subscription_failure_policy(&req.subscription, &req.topic, &failure_policy)
            .await
            .map_err(|err| Status::invalid_argument(err.to_string()))?;

        let _ = crate::audit::record_audit_event(
            &self.resources.store,
            security_context.principal.principal_name(),
            "set_subscription_failure_policy",
            &req.topic,
            serde_json::json!({
                "subscription": req.subscription,
                "max_redelivery_count": failure_policy.max_redelivery_count,
                "poison_policy": format!("{:?}", failure_policy.poison_policy),
            }),
        )
        .await;

        Ok(Response::new(SubscriptionResponse { success: true }))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn get_subscription_failure_policy(
        &self,
        request: Request<GetSubscriptionFailurePolicyRequest>,
    ) -> std::result::Result<Response<GetSubscriptionFailurePolicyResponse>, tonic::Status> {
        let security_context = get_security_context(&request)?;
        let req = request.into_inner();

        enforce_authorization(
            &security_context,
            &Resource::Topic(req.topic.clone()),
            Permission::ManageTopic,
            &self.resources.security,
        ).await?;

        trace!(
            topic = %req.topic,
            subscription = %req.subscription,
            "get subscription failure policy"
        );

        let failure_policy = self
            .resources
            .topic
            .get_subscription_failure_policy(&req.subscription, &req.topic)
            .await
            .unwrap_or(None);

        let sub_options_val = self
            .resources
            .topic
            .get_subscription_options(&req.subscription, &req.topic)
            .await
            .unwrap_or(None);

        let subscription_type = if let Some(options_val) = sub_options_val {
            options_val
                .get("subscription_type")
                .and_then(|v| v.as_i64())
                .unwrap_or(0) as i32
        } else {
            0
        };

        Ok(Response::new(GetSubscriptionFailurePolicyResponse {
            failure_policy: failure_policy.map(broker_failure_policy_to_admin),
            subscription_type,
        }))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn describe_topic(
        &self,
        request: Request<DescribeTopicRequest>,
    ) -> std::result::Result<Response<DescribeTopicResponse>, tonic::Status> {
        let security_context = get_security_context(&request)?;
        let req = request.into_inner();

        enforce_authorization(
            &security_context,
            &Resource::Topic(req.name.clone()),
            Permission::ManageTopic,
            &self.resources.security,
        ).await?;

        // Subscriptions
        let subscriptions = self
            .resources
            .topic
            .get_subscription_for_topic(&req.name)
            .await;

        let service = self.broker_service.as_ref();

        // Identify serving broker id for this topic if known
        let broker_id = service
            .get_topic_broker_id(&req.name)
            .await
            .unwrap_or_default();

        // Delivery
        let lookup = req.name.trim_start_matches('/');
        let delivery = match self.resources.topic.get_dispatch_strategy(lookup).await {
            Some(ConfigDispatchStrategy::Reliable) => "Reliable".to_string(),
            Some(ConfigDispatchStrategy::NonReliable) => "NonReliable".to_string(),
            None => "NonReliable".to_string(),
        };
        let intent = self
            .resources
            .topic
            .get_topic_intent(lookup)
            .await
            .map(|settings| settings.summary_for_topic(&req.name));

        // Get schema registry information if topic has a schema
        let (schema_subject, schema_id, schema_version, schema_type, compatibility_mode) =
            self.get_topic_schema_info(&req.name).await;

        let response = DescribeTopicResponse {
            name: req.name,
            subscriptions,
            broker_id,
            delivery,
            schema_subject,
            schema_id,
            schema_version,
            schema_type,
            compatibility_mode,
            intent,
        };
        Ok(tonic::Response::new(response))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn unload_topic(
        &self,
        request: Request<TopicRequest>,
    ) -> std::result::Result<Response<TopicResponse>, tonic::Status> {
        let security_context = get_security_context(&request)?;
        let req = request.into_inner();

        enforce_authorization(
            &security_context,
            &Resource::Topic(req.name.clone()),
            Permission::ManageTopic,
            &self.resources.security,
        ).await?;

        // Unload requires cluster mode (≥2 brokers to move topic to)
        use crate::args_parse::BrokerMode;
        match self.broker_service.mode {
            BrokerMode::Standalone | BrokerMode::Edge => {
                return Err(Status::unimplemented(
                    "topic unload not available in standalone/edge mode"
                ));
            }
            BrokerMode::Cluster => {} // proceed
        }

        trace!(topic = %req.name, "unload the topic");
        let service = self.broker_service.as_ref();

        // 1) Ensure there is an alternative broker available (cluster size >= 2)
        let brokers = self.resources.cluster.get_brokers().await;
        if brokers.len() < 2 {
            return Err(Status::failed_precondition(
                "Cannot unload topic: single broker cluster detected. Unload requires at least 2 brokers.",
            ));
        }

        let _ = crate::audit::snapshot_topic_cursors(
            &self.resources.topic,
            &self.resources.store,
            &req.name,
            "before_unload_topic",
        )
        .await;

        // 2) Delegate unload to cluster: this will
        //    - create an unload marker under /cluster/unassigned with from_broker
        //    - schedule deletion of the current assignment at the hosting broker
        // The hosting broker will observe the deletion via watch and perform local unload safely.
        match service.topic_cluster.post_unload_topic(&req.name).await {
            Ok(()) => {
                let _ = crate::audit::record_audit_event(
                    &self.resources.store,
                    security_context.principal.principal_name(),
                    "unload_topic",
                    &req.name,
                    serde_json::json!({}),
                )
                .await;
                Ok(Response::new(TopicResponse { success: true }))
            }
            Err(e) => Err(Status::not_found(format!(
                "Unable to unload the topic {} due to {}",
                req.name, e
            ))),
        }
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn set_subscription_dispatch_config(
        &self,
        request: Request<SetSubscriptionDispatchConfigRequest>,
    ) -> std::result::Result<Response<SubscriptionResponse>, tonic::Status> {
        let security_context = get_security_context(&request)?;
        let req = request.into_inner();

        enforce_authorization(
            &security_context,
            &Resource::Topic(req.topic.clone()),
            Permission::ManageTopic,
            &self.resources.security,
        ).await?;

        let dispatch_config = req
            .dispatch_config
            .ok_or_else(|| Status::invalid_argument("dispatch_config is required"))?;

        let max_unacked = dispatch_config.max_unacked_messages as usize;
        if max_unacked < 1 || max_unacked > 10_000 {
            return Err(Status::invalid_argument(
                "max_unacked_messages must be between 1 and 10000",
            ));
        }

        self.resources
            .topic
            .set_subscription_dispatch_config(&req.subscription, &req.topic, max_unacked)
            .await
            .map_err(|e| {
                Status::internal(format!("Failed to set dispatch config: {}", e))
            })?;

        trace!(
            topic = %req.topic,
            subscription = %req.subscription,
            max_unacked_messages = max_unacked,
            "set subscription dispatch config"
        );

        let _ = crate::audit::record_audit_event(
            &self.resources.store,
            security_context.principal.principal_name(),
            "set_subscription_dispatch_config",
            &req.topic,
            serde_json::json!({
                "subscription": req.subscription,
                "max_unacked_messages": max_unacked,
            }),
        )
        .await;

        Ok(Response::new(SubscriptionResponse { success: true }))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn get_subscription_dispatch_config(
        &self,
        request: Request<GetSubscriptionDispatchConfigRequest>,
    ) -> std::result::Result<Response<GetSubscriptionDispatchConfigResponse>, tonic::Status> {
        let security_context = get_security_context(&request)?;
        let req = request.into_inner();

        enforce_authorization(
            &security_context,
            &Resource::Topic(req.topic.clone()),
            Permission::ManageTopic,
            &self.resources.security,
        ).await?;

        let max_unacked = self
            .resources
            .topic
            .get_subscription_dispatch_config(&req.subscription, &req.topic)
            .await
            .map_err(|e| {
                Status::internal(format!("Failed to get dispatch config: {}", e))
            })?;

        let dispatch_config = max_unacked.map(|max| {
            AdminSubscriptionDispatchConfig {
                max_unacked_messages: max as u32,
            }
        });

        Ok(Response::new(GetSubscriptionDispatchConfigResponse {
            dispatch_config,
        }))
    }

    /// Live-upgrade topic storage tiering (`local` -> `cloud_tiered`), or record `local` with force.
    async fn update_topic_storage_mode(
        &self,
        request: Request<UpdateTopicStorageModeRequest>,
    ) -> std::result::Result<Response<TopicResponse>, Status> {
        let security_context = get_security_context(&request)?;
        let req = request.into_inner();

        enforce_authorization(
            &security_context,
            &Resource::Topic(req.topic_name.clone()),
            Permission::ManageTopic,
            &self.resources.security,
        )
        .await?;

        let exists = self
            .resources
            .topic
            .topic_exists(&req.topic_name)
            .await
            .map_err(|e| Status::internal(format!("failed to check topic existence: {}", e)))?;
        if !exists {
            return Err(Status::not_found(format!(
                "Topic '{}' not found",
                req.topic_name
            )));
        }

        match req.storage_mode.trim().to_ascii_lowercase().as_str() {
            "cloud_tiered" | "cloud-tiered" | "object_store" => {
                self.broker_service
                    .storage_factory()
                    .enable_cloud_tiering_for_topic(&req.topic_name)
                    .await
                    .map_err(|e| {
                        Status::failed_precondition(format!(
                            "failed to enable cloud tiering for '{}': {}",
                            req.topic_name, e
                        ))
                    })?;
                self.resources
                    .topic
                    .set_topic_storage_mode(&req.topic_name, "cloud_tiered")
                    .await
                    .map_err(|e| Status::internal(format!("failed to persist storage mode: {}", e)))?;
                let _ = crate::audit::record_audit_event(
                    &self.resources.store,
                    security_context.principal.principal_name(),
                    "update_topic_storage_mode",
                    &req.topic_name,
                    serde_json::json!({ "storage_mode": "cloud_tiered" }),
                )
                .await;
                Ok(Response::new(TopicResponse { success: true }))
            }
            "local" => {
                if !req.force {
                    return Err(Status::failed_precondition(
                        "downgrading a topic from cloud_tiered to local requires draining first or force=true",
                    ));
                }
                self.broker_service
                    .storage_factory()
                    .disable_cloud_tiering_for_topic(&req.topic_name);
                self.resources
                    .topic
                    .set_topic_storage_mode(&req.topic_name, "local")
                    .await
                    .map_err(|e| Status::internal(format!("failed to persist storage mode: {}", e)))?;
                let _ = crate::audit::record_audit_event(
                    &self.resources.store,
                    security_context.principal.principal_name(),
                    "update_topic_storage_mode",
                    &req.topic_name,
                    serde_json::json!({ "storage_mode": "local", "force": req.force }),
                )
                .await;
                Ok(Response::new(TopicResponse { success: true }))
            }
            other => Err(Status::invalid_argument(format!(
                "Invalid storage_mode '{}'. Expected 'local' or 'cloud_tiered'",
                other
            ))),
        }
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn peek_messages(
        &self,
        request: Request<PeekMessagesRequest>,
    ) -> std::result::Result<Response<PeekMessagesResponse>, Status> {
        let security_context = get_security_context(&request)?;
        let req = request.into_inner();

        enforce_authorization(
            &security_context,
            &Resource::Topic(req.topic.clone()),
            Permission::ReadPayload,
            &self.resources.security,
        )
        .await?;

        let count = req.count.clamp(1, 100);
        let store = self.resolve_topic_store(&req.topic).await?;
        let raw_msgs = collect_topic_messages(&store, req.start_offset, count).await?;

        let messages = raw_msgs
            .into_iter()
            .map(|msg| PeekedMessage {
                offset: msg.msg_id.topic_offset,
                publish_time: msg.publish_time,
                producer_name: msg.producer_name,
                payload: msg.payload.to_vec(),
                attributes: msg.attributes,
                routing_key: msg.routing_key.unwrap_or_default(),
            })
            .collect();

        Ok(Response::new(PeekMessagesResponse {
            topic: req.topic,
            messages,
        }))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn get_topic_time_range(
        &self,
        request: Request<TopicRequest>,
    ) -> std::result::Result<Response<GetTopicTimeRangeResponse>, Status> {
        let security_context = get_security_context(&request)?;
        let req = request.into_inner();

        enforce_authorization(
            &security_context,
            &Resource::Topic(req.name.clone()),
            Permission::ManageTopic,
            &self.resources.security,
        )
        .await?;

        let store = self.resolve_topic_store(&req.name).await?;
        let head = store.current_offset();
        if head == 0 {
            return Ok(Response::new(GetTopicTimeRangeResponse {
                topic: req.name,
                has_messages: false,
                min_offset: 0,
                max_offset: 0,
                earliest_publish_time: 0,
                latest_publish_time: 0,
            }));
        }

        let first = collect_topic_messages(&store, 0, 1).await?;
        let latest_start = head.saturating_sub(1);
        let last = collect_topic_messages(&store, latest_start, 1).await?;

        let earliest = first.first();
        let latest = last.last().or(earliest);

        Ok(Response::new(GetTopicTimeRangeResponse {
            topic: req.name,
            has_messages: earliest.is_some() || latest.is_some(),
            min_offset: earliest.map(|m| m.msg_id.topic_offset).unwrap_or(0),
            max_offset: latest.map(|m| m.msg_id.topic_offset).unwrap_or(head.saturating_sub(1)),
            earliest_publish_time: earliest.map(|m| m.publish_time).unwrap_or(0),
            latest_publish_time: latest.map(|m| m.publish_time).unwrap_or(0),
        }))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn sample_topic_json(
        &self,
        request: Request<SampleTopicJsonRequest>,
    ) -> std::result::Result<Response<SampleTopicJsonResponse>, Status> {
        let security_context = get_security_context(&request)?;
        let req = request.into_inner();

        enforce_authorization(
            &security_context,
            &Resource::Topic(req.topic.clone()),
            Permission::ReadPayload,
            &self.resources.security,
        )
        .await?;

        let count = req.count.clamp(1, 100);
        let store = self.resolve_topic_store(&req.topic).await?;
        let head = store.current_offset();
        let start = head.saturating_sub(count as u64);
        let msgs = collect_topic_messages(&store, start, count).await?;

        let json_samples = msgs
            .into_iter()
            .filter_map(|m| {
                let s = String::from_utf8(m.payload.to_vec()).ok()?;
                let parsed: serde_json::Value = serde_json::from_str(&s).ok()?;
                serde_json::to_string(&parsed).ok()
            })
            .collect();

        Ok(Response::new(SampleTopicJsonResponse {
            topic: req.topic,
            json_samples,
        }))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn explain_consumer_stall(
        &self,
        request: Request<ExplainConsumerStallRequest>,
    ) -> std::result::Result<Response<ExplainConsumerStallResponse>, Status> {
        let security_context = get_security_context(&request)?;
        let req = request.into_inner();

        enforce_authorization(
            &security_context,
            &Resource::Topic(req.topic.clone()),
            Permission::ManageTopic,
            &self.resources.security,
        )
        .await?;

        let subs = self
            .resources
            .topic
            .get_subscription_for_topic(&req.topic)
            .await;
        if !subs.iter().any(|s| s == &req.subscription) {
            return Ok(Response::new(ExplainConsumerStallResponse {
                topic: req.topic,
                subscription: req.subscription,
                status: "subscription_not_found".to_string(),
                consumer_count: 0,
                backlog: 0,
                in_flight_count: 0,
                ack_timeout_pending: false,
                diagnosis: "Subscription does not exist on this topic yet.".to_string(),
                suggestions: vec![
                    "Verify the subscription name or wait for a consumer to subscribe.".to_string(),
                ],
            }));
        }

        let cursor = self
            .resources
            .topic
            .get_subscription_cursor(&req.subscription, &req.topic)
            .await
            .unwrap_or(None);
        let policy = self
            .resources
            .topic
            .get_subscription_failure_policy(&req.subscription, &req.topic)
            .await
            .unwrap_or(None)
            .unwrap_or_default();

        let topic_opt = self
            .broker_service
            .topic_registry
            .get_topic(&req.topic)
            .or_else(|| {
                self.broker_service
                    .topic_registry
                    .get_topic(req.topic.trim_start_matches('/'))
            });

        let (consumer_count, max_unacked, wal_head, runtime_policy) =
            if let Some(ref topic) = topic_opt {
                if let Some((c, max_u, head, p)) = topic.inspect_subscription(&req.subscription).await
                {
                    (c as u64, max_u as u64, head, p)
                } else {
                    (
                        0,
                        1000,
                        topic.topic_store().map(|s| s.current_offset()).unwrap_or(0),
                        policy.clone(),
                    )
                }
            } else {
                let store_head = self
                    .resolve_topic_store(&req.topic)
                    .await
                    .map(|s| s.current_offset())
                    .unwrap_or(0);
                (0, 1000, store_head, policy.clone())
            };

        let backlog = if wal_head == 0 {
            0
        } else {
            match cursor {
                Some(c) => wal_head.saturating_sub(1).saturating_sub(c),
                None => wal_head,
            }
        };

        let mut suggestions = Vec::new();
        let (status, diagnosis, in_flight_count, ack_timeout_pending) = if backlog == 0 {
            (
                "caught_up".to_string(),
                "Subscription cursor is caught up with the topic WAL head.".to_string(),
                0,
                false,
            )
        } else if consumer_count == 0 {
            suggestions.push("Reconnect or start at least one consumer on this subscription.".into());
            (
                "no_active_consumers".to_string(),
                format!(
                    "{backlog} messages are backlogged, but 0 active consumers are connected."
                ),
                0,
                false,
            )
        } else {
            let est_inflight = backlog.min(max_unacked);
            let poison_block =
                matches!(runtime_policy.poison_policy, BrokerSubscriptionPoisonPolicy::Block);
            if poison_block {
                suggestions.push(
                    "Subscription uses poison_policy=Block; inspect poison/head message or switch to DeadLetter."
                        .into(),
                );
            }
            suggestions.push(format!(
                "ack_timeout_ms={} max_redelivery_count={}; check slow handlers or call extend_lease.",
                runtime_policy.ack_timeout_ms, runtime_policy.max_redelivery_count
            ));
            suggestions.push(
                "Use `danube ctl topics seek` or `peek_messages` on the cursor offset to inspect stuck payloads."
                    .into(),
            );
            (
                "backlogged_or_stalled".to_string(),
                format!(
                    "{consumer_count} active consumer(s) connected with {backlog} unacked messages pending (window limit {max_unacked})."
                ),
                est_inflight,
                est_inflight > 0,
            )
        };

        Ok(Response::new(ExplainConsumerStallResponse {
            topic: req.topic,
            subscription: req.subscription,
            status,
            consumer_count,
            backlog,
            in_flight_count,
            ack_timeout_pending,
            diagnosis,
            suggestions,
        }))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn infer_topic_schema(
        &self,
        request: Request<InferTopicSchemaRequest>,
    ) -> std::result::Result<Response<InferTopicSchemaResponse>, Status> {
        let security_context = get_security_context(&request)?;
        let req = request.into_inner();

        enforce_authorization(
            &security_context,
            &Resource::Topic(req.topic.clone()),
            Permission::ReadPayload,
            &self.resources.security,
        )
        .await?;

        let sample_size = if req.sample_size == 0 {
            100
        } else {
            req.sample_size.clamp(1, 500)
        };

        let store = self.resolve_topic_store(&req.topic).await?;
        let head = store.current_offset();
        let start = head.saturating_sub(sample_size as u64);
        let msgs = collect_topic_messages(&store, start, sample_size).await?;
        let sampled_count = msgs.len() as u32;

        let mut json_values = Vec::new();
        for msg in &msgs {
            if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&msg.payload) {
                json_values.push(val);
            }
        }
        let valid_json_count = json_values.len() as u32;
        if json_values.is_empty() {
            return Err(Status::failed_precondition(format!(
                "no valid JSON payloads found in {} sampled messages for '{}'",
                sampled_count, req.topic
            )));
        }

        let schema = infer_draft07_schema_from_json_samples(&json_values);
        let json_schema = serde_json::to_string_pretty(&schema)
            .map_err(|e| Status::internal(format!("failed to serialize JSON schema: {e}")))?;

        Ok(Response::new(InferTopicSchemaResponse {
            topic: req.topic,
            json_schema,
            sampled_count,
            valid_json_count,
        }))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn branch_topic(
        &self,
        request: Request<BranchTopicRequest>,
    ) -> std::result::Result<Response<BranchTopicResponse>, Status> {
        let security_context = get_security_context(&request)?;
        let req = request.into_inner();
        let source_topic = crate::broker_service::normalize_topic_path(&req.source_topic)?;
        let target_topic = crate::broker_service::normalize_topic_path(&req.target_topic)?;
        if source_topic == target_topic {
            return Err(Status::invalid_argument(
                "source_topic and target_topic must be different",
            ));
        }

        enforce_authorization(
            &security_context,
            &Resource::Topic(source_topic.clone()),
            Permission::ManageTopic,
            &self.resources.security,
        )
        .await?;
        enforce_authorization(
            &security_context,
            &Resource::Topic(target_topic.clone()),
            Permission::ManageTopic,
            &self.resources.security,
        )
        .await?;

        let source_store = self.resolve_topic_store(&source_topic).await?;
        let head = source_store.current_offset();

        let target_max_offset: Option<u64> = if let Some(at_off) = req.at_offset {
            if head > 0 {
                Some(at_off.min(head.saturating_sub(1)))
            } else {
                Some(at_off)
            }
        } else if let Some(at_ts_ns) = req.at_timestamp_ns {
            let scan_limit = head.min(5_000) as u32;
            let msgs = collect_topic_messages(&source_store, 0, scan_limit).await?;
            msgs.into_iter()
                .filter(|m| m.publish_time <= at_ts_ns)
                .map(|m| m.msg_id.topic_offset)
                .max()
        } else {
            head.checked_sub(1)
        };

        let storage_factory = self.broker_service.storage_factory();
        let copied_segments = storage_factory
            .branch_topic_segments(&source_topic, &target_topic, target_max_offset)
            .await
            .map_err(|e| Status::internal(format!("failed to branch segments: {e}")))?;

        let durable_end = if copied_segments > 0 {
            storage_factory
                .list_segment_descriptors(&target_topic, 0)
                .await
                .unwrap_or_default()
                .into_iter()
                .map(|d| d.end_offset)
                .max()
        } else {
            None
        };

        let dispatch_strategy = self
            .broker_service
            .topic_registry
            .get_topic(&source_topic)
            .map(|t| match t.dispatch_strategy {
                crate::dispatcher::DispatchStrategy::NonReliable => {
                    CoreDispatchStrategy::NonReliable
                }
                crate::dispatcher::DispatchStrategy::Reliable => CoreDispatchStrategy::Reliable,
            })
            .unwrap_or(CoreDispatchStrategy::Reliable);

        self.broker_service
            .create_topic(&target_topic, Some(dispatch_strategy), None)
            .await
            .map_err(|e| Status::internal(format!("failed to create branch topic: {e}")))?;

        let mut copied_local_messages = 0u64;
        if let Some(limit_off) = target_max_offset {
            let local_start = durable_end.map(|e| e.saturating_add(1)).unwrap_or(0);
            if local_start <= limit_off && local_start < head {
                let target_store = self.resolve_topic_store(&target_topic).await?;
                let count = (limit_off - local_start + 1).min(5_000) as u32;
                let msgs = collect_topic_messages(&source_store, local_start, count).await?;
                for mut msg in msgs {
                    if msg.msg_id.topic_offset > limit_off {
                        break;
                    }
                    msg.msg_id.topic_name = target_topic.clone();
                    target_store
                        .store_message(msg)
                        .await
                        .map_err(|e| Status::internal(format!("failed copying message: {e}")))?;
                    copied_local_messages += 1;
                }
            }
        }

        let _ = crate::audit::record_audit_event(
            &self.resources.store,
            security_context.principal.principal_name(),
            "branch_topic",
            &target_topic,
            serde_json::json!({
                "source_topic": source_topic,
                "branched_up_to_offset": target_max_offset.unwrap_or(0),
                "copied_segments": copied_segments,
                "copied_local_messages": copied_local_messages,
            }),
        )
        .await;

        Ok(Response::new(BranchTopicResponse {
            success: true,
            source_topic,
            target_topic,
            branched_up_to_offset: target_max_offset.unwrap_or(0),
            copied_segments: copied_segments as u32,
            copied_local_messages,
        }))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn seek_subscription(
        &self,
        request: Request<SeekSubscriptionRequest>,
    ) -> std::result::Result<Response<SeekSubscriptionResponse>, Status> {
        let security_context = get_security_context(&request)?;
        let req = request.into_inner();

        enforce_authorization(
            &security_context,
            &Resource::Topic(req.topic.clone()),
            Permission::ManageTopic,
            &self.resources.security,
        )
        .await?;

        if req.subscription.trim().is_empty() {
            return Err(Status::invalid_argument("subscription must be provided"));
        }

        let resolved_offset = if let Some(to_offset) = req.to_offset {
            to_offset
        } else if let Some(to_ts_ns) = req.to_timestamp_ns {
            let store = self.resolve_topic_store(&req.topic).await?;
            let head = store.current_offset();
            if head == 0 {
                0
            } else {
                let mut found: Option<u64> = None;
                let mut cursor = 0u64;
                while cursor < head {
                    let chunk = collect_topic_messages(&store, cursor, 1_000).await?;
                    if chunk.is_empty() {
                        break;
                    }
                    let last_off = chunk.last().map(|m| m.msg_id.topic_offset).unwrap_or(cursor);
                    if let Some(m) = chunk.iter().find(|m| m.publish_time >= to_ts_ns) {
                        found = Some(m.msg_id.topic_offset);
                        break;
                    }
                    cursor = last_off.saturating_add(1);
                }
                found.unwrap_or(head)
            }
        } else {
            return Err(Status::invalid_argument(
                "must specify either to_offset or to_timestamp_ns",
            ));
        };

        self.resources
            .topic
            .set_subscription_next_offset(&req.subscription, &req.topic, resolved_offset)
            .await
            .map_err(|e| Status::internal(format!("failed to persist subscription seek: {e}")))?;

        let trimmed = req.topic.trim_start_matches('/');
        if let Some(topic) = self
            .broker_service
            .topic_registry
            .get_topic(&req.topic)
            .or_else(|| self.broker_service.topic_registry.get_topic(trimmed))
        {
            topic.unsubscribe(&req.subscription).await;
        }

        let _ = crate::audit::record_audit_event(
            &self.resources.store,
            security_context.principal.principal_name(),
            "seek_subscription",
            &req.topic,
            serde_json::json!({
                "subscription": req.subscription,
                "to_offset": req.to_offset,
                "to_timestamp_ns": req.to_timestamp_ns,
                "resolved_offset": resolved_offset,
            }),
        )
        .await;

        Ok(Response::new(SeekSubscriptionResponse {
            success: true,
            topic: req.topic,
            subscription: req.subscription.clone(),
            resolved_offset,
            message: format!(
                "Subscription '{}' cursor reset to deliver starting at offset {}",
                req.subscription, resolved_offset
            ),
        }))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn replay_dlq(
        &self,
        request: Request<ReplayDlqRequest>,
    ) -> std::result::Result<Response<ReplayDlqResponse>, Status> {
        let security_context = get_security_context(&request)?;
        let req = request.into_inner();

        enforce_authorization(
            &security_context,
            &Resource::Topic(req.topic.clone()),
            Permission::ManageTopic,
            &self.resources.security,
        )
        .await?;

        if req.subscription.trim().is_empty() {
            return Err(Status::invalid_argument("subscription must be provided"));
        }

        let source_dlq_topic = if let Some(explicit) = req
            .dlq_topic
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            explicit.to_string()
        } else if let Ok(Some(policy)) = self
            .resources
            .topic
            .get_subscription_failure_policy(&req.subscription, &req.topic)
            .await
        {
            policy.dead_letter_topic.unwrap_or_else(|| {
                format!("{}/{}-dlq", req.topic.trim_end_matches('/'), req.subscription)
            })
        } else {
            format!("{}/{}-dlq", req.topic.trim_end_matches('/'), req.subscription)
        };

        let max_messages = if req.max_messages == 0 {
            100
        } else {
            req.max_messages.min(1_000)
        };

        let dlq_store = self.resolve_topic_store(&source_dlq_topic).await?;
        let msgs = collect_topic_messages(&dlq_store, req.start_offset, max_messages).await?;

        let mut replayed_count = 0u32;
        let mut last_replayed_offset = req.start_offset;

        for mut msg in msgs {
            let source_offset = msg.msg_id.topic_offset;
            msg.attributes.retain(|k, _| {
                !matches!(
                    k.as_str(),
                    "x-original-topic"
                        | "x-original-subscription"
                        | "x-original-topic-offset"
                        | "x-original-producer-id"
                        | "x-original-broker-addr"
                        | "x-poison-policy"
                        | "x-delivery-attempt"
                        | "x-failure-reason"
                )
            });
            msg.attributes
                .insert("x-danube-replayed-from-dlq".to_string(), source_dlq_topic.clone());
            msg.attributes
                .insert("x-danube-replayed-subscription".to_string(), req.subscription.clone());
            msg.msg_id.producer_id = 0;
            msg.msg_id.topic_name = req.topic.clone();
            msg.msg_id.topic_offset = 0;
            msg.subscription_name = None;

            if let Err(err) = self
                .broker_service
                .replicator
                .publish_message_async(&req.topic, msg.clone())
                .await
            {
                let trimmed = req.topic.trim_start_matches('/');
                if let Some(topic) = self
                    .broker_service
                    .topic_registry
                    .get_topic(&req.topic)
                    .or_else(|| self.broker_service.topic_registry.get_topic(trimmed))
                {
                    if let Some(store) = topic.topic_store() {
                        store.store_message(msg).await.map_err(|e| {
                            Status::internal(format!("failed replaying DLQ message: {e}"))
                        })?;
                    } else {
                        return Err(Status::internal(format!(
                            "failed replaying DLQ message via replicator: {err}"
                        )));
                    }
                } else {
                    return Err(Status::internal(format!(
                        "failed replaying DLQ message via replicator: {err}"
                    )));
                }
            }

            replayed_count += 1;
            last_replayed_offset = source_offset;
        }

        let _ = crate::audit::record_audit_event(
            &self.resources.store,
            security_context.principal.principal_name(),
            "replay_dlq",
            &req.topic,
            serde_json::json!({
                "subscription": req.subscription,
                "source_dlq_topic": source_dlq_topic,
                "replayed_count": replayed_count,
                "last_replayed_offset": last_replayed_offset,
            }),
        )
        .await;

        Ok(Response::new(ReplayDlqResponse {
            success: true,
            source_dlq_topic,
            target_topic: req.topic,
            replayed_count,
            last_replayed_offset,
        }))
    }
}

async fn collect_topic_messages(
    store: &TopicStore,
    start_offset: u64,
    count: u32,
) -> Result<Vec<StreamMessage>, Status> {
    let head = store.current_offset();
    if head == 0 || start_offset >= head {
        return Ok(Vec::new());
    }
    let mut stream = store
        .create_reader(StartPosition::Offset(start_offset))
        .await
        .map_err(|e| Status::internal(format!("failed to create topic reader: {e}")))?;
    let mut out = Vec::with_capacity(count as usize);
    while out.len() < count as usize {
        match tokio::time::timeout(std::time::Duration::from_millis(150), stream.next()).await {
            Ok(Some(Ok(msg))) => {
                let offset = msg.msg_id.topic_offset;
                out.push(msg);
                if offset + 1 >= head {
                    break;
                }
            }
            Ok(Some(Err(e))) => {
                return Err(Status::internal(format!("failed reading message stream: {e}")));
            }
            Ok(None) | Err(_) => break,
        }
    }
    Ok(out)
}

#[derive(Debug, Clone, Default)]
struct SchemaAccumulator {
    types: BTreeSet<&'static str>,
    object_fields: BTreeMap<String, SchemaAccumulator>,
    field_presence: BTreeMap<String, usize>,
    object_count: usize,
    array_item: Option<Box<SchemaAccumulator>>,
}

impl SchemaAccumulator {
    fn observe(&mut self, val: &serde_json::Value) {
        match val {
            serde_json::Value::Null => {
                self.types.insert("null");
            }
            serde_json::Value::Bool(_) => {
                self.types.insert("boolean");
            }
            serde_json::Value::Number(n) => {
                if n.is_i64() || n.is_u64() {
                    self.types.insert("integer");
                } else {
                    self.types.insert("number");
                }
            }
            serde_json::Value::String(_) => {
                self.types.insert("string");
            }
            serde_json::Value::Array(items) => {
                self.types.insert("array");
                let entry = self
                    .array_item
                    .get_or_insert_with(|| Box::new(SchemaAccumulator::default()));
                for item in items {
                    entry.observe(item);
                }
            }
            serde_json::Value::Object(map) => {
                self.types.insert("object");
                self.object_count += 1;
                for (k, v) in map {
                    *self.field_presence.entry(k.clone()).or_insert(0) += 1;
                    self.object_fields
                        .entry(k.clone())
                        .or_default()
                        .observe(v);
                }
            }
        }
    }

    fn to_json_schema(&self, is_root: bool) -> serde_json::Value {
        let mut obj = serde_json::Map::new();
        if is_root {
            obj.insert(
                "$schema".to_string(),
                serde_json::Value::String("http://json-schema.org/draft-07/schema#".to_string()),
            );
        }

        // Normalize {integer, number} -> number
        let mut normalized_types = self.types.clone();
        if normalized_types.contains("integer") && normalized_types.contains("number") {
            normalized_types.remove("integer");
        }

        if normalized_types.len() == 1 {
            let t = normalized_types.iter().next().copied().unwrap();
            obj.insert("type".to_string(), serde_json::Value::String(t.to_string()));
        } else if normalized_types.len() > 1 {
            let arr = normalized_types
                .iter()
                .map(|t| serde_json::Value::String((*t).to_string()))
                .collect();
            obj.insert("type".to_string(), serde_json::Value::Array(arr));
        }

        if self.types.contains("object") && !self.object_fields.is_empty() {
            let mut props = serde_json::Map::new();
            let mut required = Vec::new();
            for (k, sub) in &self.object_fields {
                props.insert(k.clone(), sub.to_json_schema(false));
                if self.field_presence.get(k).copied().unwrap_or(0) == self.object_count {
                    required.push(serde_json::Value::String(k.clone()));
                }
            }
            obj.insert("properties".to_string(), serde_json::Value::Object(props));
            if !required.is_empty() {
                obj.insert("required".to_string(), serde_json::Value::Array(required));
            }
        }

        if self.types.contains("array") {
            if let Some(ref items) = self.array_item {
                if !items.types.is_empty() {
                    obj.insert("items".to_string(), items.to_json_schema(false));
                }
            }
        }

        serde_json::Value::Object(obj)
    }
}

/// Infer a Draft-07 JSON Schema across multiple JSON samples:
/// fields present in every valid object sample are marked `required`;
/// fields missing in some objects stay optional, and mixed/nullable fields emit type unions.
pub(crate) fn infer_draft07_schema_from_json_samples(
    samples: &[serde_json::Value],
) -> serde_json::Value {
    let mut acc = SchemaAccumulator::default();
    for sample in samples {
        acc.observe(sample);
    }
    acc.to_json_schema(true)
}

/// Expand an optional `StreamIntent` profile into explicit settings and select
/// its dispatch strategy unless the caller explicitly asked for `Reliable`.
pub(super) fn resolve_intent_and_strategy(
    intent: Option<&str>,
    requested: CoreDispatchStrategy,
) -> Result<(CoreDispatchStrategy, Option<ExpandedStreamSettings>), Status> {
    let Some(raw) = intent.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok((requested, None));
    };
    let parsed: StreamIntent = raw
        .parse()
        .map_err(|e| Status::invalid_argument(format!("invalid stream intent: {}", e)))?;
    let expanded = parsed.expand();
    let strategy = match requested {
        CoreDispatchStrategy::Reliable => CoreDispatchStrategy::Reliable,
        CoreDispatchStrategy::NonReliable => match expanded.dispatch_strategy {
            ConfigDispatchStrategy::Reliable => CoreDispatchStrategy::Reliable,
            ConfigDispatchStrategy::NonReliable => CoreDispatchStrategy::NonReliable,
        },
    };
    Ok((strategy, Some(expanded)))
}

fn admin_failure_policy_to_broker(
    failure_policy: AdminSubscriptionFailurePolicy,
) -> Result<BrokerSubscriptionFailurePolicy, Status> {
    let backoff_strategy = AdminSubscriptionBackoffStrategy::try_from(failure_policy.backoff_strategy)
        .map_err(|_| Status::invalid_argument("invalid backoff_strategy value"))?;
    let poison_policy = AdminSubscriptionPoisonPolicy::try_from(failure_policy.poison_policy)
        .map_err(|_| Status::invalid_argument("invalid poison_policy value"))?;

    let backoff_strategy = match backoff_strategy {
        AdminSubscriptionBackoffStrategy::Fixed => BrokerSubscriptionBackoffStrategy::Fixed,
        AdminSubscriptionBackoffStrategy::Exponential => {
            BrokerSubscriptionBackoffStrategy::Exponential
        }
    };

    let poison_policy = match poison_policy {
        AdminSubscriptionPoisonPolicy::DeadLetter => BrokerSubscriptionPoisonPolicy::DeadLetter,
        AdminSubscriptionPoisonPolicy::Block => BrokerSubscriptionPoisonPolicy::Block,
        AdminSubscriptionPoisonPolicy::Drop => BrokerSubscriptionPoisonPolicy::Drop,
    };

    Ok(BrokerSubscriptionFailurePolicy {
        max_redelivery_count: failure_policy.max_redelivery_count,
        ack_timeout_ms: failure_policy.ack_timeout_ms,
        base_redelivery_delay_ms: failure_policy.base_redelivery_delay_ms,
        max_redelivery_delay_ms: failure_policy.max_redelivery_delay_ms,
        backoff_strategy,
        dead_letter_topic: failure_policy.dead_letter_topic,
        poison_policy,
    })
}

fn broker_failure_policy_to_admin(
    failure_policy: BrokerSubscriptionFailurePolicy,
) -> AdminSubscriptionFailurePolicy {
    let backoff_strategy = match failure_policy.backoff_strategy {
        BrokerSubscriptionBackoffStrategy::Fixed => AdminSubscriptionBackoffStrategy::Fixed,
        BrokerSubscriptionBackoffStrategy::Exponential => {
            AdminSubscriptionBackoffStrategy::Exponential
        }
    };

    let poison_policy = match failure_policy.poison_policy {
        BrokerSubscriptionPoisonPolicy::DeadLetter => AdminSubscriptionPoisonPolicy::DeadLetter,
        BrokerSubscriptionPoisonPolicy::Block => AdminSubscriptionPoisonPolicy::Block,
        BrokerSubscriptionPoisonPolicy::Drop => AdminSubscriptionPoisonPolicy::Drop,
    };

    AdminSubscriptionFailurePolicy {
        max_redelivery_count: failure_policy.max_redelivery_count,
        ack_timeout_ms: failure_policy.ack_timeout_ms,
        base_redelivery_delay_ms: failure_policy.base_redelivery_delay_ms,
        max_redelivery_delay_ms: failure_policy.max_redelivery_delay_ms,
        backoff_strategy: backoff_strategy as i32,
        dead_letter_topic: failure_policy.dead_letter_topic,
        poison_policy: poison_policy as i32,
    }
}

impl DanubeAdminImpl {
    pub(crate) async fn persist_expanded_intent(
        &self,
        topic_name: &str,
        settings: &ExpandedStreamSettings,
    ) -> Result<(), Status> {
        let val = serde_json::to_value(settings)
            .map_err(|e| Status::internal(format!("failed to serialize expanded intent: {}", e)))?;
        self.resources
            .topic
            .set_topic_intent(topic_name, val)
            .await
            .map_err(|e| Status::internal(format!("failed to persist topic intent: {}", e)))?;
        Ok(())
    }

    /// Get schema registry information for a topic
    ///
    /// Returns: (schema_subject, schema_id, schema_version, schema_type, compatibility_mode)
    async fn get_topic_schema_info(
        &self,
        topic_name: &str,
    ) -> (
        Option<String>,
        Option<u64>,
        Option<u32>,
        Option<String>,
        Option<String>,
    ) {
        // Get topic's schema subject from metadata
        let schema_subject = match self.resources.topic.get_schema_subject(topic_name).await {
            Some(subject) => subject,
            None => return (None, None, None, None, None),
        };

        // Get schema details from schema registry
        let resources = self.resources.clone();
        let schema_details = resources
            .schema
            .get_schema_by_subject(&schema_subject)
            .await;

        match schema_details {
            Some(details) => {
                // Schema found in registry
                let schema_type = match details.schema_type.as_str() {
                    "json_schema" => Some("json_schema".to_string()),
                    "avro" => Some("avro".to_string()),
                    "protobuf" => Some("protobuf".to_string()),
                    "string" => Some("string".to_string()),
                    "bytes" => Some("bytes".to_string()),
                    other => Some(other.to_string()),
                };

                (
                    Some(schema_subject),
                    Some(details.schema_id),
                    Some(details.version),
                    schema_type,
                    Some(details.compatibility_mode),
                )
            }
            None => {
                // Schema subject exists but not found in registry
                (Some(schema_subject), None, None, None, None)
            }
        }
    }

    async fn resolve_topic_store(&self, topic_name: &str) -> Result<TopicStore, Status> {
        let trimmed = topic_name.trim_start_matches('/');
        if let Some(topic) = self
            .broker_service
            .topic_registry
            .get_topic(topic_name)
            .or_else(|| self.broker_service.topic_registry.get_topic(trimmed))
        {
            if let Some(store) = topic.topic_store() {
                return Ok(store.clone());
            }
        }

        let normalized = if topic_name.starts_with('/') {
            topic_name.to_string()
        } else {
            format!("/{topic_name}")
        };
        let storage = self
            .broker_service
            .storage_factory()
            .for_topic(&normalized)
            .await
            .map_err(|e| Status::not_found(format!("topic storage unavailable: {e}")))?;
        Ok(TopicStore::new(normalized, storage))
    }
}

#[cfg(test)]
mod tests {
    use super::infer_draft07_schema_from_json_samples;

    #[test]
    fn test_infer_draft07_schema_merges_samples_and_optional_nullable_fields() {
        let samples = vec![
            serde_json::json!({"id": 1, "name": "alice", "meta": {"ok": true}, "tags": ["a"]}),
            serde_json::json!({"id": 2, "name": serde_json::Value::Null, "score": 9.5, "tags": ["b"]}),
        ];
        let schema = infer_draft07_schema_from_json_samples(&samples);
        assert_eq!(
            schema.get("$schema").and_then(|v| v.as_str()),
            Some("http://json-schema.org/draft-07/schema#")
        );
        let props = schema.get("properties").and_then(|v| v.as_object()).unwrap();
        assert_eq!(props.get("id").unwrap().get("type").unwrap(), "integer");
        assert_eq!(props.get("score").unwrap().get("type").unwrap(), "number");

        let name_types = props.get("name").unwrap().get("type").unwrap().as_array().unwrap();
        assert!(name_types.iter().any(|v| v == "null"));
        assert!(name_types.iter().any(|v| v == "string"));

        let req = schema.get("required").unwrap().as_array().unwrap();
        let req_keys: Vec<&str> = req.iter().filter_map(|v| v.as_str()).collect();
        assert!(req_keys.contains(&"id"));
        assert!(req_keys.contains(&"name"));
        assert!(req_keys.contains(&"tags"));
        // score and meta only appeared in 1 of 2 samples -> must not be required
        assert!(!req_keys.contains(&"score"));
        assert!(!req_keys.contains(&"meta"));
    }
}
