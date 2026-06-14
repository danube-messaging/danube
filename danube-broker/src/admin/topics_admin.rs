use crate::admin::DanubeAdminImpl;
use crate::subscription::{
    SubscriptionBackoffStrategy as BrokerSubscriptionBackoffStrategy,
    SubscriptionFailurePolicy as BrokerSubscriptionFailurePolicy,
    SubscriptionPoisonPolicy as BrokerSubscriptionPoisonPolicy,
};
use danube_core::admin_proto::{
    topic_admin_server::TopicAdmin, BrokerRequest, DescribeTopicRequest, DescribeTopicResponse,
    GetSubscriptionFailurePolicyRequest, GetSubscriptionFailurePolicyResponse, NamespaceRequest,
    NewTopicRequest, PartitionedTopicRequest,
    SetSubscriptionFailurePolicyRequest,
    SubscriptionBackoffStrategy as AdminSubscriptionBackoffStrategy,
    SubscriptionFailurePolicy as AdminSubscriptionFailurePolicy, SubscriptionListResponse,
    SubscriptionPoisonPolicy as AdminSubscriptionPoisonPolicy, SubscriptionRequest,
    SubscriptionResponse, TopicInfo, TopicInfoListResponse, TopicRequest, TopicResponse,
};
use danube_core::dispatch_strategy::ConfigDispatchStrategy;
use danube_core::proto::{
    schema_reference::VersionRef, DispatchStrategy as CoreDispatchStrategy, SchemaReference,
};

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
        let dispatch_strategy =
            <CoreDispatchStrategy as core::convert::TryFrom<i32>>::try_from(req.dispatch_strategy)
                .map_err(|_| Status::invalid_argument("invalid dispatch_strategy value"))?;

        let service = self.broker_service.as_ref();

        // Unified create_topic dispatches based on BrokerMode
        match service
            .create_topic(&req.name, Some(dispatch_strategy), schema_ref)
            .await
        {
            Ok(_) => Ok(tonic::Response::new(TopicResponse { success: true })),
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
        let dispatch_strategy =
            <CoreDispatchStrategy as core::convert::TryFrom<i32>>::try_from(req.dispatch_strategy)
                .map_err(|_| Status::invalid_argument("invalid dispatch_strategy value"))?;

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
        }

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

        let service = self.broker_service.as_ref();

        // Unified delete_topic dispatches based on BrokerMode
        let success = match service.delete_topic(&req.name).await {
            Ok(()) => true,
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
            Ok(()) => true,
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

        let dispatch_strategy = self
            .resources
            .topic
            .get_dispatch_strategy(req.topic.trim_start_matches('/'))
            .await;

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

        // 2) Delegate unload to cluster: this will
        //    - create an unload marker under /cluster/unassigned with from_broker
        //    - schedule deletion of the current assignment at the hosting broker
        // The hosting broker will observe the deletion via watch and perform local unload safely.
        match service.topic_cluster.post_unload_topic(&req.name).await {
            Ok(()) => Ok(Response::new(TopicResponse { success: true })),
            Err(e) => Err(Status::not_found(format!(
                "Unable to unload the topic {} due to {}",
                req.name, e
            ))),
        }
    }
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
        max_unacked_messages: BrokerSubscriptionFailurePolicy::default().max_unacked_messages,
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
}
