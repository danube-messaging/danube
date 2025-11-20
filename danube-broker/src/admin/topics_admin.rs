use crate::admin::DanubeAdminImpl;
use crate::schema::{Schema, SchemaType};
use danube_core::admin_proto::{
    topic_admin_server::TopicAdmin, BrokerRequest, DescribeTopicRequest, DescribeTopicResponse,
    NamespaceRequest, NewTopicRequest, PartitionedTopicRequest, SubscriptionListResponse,
    SubscriptionRequest, SubscriptionResponse, TopicInfo, TopicInfoListResponse, TopicRequest,
    TopicResponse,
};
use danube_core::proto::DispatchStrategy as CoreDispatchStrategy;

use tonic::{Request, Response, Status};
use tracing::{trace, Level};

#[tonic::async_trait]
impl TopicAdmin for DanubeAdminImpl {
    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn list_namespace_topics(
        &self,
        request: Request<NamespaceRequest>,
    ) -> std::result::Result<Response<TopicInfoListResponse>, tonic::Status> {
        trace!("Admin: list topics with broker_id for a namespace");

        let req = request.into_inner();
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
            topics.push(TopicInfo {
                name: normalized,
                broker_id,
            });
        }

        Ok(Response::new(TopicInfoListResponse { topics }))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn list_broker_topics(
        &self,
        request: Request<BrokerRequest>,
    ) -> std::result::Result<Response<TopicInfoListResponse>, tonic::Status> {
        trace!("Admin: list topics hosted by a broker");

        let req = request.into_inner();
        let names = self
            .resources
            .cluster
            .get_topics_for_broker(&req.broker_id)
            .await;

        let mut topics: Vec<TopicInfo> = Vec::with_capacity(names.len());
        for name in names.into_iter() {
            topics.push(TopicInfo {
                name,
                broker_id: req.broker_id.clone(),
            });
        }

        Ok(Response::new(TopicInfoListResponse { topics }))
    }
    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn create_topic(
        &self,
        request: Request<NewTopicRequest>,
    ) -> std::result::Result<Response<TopicResponse>, tonic::Status> {
        let req = request.into_inner();

        trace!("Admin: creates a non-partitioned topic: {}", req.name);

        let mut schema_type = match SchemaType::from_str(&req.schema_type) {
            Some(schema_type) => schema_type,
            None => {
                let status = Status::not_found(format!(
                    "Invalid schema_type, allowed values: Bytes, String, Int64, Json "
                ));
                return Err(status);
            }
        };

        if schema_type == SchemaType::Json(String::new()) {
            schema_type = SchemaType::Json(req.schema_data);
        }

        // Map admin NewTopicRequest.dispatch_strategy (enum i32) into core proto DispatchStrategy
        let dispatch_strategy =
            <CoreDispatchStrategy as core::convert::TryFrom<i32>>::try_from(req.dispatch_strategy)
                .map_err(|_| Status::invalid_argument("invalid dispatch_strategy value"))?;

        let service = self.broker_service.as_ref();

        let schema = Schema::new(format!("{}_schema", req.name), schema_type);

        let success = match service
            .create_topic_cluster(&req.name, Some(dispatch_strategy), Some(schema.into()))
            .await
        {
            Ok(()) => true,
            Err(err) => {
                let status = Status::not_found(format!(
                    "Unable to create the topic {} due to {}",
                    req.name, err
                ));
                return Err(status);
            }
        };

        let response = TopicResponse { success };
        Ok(tonic::Response::new(response))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn create_partitioned_topic(
        &self,
        request: Request<PartitionedTopicRequest>,
    ) -> std::result::Result<Response<TopicResponse>, tonic::Status> {
        let req = request.into_inner();

        trace!(
            "Admin: creates a partitioned topic: {} with {} partitions",
            req.base_name,
            req.partitions
        );

        // Schema mapping
        let mut schema_type = match SchemaType::from_str(&req.schema_type) {
            Some(schema_type) => schema_type,
            None => {
                let status = Status::not_found(
                    "Invalid schema_type, allowed values: Bytes, String, Int64, Json ",
                );
                return Err(status);
            }
        };

        if schema_type == SchemaType::Json(String::new()) {
            schema_type = SchemaType::Json(req.schema_data);
        }

        // Dispatch mapping: admin enum (i32) -> core proto enum
        let dispatch_strategy =
            <CoreDispatchStrategy as core::convert::TryFrom<i32>>::try_from(req.dispatch_strategy)
                .map_err(|_| Status::invalid_argument("invalid dispatch_strategy value"))?;

        let service = self.broker_service.as_ref();
        let schema = Schema::new(format!("{}_schema", req.base_name), schema_type);

        // Create all partitions; fail fast on error
        for partition_id in 0..req.partitions {
            let topic = format!("{}-part-{}", req.base_name, partition_id);
            if let Err(err) = service
                .create_topic_cluster(&topic, Some(dispatch_strategy), Some(schema.clone().into()))
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
        let req = request.into_inner();

        trace!("Admin: delete the topic: {}", req.name);

        let service = self.broker_service.as_ref();

        let success = match service.post_delete_topic(&req.name).await {
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
        let req = request.into_inner();

        trace!(
            "Admin: get the list of subscriptions on the topic: {}",
            req.name
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
        let req = request.into_inner();

        trace!(
            "Admin: Unsubscribe subscription {} from topic: {}",
            req.subscription,
            req.topic
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
    async fn describe_topic(
        &self,
        request: Request<DescribeTopicRequest>,
    ) -> std::result::Result<Response<DescribeTopicResponse>, tonic::Status> {
        let req = request.into_inner();

        // Subscriptions
        let subscriptions = self
            .resources
            .topic
            .get_subscription_for_topic(&req.name)
            .await;

        // Schema
        let service = self.broker_service.as_ref();
        let schema = service.get_schema_async(&req.name).await;

        let (type_schema, schema_data) = if let Some(s) = schema {
            (s.type_schema, s.schema_data)
        } else {
            (0, Vec::new())
        };

        // Identify serving broker id for this topic if known
        let broker_id = service
            .get_topic_broker_id(&req.name)
            .await
            .unwrap_or_default();

        let response = DescribeTopicResponse {
            name: req.name,
            type_schema,
            schema_data,
            subscriptions,
            broker_id,
        };
        Ok(tonic::Response::new(response))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn unload_topic(
        &self,
        request: Request<TopicRequest>,
    ) -> std::result::Result<Response<TopicResponse>, tonic::Status> {
        let req = request.into_inner();

        trace!("Admin: unload the topic: {}", req.name);
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
