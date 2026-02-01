use crate::admin::DanubeAdminImpl;
use danube_core::admin_proto::{
    broker_admin_server::BrokerAdmin, ActivateBrokerRequest, ActivateBrokerResponse,
    BrokerListResponse, BrokerLoadInfo, BrokerResponse, ClusterBalanceRequest,
    ClusterBalanceResponse, Empty, NamespaceListResponse, ProposedMove, RebalanceRequest,
    RebalanceResponse, UnloadBrokerRequest, UnloadBrokerResponse,
};

use tonic::{Request, Response};
use tracing::{info, trace, warn, Level};

#[tonic::async_trait]
impl BrokerAdmin for DanubeAdminImpl {
    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn list_brokers(
        &self,
        _request: Request<Empty>,
    ) -> std::result::Result<Response<BrokerListResponse>, tonic::Status> {
        trace!("list brokers command");

        let mut brokers_info = Vec::new();

        let brokers = self.resources.cluster.get_brokers().await;
        // Collect broker states (mode) from /cluster/brokers/{id}/state
        let states = self.resources.cluster.get_brokers_state().await;

        for broker_id in brokers {
            if let Some(mut info) = self.resources.cluster.get_broker_info(&broker_id) {
                if let Some(mode) = states.get(&broker_id) {
                    info.broker_status = mode.clone();
                }
                brokers_info.push(info);
            }
        }

        let response = BrokerListResponse {
            brokers: brokers_info,
        };

        Ok(tonic::Response::new(response))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn get_leader_broker(
        &self,
        _request: Request<Empty>,
    ) -> std::result::Result<Response<BrokerResponse>, tonic::Status> {
        trace!("get leader broker command");

        let leader = if let Some(lead) = self.resources.cluster.get_cluster_leader() {
            lead.to_string()
        } else {
            "not_found".to_string()
        };

        let response = BrokerResponse { leader };
        Ok(tonic::Response::new(response))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn list_namespaces(
        &self,
        _request: Request<Empty>,
    ) -> std::result::Result<Response<NamespaceListResponse>, tonic::Status> {
        trace!("get cluster namespaces command");

        let namespaces = self.resources.cluster.get_namespaces().await;

        let response = NamespaceListResponse { namespaces };
        Ok(tonic::Response::new(response))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn activate_broker(
        &self,
        request: Request<ActivateBrokerRequest>,
    ) -> std::result::Result<Response<ActivateBrokerResponse>, tonic::Status> {
        let req = request.into_inner();
        if req.broker_id.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "broker_id must be provided to activate a broker",
            ));
        }

        let reason = if req.reason.is_empty() {
            "admin_activate"
        } else {
            req.reason.as_str()
        };

        if let Err(e) = self
            .resources
            .cluster
            .set_broker_state(&req.broker_id, "active", Some(reason))
            .await
        {
            return Err(tonic::Status::internal(format!(
                "Failed to set broker state to active: {}",
                e
            )));
        }

        Ok(Response::new(ActivateBrokerResponse { success: true }))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn unload_broker(
        &self,
        request: Request<UnloadBrokerRequest>,
    ) -> std::result::Result<Response<UnloadBrokerResponse>, tonic::Status> {
        let req = request.into_inner();

        // Preconditions: at least 2 brokers in cluster
        let brokers = self.resources.cluster.get_brokers().await;
        if brokers.len() < 2 {
            return Err(tonic::Status::failed_precondition(
                "Cannot unload broker: single broker cluster detected.",
            ));
        }

        if req.broker_id.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "broker_id must be provided to unload a broker",
            ));
        }
        let target_broker = req.broker_id;

        // Verify there is at least one OTHER active broker available
        let mut active_others = 0usize;
        for b in brokers.iter() {
            if *b != target_broker && self.resources.cluster.is_broker_active(b).await {
                active_others += 1;
            }
        }
        if active_others == 0 {
            return Err(tonic::Status::failed_precondition(
                "Cannot unload broker: no other active brokers available for reassignment.",
            ));
        }

        // Optionally filter namespaces
        let include_ns: Option<Vec<String>> = if req.namespaces_include.is_empty() {
            None
        } else {
            Some(req.namespaces_include.clone())
        };
        let exclude_ns: Option<Vec<String>> = if req.namespaces_exclude.is_empty() {
            None
        } else {
            Some(req.namespaces_exclude.clone())
        };

        // Dry run: only compute candidate topics
        let mut topics = self
            .resources
            .cluster
            .get_topics_for_broker(&target_broker)
            .await;
        if let Some(include) = &include_ns {
            topics.retain(|t| include.iter().any(|ns| t.starts_with(&format!("/{}/", ns))));
        }
        if let Some(exclude) = &exclude_ns {
            topics.retain(|t| !exclude.iter().any(|ns| t.starts_with(&format!("/{}/", ns))));
        }

        if req.dry_run {
            let response = UnloadBrokerResponse {
                started: false,
                total: topics.len() as u32,
                succeeded: 0,
                failed: 0,
                pending: topics.len() as u32,
                failed_topics: vec![],
            };
            return Ok(Response::new(response));
        }

        // If there are no topics, directly mark broker as drained and return
        if topics.is_empty() {
            if let Err(e) = self
                .resources
                .cluster
                .set_broker_state(&target_broker, "drained", Some("no_topics"))
                .await
            {
                return Err(tonic::Status::internal(format!(
                    "Failed to set broker state to drained: {}",
                    e
                )));
            }

            let response = UnloadBrokerResponse {
                started: true,
                total: 0,
                succeeded: 0,
                failed: 0,
                pending: 0,
                failed_topics: vec![],
            };
            return Ok(Response::new(response));
        }

        // Set broker to draining
        if let Err(e) = self
            .resources
            .cluster
            .set_broker_state(&target_broker, "draining", Some("admin_unload"))
            .await
        {
            return Err(tonic::Status::internal(format!(
                "Failed to set broker state: {}",
                e
            )));
        }

        // Iterate topics and request unload (sequential for now)
        let mut succeeded = 0u32;
        let mut failed = 0u32;
        let mut failed_topics = Vec::new();
        for topic in topics.iter() {
            match self
                .broker_service
                .topic_cluster
                .post_unload_topic(topic)
                .await
            {
                Ok(()) => succeeded += 1,
                Err(e) => {
                    failed += 1;
                    failed_topics.push(format!("{}: {}", topic, e));
                }
            }
        }

        // Do not set to drained here; the broker-side watcher will unload locally and when empty, an admin can set drained.
        // For now, report summary.
        let total = topics.len() as u32;
        let pending = total.saturating_sub(succeeded + failed);
        let response = UnloadBrokerResponse {
            started: true,
            total,
            succeeded,
            failed,
            pending,
            failed_topics,
        };
        Ok(Response::new(response))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn get_cluster_balance(
        &self,
        _request: Request<ClusterBalanceRequest>,
    ) -> std::result::Result<Response<ClusterBalanceResponse>, tonic::Status> {
        trace!("get cluster balance command");

        // Calculate current imbalance metrics
        let metrics = self.load_manager.calculate_imbalance().await.map_err(|e| {
            tonic::Status::internal(format!("Failed to calculate imbalance: {}", e))
        })?;

        // Get broker rankings for detailed information
        let rankings = self.load_manager.get_rankings().await;

        // Get actual topic counts from broker usage reports
        let brokers_usage = self.load_manager.get_brokers_usage().await;

        // Build broker load info list
        let mut broker_infos = Vec::new();
        for (broker_id, load) in rankings.iter() {
            let is_overloaded = metrics.overloaded_brokers.contains(broker_id);
            let is_underloaded = metrics.underloaded_brokers.contains(broker_id);

            // Get actual topic count from load report (not the ranking score!)
            let actual_topic_count = brokers_usage
                .get(broker_id)
                .map(|report| report.topics.len() as u32)
                .unwrap_or(0);

            broker_infos.push(BrokerLoadInfo {
                broker_id: *broker_id,
                load: *load as f64,
                topic_count: actual_topic_count,
                is_overloaded,
                is_underloaded,
            });
        }

        let response = ClusterBalanceResponse {
            coefficient_of_variation: metrics.coefficient_of_variation,
            mean_load: metrics.mean_load,
            max_load: metrics.max_load,
            min_load: metrics.min_load,
            std_deviation: metrics.std_deviation,
            broker_count: rankings.len() as u32,
            brokers: broker_infos,
            assignment_strategy: format!("{:?}", self.load_manager.get_assignment_strategy()),
        };

        Ok(Response::new(response))
    }

    #[tracing::instrument(level = Level::INFO, skip_all)]
    async fn trigger_rebalance(
        &self,
        request: Request<RebalanceRequest>,
    ) -> std::result::Result<Response<RebalanceResponse>, tonic::Status> {
        let req = request.into_inner();

        info!(
            dry_run = req.dry_run,
            max_moves = ?req.max_moves,
            "trigger rebalance command received"
        );

        // Get load manager config
        let config = self
            .load_manager
            .get_rebalancing_config()
            .await
            .ok_or_else(|| {
                tonic::Status::failed_precondition("Rebalancing is not configured on this broker")
            })?;

        // Calculate imbalance
        let metrics = self.load_manager.calculate_imbalance().await.map_err(|e| {
            tonic::Status::internal(format!("Failed to calculate imbalance: {}", e))
        })?;

        // Check if rebalancing is needed based on CV threshold
        // Note: For manual triggers via CLI, we do NOT check config.enabled
        // The enabled flag is for automated background rebalancing only
        let threshold = config.aggressiveness.threshold();
        if metrics.coefficient_of_variation < threshold {
            return Ok(Response::new(RebalanceResponse {
                success: true,
                moves_executed: 0,
                proposed_moves: vec![],
                error_message: format!(
                    "Cluster is balanced (CV: {:.3}, threshold: {:.3})",
                    metrics.coefficient_of_variation, threshold
                ),
            }));
        }

        // Select first rebalancing candidate to return to CLI
        let first_candidate = self
            .load_manager
            .select_rebalancing_candidate(&metrics, &config)
            .await
            .map_err(|e| tonic::Status::internal(format!("Failed to select candidate: {}", e)))?;

        if first_candidate.is_none() {
            return Ok(Response::new(RebalanceResponse {
                success: true,
                moves_executed: 0,
                proposed_moves: vec![],
                error_message: "No suitable topics found for rebalancing".to_string(),
            }));
        }

        let first_move = first_candidate.unwrap();
        let proposed_moves = vec![ProposedMove {
            topic_name: first_move.topic_name.clone(),
            from_broker: first_move.from_broker,
            to_broker: first_move.to_broker,
            estimated_load: first_move.estimated_load,
            reason: format!("{:?}", first_move.reason),
        }];

        // If dry-run, just return the first proposed move
        if req.dry_run {
            info!("dry-run completed, showing first proposed move");
            return Ok(Response::new(RebalanceResponse {
                success: true,
                moves_executed: 0,
                proposed_moves,
                error_message: "Dry run: rebalancing will continue until balanced".to_string(),
            }));
        }

        // Execute rebalancing in background task - loop until cluster is balanced
        let load_manager = self.load_manager.clone();
        let max_moves = req.max_moves.map(|m| m as usize);
        tokio::spawn(async move {
            let mut total_executed = 0usize;
            let mut current_move = Some(first_move);

            loop {
                // Execute the current move
                if let Some(mv) = current_move.take() {
                    match load_manager.execute_rebalancing(vec![mv], &config).await {
                        Ok(executed) => {
                            total_executed += executed;
                            info!(
                                total_executed = total_executed,
                                "rebalancing move completed"
                            );
                        }
                        Err(e) => {
                            warn!(error = %e, "rebalancing move failed");
                            break;
                        }
                    }
                }

                // Check max_moves limit
                if let Some(max) = max_moves {
                    if total_executed >= max {
                        info!(
                            total_executed = total_executed,
                            max = max,
                            "max moves reached"
                        );
                        break;
                    }
                }

                // Recalculate imbalance
                let new_metrics = match load_manager.calculate_imbalance().await {
                    Ok(m) => m,
                    Err(e) => {
                        warn!(error = %e, "failed to recalculate imbalance");
                        break;
                    }
                };

                // Check if balanced
                if new_metrics.coefficient_of_variation < threshold {
                    info!(
                        cv = new_metrics.coefficient_of_variation,
                        threshold = threshold,
                        total_executed = total_executed,
                        "cluster is now balanced"
                    );
                    break;
                }

                // Select next candidate
                current_move = match load_manager
                    .select_rebalancing_candidate(&new_metrics, &config)
                    .await
                {
                    Ok(Some(mv)) => Some(mv),
                    Ok(None) => {
                        info!(
                            total_executed = total_executed,
                            "no more candidates available"
                        );
                        break;
                    }
                    Err(e) => {
                        warn!(error = %e, "failed to select next candidate");
                        break;
                    }
                };
            }

            info!(
                total_executed = total_executed,
                "background rebalancing finished"
            );
        });

        info!("rebalancing started in background (will continue until balanced)");

        Ok(Response::new(RebalanceResponse {
            success: true,
            moves_executed: 1, // First move initiated
            proposed_moves,
            error_message: "Rebalancing started in background, will continue until balanced"
                .to_string(),
        }))
    }
}
