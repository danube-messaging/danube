use metrics_exporter_prometheus::PrometheusBuilder;
use tracing::info;

pub(crate) struct Metric {
    pub name: &'static str,
    description: &'static str,
}

pub(crate) const COUNTERS: [Metric; 14] = [
    TOPIC_MESSAGES_IN_TOTAL,
    TOPIC_BYTES_IN_TOTAL,
    CONSUMER_MESSAGES_OUT_TOTAL,
    CONSUMER_BYTES_OUT_TOTAL,
    BROKER_ASSIGNMENTS_TOTAL,
    BROKER_RPC_TOTAL,
    CLIENT_REDIRECTS_TOTAL,
    PRODUCER_SEND_TOTAL,
    DISPATCHER_HEARTBEAT_POLLS_TOTAL,
    DISPATCHER_NOTIFIER_POLLS_TOTAL,
    SCHEMA_VALIDATION_TOTAL,
    SCHEMA_VALIDATION_FAILURES_TOTAL,
    REBALANCING_MOVES_TOTAL,
    REBALANCING_FAILURES_TOTAL,
];
pub(crate) const GAUGES: [Metric; 8] = [
    BROKER_TOPICS_OWNED,
    TOPIC_ACTIVE_PRODUCERS,
    TOPIC_ACTIVE_CONSUMERS,
    LEADER_ELECTION_STATE,
    TOPIC_ACTIVE_SUBSCRIPTIONS,
    SUBSCRIPTION_ACTIVE_CONSUMERS,
    SUBSCRIPTION_LAG_MESSAGES,
    CLUSTER_IMBALANCE_CV,
];
pub(crate) const HISTOGRAMS: [Metric; 3] = [
    PRODUCER_SEND_LATENCY_MS,
    TOPIC_MESSAGE_SIZE_BYTES,
    REBALANCING_CYCLE_DURATION_SECONDS,
];

// BROKER Metrics --------------------------

pub(crate) const BROKER_TOPICS_OWNED: Metric = Metric {
    name: "danube_broker_topics_owned",
    description: "Total number of topics served by broker",
};

pub(crate) const BROKER_ASSIGNMENTS_TOTAL: Metric = Metric {
    name: "danube_broker_assignments_total",
    description: "Total number of topic assignment operations performed (assign/unassign)",
};

pub(crate) const LEADER_ELECTION_STATE: Metric = Metric {
    name: "danube_leader_election_state",
    description: "Leader election state of this broker (0=follower,1=leader)",
};

pub(crate) const BROKER_RPC_TOTAL: Metric = Metric {
    name: "danube_broker_rpc_total",
    description: "Total RPC requests handled by the broker",
};

pub(crate) const CLIENT_REDIRECTS_TOTAL: Metric = Metric {
    name: "danube_client_redirects_total",
    description: "Total number of client redirects suggested by the broker during lookup",
};

pub(crate) const TOPIC_ACTIVE_SUBSCRIPTIONS: Metric = Metric {
    name: "danube_topic_active_subscriptions",
    description: "Total number of subscriptions per topic",
};

pub(crate) const SUBSCRIPTION_ACTIVE_CONSUMERS: Metric = Metric {
    name: "danube_subscription_active_consumers",
    description: "Total number of active consumers per subscription",
};

pub(crate) const SUBSCRIPTION_LAG_MESSAGES: Metric = Metric {
    name: "danube_subscription_lag_messages",
    description: "Number of messages the subscription is behind the topic WAL head",
};

// DISPATCHER Metrics --------------------------

pub(crate) const DISPATCHER_HEARTBEAT_POLLS_TOTAL: Metric = Metric {
    name: "danube_dispatcher_heartbeat_polls_total",
    description:
        "Total number of polls triggered by the heartbeat watchdog (indicates missed notifications)",
};

pub(crate) const DISPATCHER_NOTIFIER_POLLS_TOTAL: Metric = Metric {
    name: "danube_dispatcher_notifier_polls_total",
    description: "Total number of polls triggered by notifier (normal fast path)",
};

// TOPIC Metrics --------------------------

pub(crate) const TOPIC_MESSAGES_IN_TOTAL: Metric = Metric {
    name: "danube_topic_messages_in_total",
    description: "Total messages published to the topic (msg).",
};

pub(crate) const TOPIC_BYTES_IN_TOTAL: Metric = Metric {
    name: "danube_topic_bytes_in_total",
    description: "Total bytes published to the topic (bytes)",
};

pub(crate) const TOPIC_MESSAGE_SIZE_BYTES: Metric = Metric {
    name: "danube_topic_message_size_bytes",
    description: "Distribution of incoming message sizes per topic (bytes)",
};

pub(crate) const TOPIC_ACTIVE_PRODUCERS: Metric = Metric {
    name: "danube_topic_active_producers",
    description: "Total number of producers per topic",
};

pub(crate) const TOPIC_ACTIVE_CONSUMERS: Metric = Metric {
    name: "danube_topic_active_consumers",
    description: "Total number of consumers per topic",
};

// SCHEMA Metrics --------------------------

pub(crate) const SCHEMA_VALIDATION_TOTAL: Metric = Metric {
    name: "danube_schema_validation_total",
    description: "Total number of schema validation attempts",
};

pub(crate) const SCHEMA_VALIDATION_FAILURES_TOTAL: Metric = Metric {
    name: "danube_schema_validation_failures_total",
    description: "Total number of schema validation failures",
};

// PRODUCER Metrics --------------------------

pub(crate) const PRODUCER_SEND_LATENCY_MS: Metric = Metric {
    name: "danube_producer_send_latency_ms",
    description: "End-to-end producer send latency in milliseconds (broker-side).",
};

pub(crate) const PRODUCER_SEND_TOTAL: Metric = Metric {
    name: "danube_producer_send_total",
    description: "Total producer send attempts at the broker API boundary",
};

// CONSUMER Metrics --------------------------

pub(crate) const CONSUMER_MESSAGES_OUT_TOTAL: Metric = Metric {
    name: "danube_consumer_messages_out_total",
    description: "Total messages delivered to consumer (msg).",
};

pub(crate) const CONSUMER_BYTES_OUT_TOTAL: Metric = Metric {
    name: "danube_consumer_bytes_out_total",
    description: "Total bytes delivered to consumer (bytes)",
};

// REBALANCING Metrics (Phase 3) --------------------------

pub(crate) const REBALANCING_MOVES_TOTAL: Metric = Metric {
    name: "danube_rebalancing_moves_total",
    description: "Total number of topic rebalancing moves executed",
};

pub(crate) const REBALANCING_FAILURES_TOTAL: Metric = Metric {
    name: "danube_rebalancing_failures_total",
    description: "Total number of failed rebalancing move attempts",
};

pub(crate) const CLUSTER_IMBALANCE_CV: Metric = Metric {
    name: "danube_cluster_imbalance_cv",
    description: "Current cluster load imbalance coefficient of variation (CV). Lower is better, 0 is perfectly balanced.",
};

pub(crate) const REBALANCING_CYCLE_DURATION_SECONDS: Metric = Metric {
    name: "danube_rebalancing_cycle_duration_seconds",
    description: "Duration of rebalancing cycle execution in seconds",
};

pub(crate) fn init_metrics(prom_addr: Option<std::net::SocketAddr>, broker_id: u64) {
    info!("initializing metrics exporter");

    if let Some(addr) = prom_addr {
        PrometheusBuilder::new()
            .with_http_listener(addr)
            .add_global_label("broker", broker_id.to_string())
            .install()
            .expect("failed to install Prometheus recorder");
    }

    for name in COUNTERS {
        register_counter(name)
    }

    for name in GAUGES {
        register_gauge(name)
    }

    for name in HISTOGRAMS {
        register_histogram(name)
    }
}

/// Registers a counter with the given name.
fn register_counter(metric: Metric) {
    metrics::describe_counter!(metric.name, metric.description);
    let _counter = metrics::counter!(metric.name);
}

/// Registers a gauge with the given name.
fn register_gauge(metric: Metric) {
    metrics::describe_gauge!(metric.name, metric.description);
    let _gauge = metrics::gauge!(metric.name);
}

/// Registers a histogram with the given name.
fn register_histogram(metric: Metric) {
    metrics::describe_histogram!(metric.name, metric.description);
    let _histogram = metrics::histogram!(metric.name);
}
