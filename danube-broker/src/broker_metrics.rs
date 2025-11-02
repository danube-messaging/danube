use metrics_exporter_prometheus::PrometheusBuilder;
use tracing::info;

pub(crate) struct Metric {
    pub name: &'static str,
    description: &'static str,
}

pub(crate) const COUNTERS: [Metric; 4] = [
    TOPIC_MESSAGES_IN_TOTAL,
    TOPIC_BYTES_IN_TOTAL,
    CONSUMER_MESSAGES_OUT_TOTAL,
    CONSUMER_BYTES_OUT_TOTAL,
];
pub(crate) const GAUGES: [Metric; 3] = [
    BROKER_TOPICS_OWNED,
    TOPIC_ACTIVE_PRODUCERS,
    TOPIC_ACTIVE_CONSUMERS,
];
pub(crate) const HISTOGRAMS: [Metric; 1] = [PRODUCER_SEND_LATENCY_MS];

// BROKER Metrics --------------------------

pub(crate) const BROKER_TOPICS_OWNED: Metric = Metric {
    name: "danube_broker_topics_owned",
    description: "Total number of topics served by broker",
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

pub(crate) const TOPIC_ACTIVE_PRODUCERS: Metric = Metric {
    name: "danube_topic_active_producers",
    description: "Total number of producers per topic",
};

pub(crate) const TOPIC_ACTIVE_CONSUMERS: Metric = Metric {
    name: "danube_topic_active_consumers",
    description: "Total number of consumers per topic",
};

// PRODUCER Metrics --------------------------

pub(crate) const PRODUCER_SEND_LATENCY_MS: Metric = Metric {
    name: "danube_producer_send_latency_ms",
    description: "End-to-end producer send latency in milliseconds (broker-side).",
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

pub(crate) fn init_metrics(prom_addr: Option<std::net::SocketAddr>) {
    info!("Initializing metrics exporter");

    if let Some(addr) = prom_addr {
        PrometheusBuilder::new()
            .with_http_listener(addr)
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
