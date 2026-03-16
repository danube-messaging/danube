// Centralized metric name constants for the persistent storage crate (WAL + Cloud)
// Mirrors the style of danube-broker/src/broker_metrics.rs without cross-crate deps.

#[derive(Debug, Clone, Copy)]
pub struct Metric {
    pub name: &'static str,
    #[allow(dead_code)]
    pub description: &'static str,
}

// WAL metrics
pub const WAL_APPEND_TOTAL: Metric = Metric {
    name: "danube_wal_append_total",
    description: "Total number of messages appended to the WAL (per topic)",
};

pub const WAL_APPEND_BYTES_TOTAL: Metric = Metric {
    name: "danube_wal_append_bytes_total",
    description: "Total bytes appended to the WAL (per topic)",
};

pub const WAL_FLUSH_LATENCY_MS: Metric = Metric {
    name: "danube_wal_flush_latency_ms",
    description: "Latency to flush WAL buffers to disk (write+flush) (per topic)",
};

pub const WAL_FSYNC_TOTAL: Metric = Metric {
    name: "danube_wal_fsync_total",
    description: "Total number of WAL flush/fsync operations (per topic)",
};

pub const WAL_FILE_ROTATE_TOTAL: Metric = Metric {
    name: "danube_wal_file_rotate_total",
    description: "Total number of WAL file rotations (per topic, reason={size,time})",
};

pub const WAL_READER_CREATE_TOTAL: Metric = Metric {
    name: "danube_wal_reader_create_total",
    description: "Total WAL readers created (per topic, mode={wal_only,durable_history_then_hot})",
};

pub const WAL_DELETE_TOTAL: Metric = Metric {
    name: "danube_wal_delete_total",
    description: "Total number of WAL files deleted by retention (per topic)",
};

// Cloud metrics (proposed; not yet wired)
pub const CLOUD_UPLOAD_OBJECTS_TOTAL: Metric = Metric {
    name: "danube_cloud_upload_objects_total",
    description: "Total number of cloud objects uploaded (per topic, provider, result)",
};

pub const CLOUD_UPLOAD_BYTES_TOTAL: Metric = Metric {
    name: "danube_cloud_upload_bytes_total",
    description: "Total bytes uploaded to cloud (per topic, provider)",
};

pub const CLOUD_UPLOAD_LATENCY_MS: Metric = Metric {
    name: "danube_cloud_upload_latency_ms",
    description: "Latency to upload and finalize one cloud object (provider)",
};

#[allow(dead_code)]
pub const CLOUD_LIST_TOTAL: Metric = Metric {
    name: "danube_cloud_list_total",
    description: "Total number of cloud list operations (provider)",
};

#[allow(dead_code)]
pub const CLOUD_LIST_LATENCY_MS: Metric = Metric {
    name: "danube_cloud_list_latency_ms",
    description: "Latency of cloud list operations (provider)",
};

pub const DURABLE_HISTORY_TO_HOT_TOTAL: Metric = Metric {
    name: "danube_handoff_durable_history_to_hot_total",
    description: "Total number of reader handoffs from durable history to hot state (per topic)",
};

pub const DURABLE_HISTORY_SEGMENTS_READ_TOTAL: Metric = Metric {
    name: "danube_durable_history_segments_read_total",
    description: "Total number of durable history segments opened for reading (per topic, provider)",
};

pub const DURABLE_HISTORY_READ_BYTES_TOTAL: Metric = Metric {
    name: "danube_durable_history_read_bytes_total",
    description: "Total bytes read from durable history segments (per topic, provider)",
};

pub const DURABLE_HISTORY_READER_ERRORS_TOTAL: Metric = Metric {
    name: "danube_durable_history_reader_errors_total",
    description: "Total number of durable history reader decode errors (provider, reason)",
};

// Optional registrations if you want arrays for initialization/checks
#[allow(dead_code)]
pub const COUNTERS: &[Metric] = &[
    WAL_APPEND_TOTAL,
    WAL_APPEND_BYTES_TOTAL,
    WAL_FSYNC_TOTAL,
    WAL_FILE_ROTATE_TOTAL,
    WAL_READER_CREATE_TOTAL,
    WAL_DELETE_TOTAL,
    CLOUD_UPLOAD_OBJECTS_TOTAL,
    CLOUD_UPLOAD_BYTES_TOTAL,
    CLOUD_LIST_TOTAL,
    DURABLE_HISTORY_TO_HOT_TOTAL,
];

#[allow(dead_code)]
pub const HISTOGRAMS: &[Metric] = &[
    WAL_FLUSH_LATENCY_MS,
    CLOUD_UPLOAD_LATENCY_MS,
    CLOUD_LIST_LATENCY_MS,
];
