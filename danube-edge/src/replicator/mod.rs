//! Edge replication pipeline: WAL tailing, batching, cloud client, and checkpointing.

pub mod checkpoint;
pub mod cluster_client;
pub mod replicator;
pub mod topic_replicator;
