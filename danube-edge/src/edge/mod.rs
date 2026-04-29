//! Edge-side replication: WAL tailing, batching, cloud client, and checkpointing.

pub mod checkpoint;
pub mod cloud_client;
pub mod replicator;
pub mod topic_replicator;
