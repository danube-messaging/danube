//! Broker-side adapters for the edge replicator traits.
//!
//! Implements `EdgeAuth` and `ReplicationStorage` using the broker's
//! existing `Resources` and `StorageFactory`.

pub(crate) mod auth_adapter;
pub(crate) mod storage_adapter;

/// Concrete edge replicator service using the broker's auth and storage adapters.
pub(crate) type BrokerEdgeService = danube_edge::cluster::service::EdgeReplicatorServiceImpl<
    storage_adapter::BrokerReplicationStorage,
    auth_adapter::BrokerEdgeAuth,
>;
