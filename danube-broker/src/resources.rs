use danube_metadata_store::MetadataStorage;

use crate::LocalCache;

mod cluster;
mod namespace;
mod schema;
mod topic;

pub(crate) use cluster::ClusterResources;
pub(crate) use namespace::NamespaceResources;
pub(crate) use schema::SchemaResources;
pub(crate) use topic::TopicResources;

pub(crate) static BASE_CLUSTER_PATH: &str = "/cluster";
pub(crate) static BASE_REGISTER_PATH: &str = "/cluster/register";
pub(crate) static BASE_BROKER_PATH: &str = "/cluster/brokers";
pub(crate) static BASE_NAMESPACES_PATH: &str = "/namespaces";
pub(crate) static BASE_TOPICS_PATH: &str = "/topics";
pub(crate) static BASE_SUBSCRIPTIONS_PATH: &str = "/subscriptions";
pub(crate) static BASE_SCHEMAS_PATH: &str = "/schemas";

// Once new topic is created, it is posted to unassigned path in order to be alocated by Load Manager to a broker
pub(crate) static BASE_UNASSIGNED_PATH: &str = "/cluster/unassigned";

// Brokers post load reports on "/cluster/load/{broker_id}"
// Load Manager watch this path to calculate broker load rankings for the cluster
pub(crate) static BASE_BROKER_LOAD_PATH: &str = "/cluster/load";

// The Load Manager post it's decision and the calculated load metrics on this path
#[allow(dead_code)]
pub(crate) static LOADBALANCE_DECISION_PATH: &str = "/cluster/load_balance";

// Cluster Leader broker_id, posted by Leader Election Service
pub(crate) static LEADER_ELECTION_PATH: &str = "/cluster/leader";

pub(crate) static DEFAULT_NAMESPACE: &str = "default";
pub(crate) static SYSTEM_NAMESPACE: &str = "system";

// Resources provides the mechanisms to store and retrieve specific information from MetadataStore
//
// The docs/internal_resources.md document describe how the resources are organized in Metadata Store
//
// Different starting paths create a clear hierarchical structure
// that reflects the logical organization of the Danube messaging system.
// It allows for the separation of concerns & efficient querying, ensuring that publisher, consumer, topic,
// and namespace configurations are neatly organized and do not get mixed up.
// Reduced Overhead: By limiting the scope of data retrievals to a specific path,
// the overhead of processing unnecessary data, is reduced.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct Resources {
    store: MetadataStorage,
    pub(crate) cluster: ClusterResources,
    pub(crate) namespace: NamespaceResources,
    pub(crate) topic: TopicResources,
    pub(crate) schema: SchemaResources,
    // should hold also the MetadataStore,
    // as the resources translate the Danube requests into MetadataStore paths puts & gets
}

// A wrapper for interacting with Metadata Storage
impl Resources {
    pub(crate) fn new(local_cache: LocalCache, store: MetadataStorage) -> Self {
        Resources {
            store: store.clone(),
            cluster: ClusterResources::new(local_cache.clone(), store.clone()),
            namespace: NamespaceResources::new(local_cache.clone(), store.clone()),
            topic: TopicResources::new(local_cache.clone(), store.clone()),
            schema: SchemaResources::new(local_cache, store),
        }
    }
}
