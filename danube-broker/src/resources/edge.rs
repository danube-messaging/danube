use crate::metadata_storage::MetadataStorage;
use crate::resources::BASE_EDGE_REGISTRATIONS_PATH;
use crate::utils::join_path;
use anyhow::Result;
use danube_core::metadata::{MetaOptions, MetadataStore};
use danube_edge::cluster::auth::EdgeRegistration;
use tracing::debug;

/// Edge registration resources stored in the Raft metadata store.
///
/// Manages edge broker registrations under `/edge/registrations/{edge_name}`.
#[derive(Debug, Clone)]
pub(crate) struct EdgeResources {
    store: MetadataStorage,
}

impl EdgeResources {
    pub(crate) fn new(store: MetadataStorage) -> Self {
        EdgeResources { store }
    }

    /// Check if an edge is registered.
    pub(crate) async fn is_edge_registered(&self, edge_name: &str) -> bool {
        let path = join_path(&[BASE_EDGE_REGISTRATIONS_PATH, edge_name]);
        matches!(self.store.get(&path, MetaOptions::None).await, Ok(Some(_)))
    }

    /// Get the namespace for a registered edge.
    pub(crate) async fn get_edge_namespace(&self, edge_name: &str) -> Option<String> {
        let path = join_path(&[BASE_EDGE_REGISTRATIONS_PATH, edge_name]);
        match self.store.get(&path, MetaOptions::None).await {
            Ok(Some(value)) => {
                let reg: EdgeRegistration = serde_json::from_value(value).ok()?;
                Some(reg.namespace)
            }
            _ => None,
        }
    }

    /// Persist edge registration.
    pub(crate) async fn register_edge(&self, registration: &EdgeRegistration) -> Result<()> {
        let path = join_path(&[BASE_EDGE_REGISTRATIONS_PATH, &registration.edge_name]);
        let value = serde_json::to_value(registration)?;
        self.store.put(&path, value, MetaOptions::None).await?;
        debug!(
            edge_name = %registration.edge_name,
            namespace = %registration.namespace,
            "edge registration persisted in metadata store"
        );
        Ok(())
    }

    /// List all registered edges.
    #[allow(dead_code)]
    pub(crate) async fn list_edges(&self) -> Vec<EdgeRegistration> {
        let keys = self
            .store
            .get_childrens(BASE_EDGE_REGISTRATIONS_PATH)
            .await
            .unwrap_or_default();

        let mut edges = Vec::new();
        for key in keys {
            if let Ok(Some(value)) = self.store.get(&key, MetaOptions::None).await {
                if let Ok(reg) = serde_json::from_value::<EdgeRegistration>(value) {
                    edges.push(reg);
                }
            }
        }
        edges
    }
}
