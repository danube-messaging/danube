use crate::metadata_storage::MetadataStorage;
use crate::resources::{
    BASE_AUTH_BINDINGS_PATH, BASE_AUTH_ROLES_PATH,
};
use crate::security::authz::{Binding, Role};
use crate::utils::join_path;
use anyhow::Result;
use danube_core::metadata::{MetaOptions, MetadataStore};
use tracing::warn;

#[derive(Debug, Clone)]
pub(crate) struct SecurityResources {
    store: MetadataStorage,
}

impl SecurityResources {
    pub(crate) fn new(store: MetadataStorage) -> Self {
        Self { store }
    }

    // ── Roles ──────────────────────────────────────────────────────

    pub(crate) async fn put_role(&self, role: &Role) -> Result<()> {
        let path = join_path(&[BASE_AUTH_ROLES_PATH, &role.name]);
        let value = serde_json::to_value(role)?;
        self.store.put(&path, value, MetaOptions::None).await?;
        Ok(())
    }

    pub(crate) async fn get_role(&self, role_name: &str) -> Result<Option<Role>> {
        let path = join_path(&[BASE_AUTH_ROLES_PATH, role_name]);
        match self.store.get(&path, MetaOptions::None).await? {
            Some(value) => Ok(Some(serde_json::from_value(value)?)),
            None => Ok(None),
        }
    }

    pub(crate) async fn list_role_names(&self) -> Result<Vec<String>> {
        Ok(self.store.get_childrens(BASE_AUTH_ROLES_PATH).await?)
    }

    // ── Bindings ───────────────────────────────────────────────────
    //
    // Storage layout:
    //   /auth/bindings/cluster/{binding_id}          – cluster-scoped
    //   /auth/bindings/namespace/{ns}/{binding_id}    – namespace-scoped
    //   /auth/bindings/topic/{topic}/{binding_id}     – topic-scoped

    pub(crate) async fn put_binding(&self, binding: &Binding) -> Result<()> {
        let path = self.binding_path(&binding.scope, &binding.resource_name, &binding.id);
        let value = serde_json::to_value(binding)?;
        self.store.put(&path, value, MetaOptions::None).await?;
        Ok(())
    }

    pub(crate) async fn get_binding(
        &self,
        scope: &str,
        resource_name: &str,
        binding_id: &str,
    ) -> Result<Option<Binding>> {
        let path = self.binding_path(scope, resource_name, binding_id);
        match self.store.get(&path, MetaOptions::None).await? {
            Some(value) => Ok(Some(serde_json::from_value(value)?)),
            None => Ok(None),
        }
    }

    /// List all bindings under a specific scope+resource.
    /// For cluster scope, resource_name is ignored (pass "").
    pub(crate) async fn list_bindings(
        &self,
        scope: &str,
        resource_name: &str,
    ) -> Result<Vec<Binding>> {
        let parent = self.binding_scope_path(scope, resource_name);
        let ids = self.store.get_childrens(&parent).await?;
        let mut bindings = Vec::with_capacity(ids.len());
        for id in ids {
            let path = join_path(&[&parent, &id]);
            if let Some(value) = self.store.get(&path, MetaOptions::None).await? {
                match serde_json::from_value::<Binding>(value) {
                    Ok(b) => bindings.push(b),
                    Err(e) => warn!(binding_id = %id, "skipping malformed binding: {}", e),
                }
            }
        }
        Ok(bindings)
    }

    /// Collect bindings from all relevant scopes for a given resource.
    /// For a topic `/ns/topic`, this returns: cluster bindings + namespace bindings for `ns` + topic bindings for the full topic path.
    /// For a namespace resource, this returns: cluster bindings + namespace bindings.
    /// For cluster/broker resources, this returns only cluster bindings.
    pub(crate) async fn list_bindings_for_resource(
        &self,
        scope: &str,
        resource_name: &str,
    ) -> Result<Vec<Binding>> {
        let mut all = self.list_bindings("cluster", "").await?;

        if scope == "namespace" || scope == "topic" {
            // Extract namespace from resource_name: for topics like "/ns/topic", namespace is "ns"
            let ns = if scope == "topic" {
                extract_namespace(resource_name)
            } else {
                resource_name.to_string()
            };
            if !ns.is_empty() {
                all.extend(self.list_bindings("namespace", &ns).await?);
            }
        }

        if scope == "topic" && !resource_name.is_empty() {
            all.extend(self.list_bindings("topic", resource_name).await?);
        }

        Ok(all)
    }

    // ── Deletions ──────────────────────────────────────────────────

    pub(crate) async fn delete_role(&self, role_name: &str) -> Result<()> {
        let path = join_path(&[BASE_AUTH_ROLES_PATH, role_name]);
        self.store.delete(&path).await?;
        Ok(())
    }

    pub(crate) async fn delete_binding(
        &self,
        scope: &str,
        resource_name: &str,
        binding_id: &str,
    ) -> Result<()> {
        let path = self.binding_path(scope, resource_name, binding_id);
        self.store.delete(&path).await?;
        Ok(())
    }

    // ── Internal helpers ───────────────────────────────────────────

    fn binding_scope_path(&self, scope: &str, resource_name: &str) -> String {
        match scope {
            "cluster" => join_path(&[BASE_AUTH_BINDINGS_PATH, "cluster"]),
            _ => join_path(&[BASE_AUTH_BINDINGS_PATH, scope, resource_name]),
        }
    }

    fn binding_path(&self, scope: &str, resource_name: &str, binding_id: &str) -> String {
        let parent = self.binding_scope_path(scope, resource_name);
        join_path(&[&parent, binding_id])
    }
}

/// Extract namespace from a topic path like "/namespace/topic_name" → "namespace"
fn extract_namespace(topic_path: &str) -> String {
    let trimmed = topic_path.trim_start_matches('/');
    trimmed
        .split('/')
        .next()
        .unwrap_or("")
        .to_string()
}
