use crate::metadata_storage::MetadataStorage;
use crate::resources::{BASE_AUTH_BINDINGS_PATH, BASE_AUTH_ROLES_PATH};
use crate::security::authz::{Binding, Role};
use crate::utils::join_path;
use anyhow::Result;
use danube_core::metadata::{MetaOptions, MetadataStore};
use futures::StreamExt;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

// ── Cache ──────────────────────────────────────────────────────────────────

/// In-memory snapshot of all roles and bindings.
/// Roles and bindings are small, rarely mutated, and read on every request —
/// the textbook case for an eager cache with watch-based invalidation.
#[derive(Debug, Clone, Default)]
struct AuthzCache {
    /// role_name → Role
    roles: HashMap<String, Role>,
    /// (scope, resource_name) → Vec<Binding>
    ///
    /// For cluster scope, resource_name is "".
    /// For namespace scope, resource_name is the namespace.
    /// For topic scope, resource_name is the full topic path.
    bindings: HashMap<(String, String), Vec<Binding>>,
}

// ── SecurityResources ──────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub(crate) struct SecurityResources {
    store: MetadataStorage,
    cache: Arc<RwLock<AuthzCache>>,
    /// JWT subjects that bypass RBAC entirely (super-admin access).
    /// Loaded once from config at startup — immutable at runtime.
    super_admins: HashSet<String>,
}

impl SecurityResources {
    pub(crate) fn new(store: MetadataStorage, super_admins: Vec<String>) -> Self {
        let super_admin_set: HashSet<String> = super_admins.into_iter().collect();
        if !super_admin_set.is_empty() {
            info!(
                super_admins = ?super_admin_set,
                "super-admin subjects configured (bypass RBAC)"
            );
        }
        Self {
            store,
            cache: Arc::new(RwLock::new(AuthzCache::default())),
            super_admins: super_admin_set,
        }
    }

    /// Check if a principal name is a configured super-admin.
    pub(crate) fn is_super_admin(&self, subject: &str) -> bool {
        self.super_admins.contains(subject)
    }

    // ── Startup ────────────────────────────────────────────────────

    /// Bulk-load all roles and bindings into the in-memory cache.
    /// Call once during broker startup, before accepting requests.
    pub(crate) async fn load_cache(&self) -> Result<()> {
        let roles = self.load_all_roles_from_store().await?;
        let bindings = self.load_all_bindings_from_store().await?;

        let role_count = roles.len();
        let binding_count: usize = bindings.values().map(|v| v.len()).sum();

        let mut cache = self.cache.write().await;
        cache.roles = roles;
        cache.bindings = bindings;

        info!(
            roles = role_count,
            bindings = binding_count,
            "authorization cache loaded"
        );
        Ok(())
    }

    /// Spawn a background task that watches `/auth/` and reloads the cache
    /// Watch `/auth/` for role/binding mutations and reload the cache.
    ///
    /// Follows the same pattern as `broker_watcher::watch_events_for_broker`:
    /// creates the watch stream, spawns processing in a background task.
    /// Handles broadcast lag by doing a full resync (same as load_manager).
    pub(crate) async fn start_watcher(&self) {
        let base_path = "/auth/";

        match self.store.watch(base_path).await {
            Ok(mut watch_stream) => {
                let store = self.store.clone();
                let cache = self.cache.clone();

                tokio::spawn(async move {
                    while let Some(result) = watch_stream.next().await {
                        match result {
                            Ok(event) => {
                                debug!(%event, "auth metadata changed, reloading cache");
                                if let Err(e) = reload_cache(&store, &cache).await {
                                    warn!("failed to reload authorization cache: {}", e);
                                }
                            }
                            Err(danube_core::metadata::MetadataError::WatchError(msg))
                                if msg.contains("lagged") =>
                            {
                                warn!("authorization watcher stream lagged — resyncing cache");
                                if let Err(e) = reload_cache(&store, &cache).await {
                                    warn!("failed to resync authorization cache after lag: {}", e);
                                }
                            }
                            Err(e) => {
                                warn!(error = %e, "error receiving auth watch event");
                            }
                        }
                    }
                });
            }
            Err(e) => {
                warn!(error = %e, "failed to create auth watch stream");
            }
        }
    }

    // ── Cached reads (hot path) ────────────────────────────────────

    pub(crate) async fn get_role(&self, role_name: &str) -> Result<Option<Role>> {
        let cache = self.cache.read().await;
        Ok(cache.roles.get(role_name).cloned())
    }

    pub(crate) async fn list_role_names(&self) -> Result<Vec<String>> {
        let cache = self.cache.read().await;
        Ok(cache.roles.keys().cloned().collect())
    }

    /// Collect bindings from all relevant scopes for a given resource.
    /// For a topic `/ns/topic`, returns: cluster + namespace(ns) + topic bindings.
    /// For a namespace resource, returns: cluster + namespace bindings.
    /// For cluster/broker resources, returns only cluster bindings.
    pub(crate) async fn list_bindings_for_resource(
        &self,
        scope: &str,
        resource_name: &str,
    ) -> Result<Vec<Binding>> {
        let cache = self.cache.read().await;
        let mut all = Vec::new();

        // Always include cluster-scoped bindings
        if let Some(cluster_bindings) = cache.bindings.get(&("cluster".to_string(), String::new()))
        {
            all.extend(cluster_bindings.iter().cloned());
        }

        if scope == "namespace" || scope == "topic" {
            let ns = if scope == "topic" {
                extract_namespace(resource_name)
            } else {
                resource_name.to_string()
            };
            if !ns.is_empty() {
                if let Some(ns_bindings) = cache.bindings.get(&("namespace".to_string(), ns)) {
                    all.extend(ns_bindings.iter().cloned());
                }
            }
        }

        if scope == "topic" && !resource_name.is_empty() {
            if let Some(topic_bindings) = cache
                .bindings
                .get(&("topic".to_string(), resource_name.to_string()))
            {
                all.extend(topic_bindings.iter().cloned());
            }
        }

        Ok(all)
    }

    /// List all bindings under a specific scope+resource.
    pub(crate) async fn list_bindings(
        &self,
        scope: &str,
        resource_name: &str,
    ) -> Result<Vec<Binding>> {
        let cache = self.cache.read().await;
        let key = (scope.to_string(), resource_name.to_string());
        Ok(cache.bindings.get(&key).cloned().unwrap_or_default())
    }

    /// List all bindings across all scopes and resources.
    pub(crate) async fn list_all_bindings(&self) -> Result<Vec<Binding>> {
        let cache = self.cache.read().await;
        let mut all = Vec::new();
        for bindings in cache.bindings.values() {
            all.extend(bindings.iter().cloned());
        }
        Ok(all)
    }

    // ── Writes (update store first, then cache) ────────────────────

    pub(crate) async fn put_role(&self, role: &Role) -> Result<()> {
        let path = join_path(&[BASE_AUTH_ROLES_PATH, &role.name]);
        let value = serde_json::to_value(role)?;
        self.store.put(&path, value, MetaOptions::None).await?;

        // Cache will be updated by the watcher, but also update eagerly
        // to avoid a brief window where the just-created role isn't visible.
        let mut cache = self.cache.write().await;
        cache.roles.insert(role.name.clone(), role.clone());
        Ok(())
    }

    pub(crate) async fn put_binding(&self, binding: &Binding) -> Result<()> {
        let path = self.binding_path(&binding.scope, &binding.resource_name, &binding.id);
        let value = serde_json::to_value(binding)?;
        self.store.put(&path, value, MetaOptions::None).await?;

        // Eager cache update
        let mut cache = self.cache.write().await;
        let key = (binding.scope.clone(), binding.resource_name.clone());
        cache.bindings.entry(key).or_default().push(binding.clone());
        Ok(())
    }

    pub(crate) async fn get_binding(
        &self,
        scope: &str,
        resource_name: &str,
        binding_id: &str,
    ) -> Result<Option<Binding>> {
        let cache = self.cache.read().await;
        let key = (scope.to_string(), resource_name.to_string());
        Ok(cache
            .bindings
            .get(&key)
            .and_then(|bindings| bindings.iter().find(|b| b.id == binding_id))
            .cloned())
    }

    pub(crate) async fn delete_role(&self, role_name: &str) -> Result<()> {
        let path = join_path(&[BASE_AUTH_ROLES_PATH, role_name]);
        self.store.delete(&path).await?;

        // Eager cache removal
        let mut cache = self.cache.write().await;
        cache.roles.remove(role_name);
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

        // Eager cache removal
        let mut cache = self.cache.write().await;
        let key = (scope.to_string(), resource_name.to_string());
        if let Some(bindings) = cache.bindings.get_mut(&key) {
            bindings.retain(|b| b.id != binding_id);
            if bindings.is_empty() {
                cache.bindings.remove(&key);
            }
        }
        Ok(())
    }

    // ── Internal: store reads for cache loading ────────────────────

    async fn load_all_roles_from_store(&self) -> Result<HashMap<String, Role>> {
        let entries = self
            .store
            .get_bulk(BASE_AUTH_ROLES_PATH)
            .await
            .unwrap_or_default();
        let mut roles = HashMap::with_capacity(entries.len());
        for entry in entries {
            match serde_json::from_slice::<Role>(&entry.value) {
                Ok(role) => {
                    roles.insert(role.name.clone(), role);
                }
                Err(e) => warn!(key = %entry.key, "skipping malformed role: {}", e),
            }
        }
        Ok(roles)
    }

    async fn load_all_bindings_from_store(
        &self,
    ) -> Result<HashMap<(String, String), Vec<Binding>>> {
        let mut bindings: HashMap<(String, String), Vec<Binding>> = HashMap::new();

        let entries = self
            .store
            .get_bulk(BASE_AUTH_BINDINGS_PATH)
            .await
            .unwrap_or_default();

        for entry in entries {
            match serde_json::from_slice::<Binding>(&entry.value) {
                Ok(b) => {
                    let key = (b.scope.clone(), b.resource_name.clone());
                    bindings.entry(key).or_default().push(b);
                }
                Err(e) => {
                    warn!(key = %entry.key, "skipping malformed binding: {}", e);
                }
            }
        }

        Ok(bindings)
    }

    // ── Internal: path helpers ─────────────────────────────────────

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

// ── Module-level helpers ───────────────────────────────────────────────────

/// Full reload of the cache from the store.
/// Called by the background watcher on any `/auth/` mutation.
async fn reload_cache(store: &MetadataStorage, cache: &Arc<RwLock<AuthzCache>>) -> Result<()> {
    // Build a temporary SecurityResources to reuse the load logic.
    // This is cheap — SecurityResources is just two Arcs.
    let tmp = SecurityResources {
        store: store.clone(),
        cache: cache.clone(),
        super_admins: HashSet::new(), // Not used during reload, just satisfying the struct
    };

    let roles = tmp.load_all_roles_from_store().await?;
    let bindings = tmp.load_all_bindings_from_store().await?;

    let role_count = roles.len();
    let binding_count: usize = bindings.values().map(|v| v.len()).sum();

    let mut c = cache.write().await;
    c.roles = roles;
    c.bindings = bindings;

    debug!(
        roles = role_count,
        bindings = binding_count,
        "authorization cache reloaded"
    );
    Ok(())
}

/// Extract namespace from a topic path like "/namespace/topic_name" → "namespace"
fn extract_namespace(topic_path: &str) -> String {
    let trimmed = topic_path.trim_start_matches('/');
    trimmed.split('/').next().unwrap_or("").to_string()
}
