use std::collections::{BTreeSet, HashMap};

use danube_core::admin_proto::PlannedAction;
use danube_persistent_storage::{ObjectStoreBackend, ObjectStoreConfig};
use serde::Deserialize;
use tonic::Status;

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct DesiredStateManifest {
    #[serde(default)]
    pub(crate) namespaces: Vec<String>,
    #[serde(default)]
    pub(crate) topics: Vec<DesiredTopicSpec>,
    #[serde(default)]
    pub(crate) object_store: Option<DesiredObjectStoreSpec>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct DesiredTopicSpec {
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) intent: Option<String>,
    #[serde(default)]
    pub(crate) partitions: Option<u32>,
    #[serde(default)]
    pub(crate) dispatch_strategy: Option<String>,
    #[serde(default)]
    pub(crate) schema_subject: Option<String>,
    #[serde(default)]
    pub(crate) storage_mode: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct DesiredObjectStoreSpec {
    pub(crate) backend: String,
    pub(crate) root: String,
    #[serde(default)]
    pub(crate) options: HashMap<String, String>,
    #[serde(default)]
    pub(crate) force: bool,
}

impl DesiredStateManifest {
    /// Parse a YAML or JSON desired-state manifest.
    pub(crate) fn parse(raw: &str) -> Result<Self, Status> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(Status::invalid_argument("desired-state manifest is empty"));
        }
        serde_json::from_str(trimmed)
            .or_else(|_| serde_yaml::from_str(trimmed))
            .map_err(|e| Status::invalid_argument(format!("invalid desired-state manifest: {}", e)))
    }
}

/// Convert backend name + root + options into a `StorageFactory` `ObjectStoreConfig`.
pub(crate) fn build_object_store_config(
    backend: &str,
    root: String,
    options: HashMap<String, String>,
) -> Result<ObjectStoreConfig, Status> {
    if root.trim().is_empty() {
        return Err(Status::invalid_argument("object_store root is required"));
    }
    match backend.trim().to_ascii_lowercase().as_str() {
        "s3" => Ok(ObjectStoreConfig::new(ObjectStoreBackend::S3, root).with_options(options)),
        "gcs" | "gs" => {
            Ok(ObjectStoreConfig::new(ObjectStoreBackend::Gcs, root).with_options(options))
        }
        "azblob" | "azure" => {
            Ok(ObjectStoreConfig::new(ObjectStoreBackend::Azblob, root).with_options(options))
        }
        "filesystem" | "local" | "fs" => Ok(ObjectStoreConfig::filesystem_for_tests(root)),
        other => Err(Status::invalid_argument(format!(
            "unsupported object_store backend '{}' (expected s3, gcs, azblob, filesystem)",
            other
        ))),
    }
}

/// Extract namespace name from a Danube topic path like `/ns/topic` or `/ns/a/b`.
fn namespace_of_topic(topic: &str) -> Option<&str> {
    let trimmed = topic.trim_start_matches('/');
    trimmed.split('/').next().filter(|s| !s.is_empty())
}

/// Compute a safe dry-run plan for `DesiredStateManifest`.
///
/// Additive by default: `delete_topic` actions are only emitted when `prune == true`,
/// and `/system/*` internal topics are never pruned.
pub(crate) fn compute_plan(
    manifest: &DesiredStateManifest,
    existing_namespaces: &[String],
    existing_topics: &[String],
    prune: bool,
) -> Vec<PlannedAction> {
    let mut actions = Vec::new();
    let existing_ns: BTreeSet<&str> = existing_namespaces.iter().map(String::as_str).collect();
    let existing_topic_set: BTreeSet<&str> = existing_topics.iter().map(String::as_str).collect();
    let mut planned_namespaces: BTreeSet<String> = BTreeSet::new();

    if let Some(ref obj) = manifest.object_store {
        actions.push(PlannedAction {
            kind: "update_config".to_string(),
            resource: "cluster/object_store".to_string(),
            details: format!("backend={} root={}", obj.backend, obj.root),
        });
    }

    for ns in &manifest.namespaces {
        let ns_clean = ns.trim().trim_start_matches('/');
        if !ns_clean.is_empty()
            && !existing_ns.contains(ns_clean)
            && planned_namespaces.insert(ns_clean.to_string())
        {
            actions.push(PlannedAction {
                kind: "create_namespace".to_string(),
                resource: ns_clean.to_string(),
                details: "create namespace".to_string(),
            });
        }
    }

    let mut desired_concrete_topics: BTreeSet<String> = BTreeSet::new();

    for topic in &manifest.topics {
        if let Some(ns) = namespace_of_topic(&topic.name) {
            if !existing_ns.contains(ns) && planned_namespaces.insert(ns.to_string()) {
                actions.push(PlannedAction {
                    kind: "create_namespace".to_string(),
                    resource: ns.to_string(),
                    details: "create namespace for topic".to_string(),
                });
            }
        }

        let partitions = topic.partitions.unwrap_or(0);
        if partitions > 0 {
            let mut missing_partition = false;
            for part in 0..partitions {
                let concrete = format!("{}-part-{}", topic.name, part);
                if !existing_topic_set.contains(concrete.as_str()) {
                    missing_partition = true;
                }
                desired_concrete_topics.insert(concrete);
            }
            if missing_partition {
                actions.push(PlannedAction {
                    kind: "create_topic".to_string(),
                    resource: topic.name.clone(),
                    details: format!(
                        "partitions={} intent={}",
                        partitions,
                        topic.intent.as_deref().unwrap_or("none")
                    ),
                });
            }
        } else {
            desired_concrete_topics.insert(topic.name.clone());
            if !existing_topic_set.contains(topic.name.as_str()) {
                actions.push(PlannedAction {
                    kind: "create_topic".to_string(),
                    resource: topic.name.clone(),
                    details: format!("intent={}", topic.intent.as_deref().unwrap_or("none")),
                });
            }
        }

        if let Some(ref mode) = topic.storage_mode {
            actions.push(PlannedAction {
                kind: "update_config".to_string(),
                resource: topic.name.clone(),
                details: format!("storage_mode={}", mode),
            });
        }
    }

    if prune {
        for existing in existing_topics {
            if existing.starts_with("/system/") {
                continue;
            }
            if !desired_concrete_topics.contains(existing) {
                actions.push(PlannedAction {
                    kind: "delete_topic".to_string(),
                    resource: existing.clone(),
                    details: "prune unlisted topic".to_string(),
                });
            }
        }
    }

    actions
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_yaml_and_json_manifests() {
        let yaml = r#"
namespaces:
  - agents
topics:
  - name: /agents/events
    intent: agent_event_mesh
"#;
        let parsed_yaml = DesiredStateManifest::parse(yaml).expect("yaml manifest");
        assert_eq!(parsed_yaml.namespaces, vec!["agents"]);
        assert_eq!(parsed_yaml.topics.len(), 1);
        assert_eq!(
            parsed_yaml.topics[0].intent.as_deref(),
            Some("agent_event_mesh")
        );

        let json = r#"{"namespaces":["agents"],"topics":[{"name":"/agents/tasks","partitions":2}]}"#;
        let parsed_json = DesiredStateManifest::parse(json).expect("json manifest");
        assert_eq!(parsed_json.topics[0].partitions, Some(2));
    }

    #[test]
    fn additive_by_default_and_prune_respects_system_namespace() {
        let manifest = DesiredStateManifest {
            namespaces: vec!["default".to_string()],
            topics: vec![DesiredTopicSpec {
                name: "/default/keep".to_string(),
                intent: Some("agent_event_mesh".to_string()),
                partitions: None,
                dispatch_strategy: None,
                schema_subject: None,
                storage_mode: Some("cloud_tiered".to_string()),
            }],
            object_store: None,
        };
        let existing_ns = vec!["default".to_string(), "system".to_string()];
        let existing_topics = vec![
            "/default/existing-unlisted".to_string(),
            "/system/_schema".to_string(),
        ];

        // prune = false: never emits delete_topic
        let additive_plan = compute_plan(&manifest, &existing_ns, &existing_topics, false);
        assert!(additive_plan.iter().all(|a| a.kind != "delete_topic"));
        assert!(additive_plan
            .iter()
            .any(|a| a.kind == "create_topic" && a.resource == "/default/keep"));
        assert!(additive_plan
            .iter()
            .any(|a| a.kind == "update_config" && a.resource == "/default/keep"));

        // prune = true: emits delete_topic for unlisted user topic, skips /system/_schema
        let prune_plan = compute_plan(&manifest, &existing_ns, &existing_topics, true);
        let deletes: Vec<&str> = prune_plan
            .iter()
            .filter(|a| a.kind == "delete_topic")
            .map(|a| a.resource.as_str())
            .collect();
        assert_eq!(deletes, vec!["/default/existing-unlisted"]);
    }
}

