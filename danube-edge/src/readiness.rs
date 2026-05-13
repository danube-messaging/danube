//! Per-topic readiness tracking for the MQTT ingestion pipeline.
//!
//! A topic is **READY** for MQTT ingestion only when all conditions are met:
//! 1. Local WAL handle is provisioned
//! 2. Cluster has confirmed registration (via `RegisterEdge`)
//! 3. Schema is resolved (if `schema_subject` is configured; auto-true for raw-bytes topics)
//!
//! When a schema is resolved and `enforce_validation` is true, payloads are
//! validated against the compiled schema before WAL write.
//!
//! The `MqttIngester` checks readiness before accepting messages. Messages
//! for not-ready topics are rejected (QoS 1 devices will retry).

use std::collections::HashMap;
use std::sync::Arc;

use danube_schema::metadata::{AvroSchema, JsonSchemaDefinition, SchemaDefinition};
use danube_schema::validator::{PayloadValidator, ValidatorFactory};
use tokio::sync::RwLock;
use tracing::{info, warn};

/// Cached schema information resolved from the cluster registry.
#[derive(Debug, Clone)]
pub struct CachedSchema {
    /// Schema subject name (e.g. `"telemetry-events"`).
    pub subject: String,
    /// Cluster-assigned schema ID.
    pub schema_id: u64,
    /// Schema version number.
    pub schema_version: u32,
    /// Schema type (e.g. `"json_schema"`, `"avro"`).
    pub schema_type: String,
    /// SHA-256 fingerprint for change detection.
    pub fingerprint: String,
    /// Raw schema definition bytes (for future payload validation).
    pub schema_definition: Vec<u8>,
}

/// Per-topic readiness state.
#[derive(Debug)]
pub struct TopicState {
    /// WAL handle has been created in the ingester.
    pub local_provisioned: bool,
    /// `RegisterEdge` confirmed this topic on the cluster.
    pub cluster_registered: bool,
    /// Schema resolved from cluster (or not needed for raw-bytes topics).
    pub schema_resolved: bool,
    /// Cached schema info (None for raw-bytes topics).
    pub schema_info: Option<CachedSchema>,
    /// Compiled payload validator (built from schema_definition at resolve time).
    validator: Option<Arc<dyn PayloadValidator>>,
    /// If true, invalid payloads are rejected. If false, validation is skipped.
    pub enforce_validation: bool,
}

impl Clone for TopicState {
    fn clone(&self) -> Self {
        Self {
            local_provisioned: self.local_provisioned,
            cluster_registered: self.cluster_registered,
            schema_resolved: self.schema_resolved,
            schema_info: self.schema_info.clone(),
            validator: self.validator.clone(),
            enforce_validation: self.enforce_validation,
        }
    }
}

impl TopicState {
    fn new() -> Self {
        Self {
            local_provisioned: false,
            cluster_registered: false,
            schema_resolved: false,
            schema_info: None,
            validator: None,
            enforce_validation: false,
        }
    }

    /// A topic is ready for MQTT ingestion when all conditions are met.
    pub fn is_ready(&self) -> bool {
        self.local_provisioned && self.cluster_registered && self.schema_resolved
    }
}

/// Build a `SchemaDefinition` from schema type string and raw definition bytes.
///
/// The cluster sends `serde_json::to_vec(&SchemaDefinition)` which wraps the
/// raw schema inside a tagged enum envelope (e.g.
/// `{"JsonSchema":{"raw_schema":"…","fingerprint":"…"}}`).
/// We first try to deserialize that envelope directly; if that fails (e.g.
/// when the bytes are plain schema text), we fall back to manual construction.
fn build_schema_definition(schema_type: &str, definition: &[u8]) -> Option<SchemaDefinition> {
    if definition.is_empty() {
        return None;
    }

    // Fast path: the cluster serializes the full SchemaDefinition enum via serde_json.
    if let Ok(schema_def) = serde_json::from_slice::<SchemaDefinition>(definition) {
        return Some(schema_def);
    }

    // Fallback: treat the bytes as raw schema content (e.g. plain JSON schema text).
    let raw = String::from_utf8_lossy(definition).to_string();
    match schema_type {
        "json_schema" | "json" => {
            Some(SchemaDefinition::JsonSchema(JsonSchemaDefinition::new(
                raw,
                String::new(),
            )))
        }
        "avro" => Some(SchemaDefinition::Avro(AvroSchema::new(raw, String::new()))),
        "string" => Some(SchemaDefinition::String),
        "bytes" => Some(SchemaDefinition::Bytes),
        "number" | "int" | "float" | "double" => Some(SchemaDefinition::Number),
        _ => {
            warn!(schema_type = %schema_type, "unknown schema type, skipping validator");
            None
        }
    }
}

/// Try to compile a `PayloadValidator` from a `CachedSchema`.
fn compile_validator(schema: &CachedSchema) -> Option<Arc<dyn PayloadValidator>> {
    let schema_def = build_schema_definition(&schema.schema_type, &schema.schema_definition)?;
    match ValidatorFactory::create(&schema_def) {
        Ok(v) => Some(Arc::from(v)),
        Err(e) => {
            warn!(
                subject = %schema.subject,
                schema_type = %schema.schema_type,
                error = %e,
                "failed to compile payload validator"
            );
            None
        }
    }
}

/// Central readiness tracker for all edge topics.
///
/// Thread-safe (behind `RwLock`). Shared between `EdgeService` (writes)
/// and `MqttIngester` (reads).
#[derive(Debug, Clone)]
pub struct TopicReadiness {
    topics: Arc<RwLock<HashMap<String, TopicState>>>,
}

impl TopicReadiness {
    pub fn new() -> Self {
        Self {
            topics: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Initialize tracking for a topic (called during bootstrap).
    pub async fn track_topic(&self, topic_name: &str) {
        let mut topics = self.topics.write().await;
        topics
            .entry(topic_name.to_string())
            .or_insert_with(TopicState::new);
    }

    /// Mark a topic as locally provisioned (WAL handle created).
    pub async fn mark_local_provisioned(&self, topic_name: &str) {
        let mut topics = self.topics.write().await;
        if let Some(state) = topics.get_mut(topic_name) {
            state.local_provisioned = true;
        }
    }

    /// Mark a topic as registered on the cluster.
    pub async fn mark_cluster_registered(&self, topic_name: &str) {
        let mut topics = self.topics.write().await;
        if let Some(state) = topics.get_mut(topic_name) {
            state.cluster_registered = true;
        }
    }

    /// Mark a topic's schema as resolved. Pass `None` for raw-bytes topics.
    ///
    /// If `enforce` is true and the schema has a definition, a payload validator
    /// is compiled and stored for use during ingestion.
    pub async fn mark_schema_resolved(
        &self,
        topic_name: &str,
        schema: Option<CachedSchema>,
        enforce: bool,
    ) {
        let mut topics = self.topics.write().await;
        if let Some(state) = topics.get_mut(topic_name) {
            state.schema_resolved = true;
            state.enforce_validation = enforce;

            // Compile validator if enforce mode and schema has a definition
            if enforce {
                state.validator = schema.as_ref().and_then(compile_validator);
            } else {
                state.validator = None;
            }

            info!(
                topic = %topic_name,
                has_schema = schema.is_some(),
                enforce = enforce,
                has_validator = state.validator.is_some(),
                "topic schema resolved"
            );
            state.schema_info = schema;
        }
    }

    /// Update the cached schema for a topic (called on heartbeat schema change).
    /// Recompiles the validator if enforce mode is active.
    pub async fn update_schema(&self, topic_name: &str, schema: CachedSchema) {
        let mut topics = self.topics.write().await;
        if let Some(state) = topics.get_mut(topic_name) {
            info!(
                topic = %topic_name,
                subject = %schema.subject,
                version = schema.schema_version,
                "schema updated from cluster"
            );

            // Recompile validator if enforce mode
            if state.enforce_validation {
                state.validator = compile_validator(&schema);
            }

            state.schema_info = Some(schema);
        }
    }

    /// Mark a topic's schema as unresolved (schema removed from cluster).
    pub async fn mark_schema_unresolved(&self, topic_name: &str) {
        let mut topics = self.topics.write().await;
        if let Some(state) = topics.get_mut(topic_name) {
            state.schema_resolved = false;
            state.schema_info = None;
        }
    }

    /// Check if a topic is ready for MQTT ingestion.
    pub async fn is_ready(&self, topic_name: &str) -> bool {
        let topics = self.topics.read().await;
        topics.get(topic_name).map_or(false, |s| s.is_ready())
    }

    /// Get cached schema info for a topic (for stamping messages).
    pub async fn get_schema_info(&self, topic_name: &str) -> Option<CachedSchema> {
        let topics = self.topics.read().await;
        topics.get(topic_name).and_then(|s| s.schema_info.clone())
    }

    /// Get the fingerprint for a topic's cached schema (for heartbeat comparison).
    pub async fn get_fingerprint(&self, topic_name: &str) -> Option<String> {
        let topics = self.topics.read().await;
        topics
            .get(topic_name)
            .and_then(|s| s.schema_info.as_ref())
            .map(|s| s.fingerprint.clone())
    }

    /// Get list of topics that are not yet ready (for retry/logging).
    pub async fn not_ready_topics(&self) -> Vec<String> {
        let topics = self.topics.read().await;
        topics
            .iter()
            .filter(|(_, state)| !state.is_ready())
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// Get list of all tracked topics and their readiness state.
    pub async fn all_states(&self) -> Vec<(String, bool)> {
        let topics = self.topics.read().await;
        topics
            .iter()
            .map(|(name, state)| (name.clone(), state.is_ready()))
            .collect()
    }

    /// Validate a payload against the topic's compiled schema.
    ///
    /// Returns `Ok(())` if:
    /// - Topic has no validator (no schema, or enforce=false)
    /// - Topic is not tracked
    /// - Payload passes validation
    ///
    /// Returns `Err` only when `enforce_validation=true` and the payload
    /// fails schema validation.
    pub async fn validate_payload(
        &self,
        topic_name: &str,
        payload: &[u8],
    ) -> Result<(), anyhow::Error> {
        let topics = self.topics.read().await;
        let state = match topics.get(topic_name) {
            Some(s) => s,
            None => return Ok(()),
        };

        let validator = match &state.validator {
            Some(v) => v,
            None => return Ok(()), // no validator → no validation
        };

        match validator.validate(payload) {
            Ok(()) => Ok(()),
            Err(e) => {
                warn!(
                    topic = %topic_name,
                    error = %e,
                    "payload validation failed"
                );
                Err(anyhow::anyhow!(
                    "payload validation failed for topic '{}': {}",
                    topic_name,
                    e
                ))
            }
        }
    }
}

#[cfg(test)]
#[path = "readiness_test.rs"]
mod tests;
