//! Topic Schema Context
//!
//! This module encapsulates all schema-related functionality for topics,
//! including schema reference management, validation policies, and message validation.

use anyhow::{anyhow, Result};
use std::collections::HashMap;
use tokio::sync::Mutex;
use tracing::{info, warn};

use danube_core::{message::StreamMessage, proto::SchemaReference};

use crate::{
    resources::SchemaResources,
    types::ValidationPolicy,
    validator::ValidatorFactory,
};
use metrics::counter;

// Schema validation metric names (same names as broker_metrics for compatibility)
const SCHEMA_VALIDATION_TOTAL: &str = "danube_schema_validation_total";
const SCHEMA_VALIDATION_FAILURES_TOTAL: &str = "danube_schema_validation_failures_total";

/// Cached schema information for validation
#[derive(Debug, Clone)]
pub struct CachedSchemaInfo {
    pub subject: String,
    pub version: u32,
    #[allow(dead_code)] // Stored for future use and debugging
    pub schema_id: u64,
}

/// Schema configuration and caching for a topic
///
/// This struct manages all schema-related state for a topic, including:
/// - Schema subject assigned to this topic (single subject per topic)
/// - Cache of all schema versions seen (by schema_id)
/// - Validation policy (None/Warn/Enforce) - topic-level
/// - Payload validation settings - topic-level
#[derive(Debug)]
pub struct TopicSchemaContext {
    /// Schema subject assigned to this topic (e.g., "user-events-value")
    schema_subject: Mutex<Option<String>>,

    /// Cache of schema info by schema_id (supports multiple versions)
    schema_cache: Mutex<HashMap<u64, CachedSchemaInfo>>,

    /// Validation policy: None/Warn/Enforce (topic-level)
    validation_policy: Mutex<ValidationPolicy>,

    /// Enable deep payload validation (topic-level)
    enable_payload_validation: Mutex<bool>,

    /// Handle to schema resources for resolution and lookup
    resources: SchemaResources,
}

impl TopicSchemaContext {
    /// Create a new schema context with default settings
    pub fn new(resources: SchemaResources) -> Self {
        Self {
            schema_subject: Mutex::new(None),
            schema_cache: Mutex::new(HashMap::new()),
            validation_policy: Mutex::new(ValidationPolicy::Warn),
            enable_payload_validation: Mutex::new(false),
            resources,
        }
    }

    /// Set schema subject for this topic
    pub async fn set_schema_subject(&self, subject: String, topic_name: &str) -> Result<()> {
        let schema_id = self
            .resources
            .get_schema_id(&subject)
            .await
            .ok_or_else(|| {
                anyhow!(
                    "Schema subject '{}' not found in registry. Please register schema first.",
                    subject
                )
            })?;
        *self.schema_subject.lock().await = Some(subject.clone());
        info!(topic = %topic_name, subject = %subject, schema_id = %schema_id, "topic schema subject set");
        Ok(())
    }

    /// Set schema reference and resolve to schema ID (backward compatibility)
    pub async fn set_schema_ref(&self, schema_ref: SchemaReference, topic_name: &str) -> Result<()> {
        self.set_schema_subject(schema_ref.subject, topic_name).await
    }

    /// Get the topic's schema subject
    pub async fn get_schema_subject(&self) -> Option<String> {
        self.schema_subject.lock().await.clone()
    }

    /// Configure validation settings
    pub async fn configure(&self, validation_policy: ValidationPolicy, enable_payload_validation: bool) {
        *self.validation_policy.lock().await = validation_policy;
        *self.enable_payload_validation.lock().await = enable_payload_validation;
        info!(policy = ?validation_policy, payload_validation = %enable_payload_validation, "Validation configured");
    }

    /// Get the current validation policy
    pub async fn validation_policy(&self) -> ValidationPolicy {
        *self.validation_policy.lock().await
    }

    /// Get payload validation enabled setting
    pub async fn get_payload_validation_enabled(&self) -> bool {
        *self.enable_payload_validation.lock().await
    }

    /// Get cached schema info for a specific schema_id
    pub async fn get_cached_schema(&self, schema_id: u64) -> Option<CachedSchemaInfo> {
        self.schema_cache.lock().await.get(&schema_id).cloned()
    }

    /// Get the subject's schema_id
    pub async fn get_subject_schema_id(&self) -> Option<u64> {
        let subject = self.schema_subject.lock().await.clone()?;
        self.resources.get_schema_id(&subject).await
    }

    /// Cache schema information from message metadata
    async fn cache_schema_info(&self, schema_id: u64, version: u32, subject: String) {
        let info = CachedSchemaInfo { subject, version, schema_id };
        self.schema_cache.lock().await.insert(schema_id, info);
    }

    /// Validate a message against topic's schema
    pub async fn validate_message(&self, message: &StreamMessage, topic_name: &str) -> Result<()> {
        let policy = *self.validation_policy.lock().await;

        if matches!(policy, ValidationPolicy::None) {
            return Ok(());
        }

        counter!(SCHEMA_VALIDATION_TOTAL, "topic" => topic_name.to_string(), "policy" => format!("{:?}", policy)).increment(1);

        let topic_subject = self.schema_subject.lock().await.clone();

        if topic_subject.is_none() {
            if matches!(policy, ValidationPolicy::Enforce) {
                return Err(anyhow!("Topic {} requires schema but none is configured", topic_name));
            }
            return Ok(());
        }

        let topic_subject = topic_subject.unwrap();

        let message_schema_id = match message.schema_id {
            Some(id) => id,
            None => {
                let err = anyhow!("Message missing schema_id for topic {} (expected subject: {})", topic_name, topic_subject);
                counter!(SCHEMA_VALIDATION_FAILURES_TOTAL, "topic" => topic_name.to_string(), "reason" => "missing_schema_id").increment(1);
                match policy {
                    ValidationPolicy::Warn => {
                        warn!(topic = %topic_name, subject = %topic_subject, error = %err, "Message missing schema_id");
                        return Ok(());
                    }
                    ValidationPolicy::Enforce => return Err(err),
                    ValidationPolicy::None => return Ok(()),
                }
            }
        };

        if let Some(cached) = self.get_cached_schema(message_schema_id).await {
            if cached.subject != topic_subject {
                let err = anyhow!(
                    "Schema subject mismatch for topic {}: message schema_id={} belongs to subject '{}', but topic requires '{}'",
                    topic_name, message_schema_id, cached.subject, topic_subject
                );
                counter!(SCHEMA_VALIDATION_FAILURES_TOTAL, "topic" => topic_name.to_string(), "reason" => "subject_mismatch").increment(1);
                match policy {
                    ValidationPolicy::Warn => {
                        warn!(topic = %topic_name, message_schema_id = %message_schema_id, cached_subject = %cached.subject, topic_subject = %topic_subject, "Schema subject mismatch (cached)");
                        return Ok(());
                    }
                    ValidationPolicy::Enforce => return Err(err),
                    ValidationPolicy::None => return Ok(()),
                }
            }
        } else {
            let schema_subject = self
                .resources
                .get_subject_by_schema_id(message_schema_id)
                .await
                .ok_or_else(|| anyhow!("Schema ID {} not found in registry", message_schema_id))?;

            if schema_subject != topic_subject {
                let err = anyhow!(
                    "Schema subject mismatch for topic {}: message schema_id={} belongs to subject '{}', but topic requires '{}'",
                    topic_name, message_schema_id, schema_subject, topic_subject
                );
                counter!(SCHEMA_VALIDATION_FAILURES_TOTAL, "topic" => topic_name.to_string(), "reason" => "subject_mismatch").increment(1);
                match policy {
                    ValidationPolicy::Warn => {
                        warn!(topic = %topic_name, message_schema_id = %message_schema_id, registry_subject = %schema_subject, topic_subject = %topic_subject, "Schema subject mismatch (registry lookup)");
                        return Ok(());
                    }
                    ValidationPolicy::Enforce => return Err(err),
                    ValidationPolicy::None => return Ok(()),
                }
            }

            let version = message.schema_version.unwrap_or(1);
            self.cache_schema_info(message_schema_id, version, schema_subject).await;
        }

        let enable_payload_validation = *self.enable_payload_validation.lock().await;
        if enable_payload_validation {
            self.validate_payload_content(message, topic_name).await?;
        }

        Ok(())
    }

    /// Validate message payload content against schema definition (deep validation)
    async fn validate_payload_content(&self, message: &StreamMessage, topic_name: &str) -> Result<()> {
        let schema_id = match message.schema_id {
            Some(id) => id,
            None => return Ok(()),
        };

        let cached = match self.get_cached_schema(schema_id).await {
            Some(c) => c,
            None => {
                let subject = self
                    .resources
                    .get_subject_by_schema_id(schema_id)
                    .await
                    .ok_or_else(|| anyhow!("Schema ID {} not found", schema_id))?;
                let version = message.schema_version.unwrap_or(1);
                CachedSchemaInfo { subject, version, schema_id }
            }
        };

        let schema_version = self
            .resources
            .get_version(&cached.subject, cached.version)
            .await
            .map_err(|e| anyhow!("Schema version not found: {}/{} - {}", cached.subject, cached.version, e))?;

        let validator = ValidatorFactory::create(&schema_version.schema_def)
            .map_err(|e| anyhow!("Failed to create validator: {}", e))?;

        validator.validate(&message.payload).map_err(|e| {
            counter!(SCHEMA_VALIDATION_FAILURES_TOTAL, "topic" => topic_name.to_string(), "reason" => "payload_invalid").increment(1);
            anyhow!("Payload validation failed: {}", e)
        })?;

        Ok(())
    }
}
