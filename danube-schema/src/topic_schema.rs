//! Topic Schema Context
//!
//! This module encapsulates all schema-related functionality for topics,
//! including schema reference management, validation policies, and message validation.

use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, RwLock};
use tracing::{info, warn};

use danube_core::{message::StreamMessage, proto::SchemaReference};

use crate::{
    resources::SchemaResources,
    types::ValidationPolicy,
    validator::{PayloadValidator, ValidatorFactory},
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
    /// Cached compiled validator to eliminate AST parsing / compilation on every message
    pub validator: Option<Arc<dyn PayloadValidator>>,
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
    schema_subject: RwLock<Option<String>>,

    /// Cache of schema info by schema_id (supports multiple versions)
    schema_cache: RwLock<HashMap<u64, CachedSchemaInfo>>,

    /// Validation policy: None/Warn/Enforce (topic-level, lock-free)
    validation_policy: AtomicU8,

    /// Enable deep payload validation (topic-level, lock-free)
    enable_payload_validation: AtomicBool,

    /// Handle to schema resources for resolution and lookup
    resources: SchemaResources,
}

impl TopicSchemaContext {
    /// Create a new schema context with default settings
    pub fn new(resources: SchemaResources) -> Self {
        Self {
            schema_subject: RwLock::new(None),
            schema_cache: RwLock::new(HashMap::new()),
            validation_policy: AtomicU8::new(ValidationPolicy::Warn.to_u8()),
            enable_payload_validation: AtomicBool::new(false),
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
        *self.schema_subject.write().unwrap() = Some(subject.clone());
        info!(topic = %topic_name, subject = %subject, schema_id = %schema_id, "topic schema subject set");
        Ok(())
    }

    /// Set schema reference and resolve to schema ID (backward compatibility)
    pub async fn set_schema_ref(
        &self,
        schema_ref: SchemaReference,
        topic_name: &str,
    ) -> Result<()> {
        self.set_schema_subject(schema_ref.subject, topic_name)
            .await
    }

    /// Get the topic's schema subject
    pub async fn get_schema_subject(&self) -> Option<String> {
        self.schema_subject.read().unwrap().clone()
    }

    /// Configure validation settings
    pub async fn configure(
        &self,
        validation_policy: ValidationPolicy,
        enable_payload_validation: bool,
    ) {
        self.validation_policy
            .store(validation_policy.to_u8(), Ordering::Release);
        self.enable_payload_validation
            .store(enable_payload_validation, Ordering::Release);
        info!(policy = ?validation_policy, payload_validation = %enable_payload_validation, "Validation configured");
    }

    /// Get the current validation policy
    pub async fn validation_policy(&self) -> ValidationPolicy {
        ValidationPolicy::from_u8(self.validation_policy.load(Ordering::Acquire))
    }

    /// Get payload validation enabled setting
    pub async fn get_payload_validation_enabled(&self) -> bool {
        self.enable_payload_validation.load(Ordering::Acquire)
    }

    /// Get cached schema info for a specific schema_id
    pub async fn get_cached_schema(&self, schema_id: u64) -> Option<CachedSchemaInfo> {
        self.schema_cache.read().unwrap().get(&schema_id).cloned()
    }

    /// Get the subject's schema_id
    pub async fn get_subject_schema_id(&self) -> Option<u64> {
        let subject = self.schema_subject.read().unwrap().clone()?;
        self.resources.get_schema_id(&subject).await
    }

    /// Cache schema information from message metadata
    fn cache_schema_info(
        &self,
        schema_id: u64,
        version: u32,
        subject: String,
        validator: Option<Arc<dyn PayloadValidator>>,
    ) -> CachedSchemaInfo {
        let info = CachedSchemaInfo {
            subject,
            version,
            schema_id,
            validator,
        };
        self.schema_cache
            .write()
            .unwrap()
            .insert(schema_id, info.clone());
        info
    }

    /// Validate a message against topic's schema
    pub async fn validate_message(&self, message: &StreamMessage, topic_name: &str) -> Result<()> {
        let policy = ValidationPolicy::from_u8(self.validation_policy.load(Ordering::Acquire));

        if matches!(policy, ValidationPolicy::None) {
            return Ok(());
        }

        counter!(SCHEMA_VALIDATION_TOTAL, "topic" => topic_name.to_string(), "policy" => policy.as_str()).increment(1);

        let topic_subject = match self.schema_subject.read().unwrap().clone() {
            Some(s) => s,
            None => {
                if matches!(policy, ValidationPolicy::Enforce) {
                    return Err(anyhow!(
                        "Topic {} requires schema but none is configured",
                        topic_name
                    ));
                }
                return Ok(());
            }
        };

        let message_schema_id = match message.schema_id {
            Some(id) => id,
            None => {
                let err = anyhow!(
                    "Message missing schema_id for topic {} (expected subject: {})",
                    topic_name,
                    topic_subject
                );
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

        let cached_opt = self
            .schema_cache
            .read()
            .unwrap()
            .get(&message_schema_id)
            .cloned();

        let cached = if let Some(cached) = cached_opt {
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
            cached
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
            self.cache_schema_info(message_schema_id, version, schema_subject, None)
        };

        let enable_payload_validation = self.enable_payload_validation.load(Ordering::Acquire);
        if enable_payload_validation {
            self.validate_payload_content(message, topic_name, &cached)
                .await?;
        }

        Ok(())
    }

    /// Validate message payload content against schema definition (deep validation)
    async fn validate_payload_content(
        &self,
        message: &StreamMessage,
        topic_name: &str,
        cached: &CachedSchemaInfo,
    ) -> Result<()> {
        let validator = if let Some(ref v) = cached.validator {
            Arc::clone(v)
        } else {
            // Double-check if another concurrent caller populated the cached validator
            let recheck = self
                .schema_cache
                .read()
                .unwrap()
                .get(&cached.schema_id)
                .and_then(|c| c.validator.clone());

            if let Some(v) = recheck {
                v
            } else {
                let schema_version = self
                    .resources
                    .get_version(&cached.subject, cached.version)
                    .await
                    .map_err(|e| {
                        anyhow!(
                            "Schema version not found: {}/{} - {}",
                            cached.subject,
                            cached.version,
                            e
                        )
                    })?;

                let validator_box = ValidatorFactory::create(&schema_version.schema_def)
                    .map_err(|e| anyhow!("Failed to create validator: {}", e))?;
                let compiled_validator: Arc<dyn PayloadValidator> = Arc::from(validator_box);

                if let Some(entry) = self
                    .schema_cache
                    .write()
                    .unwrap()
                    .get_mut(&cached.schema_id)
                {
                    entry.validator = Some(Arc::clone(&compiled_validator));
                }

                compiled_validator
            }
        };

        validator.validate(&message.payload).map_err(|e| {
            counter!(SCHEMA_VALIDATION_FAILURES_TOTAL, "topic" => topic_name.to_string(), "reason" => "payload_invalid").increment(1);
            anyhow!("Payload validation failed: {}", e)
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{SchemaDefinition, SchemaMetadata, SchemaVersion};
    use bytes::Bytes;
    use danube_core::message::{MessageID, StreamMessage};
    use danube_core::metadata::MemoryStore;

    fn create_test_message(
        schema_id: Option<u64>,
        schema_version: Option<u32>,
        payload: Bytes,
    ) -> StreamMessage {
        StreamMessage {
            request_id: 1,
            msg_id: MessageID {
                producer_id: 1,
                topic_name: "/default/test_topic".to_string(),
                broker_addr: "127.0.0.1:6650".to_string(),
                topic_offset: 0,
            },
            payload,
            publish_time: 0,
            producer_name: "test_producer".to_string(),
            subscription_name: None,
            attributes: HashMap::new(),
            schema_id,
            schema_version,
            routing_key: None,
        }
    }

    async fn setup_test_context() -> (TopicSchemaContext, SchemaResources) {
        let mem = MemoryStore::new().await.unwrap();
        let resources = SchemaResources::new(Arc::new(mem));
        let ctx = TopicSchemaContext::new(resources.clone());
        (ctx, resources)
    }

    #[tokio::test]
    async fn test_validation_policy_none_skips_immediately() {
        let (ctx, _) = setup_test_context().await;
        ctx.configure(ValidationPolicy::None, false).await;

        let msg = create_test_message(None, None, Bytes::new());
        // Even without schema configured on topic or message, Policy::None succeeds immediately
        assert!(ctx
            .validate_message(&msg, "/default/test_topic")
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn test_validation_policy_warn_does_not_fail() {
        let (ctx, _) = setup_test_context().await;
        ctx.configure(ValidationPolicy::Warn, false).await;

        let msg = create_test_message(None, None, Bytes::new());
        // Warn logs error/warning but returns Ok(())
        assert!(ctx
            .validate_message(&msg, "/default/test_topic")
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn test_validation_policy_enforce_rejects_missing_schema() {
        let (ctx, _) = setup_test_context().await;
        ctx.configure(ValidationPolicy::Enforce, false).await;

        let msg = create_test_message(None, None, Bytes::new());
        let res = ctx.validate_message(&msg, "/default/test_topic").await;
        assert!(res.is_err());
        assert!(res
            .unwrap_err()
            .to_string()
            .contains("requires schema but none is configured"));
    }

    #[tokio::test]
    async fn test_validation_cached_validator_compiles_once_and_reuses() {
        let (ctx, resources) = setup_test_context().await;

        // Register a String schema in resources
        let subject = "test-subject".to_string();
        let schema_id = 42u64;

        let version = SchemaVersion::new(
            1,
            SchemaDefinition::String,
            "fp_test".to_string(),
            "user".to_string(),
            "test string schema".to_string(),
        );
        let metadata = SchemaMetadata::new(
            schema_id,
            subject.clone(),
            version.clone(),
            "user".to_string(),
        );
        resources.store_schema_metadata(&metadata).await.unwrap();
        resources
            .store_schema_id_index(schema_id, &subject)
            .await
            .unwrap();
        resources
            .store_schema_version(&subject, &version)
            .await
            .unwrap();

        // Assign topic subject
        ctx.set_schema_subject(subject.clone(), "/default/test_topic")
            .await
            .unwrap();
        ctx.configure(ValidationPolicy::Enforce, true).await;

        // Message with valid UTF-8 payload
        let mut msg = create_test_message(
            Some(schema_id),
            Some(1),
            Bytes::from_static(b"hello danube"),
        );

        // 1st message: compiles and caches validator
        assert!(ctx
            .validate_message(&msg, "/default/test_topic")
            .await
            .is_ok());

        // Verify validator is now in cache
        let cached = ctx
            .get_cached_schema(schema_id)
            .await
            .expect("cached schema exists");
        assert!(cached.validator.is_some());

        // 2nd message: valid UTF-8 executes against cached validator
        assert!(ctx
            .validate_message(&msg, "/default/test_topic")
            .await
            .is_ok());

        // 3rd message: invalid UTF-8 is rejected by cached StringValidator
        msg.payload = Bytes::from_static(&[0xFF, 0xFE]);
        let err = ctx.validate_message(&msg, "/default/test_topic").await;
        assert!(err.is_err());
        assert!(err.unwrap_err().to_string().contains("Invalid UTF-8"));
    }

    #[tokio::test]
    async fn test_subject_mismatch_detected() {
        let (ctx, resources) = setup_test_context().await;

        let subject1 = "subject-one".to_string();
        let schema_id1 = 101u64;
        let v1 = SchemaVersion::new(
            1,
            SchemaDefinition::String,
            "fp1".to_string(),
            "user".to_string(),
            "desc".to_string(),
        );
        let metadata = SchemaMetadata::new(schema_id1, subject1.clone(), v1, "user".to_string());
        resources.store_schema_metadata(&metadata).await.unwrap();
        resources
            .store_schema_id_index(schema_id1, &subject1)
            .await
            .unwrap();

        let subject2 = "subject-two".to_string();
        let schema_id2 = 102u64;
        let v2 = SchemaVersion::new(
            1,
            SchemaDefinition::String,
            "fp2".to_string(),
            "user".to_string(),
            "desc".to_string(),
        );
        let metadata2 = SchemaMetadata::new(schema_id2, subject2.clone(), v2, "user".to_string());
        resources.store_schema_metadata(&metadata2).await.unwrap();
        resources
            .store_schema_id_index(schema_id2, &subject2)
            .await
            .unwrap();

        ctx.set_schema_subject(subject1.clone(), "/default/test_topic")
            .await
            .unwrap();
        ctx.configure(ValidationPolicy::Enforce, false).await;

        // Send message with schema_id2 (which belongs to subject2, not subject1)
        let msg = create_test_message(Some(schema_id2), None, Bytes::new());

        let err = ctx.validate_message(&msg, "/default/test_topic").await;
        assert!(err.is_err());
        assert!(err
            .unwrap_err()
            .to_string()
            .contains("Schema subject mismatch"));
    }
}
