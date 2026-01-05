//! Topic Schema Context
//!
//! This module encapsulates all schema-related functionality for topics,
//! including schema reference management, validation policies, and message validation.

use anyhow::{anyhow, Result};
use tokio::sync::Mutex;
use tracing::{info, warn};

use danube_core::{message::StreamMessage, proto::SchemaReference};

use crate::{
    broker_metrics::{SCHEMA_VALIDATION_FAILURES_TOTAL, SCHEMA_VALIDATION_TOTAL},
    resources::SchemaResources,
    schema::ValidationPolicy,
};
use metrics::counter;

/// Schema configuration and caching for a topic
///
/// This struct manages all schema-related state for a topic, including:
/// - Schema subject assigned to this topic
/// - Cached schema ID for fast validation
/// - Validation policy (None/Warn/Enforce) - topic-level
/// - Payload validation settings - topic-level
#[derive(Debug)]
pub(crate) struct TopicSchemaContext {
    /// Schema subject assigned to this topic (e.g., "user-events-value")
    schema_subject: Mutex<Option<String>>,
    /// Cached schema ID for the subject (for fast validation)
    /// This is the subject's schema_id, NOT a specific version's ID
    resolved_schema_id: Mutex<Option<u64>>,
    /// Validation policy: None/Warn/Enforce (topic-level, can be changed by admin)
    validation_policy: Mutex<ValidationPolicy>,
    /// Enable deep payload validation (topic-level, can be changed by admin)
    enable_payload_validation: Mutex<bool>,
    /// Handle to schema resources for resolution
    resources: SchemaResources,
}

impl TopicSchemaContext {
    /// Create a new schema context with default settings
    ///
    /// Default: no schema assigned yet, ValidationPolicy::Warn for visibility
    pub(crate) fn new(resources: SchemaResources) -> Self {
        Self {
            schema_subject: Mutex::new(None),
            resolved_schema_id: Mutex::new(None),
            validation_policy: Mutex::new(ValidationPolicy::Warn),  // Default: Warn for visibility
            enable_payload_validation: Mutex::new(false),
            resources,
        }
    }

    /// Set schema subject for this topic (first producer or admin)
    ///
    /// This method is called when a producer sets a schema for the topic.
    /// It validates that the schema exists in the registry and caches the schema ID.
    ///
    /// # Returns
    /// - `Ok(())` if schema is found and resolved
    /// - `Err` if schema subject is not found in registry
    pub(crate) async fn set_schema_subject(
        &self,
        subject: String,
        topic_name: &str,
    ) -> Result<()> {
        // Verify subject exists and get schema_id
        let schema_id = self.resources.get_schema_id(&subject)
            .ok_or_else(|| anyhow!(
                "Schema subject '{}' not found in registry. Please register schema first.",
                subject
            ))?;

        // Cache subject and schema_id
        *self.schema_subject.lock().await = Some(subject.clone());
        *self.resolved_schema_id.lock().await = Some(schema_id);

        info!(
            "Topic {} schema subject set: {} (schema_id={})",
            topic_name, subject, schema_id
        );
        Ok(())
    }

    /// Set schema reference and resolve to schema ID (backward compatibility)
    ///
    /// This method maintains backward compatibility with existing code.
    pub(crate) async fn set_schema_ref(
        &self,
        schema_ref: SchemaReference,
        topic_name: &str,
    ) -> Result<()> {
        self.set_schema_subject(schema_ref.subject, topic_name).await
    }

    /// Get the topic's schema subject
    pub(crate) async fn get_schema_subject(&self) -> Option<String> {
        self.schema_subject.lock().await.clone()
    }

    /// Configure validation settings (admin-only via admin API)
    pub(crate) async fn configure(
        &self,
        validation_policy: ValidationPolicy,
        enable_payload_validation: bool,
    ) {
        *self.validation_policy.lock().await = validation_policy;
        *self.enable_payload_validation.lock().await = enable_payload_validation;
        info!(
            "Validation configured: policy={:?}, payload_validation={}",
            validation_policy, enable_payload_validation
        );
    }

    /// Get the current validation policy
    pub(crate) async fn validation_policy(&self) -> ValidationPolicy {
        *self.validation_policy.lock().await
    }

    /// Get payload validation enabled setting
    pub(crate) async fn get_payload_validation_enabled(&self) -> bool {
        *self.enable_payload_validation.lock().await
    }

    /// Get cached schema_id
    pub(crate) async fn get_cached_schema_id(&self) -> Option<u64> {
        *self.resolved_schema_id.lock().await
    }

    /// Validate a message against topic's schema
    ///
    /// This performs schema ID validation and optionally deep payload validation.
    ///
    /// # Returns
    /// - `Ok(())` if validation passes or is skipped
    /// - `Err` if validation fails and policy is Enforce
    pub(crate) async fn validate_message(
        &self,
        message: &StreamMessage,
        topic_name: &str,
    ) -> Result<()> {
        // Get current validation policy
        let policy = *self.validation_policy.lock().await;
        
        // Skip validation if policy is None
        if matches!(policy, ValidationPolicy::None) {
            return Ok(());
        }

        // Record validation attempt
        counter!(
            SCHEMA_VALIDATION_TOTAL.name,
            "topic" => topic_name.to_string(),
            "policy" => format!("{:?}", policy)
        )
        .increment(1);

        // Check if topic has schema enforcement enabled
        let expected_schema_id = {
            let resolved_id_guard = self
                .resolved_schema_id
                .try_lock()
                .map_err(|_| anyhow!("Failed to acquire schema lock"))?;

            match *resolved_id_guard {
                Some(id) => id,
                None => {
                    // No schema set for topic
                    if matches!(policy, ValidationPolicy::Enforce) {
                        return Err(anyhow!(
                            "Topic {} requires schema but none is configured",
                            topic_name
                        ));
                    }
                    return Ok(()); // Warn mode: allow messages without schema
                }
            }
        };

        // Check if message has schema_id
        let message_schema_id = match message.schema_id {
            Some(id) => id,
            None => {
                let err = anyhow!(
                    "Message missing schema_id for topic {} (expected schema_id: {})",
                    topic_name,
                    expected_schema_id
                );

                // Record failure
                counter!(
                    SCHEMA_VALIDATION_FAILURES_TOTAL.name,
                    "topic" => topic_name.to_string(),
                    "reason" => "missing_schema_id"
                )
                .increment(1);

                match policy {
                    ValidationPolicy::Warn => {
                        warn!("{}", err);
                        return Ok(()); // Warn but allow
                    }
                    ValidationPolicy::Enforce => return Err(err),
                    ValidationPolicy::None => return Ok(()),
                }
            }
        };

        // Validate schema_id matches
        if message_schema_id != expected_schema_id {
            let err = anyhow!(
                "Schema mismatch for topic {}: message has schema_id={}, expected={}",
                topic_name,
                message_schema_id,
                expected_schema_id
            );

            // Record failure
            counter!(
                SCHEMA_VALIDATION_FAILURES_TOTAL.name,
                "topic" => topic_name.to_string(),
                "reason" => "schema_mismatch"
            )
            .increment(1);

            match policy {
                ValidationPolicy::Warn => {
                    warn!("{}", err);
                    Ok(()) // Warn but allow
                }
                ValidationPolicy::Enforce => Err(err),
                ValidationPolicy::None => Ok(()),
            }
        } else {
            // Schema ID validation passed

            // Deep payload validation (optional, disabled by default)
            let enable_payload_validation = *self.enable_payload_validation.lock().await;
            if enable_payload_validation {
                self.validate_payload_content(message, topic_name).await?;
            }

            Ok(())
        }
    }

    /// Validate message payload content against schema definition (deep validation)
    ///
    /// This is CPU-intensive and disabled by default.
    async fn validate_payload_content(
        &self,
        message: &StreamMessage,
        topic_name: &str,
    ) -> Result<()> {
        use crate::schema::validator::ValidatorFactory;

        // Get schema definition from resources
        let schema_id_guard = self
            .resolved_schema_id
            .try_lock()
            .map_err(|_| anyhow!("Failed to acquire schema lock"))?;

        let _schema_id = match *schema_id_guard {
            Some(id) => id,
            None => return Ok(()), // No schema configured, skip validation
        };
        drop(schema_id_guard);

        // Get the subject from schema_subject
        let schema_subject_guard = self
            .schema_subject
            .try_lock()
            .map_err(|_| anyhow!("Failed to acquire schema subject lock"))?;

        let subject = match &*schema_subject_guard {
            Some(subj) => subj.clone(),
            None => return Ok(()), // No subject, skip
        };
        drop(schema_subject_guard);

        // Fetch schema version from resources
        let version = message.schema_version.unwrap_or(1);
        let schema_version = self
            .resources
            .get_version(&subject, version)
            .await
            .map_err(|e| anyhow!("Schema version not found: {}/{} - {}", subject, version, e))?;

        // Create validator and validate payload
        let validator = ValidatorFactory::create(&schema_version.schema_def)
            .map_err(|e| anyhow!("Failed to create validator: {}", e))?;

        validator.validate(&message.payload).map_err(|e| {
            // Record payload validation failure
            counter!(
                SCHEMA_VALIDATION_FAILURES_TOTAL.name,
                "topic" => topic_name.to_string(),
                "reason" => "payload_invalid"
            )
            .increment(1);

            anyhow!("Payload validation failed: {}", e)
        })?;

        Ok(())
    }
}
