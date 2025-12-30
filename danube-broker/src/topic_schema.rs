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
/// - Schema reference from producer/topic configuration
/// - Cached schema ID for fast validation
/// - Validation policy (None/Warn/Enforce)
/// - Payload validation settings
#[derive(Debug)]
pub(crate) struct TopicSchemaContext {
    /// Reference to schema subject (from producer/topic config)
    schema_ref: Mutex<Option<SchemaReference>>,
    /// Cached schema ID for fast validation (resolved from registry)
    resolved_schema_id: Mutex<Option<u64>>,
    /// Validation policy: None/Warn/Enforce
    validation_policy: ValidationPolicy,
    /// Enable deep payload validation (default: false for performance)
    enable_payload_validation: bool,
    /// Handle to schema resources for resolution
    resources: SchemaResources,
}

impl TopicSchemaContext {
    /// Create a new schema context with default settings
    ///
    /// Default: no schema, no validation, no deep payload checks
    pub(crate) fn new(resources: SchemaResources) -> Self {
        Self {
            schema_ref: Mutex::new(None),
            resolved_schema_id: Mutex::new(None),
            validation_policy: ValidationPolicy::None,
            enable_payload_validation: false,
            resources,
        }
    }

    /// Set schema reference and resolve to schema ID
    ///
    /// This method is called when a producer sets a schema for the topic.
    /// It validates that the schema exists in the registry and caches the schema ID.
    ///
    /// # Returns
    /// - `Ok(())` if schema is found and resolved
    /// - `Err` if schema subject is not found in registry
    pub(crate) fn set_schema_ref(
        &self,
        schema_ref: SchemaReference,
        topic_name: &str,
    ) -> Result<()> {
        let subject = schema_ref.subject.clone();

        // Resolve schema_ref to schema_id using SchemaResources
        if let Some(schema_id) = self.resources.get_schema_id(&subject) {
            // Update both fields atomically
            if let Ok(mut schema_ref_guard) = self.schema_ref.try_lock() {
                *schema_ref_guard = Some(schema_ref);
            }
            if let Ok(mut resolved_id_guard) = self.resolved_schema_id.try_lock() {
                *resolved_id_guard = Some(schema_id);
            }

            info!(
                "Topic {} resolved schema: subject={}, schema_id={}",
                topic_name, subject, schema_id
            );
            Ok(())
        } else {
            // Schema not found in registry - this is now an error
            Err(anyhow!(
                "Schema '{}' not found in registry. Please register schema before using .with_schema_subject()",
                subject
            ))
        }
    }

    /// Set validation policy for this topic
    ///
    /// # Arguments
    /// - `policy`: ValidationPolicy (None, Warn, or Enforce)
    #[allow(dead_code)]
    pub(crate) fn set_validation_policy(&mut self, policy: ValidationPolicy) {
        self.validation_policy = policy;
    }

    /// Enable or disable deep payload validation (default: false for performance)
    ///
    /// When enabled, validates message payload content against schema definition.
    /// This is CPU-intensive and should only be enabled when needed.
    #[allow(dead_code)]
    pub(crate) fn set_payload_validation(&mut self, enabled: bool) {
        self.enable_payload_validation = enabled;
    }

    /// Get the current validation policy
    #[allow(dead_code)]
    pub(crate) fn validation_policy(&self) -> &ValidationPolicy {
        &self.validation_policy
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
        // Skip validation if policy is None
        if matches!(self.validation_policy, ValidationPolicy::None) {
            return Ok(());
        }

        // Record validation attempt
        counter!(
            SCHEMA_VALIDATION_TOTAL.name,
            "topic" => topic_name.to_string(),
            "policy" => format!("{:?}", self.validation_policy)
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
                    if matches!(self.validation_policy, ValidationPolicy::Enforce) {
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

                match self.validation_policy {
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

            match self.validation_policy {
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
            if self.enable_payload_validation {
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

        // Get the subject from schema_ref
        let schema_ref_guard = self
            .schema_ref
            .try_lock()
            .map_err(|_| anyhow!("Failed to acquire schema ref lock"))?;

        let subject = match &*schema_ref_guard {
            Some(ref_val) => ref_val.subject.clone(),
            None => return Ok(()), // No subject, skip
        };
        drop(schema_ref_guard);

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
