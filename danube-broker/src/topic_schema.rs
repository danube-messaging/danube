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
    broker_metrics::{SCHEMA_VALIDATION_FAILURES_TOTAL, SCHEMA_VALIDATION_TOTAL},
    resources::SchemaResources,
    schema::ValidationPolicy,
};
use metrics::counter;

/// Cached schema information for validation
#[derive(Debug, Clone)]
pub(crate) struct CachedSchemaInfo {
    pub(crate) subject: String,
    pub(crate) version: u32,
    #[allow(dead_code)]  // Stored for future use and debugging
    pub(crate) schema_id: u64,
}

/// Schema configuration and caching for a topic
///
/// This struct manages all schema-related state for a topic, including:
/// - Schema subject assigned to this topic (single subject per topic)
/// - Cache of all schema versions seen (by schema_id)
/// - Validation policy (None/Warn/Enforce) - topic-level
/// - Payload validation settings - topic-level
#[derive(Debug)]
pub(crate) struct TopicSchemaContext {
    /// Schema subject assigned to this topic (e.g., "user-events-value")
    /// Once set, only schemas from this subject are allowed
    schema_subject: Mutex<Option<String>>,
    
    /// Cache of schema info by schema_id (supports multiple versions)
    /// Key: schema_id (e.g., 201, 202, 203 for V1, V2, V3)
    /// Value: CachedSchemaInfo with subject, version, schema_id
    schema_cache: Mutex<HashMap<u64, CachedSchemaInfo>>,
    
    /// Validation policy: None/Warn/Enforce (topic-level, can be changed by admin)
    validation_policy: Mutex<ValidationPolicy>,
    
    /// Enable deep payload validation (topic-level, can be changed by admin)
    enable_payload_validation: Mutex<bool>,
    
    /// Handle to schema resources for resolution and lookup
    resources: SchemaResources,
}

impl TopicSchemaContext {
    /// Create a new schema context with default settings
    ///
    /// Default: no schema assigned yet, ValidationPolicy::Warn for visibility
    pub(crate) fn new(resources: SchemaResources) -> Self {
        Self {
            schema_subject: Mutex::new(None),
            schema_cache: Mutex::new(HashMap::new()),
            validation_policy: Mutex::new(ValidationPolicy::Warn),  // Default: Warn for visibility
            enable_payload_validation: Mutex::new(false),
            resources,
        }
    }

    /// Set schema subject for this topic (first producer or admin)
    ///
    /// This method is called when a producer sets a schema for the topic.
    /// It validates that the schema exists in the registry.
    ///
    /// # Returns
    /// - `Ok(())` if schema is found
    /// - `Err` if schema subject is not found in registry
    pub(crate) async fn set_schema_subject(
        &self,
        subject: String,
        topic_name: &str,
    ) -> Result<()> {
        // Verify subject exists in registry
        let schema_id = self.resources.get_schema_id(&subject)
            .ok_or_else(|| anyhow!(
                "Schema subject '{}' not found in registry. Please register schema first.",
                subject
            ))?;

        // Set the subject (schema versions will be cached as messages arrive)
        *self.schema_subject.lock().await = Some(subject.clone());

        info!(
            topic = %topic_name,
            subject = %subject,
            schema_id = %schema_id,
            "topic schema subject set"
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
            policy = ?validation_policy,
            payload_validation = %enable_payload_validation,
            "Validation configured"
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

    /// Get cached schema info for a specific schema_id
    pub(crate) async fn get_cached_schema(&self, schema_id: u64) -> Option<CachedSchemaInfo> {
        self.schema_cache.lock().await.get(&schema_id).cloned()
    }
    
    /// Get the subject's schema_id (base ID, not version-specific)
    /// Returns None if no schema subject is configured
    pub(crate) async fn get_subject_schema_id(&self) -> Option<u64> {
        let subject = self.schema_subject.lock().await.clone()?;
        self.resources.get_schema_id(&subject)
    }
    
    /// Cache schema information from message metadata
    async fn cache_schema_info(&self, schema_id: u64, version: u32, subject: String) {
        let info = CachedSchemaInfo {
            subject,
            version,
            schema_id,
        };
        self.schema_cache.lock().await.insert(schema_id, info);
    }

    /// Validate a message against topic's schema
    ///
    /// NEW APPROACH: Validates that message's schema_id belongs to the topic's subject.
    /// Supports multiple versions of the same subject.
    /// Caches schema info on-demand as messages arrive.
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

        // Check if topic has a schema subject assigned
        let topic_subject = self.schema_subject.lock().await.clone();
        
        if topic_subject.is_none() {
            // Topic has no schema configured
            if matches!(policy, ValidationPolicy::Enforce) {
                return Err(anyhow!(
                    "Topic {} requires schema but none is configured",
                    topic_name
                ));
            }
            return Ok(()); // Warn/None mode: allow messages without schema
        }
        
        let topic_subject = topic_subject.unwrap();

        // Check if message has schema_id
        let message_schema_id = match message.schema_id {
            Some(id) => id,
            None => {
                let err = anyhow!(
                    "Message missing schema_id for topic {} (expected subject: {})",
                    topic_name,
                    topic_subject
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
                        warn!(topic = %topic_name, subject = %topic_subject, error = %err, "Message missing schema_id");
                        return Ok(()); // Warn but allow
                    }
                    ValidationPolicy::Enforce => return Err(err),
                    ValidationPolicy::None => return Ok(()),
                }
            }
        };

        // Check cache first
        if let Some(cached) = self.get_cached_schema(message_schema_id).await {
            // Validate subject matches
            if cached.subject != topic_subject {
                let err = anyhow!(
                    "Schema subject mismatch for topic {}: message schema_id={} belongs to subject '{}', but topic requires '{}'",
                    topic_name,
                    message_schema_id,
                    cached.subject,
                    topic_subject
                );

                counter!(
                    SCHEMA_VALIDATION_FAILURES_TOTAL.name,
                    "topic" => topic_name.to_string(),
                    "reason" => "subject_mismatch"
                )
                .increment(1);

                match policy {
                    ValidationPolicy::Warn => {
                        warn!(
                            topic = %topic_name,
                            message_schema_id = %message_schema_id,
                            cached_subject = %cached.subject,
                            topic_subject = %topic_subject,
                            "Schema subject mismatch (cached)"
                        );
                        return Ok(());
                    }
                    ValidationPolicy::Enforce => return Err(err),
                    ValidationPolicy::None => return Ok(()),
                }
            }
            
            // Cached and valid - success!
        } else {
            // Not in cache - look up schema_id in registry
            let schema_subject = self.resources.get_subject_by_schema_id(message_schema_id)
                .ok_or_else(|| anyhow!(
                    "Schema ID {} not found in registry",
                    message_schema_id
                ))?;
                
            // Validate subject matches topic
            if schema_subject != topic_subject {
                let err = anyhow!(
                    "Schema subject mismatch for topic {}: message schema_id={} belongs to subject '{}', but topic requires '{}'",
                    topic_name,
                    message_schema_id,
                    schema_subject,
                    topic_subject
                );

                counter!(
                    SCHEMA_VALIDATION_FAILURES_TOTAL.name,
                    "topic" => topic_name.to_string(),
                    "reason" => "subject_mismatch"
                )
                .increment(1);

                match policy {
                    ValidationPolicy::Warn => {
                        warn!(
                            topic = %topic_name,
                            message_schema_id = %message_schema_id,
                            registry_subject = %schema_subject,
                            topic_subject = %topic_subject,
                            "Schema subject mismatch (registry lookup)"
                        );
                        return Ok(());
                    }
                    ValidationPolicy::Enforce => return Err(err),
                    ValidationPolicy::None => return Ok(()),
                }
            }
            
            // Cache it for next time
            let version = message.schema_version.unwrap_or(1);
            self.cache_schema_info(message_schema_id, version, schema_subject).await;
        }

        // Schema validation passed - optionally do deep payload validation
        let enable_payload_validation = *self.enable_payload_validation.lock().await;
        if enable_payload_validation {
            self.validate_payload_content(message, topic_name).await?;
        }

        Ok(())
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

        // Get schema_id from message
        let schema_id = match message.schema_id {
            Some(id) => id,
            None => return Ok(()), // No schema_id, skip validation
        };

        // Get cached schema info (should be cached by now from validate_message)
        let cached = match self.get_cached_schema(schema_id).await {
            Some(c) => c,
            None => {
                // Not cached yet - look it up
                let subject = self.resources.get_subject_by_schema_id(schema_id)
                    .ok_or_else(|| anyhow!("Schema ID {} not found", schema_id))?;
                let version = message.schema_version.unwrap_or(1);
                CachedSchemaInfo {
                    subject,
                    version,
                    schema_id,
                }
            }
        };

        // Fetch schema version from resources
        let schema_version = self
            .resources
            .get_version(&cached.subject, cached.version)
            .await
            .map_err(|e| anyhow!("Schema version not found: {}/{} - {}", cached.subject, cached.version, e))?;

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
