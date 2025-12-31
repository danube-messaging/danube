// Schema Registry Module
// This module provides comprehensive schema management for Danube messaging

pub mod compatibility;
pub mod formats;
pub mod metadata;
pub mod registry;
// Phase 6: storage.rs deleted - SchemaRegistry now uses SchemaResources directly
pub mod types;
pub mod validator;

// Re-export only the public API types that are used externally
pub use registry::SchemaRegistry;
pub use types::{CompatibilityMode, ValidationPolicy};

// Internal types are available via their modules but not re-exported
// (e.g., schema::metadata::SchemaMetadata, schema::compatibility::CompatibilityChecker)
// This keeps the public API surface clean and focused
