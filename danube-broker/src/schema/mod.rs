// Schema Registry Module
// This module provides comprehensive schema management for Danube messaging
//
// The module is organized by schema type, where each schema format (Avro, JSON, Protobuf)
// has its own submodule containing:
// - Handler: Parsing and fingerprinting
// - Compatibility: Schema evolution checking
// - Validator: Payload validation

pub mod avro;
pub mod compatibility;
pub mod json;
pub mod metadata;
pub mod protobuf;
pub mod registry;
pub mod types;
pub mod validator;

// Re-export only the public API types that are used externally
pub use registry::SchemaRegistry;
pub use types::{CompatibilityMode, ValidationPolicy};

// Internal types are available via their modules but not re-exported
// (e.g., schema::metadata::SchemaMetadata, schema::compatibility::CompatibilityChecker)
// This keeps the public API surface clean and focused
