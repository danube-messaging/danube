// Schema Registry Module for Danube Messaging
//
// This crate provides comprehensive schema management, extracted from the broker
// to enable reuse across crates (broker, edge, admin) without circular dependencies.
//
// The crate is organized by schema format, where each schema type (Avro, JSON, Protobuf)
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
pub mod resources;
pub mod topic_schema;
pub mod types;
pub mod validator;

// Re-export the public API types
pub use registry::SchemaRegistry;
pub use resources::{SchemaDetails, SchemaResources};
pub use topic_schema::TopicSchemaContext;
pub use types::{CompatibilityMode, SchemaType, ValidationPolicy};
