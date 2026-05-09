//! Avro Schema Implementation
//!
//! This module contains all Avro-specific functionality for the schema registry:
//! - Schema parsing and fingerprinting (avro_handler)
//! - Compatibility checking (avro_compatibility)
//! - Payload validation (avro_validator)
//!
//! Apache Avro is a data serialization system with rich schema evolution support.

mod avro_compatibility;
mod avro_handler;
mod avro_validator;

pub use avro_compatibility::AvroCompatibilityChecker;
pub use avro_handler::AvroHandler;
pub use avro_validator::AvroValidator;
