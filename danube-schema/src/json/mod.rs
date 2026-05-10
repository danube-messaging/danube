//! JSON Schema Implementation
//!
//! This module contains all JSON Schema-specific functionality for the schema registry:
//! - Schema parsing and fingerprinting (json_handler)
//! - Compatibility checking (json_compatibility)
//! - Payload validation (json_validator)
//!
//! JSON Schema is a vocabulary that allows you to annotate and validate JSON documents.
//! It provides a contract for what JSON data is required for a given application.

mod json_compatibility;
mod json_handler;
mod json_validator;

pub use json_compatibility::JsonCompatibilityChecker;
pub use json_handler::JsonHandler;
pub use json_validator::JsonValidator;
