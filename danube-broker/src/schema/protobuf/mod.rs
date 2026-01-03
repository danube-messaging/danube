//! Protobuf Schema Implementation
//!
//! This module contains all Protocol Buffers-specific functionality for the schema registry:
//! - Schema parsing and fingerprinting (protobuf_handler)
//! - Compatibility checking (protobuf_compatibility)
//! - Payload validation (protobuf_validator)
//!
//! Protocol Buffers (protobuf) is Google's language-neutral, platform-neutral,
//! extensible mechanism for serializing structured data.
//!
//! **Note**: This implementation is currently a stub. Full protobuf support is planned
//! for future releases.

mod protobuf_compatibility;
mod protobuf_handler;
mod protobuf_validator;

pub use protobuf_compatibility::ProtobufCompatibilityChecker;
pub use protobuf_handler::ProtobufHandler;
pub use protobuf_validator::ProtobufValidator;
