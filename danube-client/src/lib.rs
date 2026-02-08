//! Danube-Client
//!
//! Danube-Client -- the Danube stream service client

mod client;
pub use client::{DanubeClient, DanubeClientBuilder};

mod auth_service;

pub mod errors;

mod producer;
pub use producer::{Producer, ProducerBuilder, ProducerOptions};

mod topic_producer;

mod consumer;
pub use consumer::{Consumer, ConsumerBuilder, ConsumerOptions, SubType};

mod topic_consumer;

mod message_router;

mod schema_registry_client;
pub use schema_registry_client::{SchemaRegistrationBuilder, SchemaRegistryClient};

mod schema_types;
pub use schema_types::{CompatibilityMode, SchemaInfo, SchemaType};

// Re-export proto types for schema reference (advanced use)
pub use danube_core::proto::schema_reference::VersionRef;
pub use danube_core::proto::SchemaReference;

mod lookup_service;

mod connection_manager;

mod health_check;

mod retry_manager;
