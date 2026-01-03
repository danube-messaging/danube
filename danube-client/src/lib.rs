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

// New Schema Registry Client
mod schema_registry_client;
pub use schema_registry_client::SchemaRegistryClient;

mod schema_types;
pub use schema_types::{CompatibilityMode, SchemaType};

mod lookup_service;

mod connection_manager;

mod health_check;

mod retry_manager;
