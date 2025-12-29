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

// TODO Phase 4: Old schema module removed - migrate to SchemaReference
// mod schema;
// pub use schema::{Schema, SchemaType};

// mod schema_service;

// Phase 5: New Schema Registry Client
mod schema_registry_client;
pub use schema_registry_client::SchemaRegistryClient;

mod lookup_service;

mod connection_manager;

mod health_check;

mod retry_manager;
