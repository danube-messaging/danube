//! Danube-Client
//!
//! Danube-Client -- the Danube stream service client

mod client;
pub use client::{DanubeClient, DanubeClientBuilder};

pub mod errors;

mod producer;
pub use producer::{Producer, ProducerBuilder, ProducerOptions};

mod topic_producer;

mod consumer;
pub use consumer::{Consumer, ConsumerBuilder, ConsumerOptions, SubType};

mod topic_consumer;

mod message_router;

mod schema;
pub use schema::{Schema, SchemaType};

mod schema_service;

mod lookup_service;

mod connection_manager;

mod rpc_connection;

mod health_check;

mod dispatch_strategy;
pub use dispatch_strategy::{ConfigDispatchStrategy, ReliableOptions, RetentionPolicy};
