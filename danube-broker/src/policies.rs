use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Policies {
    /// Limits the maximum number of producers that can simultaneously publish messages to a specific topic.
    /// Default is 0, unlimited.
    #[serde(default)]
    max_producers_per_topic: u32,
    /// Limits the maximum number of subscriptions that can be created on the topic.
    /// Default is 0, unlimited.
    #[serde(default)]
    max_subscriptions_per_topic: u32,
    /// Limits the maximum number of consumers that can simultaneously consume messages from a specific topic.
    /// Default is 0, unlimited.
    #[serde(default)]
    max_consumers_per_topic: u32,
    /// Limits the maximum number of consumers that can simultaneously use a single subscription on a topic.
    /// Default is 0, unlimited.
    #[serde(default)]
    max_consumers_per_subscription: u32,

    /// Defines the Max publish rate (number of messages and/or bytes per second) for producers publishing to the topic.
    /// Default is 0, unlimited.
    #[serde(default)]
    max_publish_rate: u32,
    /// Defines the Max dispatch rate (number of messages and/or bytes per second) for the topic.
    /// Default is 0, unlimited.
    #[serde(default)]
    max_dispatch_rate: u32,
    /// Defines the dispatch rate for each subscription on the topic.
    /// Default is 0, unlimited.
    #[serde(default)]
    max_subscription_dispatch_rate: u32,

    // Limits the maximum size of a single message that can be published to the topic.
    #[serde(default = "default_max_message_size")]
    max_message_size: u32,
}

// Custom function to return the default 10 MB value
fn default_max_message_size() -> u32 {
    10 * 1024 * 1024 // 10 MB in bytes
}

impl Policies {
    pub(crate) fn new() -> Self {
        Policies {
            ..Default::default()
        }
    }

    // Read-only getters for policy fields
    pub fn get_max_producers_per_topic(&self) -> u32 {
        self.max_producers_per_topic
    }

    pub fn get_max_subscriptions_per_topic(&self) -> u32 {
        self.max_subscriptions_per_topic
    }

    pub fn get_max_consumers_per_topic(&self) -> u32 {
        self.max_consumers_per_topic
    }

    pub fn get_max_consumers_per_subscription(&self) -> u32 {
        self.max_consumers_per_subscription
    }

    pub fn get_max_publish_rate(&self) -> u32 {
        self.max_publish_rate
    }

    pub fn get_max_dispatch_rate(&self) -> u32 {
        self.max_dispatch_rate
    }

    pub fn get_max_subscription_dispatch_rate(&self) -> u32 {
        self.max_subscription_dispatch_rate
    }

    pub fn get_max_message_size(&self) -> u32 {
        self.max_message_size
    }

    #[allow(dead_code)]
    pub(crate) fn get_fields_as_map(&self) -> Map<String, Value> {
        let serialized = serde_json::to_value(self).unwrap();
        let map: Map<String, Value> = serialized.as_object().unwrap().clone();
        map
    }

    // ensure that we can construct a complete Policies with the fields retrieve from the Store
    #[allow(dead_code)]
    pub fn from_hashmap(map: HashMap<String, Value>) -> Result<Self> {
        let mut policies = Policies::default();
        let mut found_fields = HashSet::new();
        let expected_fields = [
            "max_producers_per_topic",
            "max_subscriptions_per_topic",
            "max_consumers_per_topic",
            "max_consumers_per_subscription",
            "max_publish_rate",
            "max_dispatch_rate",
            "max_subscription_dispatch_rate",
            "max_message_size",
        ];

        for (key, value) in map {
            match key.as_str() {
                "max_producers_per_topic" => {
                    if let Some(val) = value.as_u64() {
                        policies.max_producers_per_topic = val as u32;
                    }
                    found_fields.insert("max_producers_per_topic");
                }
                "max_subscriptions_per_topic" => {
                    if let Some(val) = value.as_u64() {
                        policies.max_subscriptions_per_topic = val as u32;
                    }
                    found_fields.insert("max_subscriptions_per_topic");
                }
                "max_consumers_per_topic" => {
                    if let Some(val) = value.as_u64() {
                        policies.max_consumers_per_topic = val as u32;
                    }
                    found_fields.insert("max_consumers_per_topic");
                }
                "max_consumers_per_subscription" => {
                    if let Some(val) = value.as_u64() {
                        policies.max_consumers_per_subscription = val as u32;
                    }
                    found_fields.insert("max_consumers_per_subscription");
                }
                "max_publish_rate" => {
                    if let Some(val) = value.as_u64() {
                        policies.max_publish_rate = val as u32;
                    }
                    found_fields.insert("max_publish_rate");
                }
                "max_dispatch_rate" => {
                    if let Some(val) = value.as_u64() {
                        policies.max_dispatch_rate = val as u32;
                    }
                    found_fields.insert("max_dispatch_rate");
                }
                "max_subscription_dispatch_rate" => {
                    if let Some(val) = value.as_u64() {
                        policies.max_subscription_dispatch_rate = val as u32;
                    }
                    found_fields.insert("max_subscription_dispatch_rate");
                }
                "max_message_size" => {
                    if let Some(val) = value.as_u64() {
                        policies.max_message_size = val as u32;
                    }
                    found_fields.insert("max_message_size");
                }
                _ => {} // Ignore unknown fields
            }
        }

        // Check for missing fields
        let missing_fields: Vec<String> = expected_fields
            .iter()
            .filter(|&&field| !found_fields.contains(field))
            .map(|&field| field.to_string())
            .collect();

        if !missing_fields.is_empty() {
            return Err(anyhow!(
                "Can't construct a Policies struct, as some fields are missing: {:?}",
                missing_fields
            ));
        }

        Ok(policies)
    }
}
