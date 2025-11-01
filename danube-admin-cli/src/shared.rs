//! This module contain the struct defined on the server side
//! so all the changes on the server side should reflect down here as well
//! in the future maybe create a shared resources module

use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct Policies {
    // Limits the maximum number of producers that can simultaneously publish messages to a specific topic.
    max_producers_per_topic: Option<u32>,
    // Limits the maximum number of subscriptions that can be created on the topic.
    max_subscriptions_per_topic: Option<u32>,
    // Limits the maximum number of consumers that can simultaneously consume messages from a specific topic.
    max_consumers_per_topic: Option<u32>,
    // Limits the maximum number of consumers that can simultaneously use a single subscription on a topic.
    max_consumers_per_subscription: Option<u32>,

    // Defines the Max publish rate (number of messages and/or bytes per second) for producers publishing to the topic.
    max_publish_rate: Option<u32>,
    // Defines the dispatch rate for each subscription on the topic.
    max_subscription_dispatch_rate: Option<u32>,

    // Limits the maximum size of a single message that can be published to the topic.
    max_message_size: Option<u32>,
}

pub(crate) fn display_policies(policies: &Policies) {
    println!("Policies Configuration:");
    println!("-----------------------");
    match policies.max_producers_per_topic {
        Some(value) => println!("Max Producers per Topic: {}", value),
        None => println!("Max Producers per Topic: Not Set"),
    }
    match policies.max_subscriptions_per_topic {
        Some(value) => println!("Max Subscriptions per Topic: {}", value),
        None => println!("Max Subscriptions per Topic: Not Set"),
    }
    match policies.max_consumers_per_topic {
        Some(value) => println!("Max Consumers per Topic: {}", value),
        None => println!("Max Consumers per Topic: Not Set"),
    }
    match policies.max_consumers_per_subscription {
        Some(value) => println!("Max Consumers per Subscription: {}", value),
        None => println!("Max Consumers per Subscription: Not Set"),
    }
    match policies.max_publish_rate {
        Some(value) => println!("Max publish rate: {}", value),
        None => println!("Max publish rate Not Set"),
    }

    match policies.max_subscription_dispatch_rate {
        Some(value) => println!("Dispatch rate for subscription: {}", value),
        None => println!("dispatch rate for subscription Not Set"),
    }

    match policies.max_message_size {
        Some(value) => println!("Max message size: {}", value),
        None => println!("Max message size Not Set"),
    }

    println!("-----------------------");
}
