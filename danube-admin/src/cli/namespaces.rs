use anyhow::Result;
use clap::{Args, Subcommand};
use danube_core::admin_proto::NamespaceRequest;
use serde::{Deserialize, Serialize};

use crate::core::{AdminGrpcClient, GrpcClientConfig};

#[derive(Debug, Args)]
pub struct Namespaces {
    #[command(subcommand)]
    command: NamespacesCommands,
}

#[derive(Debug, Subcommand)]
enum NamespacesCommands {
    #[command(about = "List topics in the specified namespace")]
    Topics {
        namespace: String,
        #[arg(long, value_parser = ["json"], help = "Output format: json (default: plain)")]
        output: Option<String>,
    },
    #[command(about = "List the configuration policies for a specified namespace")]
    Policies {
        namespace: String,
        #[arg(long, value_parser = ["json"], help = "Output format: json (default: pretty text)")]
        output: Option<String>,
    },
    #[command(about = "Create a new namespace")]
    Create { namespace: String },
    #[command(about = "Delete an existing namespace")]
    Delete { namespace: String },
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct Policies {
    max_producers_per_topic: Option<u32>,
    max_subscriptions_per_topic: Option<u32>,
    max_consumers_per_topic: Option<u32>,
    max_consumers_per_subscription: Option<u32>,
    max_publish_rate: Option<u32>,
    max_subscription_dispatch_rate: Option<u32>,
    max_message_size: Option<u32>,
}

fn display_policies(policies: &Policies) {
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

pub async fn handle(namespaces: Namespaces) -> Result<()> {
    let config = GrpcClientConfig::default();
    let client = AdminGrpcClient::connect(config).await?;

    match namespaces.command {
        // Get the list of topics of a namespace
        NamespacesCommands::Topics { namespace, output } => {
            let request = NamespaceRequest {
                name: namespace.clone(),
            };
            let response = client.list_namespace_topics(request).await?;

            let topics: Vec<String> = response.topics.into_iter().map(|t| t.name).collect();
            
            if matches!(output.as_deref(), Some("json")) {
                println!("{}", serde_json::to_string_pretty(&topics)?);
            } else {
                for topic in topics {
                    println!("Topic: {}", topic);
                }
            }
        }

        // Get the configuration policies of a namespace
        NamespacesCommands::Policies { namespace, output } => {
            let request = NamespaceRequest { name: namespace };
            let response = client.get_namespace_policies(request).await?;

            let policy = response.policies;
            
            if matches!(output.as_deref(), Some("json")) {
                // already a JSON string from server; pretty print if valid JSON
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&policy) {
                    println!("{}", serde_json::to_string_pretty(&v)?);
                } else {
                    println!("{}", policy);
                }
            } else {
                let policies: Policies = serde_json::from_str(&policy)?;
                display_policies(&policies);
            }
        }

        // Create a new namespace
        NamespacesCommands::Create { namespace } => {
            let request = NamespaceRequest { name: namespace };
            let response = client.create_namespace(request).await?;
            println!("Namespace Created: {:?}", response.success);
        }

        // Deletes a namespace. The namespace needs to be empty
        NamespacesCommands::Delete { namespace } => {
            let request = NamespaceRequest { name: namespace };
            let response = client.delete_namespace(request).await?;
            println!("Namespace Deleted: {:?}", response.success);
        }
    }

    Ok(())
}
