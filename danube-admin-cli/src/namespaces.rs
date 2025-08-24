use crate::shared::{display_policies, Policies};
use crate::client::namespace_admin_client;
use danube_core::admin_proto::NamespaceRequest;

use clap::{Args, Subcommand};

#[derive(Debug, Args)]
pub(crate) struct Namespaces {
    #[command(subcommand)]
    command: NamespacesCommands,
}

#[derive(Debug, Subcommand)]
pub(crate) enum NamespacesCommands {
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

pub async fn handle_command(namespaces: Namespaces) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = namespace_admin_client().await?;

    match namespaces.command {
        // Get the list of topics of a namespace
        NamespacesCommands::Topics { namespace, output } => {
            let request = NamespaceRequest { name: namespace };
            let response = client.get_namespace_topics(request).await?;

            let topics = response.into_inner().topics;
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

            let policy = response.into_inner().policies;
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
            println!("Namespace Created: {:?}", response.into_inner().success);
        }

        // Deletes a namespace. The namespace needs to be empty
        NamespacesCommands::Delete { namespace } => {
            let request = NamespaceRequest { name: namespace };
            let response = client.delete_namespace(request).await?;
            println!("Namespace Deleted: {:?}", response.into_inner().success);
        }
    }

    Ok(())
}
