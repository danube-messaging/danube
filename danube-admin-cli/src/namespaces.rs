use crate::shared::{display_policies, Policies};
use danube_core::admin_proto::{namespace_admin_client::NamespaceAdminClient, NamespaceRequest};

use clap::{Args, Subcommand};

#[derive(Debug, Args)]
pub(crate) struct Namespaces {
    #[command(subcommand)]
    command: NamespacesCommands,
}

#[derive(Debug, Subcommand)]
pub(crate) enum NamespacesCommands {
    #[command(about = "List topics in the specified namespace")]
    Topics { namespace: String },
    #[command(about = "List the configuration policies for a specified namespace")]
    Policies { namespace: String },
    #[command(about = "Create a new namespace")]
    Create { namespace: String },
    #[command(about = "Delete an existing namespace")]
    Delete { namespace: String },
}

pub async fn handle_command(namespaces: Namespaces) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = NamespaceAdminClient::connect("http://127.0.0.1:50051").await?;

    match namespaces.command {
        // Get the list of topics of a namespace
        NamespacesCommands::Topics { namespace } => {
            let request = NamespaceRequest { name: namespace };
            let response = client.get_namespace_topics(request).await?;

            let topics = response.into_inner().topics;

            for topic in topics {
                println!("Topic: {}", topic);
            }
        }

        // Get the configuration policies of a namespace
        NamespacesCommands::Policies { namespace } => {
            let request = NamespaceRequest { name: namespace };
            let response = client.get_namespace_policies(request).await?;

            let policy = response.into_inner().policies;

            let policies: Policies = serde_json::from_str(&policy)?;

            display_policies(&policies);
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
