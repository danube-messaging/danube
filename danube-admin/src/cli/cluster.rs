use anyhow::Result;
use clap::{Args, Subcommand};
use danube_core::admin_proto::{AddNodeRequest, PromoteNodeRequest, RemoveNodeRequest};

use crate::core::{AdminGrpcClient, GrpcClientConfig};

#[derive(Debug, Args)]
pub struct Cluster {
    #[command(subcommand)]
    command: ClusterCommands,
}

#[derive(Debug, Subcommand)]
enum ClusterCommands {
    #[command(
        about = "Show current Raft cluster state (leader, term, voters, learners)",
        after_help = "Examples:
  danube-admin cluster status

Env:
  DANUBE_ADMIN_ENDPOINT (default http://127.0.0.1:50051)"
    )]
    Status,

    #[command(
        about = "Add a new broker to the cluster as a Raft learner (non-voting)",
        after_help = "Examples:
  danube-admin cluster add-node --node-addr http://new-broker:50054

The CLI connects to the new node's admin port to discover its node_id and raft_addr,
then tells the current leader to add it as a learner.

Env:
  DANUBE_ADMIN_ENDPOINT (default http://127.0.0.1:50051)"
    )]
    AddNode {
        #[arg(long, required = true, help = "Admin endpoint of the new broker")]
        node_addr: String,
    },

    #[command(
        about = "Promote a Raft learner to a full voting member",
        after_help = "Examples:
  danube-admin cluster promote-node --node-id 12345678

Env:
  DANUBE_ADMIN_ENDPOINT (default http://127.0.0.1:50051)"
    )]
    PromoteNode {
        #[arg(long, required = true, help = "Node ID to promote")]
        node_id: u64,
    },

    #[command(
        about = "Remove a node from the Raft cluster",
        after_help = "Examples:
  danube-admin cluster remove-node --node-id 12345678

Env:
  DANUBE_ADMIN_ENDPOINT (default http://127.0.0.1:50051)"
    )]
    RemoveNode {
        #[arg(long, required = true, help = "Node ID to remove")]
        node_id: u64,
    },
}

pub async fn handle(cmd: Cluster, endpoint: &str) -> Result<()> {
    match cmd.command {
        ClusterCommands::Status => handle_status(endpoint).await,
        ClusterCommands::AddNode { node_addr } => handle_add_node(node_addr, endpoint).await,
        ClusterCommands::PromoteNode { node_id } => handle_promote_node(node_id, endpoint).await,
        ClusterCommands::RemoveNode { node_id } => handle_remove_node(node_id, endpoint).await,
    }
}

async fn handle_status(endpoint: &str) -> Result<()> {
    let client = AdminGrpcClient::connect(GrpcClientConfig {
        endpoint: endpoint.to_string(),
        request_timeout_ms: 5000,
        ..Default::default()
    })
    .await?;

    let status = client.cluster_status().await?;

    println!("Raft Cluster Status:");
    println!("  Self Node ID:  {}", status.self_node_id);
    println!("  Raft Address:  {}", status.raft_addr);
    println!(
        "  Leader:        {}",
        if status.leader_id == 0 {
            "none".to_string()
        } else {
            status.leader_id.to_string()
        }
    );
    println!("  Term:          {}", status.current_term);
    println!("  Last Applied:  {}", status.last_applied);
    println!(
        "  Voters:        [{}]",
        status
            .voters
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    if !status.learners.is_empty() {
        println!(
            "  Learners:      [{}]",
            status
                .learners
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    Ok(())
}

async fn handle_add_node(node_addr: String, endpoint: &str) -> Result<()> {
    // Step 1: Discover the new node's identity
    println!("Discovering node at {}...", node_addr);
    let new_node_client = AdminGrpcClient::connect(GrpcClientConfig {
        endpoint: node_addr.clone(),
        request_timeout_ms: 5000,
        ..Default::default()
    })
    .await?;

    let status = new_node_client.cluster_status().await?;
    let node_id = status.self_node_id;
    let raft_addr = status.raft_addr;
    println!("  Discovered: node_id={}, raft_addr={}", node_id, raft_addr);

    // Step 2: Tell the leader to add it as a learner
    let leader_client = AdminGrpcClient::connect(GrpcClientConfig {
        endpoint: endpoint.to_string(),
        request_timeout_ms: 10000,
        ..Default::default()
    })
    .await?;

    println!("Adding node {} as learner via {}...", node_id, endpoint);
    let resp = leader_client
        .add_node(AddNodeRequest {
            addr: raft_addr,
            node_id,
        })
        .await?;

    if resp.success {
        println!("Success: {}", resp.message);
    } else {
        println!("Failed: {}", resp.message);
    }

    Ok(())
}

async fn handle_promote_node(node_id: u64, endpoint: &str) -> Result<()> {
    let client = AdminGrpcClient::connect(GrpcClientConfig {
        endpoint: endpoint.to_string(),
        request_timeout_ms: 10000,
        ..Default::default()
    })
    .await?;

    println!("Promoting node {} to voter...", node_id);
    let resp = client.promote_node(PromoteNodeRequest { node_id }).await?;

    if resp.success {
        println!("Success: {}", resp.message);
    } else {
        println!("Failed: {}", resp.message);
    }

    Ok(())
}

async fn handle_remove_node(node_id: u64, endpoint: &str) -> Result<()> {
    let client = AdminGrpcClient::connect(GrpcClientConfig {
        endpoint: endpoint.to_string(),
        request_timeout_ms: 10000,
        ..Default::default()
    })
    .await?;

    println!("Removing node {} from cluster...", node_id);
    let resp = client.remove_node(RemoveNodeRequest { node_id }).await?;

    if resp.success {
        println!("Success: {}", resp.message);
    } else {
        println!("Failed: {}", resp.message);
    }

    Ok(())
}
