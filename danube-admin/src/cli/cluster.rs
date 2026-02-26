use anyhow::{anyhow, Result};
use clap::{Args, Subcommand};
use danube_core::admin_proto::{
    AddNodeRequest, ClusterInitRequest, ClusterNodeInfo, PromoteNodeRequest, RemoveNodeRequest,
};

use crate::core::{AdminGrpcClient, GrpcClientConfig};

#[derive(Debug, Args)]
pub struct Cluster {
    #[command(subcommand)]
    command: ClusterCommands,
}

#[derive(Debug, Subcommand)]
enum ClusterCommands {
    #[command(
        about = "Bootstrap a new Raft cluster from one or more broker admin endpoints",
        after_help = "Examples:
  # Single-node (development)
  danube-admin cluster init --peers http://0.0.0.0:50051

  # Three-node cluster
  danube-admin cluster init --peers http://broker1:50051,http://broker2:50052,http://broker3:50053

The CLI connects to each peer's admin port, discovers its node_id and raft_addr
via ClusterStatus, then sends the full membership to the first peer to initialize.

Env:
  DANUBE_ADMIN_ENDPOINT (default http://127.0.0.1:50051)"
    )]
    Init {
        #[arg(
            long,
            required = true,
            value_delimiter = ',',
            help = "Comma-separated admin endpoints of all initial brokers"
        )]
        peers: Vec<String>,
    },

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
        ClusterCommands::Init { peers } => handle_init(peers, endpoint).await,
        ClusterCommands::Status => handle_status(endpoint).await,
        ClusterCommands::AddNode { node_addr } => handle_add_node(node_addr, endpoint).await,
        ClusterCommands::PromoteNode { node_id } => handle_promote_node(node_id, endpoint).await,
        ClusterCommands::RemoveNode { node_id } => handle_remove_node(node_id, endpoint).await,
    }
}

async fn handle_init(peers: Vec<String>, _endpoint: &str) -> Result<()> {
    if peers.is_empty() {
        return Err(anyhow!("at least one peer is required"));
    }

    println!("Discovering nodes from {} peer(s)...", peers.len());

    // Step 1: Connect to each peer's admin port and discover (node_id, raft_addr)
    let mut nodes = Vec::new();
    for peer in &peers {
        let client = AdminGrpcClient::connect(GrpcClientConfig {
            endpoint: peer.clone(),
            request_timeout_ms: 5000,
            ..Default::default()
        })
        .await?;

        let status = client.cluster_status().await?;
        println!(
            "  Discovered: node_id={}, raft_addr={} (from {})",
            status.self_node_id, status.raft_addr, peer
        );

        nodes.push(ClusterNodeInfo {
            node_id: status.self_node_id,
            raft_addr: status.raft_addr,
        });
    }

    // Step 2: Send cluster init to the first peer with all discovered nodes
    let first_peer = &peers[0];
    let client = AdminGrpcClient::connect(GrpcClientConfig {
        endpoint: first_peer.clone(),
        request_timeout_ms: 10000,
        ..Default::default()
    })
    .await?;

    println!(
        "Initializing cluster via {} with {} node(s)...",
        first_peer,
        nodes.len()
    );

    let resp = client
        .cluster_init(ClusterInitRequest { nodes })
        .await?;

    if resp.already_initialized {
        println!("Cluster already initialized (idempotent no-op)");
        println!("  Leader: node_id={}", resp.leader_id);
        println!("  Voters: {}", resp.voter_count);
    } else {
        println!("Cluster initialized successfully!");
        println!("  Leader: node_id={}", resp.leader_id);
        println!("  Voters: {}", resp.voter_count);
        println!("  {}", resp.message);
    }

    Ok(())
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
    println!("  Leader:        {}", if status.leader_id == 0 { "none".to_string() } else { status.leader_id.to_string() });
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
    println!(
        "  Discovered: node_id={}, raft_addr={}",
        node_id, raft_addr
    );

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
    let resp = client
        .promote_node(PromoteNodeRequest { node_id })
        .await?;

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
    let resp = client
        .remove_node(RemoveNodeRequest { node_id })
        .await?;

    if resp.success {
        println!("Success: {}", resp.message);
    } else {
        println!("Failed: {}", resp.message);
    }

    Ok(())
}
