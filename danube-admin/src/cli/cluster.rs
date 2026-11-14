use anyhow::{anyhow, Result};
use clap::{Args, Subcommand};
use danube_core::admin_proto::{
    AddNodeRequest, ConfigureClusterStorageRequest, ConfigureLoadManagerRequest,
    ConfigureWalRetentionRequest, ConfigureWriteBufferRequest, ExportClusterConfigRequest,
    PromoteNodeRequest, RemoveNodeRequest,
};
use std::collections::HashMap;

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

    #[command(about = "Configure or inspect cluster durable storage mode (local, shared_fs, object_store)")]
    Storage {
        #[command(subcommand)]
        action: StorageCommands,
    },

    #[command(about = "Enable/configure or disable the Valkey warm write buffer")]
    WriteBuffer {
        #[command(subcommand)]
        action: WriteBufferCommands,
    },

    #[command(about = "Configure or inspect cluster-wide WAL and retention limits")]
    Wal {
        #[arg(long, help = "Retention window in minutes")]
        retention_time_minutes: Option<u64>,
        #[arg(long, help = "Retention size cap in MiB")]
        retention_size_mb: Option<u64>,
        #[arg(long, help = "Retention deleter check interval in minutes")]
        check_interval_minutes: Option<u64>,
        #[arg(long, help = "WAL segment rotation size in bytes")]
        rotate_max_bytes: Option<u64>,
        #[arg(long, help = "WAL segment rotation age in hours")]
        rotate_max_hours: Option<u64>,
        #[arg(long, help = "WAL in-memory cache capacity (entries)")]
        cache_capacity: Option<u64>,
        #[arg(long, help = "WAL batch flush interval in ms")]
        flush_interval_ms: Option<u64>,
        #[arg(long, help = "WAL batch flush max size in bytes")]
        flush_max_batch_bytes: Option<u64>,
        #[arg(long, help = "Clear retention limits (disable background deleter)")]
        clear_retention: bool,
    },

    #[command(about = "Configure or inspect LoadManager placement strategy and automated rebalancing")]
    LoadManager {
        #[arg(long, help = "Placement strategy: fair, balanced, or weighted_load")]
        assignment_strategy: Option<String>,
        #[arg(long, help = "Broker load report interval in seconds")]
        load_report_interval_seconds: Option<u64>,
        #[arg(long, help = "Enable or disable automated rebalancing (true/false)")]
        rebalance: Option<bool>,
        #[arg(long, help = "Rebalancing profile preset: conservative, balanced, or aggressive")]
        profile: Option<String>,
        #[arg(long, help = "Override rebalance check interval in seconds")]
        check_interval_seconds: Option<u64>,
        #[arg(long, help = "Override max topic moves per hour")]
        max_moves_per_hour: Option<u32>,
        #[arg(long, help = "Override cooldown seconds between moves")]
        cooldown_seconds: Option<u64>,
        #[arg(long, help = "Minimum active brokers required before rebalancing")]
        min_brokers: Option<u32>,
        #[arg(long, help = "Minimum topic age in seconds before eligible for move")]
        min_topic_age_seconds: Option<u64>,
        #[arg(long, value_delimiter = ',', help = "Comma-separated topic patterns to exclude from moves")]
        blacklist_topics: Vec<String>,
        #[arg(long, help = "Replace blacklist with --blacklist-topics (use with empty list to clear)")]
        replace_blacklist: bool,
    },

    #[command(about = "Export live cluster config as a break-glass RecoveryConfig YAML/JSON")]
    ExportConfig {
        #[arg(long, help = "Optional broker ID to include node bind/admin addresses")]
        broker_id: Option<u64>,
        #[arg(long, default_value = "yaml", help = "Output format: yaml or json")]
        format: String,
        #[arg(long, help = "Optional file path to write exported config")]
        output: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
enum StorageCommands {
    #[command(about = "Show effective cluster storage configuration")]
    Status,
    #[command(about = "Switch cluster storage mode to local WAL only")]
    Local {
        #[arg(long, help = "Allow switching even if cloud-tiered topics exist")]
        force: bool,
    },
    #[command(about = "Configure cluster storage mode to shared filesystem (NFS/EFS)")]
    SharedFs {
        #[arg(long, required = true, help = "Shared filesystem durable segments root path")]
        root: String,
        #[arg(long, help = "Allow replacing active durable store when cloud-tiered topics exist")]
        force: bool,
    },
    #[command(about = "Configure cluster storage mode to cloud object store (S3/GCS/Azblob)")]
    ObjectStore {
        #[arg(long, required = true, help = "Backend: s3, gcs, azblob, or filesystem")]
        backend: String,
        #[arg(long, required = true, help = "Bucket/container URI or root path")]
        root: String,
        #[arg(long = "option", value_name = "KEY=VALUE", help = "Additional backend options")]
        options: Vec<String>,
        #[arg(long, help = "Allow replacing active object store when cloud-tiered topics exist")]
        force: bool,
    },
}

#[derive(Debug, Subcommand)]
enum WriteBufferCommands {
    #[command(about = "Enable or update Valkey warm write buffer")]
    Enable {
        #[arg(long, required = true, value_delimiter = ',', help = "Comma-separated Valkey/Redis URLs")]
        endpoints: Vec<String>,
        #[arg(long, help = "Preset profile: ha or dev")]
        profile: Option<String>,
        #[arg(long, help = "Number of replicas required for WAIT")]
        wait_replicas: Option<u32>,
        #[arg(long, help = "WAIT timeout in ms")]
        wait_timeout_ms: Option<u64>,
        #[arg(long, help = "Policy on WAIT timeout: fail or ack")]
        on_wait_timeout: Option<String>,
        #[arg(long, help = "Max closed WAL segments cached in Valkey")]
        max_cached_segments: Option<u32>,
    },
    #[command(about = "Disable Valkey warm write buffer")]
    Disable,
}

pub async fn handle(cmd: Cluster, endpoint: &str) -> Result<()> {
    match cmd.command {
        ClusterCommands::Status => handle_status(endpoint).await,
        ClusterCommands::AddNode { node_addr } => handle_add_node(node_addr, endpoint).await,
        ClusterCommands::PromoteNode { node_id } => handle_promote_node(node_id, endpoint).await,
        ClusterCommands::RemoveNode { node_id } => handle_remove_node(node_id, endpoint).await,
        ClusterCommands::Storage { action } => handle_storage(action, endpoint).await,
        ClusterCommands::WriteBuffer { action } => handle_write_buffer(action, endpoint).await,
        ClusterCommands::Wal {
            retention_time_minutes,
            retention_size_mb,
            check_interval_minutes,
            rotate_max_bytes,
            rotate_max_hours,
            cache_capacity,
            flush_interval_ms,
            flush_max_batch_bytes,
            clear_retention,
        } => {
            handle_wal(
                endpoint,
                retention_time_minutes,
                retention_size_mb,
                check_interval_minutes,
                rotate_max_bytes,
                rotate_max_hours,
                cache_capacity,
                flush_interval_ms,
                flush_max_batch_bytes,
                clear_retention,
            )
            .await
        }
        ClusterCommands::LoadManager {
            assignment_strategy,
            load_report_interval_seconds,
            rebalance,
            profile,
            check_interval_seconds,
            max_moves_per_hour,
            cooldown_seconds,
            min_brokers,
            min_topic_age_seconds,
            blacklist_topics,
            replace_blacklist,
        } => {
            handle_load_manager(
                endpoint,
                assignment_strategy,
                load_report_interval_seconds,
                rebalance,
                profile,
                check_interval_seconds,
                max_moves_per_hour,
                cooldown_seconds,
                min_brokers,
                min_topic_age_seconds,
                blacklist_topics,
                replace_blacklist,
            )
            .await
        }
        ClusterCommands::ExportConfig {
            broker_id,
            format,
            output,
        } => handle_export_config(endpoint, broker_id, format, output).await,
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

async fn connect_client(endpoint: &str) -> Result<AdminGrpcClient> {
    AdminGrpcClient::connect(GrpcClientConfig {
        endpoint: endpoint.to_string(),
        request_timeout_ms: 10000,
        ..Default::default()
    })
    .await
}

fn parse_kv_options(pairs: Vec<String>) -> Result<HashMap<String, String>> {
    let mut map = HashMap::new();
    for pair in pairs {
        let (k, v) = pair
            .split_once('=')
            .ok_or_else(|| anyhow!("invalid --option '{}'; expected KEY=VALUE", pair))?;
        map.insert(k.trim().to_string(), v.trim().to_string());
    }
    Ok(map)
}

async fn handle_storage(action: StorageCommands, endpoint: &str) -> Result<()> {
    let client = connect_client(endpoint).await?;
    match action {
        StorageCommands::Status => {
            let exported = client
                .export_cluster_config(ExportClusterConfigRequest {
                    broker_id: None,
                    format: "yaml".to_string(),
                })
                .await?;
            println!("{}", exported.content);
        }
        StorageCommands::Local { force } => {
            let resp = client
                .configure_cluster_storage(ConfigureClusterStorageRequest {
                    mode: "local".to_string(),
                    backend: String::new(),
                    root: String::new(),
                    options: HashMap::new(),
                    force,
                })
                .await?;
            println!("{}", resp.message);
        }
        StorageCommands::SharedFs { root, force } => {
            let resp = client
                .configure_cluster_storage(ConfigureClusterStorageRequest {
                    mode: "shared_fs".to_string(),
                    backend: String::new(),
                    root,
                    options: HashMap::new(),
                    force,
                })
                .await?;
            println!("{}", resp.message);
        }
        StorageCommands::ObjectStore {
            backend,
            root,
            options,
            force,
        } => {
            let parsed_options = parse_kv_options(options)?;
            let resp = client
                .configure_cluster_storage(ConfigureClusterStorageRequest {
                    mode: "object_store".to_string(),
                    backend,
                    root,
                    options: parsed_options,
                    force,
                })
                .await?;
            println!("{}", resp.message);
        }
    }
    Ok(())
}

async fn handle_write_buffer(action: WriteBufferCommands, endpoint: &str) -> Result<()> {
    let client = connect_client(endpoint).await?;
    let req = match action {
        WriteBufferCommands::Enable {
            endpoints,
            profile,
            wait_replicas,
            wait_timeout_ms,
            on_wait_timeout,
            max_cached_segments,
        } => ConfigureWriteBufferRequest {
            enabled: true,
            endpoints,
            profile,
            wait_replicas,
            wait_timeout_ms,
            on_wait_timeout,
            max_cached_closed_segments: max_cached_segments,
        },
        WriteBufferCommands::Disable => ConfigureWriteBufferRequest {
            enabled: false,
            endpoints: Vec::new(),
            profile: None,
            wait_replicas: None,
            wait_timeout_ms: None,
            on_wait_timeout: None,
            max_cached_closed_segments: None,
        },
    };
    let resp = client.configure_write_buffer(req).await?;
    println!("{}", resp.message);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn handle_wal(
    endpoint: &str,
    retention_time_minutes: Option<u64>,
    retention_size_mb: Option<u64>,
    check_interval_minutes: Option<u64>,
    rotate_max_bytes: Option<u64>,
    rotate_max_hours: Option<u64>,
    cache_capacity: Option<u64>,
    flush_interval_ms: Option<u64>,
    flush_max_batch_bytes: Option<u64>,
    clear_retention: bool,
) -> Result<()> {
    let client = connect_client(endpoint).await?;
    let has_updates = clear_retention
        || retention_time_minutes.is_some()
        || retention_size_mb.is_some()
        || check_interval_minutes.is_some()
        || rotate_max_bytes.is_some()
        || rotate_max_hours.is_some()
        || cache_capacity.is_some()
        || flush_interval_ms.is_some()
        || flush_max_batch_bytes.is_some();

    if !has_updates {
        let exported = client
            .export_cluster_config(ExportClusterConfigRequest {
                broker_id: None,
                format: "yaml".to_string(),
            })
            .await?;
        println!("{}", exported.content);
        return Ok(());
    }

    let resp = client
        .configure_wal_retention(ConfigureWalRetentionRequest {
            retention_time_minutes,
            retention_size_mb,
            check_interval_minutes,
            rotate_max_bytes,
            rotate_max_hours,
            cache_capacity,
            flush_interval_ms,
            flush_max_batch_bytes,
            clear_retention,
        })
        .await?;
    println!("{}", resp.message);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn handle_load_manager(
    endpoint: &str,
    assignment_strategy: Option<String>,
    load_report_interval_seconds: Option<u64>,
    rebalancing_enabled: Option<bool>,
    rebalancing_profile: Option<String>,
    check_interval_seconds: Option<u64>,
    max_moves_per_hour: Option<u32>,
    cooldown_seconds: Option<u64>,
    min_brokers_for_rebalance: Option<u32>,
    min_topic_age_seconds: Option<u64>,
    blacklist_topics: Vec<String>,
    replace_blacklist: bool,
) -> Result<()> {
    let client = connect_client(endpoint).await?;
    let has_updates = assignment_strategy.is_some()
        || load_report_interval_seconds.is_some()
        || rebalancing_enabled.is_some()
        || rebalancing_profile.is_some()
        || check_interval_seconds.is_some()
        || max_moves_per_hour.is_some()
        || cooldown_seconds.is_some()
        || min_brokers_for_rebalance.is_some()
        || min_topic_age_seconds.is_some()
        || replace_blacklist
        || !blacklist_topics.is_empty();

    if !has_updates {
        let exported = client
            .export_cluster_config(ExportClusterConfigRequest {
                broker_id: None,
                format: "yaml".to_string(),
            })
            .await?;
        println!("{}", exported.content);
        return Ok(());
    }

    let resp = client
        .configure_load_manager(ConfigureLoadManagerRequest {
            assignment_strategy,
            load_report_interval_seconds,
            rebalancing_enabled,
            rebalancing_profile,
            check_interval_seconds,
            max_moves_per_hour,
            cooldown_seconds,
            min_brokers_for_rebalance,
            min_topic_age_seconds,
            blacklist_topics,
            replace_blacklist,
        })
        .await?;
    println!("{}", resp.message);
    Ok(())
}

async fn handle_export_config(
    endpoint: &str,
    broker_id: Option<u64>,
    format: String,
    output: Option<String>,
) -> Result<()> {
    let client = connect_client(endpoint).await?;
    let resp = client
        .export_cluster_config(ExportClusterConfigRequest { broker_id, format })
        .await?;
    if let Some(path) = output {
        std::fs::write(&path, &resp.content)?;
        println!("Exported cluster configuration ({}) to {}", resp.format, path);
    } else {
        print!("{}", resp.content);
    }
    Ok(())
}
