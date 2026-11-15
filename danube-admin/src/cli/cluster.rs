use anyhow::{anyhow, Result};
use clap::{Args, Subcommand};
use danube_core::admin_proto::{
    AddNodeRequest, ConfigureClusterStorageRequest, ConfigureLoadManagerRequest,
    ConfigureWalRetentionRequest, ConfigureWriteBufferRequest, ExportClusterConfigRequest,
    PromoteNodeRequest, RemoveNodeRequest,
};
use std::collections::HashMap;

use crate::cli::Connection;
use crate::core::{AdminGrpcClient, GrpcClientConfig};

#[derive(Debug, Args)]
#[command(
    about = "Manage the Raft cluster, storage, write buffer, WAL and load manager",
    long_about = "Manage the Raft cluster membership and cluster-wide configuration.\n\nCommon examples:\n  danube cluster status\n  danube cluster node add --node-addr http://new-broker:50051\n  danube cluster node promote --node-id 12345678\n  danube cluster storage show\n  danube cluster export-config --format yaml",
    subcommand_required = true,
    arg_required_else_help = true
)]
pub struct Cluster {
    #[command(subcommand)]
    command: ClusterCommands,

    #[command(flatten)]
    pub client: crate::cli::ClientArgs,
}

#[derive(Debug, Subcommand)]
enum ClusterCommands {
    #[command(
        about = "Show current Raft cluster state (leader, term, voters, learners)",
        after_help = "Example:\n  danube cluster status"
    )]
    Status,

    #[command(
        about = "Manage Raft cluster membership (add, promote, remove nodes)",
        subcommand_required = true,
        arg_required_else_help = true
    )]
    Node {
        #[command(subcommand)]
        action: NodeCommands,
    },

    #[command(
        about = "Configure or inspect cluster durable storage mode (local, shared_fs, object_store)",
        subcommand_required = true,
        arg_required_else_help = true
    )]
    Storage {
        #[command(subcommand)]
        action: StorageCommands,
    },

    #[command(
        about = "Enable/configure or disable the Valkey warm write buffer",
        subcommand_required = true,
        arg_required_else_help = true
    )]
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

    #[command(
        about = "Export live cluster config as a break-glass RecoveryConfig YAML/JSON",
        long_about = "Export the live cluster config as a break-glass RecoveryConfig.\n\nSecrets (JWT secret, object store credentials, Valkey passwords) are replaced \
with \"<redacted>\" unless --include-secrets is given.",
        after_help = "Examples:\n  danube cluster export-config\n  danube cluster export-config --format json --output cluster.json\n  danube cluster export-config --include-secrets --output recovery.yaml"
    )]
    ExportConfig {
        #[arg(long, help = "Optional broker ID to include node bind/admin addresses")]
        broker_id: Option<u64>,
        #[arg(long, default_value = "yaml", help = "Output format: yaml or json")]
        format: String,
        #[arg(long, help = "Optional file path to write exported config")]
        output: Option<String>,
        #[arg(
            long,
            help = "Include secrets in plain text (for a real recovery file). Store the output securely"
        )]
        include_secrets: bool,
    },
}

#[derive(Debug, Subcommand)]
enum NodeCommands {
    #[command(
        about = "Add a new broker to the cluster as a Raft learner (non-voting)",
        after_help = "Example:\n  danube cluster node add --node-addr http://new-broker:50051\n\nThe CLI connects to the new node's admin port to discover its node_id and raft_addr,\nthen tells the current leader (--endpoint) to add it as a learner."
    )]
    Add {
        #[arg(long, required = true, value_name = "URL", help = "Admin endpoint of the new broker")]
        node_addr: String,
    },

    #[command(
        about = "Promote a Raft learner to a full voting member",
        after_help = "Example:\n  danube cluster node promote --node-id 12345678"
    )]
    Promote {
        #[arg(long, required = true, help = "Node ID to promote")]
        node_id: u64,
    },

    #[command(
        about = "Remove a node from the Raft cluster",
        after_help = "Example:\n  danube cluster node remove --node-id 12345678"
    )]
    Remove {
        #[arg(long, required = true, help = "Node ID to remove")]
        node_id: u64,
    },
}

#[derive(Debug, Subcommand)]
enum StorageCommands {
    #[command(about = "Show effective cluster storage configuration")]
    Show,
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

pub async fn handle(cmd: Cluster) -> Result<()> {
    let conn = cmd.client.resolve()?;
    match cmd.command {
        ClusterCommands::Status => handle_status(&conn).await,
        ClusterCommands::Node { action } => match action {
            NodeCommands::Add { node_addr } => handle_add_node(node_addr, &conn).await,
            NodeCommands::Promote { node_id } => handle_promote_node(node_id, &conn).await,
            NodeCommands::Remove { node_id } => handle_remove_node(node_id, &conn).await,
        },
        ClusterCommands::Storage { action } => handle_storage(action, &conn).await,
        ClusterCommands::WriteBuffer { action } => handle_write_buffer(action, &conn).await,
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
                &conn,
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
                &conn,
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
            include_secrets,
        } => handle_export_config(&conn, broker_id, format, output, include_secrets).await,
    }
}

async fn handle_status(conn: &Connection) -> Result<()> {
    let client = AdminGrpcClient::connect(conn.grpc_config()).await?;

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

async fn handle_add_node(node_addr: String, conn: &Connection) -> Result<()> {
    // Step 1: Discover the new node's identity
    println!("Discovering node at {}...", node_addr);
    let new_node_client = AdminGrpcClient::connect(GrpcClientConfig {
        endpoint: node_addr.clone(),
        ..conn.grpc_config()
    })
    .await?;

    let status = new_node_client.cluster_status().await?;
    let node_id = status.self_node_id;
    let raft_addr = status.raft_addr;
    println!("  Discovered: node_id={}, raft_addr={}", node_id, raft_addr);

    // Step 2: Tell the leader to add it as a learner
    let leader_client = AdminGrpcClient::connect(GrpcClientConfig {
        request_timeout_ms: 10000,
        ..conn.grpc_config()
    })
    .await?;

    println!("Adding node {} as learner via {}...", node_id, conn.endpoint);
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

async fn handle_promote_node(node_id: u64, conn: &Connection) -> Result<()> {
    let client = AdminGrpcClient::connect(GrpcClientConfig {
        request_timeout_ms: 10000,
        ..conn.grpc_config()
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

async fn handle_remove_node(node_id: u64, conn: &Connection) -> Result<()> {
    let client = AdminGrpcClient::connect(GrpcClientConfig {
        request_timeout_ms: 10000,
        ..conn.grpc_config()
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

async fn connect_client(conn: &Connection) -> Result<AdminGrpcClient> {
    AdminGrpcClient::connect(GrpcClientConfig {
        request_timeout_ms: 10000,
        ..conn.grpc_config()
    })
    .await
}

/// Fetch the redacted cluster config and print only the selected top-level
/// keys of one section as YAML. `section` is a top-level key (e.g. "storage");
/// `keys` limits the output to those keys inside it (empty = whole section).
async fn print_config_section(
    client: &AdminGrpcClient,
    section: &str,
    keys: &[&str],
) -> Result<()> {
    let exported = client
        .export_cluster_config(ExportClusterConfigRequest {
            broker_id: None,
            format: "json".to_string(),
            include_secrets: false,
        })
        .await?;
    let doc: serde_json::Value = serde_json::from_str(&exported.content)?;
    let value = select_section(&doc, section, keys);
    if value.is_null() {
        println!("{section}: not configured");
    } else {
        print!("{}", serde_yaml::to_string(&value)?);
    }
    Ok(())
}

fn select_section(doc: &serde_json::Value, section: &str, keys: &[&str]) -> serde_json::Value {
    let Some(inner) = doc.get(section) else {
        return serde_json::Value::Null;
    };
    if keys.is_empty() {
        return serde_json::json!({ section: inner });
    }
    let mut picked = serde_json::Map::new();
    for key in keys {
        if let Some(v) = inner.get(*key) {
            if !v.is_null() {
                picked.insert((*key).to_string(), v.clone());
            }
        }
    }
    serde_json::json!({ section: picked })
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

async fn handle_storage(action: StorageCommands, conn: &Connection) -> Result<()> {
    let client = connect_client(conn).await?;
    match action {
        StorageCommands::Show => {
            print_config_section(&client, "storage", &[]).await?;
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

async fn handle_write_buffer(action: WriteBufferCommands, conn: &Connection) -> Result<()> {
    let client = connect_client(conn).await?;
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
    conn: &Connection,
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
    let client = connect_client(conn).await?;
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
        return print_config_section(&client, "storage", &["wal", "local_retention"]).await;
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
    conn: &Connection,
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
    let client = connect_client(conn).await?;
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
        return print_config_section(&client, "load_manager", &[]).await;
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
    conn: &Connection,
    broker_id: Option<u64>,
    format: String,
    output: Option<String>,
    include_secrets: bool,
) -> Result<()> {
    let client = connect_client(conn).await?;
    let resp = client
        .export_cluster_config(ExportClusterConfigRequest {
            broker_id,
            format,
            include_secrets,
        })
        .await?;
    if include_secrets {
        eprintln!("warning: output contains secrets in plain text; store it securely");
    }
    if let Some(path) = output {
        std::fs::write(&path, &resp.content)?;
        #[cfg(unix)]
        if include_secrets {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
        }
        println!("Exported cluster configuration ({}) to {}", resp.format, path);
    } else {
        print!("{}", resp.content);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{CommandFactory, Parser};

    #[derive(Debug, Parser)]
    #[command(name = "danube")]
    struct TestCli {
        #[command(subcommand)]
        cmd: TestCmd,
    }

    #[derive(Debug, Subcommand)]
    enum TestCmd {
        Cluster(Cluster),
    }

    #[test]
    fn command_tree_is_valid() {
        TestCli::command().debug_assert();
    }

    #[test]
    fn select_section_only_returns_requested_keys() {
        let doc = serde_json::json!({
            "auth": { "jwt": { "secret_key": "<redacted>" } },
            "storage": { "mode": "local", "wal": { "cache_capacity": 10 }, "local_retention": null },
            "load_manager": { "assignment_strategy": "balanced" }
        });
        let wal = select_section(&doc, "storage", &["wal", "local_retention"]);
        assert_eq!(wal, serde_json::json!({ "storage": { "wal": { "cache_capacity": 10 } } }));
        let lm = select_section(&doc, "load_manager", &[]);
        assert!(lm.get("auth").is_none());
        assert_eq!(lm["load_manager"]["assignment_strategy"], "balanced");
        assert!(select_section(&doc, "missing", &[]).is_null());
    }

    #[test]
    fn parses_export_config_include_secrets() {
        let cli = TestCli::try_parse_from([
            "danube", "cluster", "export-config", "--include-secrets", "--output", "r.yaml",
        ])
        .expect("parse");
        let TestCmd::Cluster(c) = cli.cmd;
        assert!(matches!(
            c.command,
            ClusterCommands::ExportConfig { include_secrets: true, .. }
        ));
    }

    #[test]
    fn parses_node_and_storage_subcommands() {
        let cli = TestCli::try_parse_from([
            "danube", "cluster", "node", "add", "--node-addr", "http://n2:50051",
        ])
        .expect("parse");
        let TestCmd::Cluster(c) = cli.cmd;
        assert!(matches!(
            c.command,
            ClusterCommands::Node { action: NodeCommands::Add { .. } }
        ));
        for args in [
            vec!["danube", "cluster", "node", "promote", "--node-id", "7"],
            vec!["danube", "cluster", "node", "remove", "--node-id", "7"],
            vec!["danube", "cluster", "storage", "show"],
            vec!["danube", "cluster", "write-buffer", "enable", "--endpoints", "redis://a,redis://b"],
        ] {
            TestCli::try_parse_from(&args).unwrap_or_else(|e| panic!("{args:?}: {e}"));
        }
    }
}
