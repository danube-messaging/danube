use clap::{Args, Subcommand};
use crate::client::broker_admin_client;
use danube_core::admin_proto::Empty;

use prettytable::{format, Cell, Row, Table};

#[derive(Debug, Args)]
pub(crate) struct Brokers {
    #[command(subcommand)]
    command: BrokersCommands,
}

#[derive(Debug, Subcommand)]
pub(crate) enum BrokersCommands {
    #[command(about = "List all active brokers in the cluster")]
    List {
        #[arg(long, value_parser = ["json"], help = "Output format: json (default: table)")]
        output: Option<String>,
    },
    #[command(about = "List the cluster leader broker")]
    LeaderBroker,
    #[command(about = "List all namespaces in the cluster")]
    Namespaces {
        #[arg(long, value_parser = ["json"], help = "Output format: json (default: table)")]
        output: Option<String>,
    },
    #[command(about = "Unload a broker by migrating all hosted topics off it")]
    Unload {
        #[arg(long, required = true, help = "Target broker id")]
        broker_id: String,
        #[arg(long, default_value_t = 1, help = "Max topics to unload in parallel")]
        max_parallel: u32,
        #[arg(long, help = "Namespaces to include (repeatable)")]
        namespaces_include: Vec<String>,
        #[arg(long, help = "Namespaces to exclude (repeatable)")]
        namespaces_exclude: Vec<String>,
        #[arg(long, default_value_t = false, help = "Dry run: only list topics to be unloaded")]
        dry_run: bool,
        #[arg(long, default_value_t = 60, help = "Per-topic timeout seconds")]
        timeout_seconds: u32,
    },
    #[command(about = "Activate a broker (set state to active)")]
    Activate {
        #[arg(long, help = "Target broker id")]
        broker_id: String,
        #[arg(long, default_value = "admin_activate", help = "Reason for auditability")]
        reason: String,
    },
}

pub async fn handle_command(brokers: Brokers) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = broker_admin_client().await?;

    match brokers.command {
        // List active brokers of the cluster
        BrokersCommands::List { output } => {
            let response = client.list_brokers(Empty {}).await?;
            let brokers = response.into_inner().brokers;
            if matches!(output.as_deref(), Some("json")) {
                let serializable: Vec<serde_json::Value> = brokers
                    .iter()
                    .map(|b| serde_json::json!({
                        "broker_id": b.broker_id,
                        "broker_status": b.broker_status,
                        "broker_addr": b.broker_addr,
                        "broker_role": b.broker_role,
                        "admin_addr": b.admin_addr,
                        "metrics_addr": b.metrics_addr,
                    }))
                    .collect();
                println!("{}", serde_json::to_string_pretty(&serializable)?);
            } else {
                // table output
                let mut table = Table::new();
                table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
                table.add_row(Row::new(vec![
                    Cell::new("BROKER ID"),
                    Cell::new("BROKER STATUS"),
                    Cell::new("BROKER ADDRESS"),
                    Cell::new("BROKER ROLE"),
                    Cell::new("ADMIN ADDR"),
                    Cell::new("METRICS ADDR"),
                ]));
                for broker in brokers {
                    table.add_row(Row::new(vec![
                        Cell::new(&broker.broker_id),
                        Cell::new(&broker.broker_status),
                        Cell::new(&broker.broker_addr),
                        Cell::new(&broker.broker_role),
                        Cell::new(&broker.admin_addr),
                        Cell::new(&broker.metrics_addr),
                    ]));
                }
                table.printstd();
            }
        }
        // Get the information of the leader broker
        BrokersCommands::LeaderBroker => {
            let response = client.get_leader_broker(Empty {}).await?;
            println!("Leader Broker: {:?}", response.into_inner().leader);
        }
        // List namespaces part of the cluster
        BrokersCommands::Namespaces { output } => {
            let response = client.list_namespaces(Empty {}).await?;
            let namespaces = response.into_inner().namespaces;
            if matches!(output.as_deref(), Some("json")) {
                let serializable: Vec<serde_json::Value> = namespaces
                    .iter()
                    .map(|n| serde_json::json!(n))
                    .collect();
                println!("{}", serde_json::to_string_pretty(&serializable)?);
            } else {
                for namespace in namespaces {
                    println!("Namespace: {}", namespace);
                }
            }
        }
        BrokersCommands::Unload {
            broker_id,
            max_parallel,
            namespaces_include,
            namespaces_exclude,
            dry_run,
            timeout_seconds,
        } => {
            use danube_core::admin_proto::UnloadBrokerRequest;
            let request = UnloadBrokerRequest {
                broker_id,
                max_parallel,
                namespaces_include,
                namespaces_exclude,
                dry_run,
                timeout_seconds,
            };
            let resp = client.unload_broker(request).await?.into_inner();
            println!(
                "UnloadBroker started={} total={} succeeded={} failed={} pending={}",
                resp.started, resp.total, resp.succeeded, resp.failed, resp.pending
            );
            if !resp.failed_topics.is_empty() {
                eprintln!("Failed topics:");
                for t in resp.failed_topics {
                    eprintln!("  {}", t);
                }
            }
        }
        BrokersCommands::Activate { broker_id, reason } => {
            use danube_core::admin_proto::ActivateBrokerRequest;
            let req = ActivateBrokerRequest { broker_id, reason };
            let resp = client.activate_broker(req).await?.into_inner();
            println!("ActivateBroker success={}", resp.success);
        }
    }

    Ok(())
}
