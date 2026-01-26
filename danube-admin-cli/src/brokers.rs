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
    #[command(about = "Show cluster balance metrics and broker loads")]
    Balance {
        #[arg(long, value_parser = ["json"], help = "Output format: json (default: table)")]
        output: Option<String>,
    },
    #[command(about = "Trigger manual cluster rebalancing")]
    Rebalance {
        #[arg(long, default_value_t = false, help = "Dry run: show proposed moves without executing")]
        dry_run: bool,
        #[arg(long, help = "Maximum number of topic moves to execute")]
        max_moves: Option<u32>,
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
        BrokersCommands::Balance { output } => {
            use danube_core::admin_proto::ClusterBalanceRequest;
            let response = client.get_cluster_balance(ClusterBalanceRequest {}).await?;
            let balance = response.into_inner();

            if matches!(output.as_deref(), Some("json")) {
                let serializable = serde_json::json!({
                    "coefficient_of_variation": balance.coefficient_of_variation,
                    "mean_load": balance.mean_load,
                    "max_load": balance.max_load,
                    "min_load": balance.min_load,
                    "std_deviation": balance.std_deviation,
                    "broker_count": balance.broker_count,
                    "brokers": balance.brokers.iter().map(|b| serde_json::json!({
                        "broker_id": b.broker_id,
                        "load": b.load,
                        "topic_count": b.topic_count,
                        "is_overloaded": b.is_overloaded,
                        "is_underloaded": b.is_underloaded,
                    })).collect::<Vec<_>>(),
                });
                println!("{}", serde_json::to_string_pretty(&serializable)?);
            } else {
                // Determine cluster status
                let cv_percent = balance.coefficient_of_variation * 100.0;
                let status = if cv_percent < 20.0 {
                    "✅ Well Balanced"
                } else if cv_percent < 30.0 {
                    "✅ Balanced"
                } else if cv_percent < 40.0 {
                    "⚠️  Imbalanced"
                } else {
                    "❌ Severely Imbalanced"
                };

                println!("\n╔══════════════════════════════════════════════════════════╗");
                println!("║           Cluster Balance Report                       ║");
                println!("╚══════════════════════════════════════════════════════════╝");
                println!();
                println!("Status:                    {}", status);
                println!("Coefficient of Variation:  {:.2}%", cv_percent);
                println!();
                println!("Interpretation Guide:");
                println!("  < 20%  = ✅ Well Balanced");
                println!("  20-30% = ✅ Balanced");
                println!("  30-40% = ⚠️  Imbalanced");
                println!("  > 40%  = ❌ Severely Imbalanced");
                println!();
                println!("Load Statistics:");
                println!("  Mean Load:       {:.2} topics", balance.mean_load);
                println!("  Max Load:        {:.2} topics", balance.max_load);
                println!("  Min Load:        {:.2} topics", balance.min_load);
                println!("  Std Deviation:   {:.2}", balance.std_deviation);
                println!("  Broker Count:    {}", balance.broker_count);
                println!();

                if !balance.brokers.is_empty() {
                    println!("Broker Details:");
                    let mut table = Table::new();
                    table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
                    table.add_row(Row::new(vec![
                        Cell::new("BROKER ID"),
                        Cell::new("LOAD"),
                        Cell::new("TOPICS"),
                        Cell::new("STATUS"),
                    ]));

                    for broker in &balance.brokers {
                        let status = if broker.is_overloaded {
                            "Overloaded"
                        } else if broker.is_underloaded {
                            "Underloaded"
                        } else {
                            "Normal"
                        };

                        table.add_row(Row::new(vec![
                            Cell::new(&broker.broker_id.to_string()),
                            Cell::new(&format!("{:.1}", broker.load)),
                            Cell::new(&broker.topic_count.to_string()),
                            Cell::new(status),
                        ]));
                    }
                    table.printstd();
                }

                if cv_percent >= 30.0 {
                    println!();
                    println!("💡 Recommendation: Run 'danube-admin-cli brokers rebalance' to balance the cluster");
                }
            }
        }
        BrokersCommands::Rebalance { dry_run, max_moves } => {
            use danube_core::admin_proto::RebalanceRequest;
            let request = RebalanceRequest { dry_run, max_moves };
            let response = client.trigger_rebalance(request).await?;
            let result = response.into_inner();

            if !result.success {
                eprintln!("❌ Rebalancing failed: {}", result.error_message);
                return Ok(());
            }

            if dry_run {
                println!("\n╔══════════════════════════════════════════════════════════╗");
                println!("║         Dry Run - Proposed Rebalancing Moves            ║");
                println!("╚══════════════════════════════════════════════════════════╝");
                println!();

                if result.proposed_moves.is_empty() {
                    if result.error_message.is_empty() {
                        println!("✅ Cluster is balanced - no moves needed");
                    } else {
                        println!("ℹ️  {}", result.error_message);
                    }
                } else {
                    println!("Would move {} topic(s):", result.proposed_moves.len());
                    println!();

                    for (i, mv) in result.proposed_moves.iter().enumerate() {
                        println!("  {}. {}", i + 1, mv.topic_name);
                        println!("     From: Broker {} → To: Broker {}", mv.from_broker, mv.to_broker);
                        println!("     Estimated load: {:.2}", mv.estimated_load);
                        println!("     Reason: {}", mv.reason);
                        println!();
                    }

                    println!("💡 Run without --dry-run to execute these moves");
                }
            } else {
                println!("\n╔══════════════════════════════════════════════════════════╗");
                println!("║         Cluster Rebalancing Triggered                   ║");
                println!("╚══════════════════════════════════════════════════════════╝");
                println!();

                if result.moves_executed == 0 {
                    if result.error_message.is_empty() {
                        println!("✅ Cluster is already balanced - no action needed");
                    } else {
                        println!("ℹ️  {}", result.error_message);
                    }
                } else {
                    println!("✅ Rebalancing Complete!");
                    println!();
                    println!("  Moves executed: {}", result.moves_executed);
                    println!();

                    if !result.proposed_moves.is_empty() {
                        println!("Executed moves:");
                        for (_i, mv) in result.proposed_moves.iter().take(result.moves_executed as usize).enumerate() {
                            println!("  ✓ {} (Broker {} → Broker {})", mv.topic_name, mv.from_broker, mv.to_broker);
                        }
                    }

                    println!();
                    println!("💡 Run 'danube-admin-cli brokers balance' to verify cluster state");
                }
            }
        }
    }

    Ok(())
}
