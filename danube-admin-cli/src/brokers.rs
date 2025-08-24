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
                        "broker_addr": b.broker_addr,
                        "broker_role": b.broker_role
                    }))
                    .collect();
                println!("{}", serde_json::to_string_pretty(&serializable)?);
            } else {
                // table output
                let mut table = Table::new();
                table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
                table.add_row(Row::new(vec![
                    Cell::new("BROKER ID"),
                    Cell::new("BROKER ADDRESS"),
                    Cell::new("BROKER ROLE"),
                ]));
                for broker in brokers {
                    table.add_row(Row::new(vec![
                        Cell::new(&broker.broker_id),
                        Cell::new(&broker.broker_addr),
                        Cell::new(&broker.broker_role),
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
    }

    Ok(())
}
