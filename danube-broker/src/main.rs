mod danube_service;
mod metadata_store;
mod resources;
mod service_configuration;
mod storage;

use crate::danube_service::DanubeService;
use crate::service_configuration::ServiceConfiguration;

use clap::Parser;
use tracing::info;
use tracing_subscriber;

pub(crate) mod proto {
    include!("../../proto/danube.rs");
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path to config file
    #[arg(short, long)]
    config_file: Option<String>,

    /// Danube Broker advertised address
    #[arg(short, long, default_value = "[::1]:6650")]
    advertised_address: String,

    /// ETCD address
    #[arg(short, long)]
    etcd_addr: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // install global collector configured based on RUST_LOG env var.
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    let broker_addr: std::net::SocketAddr = args.advertised_address.parse()?;

    let broker_config = ServiceConfiguration {
        broker_addr: broker_addr,
        etcd_addr: args.etcd_addr,
    };

    let danube = DanubeService::new(broker_config);

    info!("Start the Danube Broker Service");
    danube.start().await.expect("the broker unable to start");

    Ok(())
}
