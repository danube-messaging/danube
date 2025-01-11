use anyhow::Result;
use std::env;

pub(crate) struct Args {
    pub(crate) config_file: String,
    pub(crate) broker_addr: Option<String>,
    pub(crate) admin_addr: Option<String>,
    pub(crate) prom_exporter: Option<String>,
    pub(crate) advertised_addr: Option<String>,
}

impl Args {
    fn show_usage() {
        println!("Danube Broker Usage:");
        println!("  --config-file        Path to config file (required)");
        println!("  --broker-addr        Danube Broker advertised address");
        println!("  --admin-addr         Danube Broker Admin address");
        println!("  --prom-exporter      Prometheus Exporter http address");
        println!("  --advertised-addr    Advertised address (fqdn)");
    }
    pub(crate) fn parse() -> Result<Self> {
        let args: Vec<String> = env::args().collect();

        if args.len() <= 1 {
            Self::show_usage();
            return Err(anyhow::anyhow!("No arguments provided"));
        }

        let mut config_file = None;
        let mut broker_addr = None;
        let mut admin_addr = None;
        let mut prom_exporter = None;
        let mut advertised_addr = None;

        let mut args_iter = args.iter().skip(1);
        while let Some(arg) = args_iter.next() {
            match arg.as_str() {
                "--config-file" => {
                    config_file = args_iter.next().map(|s| s.to_string());
                }
                "--broker-addr" => {
                    broker_addr = args_iter.next().map(|s| s.to_string());
                }
                "--admin-addr" => {
                    admin_addr = args_iter.next().map(|s| s.to_string());
                }
                "--prom-exporter" => {
                    prom_exporter = args_iter.next().map(|s| s.to_string());
                }
                "--advertised-addr" => {
                    advertised_addr = args_iter.next().map(|s| s.to_string());
                }
                _ => return Err(anyhow::anyhow!("Unknown argument: {}", arg)),
            }
        }

        Ok(Args {
            config_file: config_file
                .ok_or_else(|| anyhow::anyhow!("Missing required --config-file"))?,
            broker_addr,
            admin_addr,
            prom_exporter,
            advertised_addr,
        })
    }
}
