use anyhow::Result;
use std::env;

#[derive(Debug)]
pub(crate) struct Args {
    pub(crate) config_file: Option<String>,
    pub(crate) single_node: bool,
    pub(crate) broker_addr: Option<String>,
    pub(crate) admin_addr: Option<String>,
    pub(crate) prom_exporter: Option<String>,
    pub(crate) advertised_addr: Option<String>,
    pub(crate) connect_url: Option<String>,
    pub(crate) raft_addr: Option<String>,
    pub(crate) data_dir: Option<String>,
    pub(crate) seed_nodes: Option<String>,
    pub(crate) join: bool,
}

impl Args {
    fn show_usage() {
        println!("Danube Broker Usage:");
        println!("  --config-file        Path to config file");
        println!("  --single-node        Start with generated single-node defaults (requires --data-dir)");
        println!("  --broker-addr        Danube Broker advertised address");
        println!("  --admin-addr         Danube Broker Admin address");
        println!("  --prom-exporter      Prometheus Exporter http address");
        println!("  --advertised-addr    Advertised address (fqdn)");
        println!("  --connect-url        External proxy/ingress address for clients");
        println!("  --raft-addr          Raft inter-node transport address (overrides config)");
        println!("  --data-dir           Base data directory in --single-node mode; otherwise overrides meta_store.data_dir");
        println!("  --seed-nodes         Comma-separated Raft seed addresses (overrides meta_store.seed_nodes)");
        println!("  --join               Join an existing cluster (skip bootstrap, wait to be added via admin CLI)");
    }

    pub(crate) fn parse() -> Result<Self> {
        Self::parse_from(env::args())
    }

    fn parse_from<I, S>(args: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let args: Vec<String> = args.into_iter().map(Into::into).collect();

        if args.len() <= 1 {
            Self::show_usage();
            return Err(anyhow::anyhow!("No arguments provided"));
        }

        let mut config_file = None;
        let mut single_node = false;
        let mut broker_addr = None;
        let mut admin_addr = None;
        let mut prom_exporter = None;
        let mut advertised_addr = None;
        let mut connect_url = None;
        let mut raft_addr = None;
        let mut data_dir = None;
        let mut seed_nodes = None;
        let mut join = false;

        let mut args_iter = args.iter().skip(1);
        while let Some(arg) = args_iter.next() {
            match arg.as_str() {
                "--config-file" => {
                    config_file = args_iter.next().map(|s| s.to_string());
                }
                "--single-node" => {
                    single_node = true;
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
                "--connect-url" => {
                    connect_url = args_iter.next().map(|s| s.to_string());
                }
                "--raft-addr" => {
                    raft_addr = args_iter.next().map(|s| s.to_string());
                }
                "--data-dir" => {
                    data_dir = args_iter.next().map(|s| s.to_string());
                }
                "--seed-nodes" => {
                    seed_nodes = args_iter.next().map(|s| s.to_string());
                }
                "--join" => {
                    join = true;
                }
                _ => return Err(anyhow::anyhow!("Unknown argument: {}", arg)),
            }
        }

        if single_node && config_file.is_some() {
            return Err(anyhow::anyhow!(
                "--single-node cannot be used together with --config-file"
            ));
        }

        if single_node && join {
            return Err(anyhow::anyhow!(
                "--single-node cannot be used together with --join"
            ));
        }

        if single_node && seed_nodes.is_some() {
            return Err(anyhow::anyhow!(
                "--single-node cannot be used together with --seed-nodes"
            ));
        }

        if single_node && data_dir.is_none() {
            return Err(anyhow::anyhow!("--single-node requires --data-dir"));
        }

        if !single_node && config_file.is_none() {
            return Err(anyhow::anyhow!(
                "Missing required --config-file (or use --single-node --data-dir <path>)"
            ));
        }

        Ok(Args {
            config_file,
            single_node,
            broker_addr,
            admin_addr,
            prom_exporter,
            advertised_addr,
            connect_url,
            raft_addr,
            data_dir,
            seed_nodes,
            join,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::Args;

    #[test]
    fn parses_config_file_mode() {
        let args = Args::parse_from(["danube-broker", "--config-file", "config/danube_broker.yml"])
            .expect("parse args");

        assert_eq!(args.config_file.as_deref(), Some("config/danube_broker.yml"));
        assert!(!args.single_node);
    }

    #[test]
    fn parses_single_node_mode() {
        let args = Args::parse_from([
            "danube-broker",
            "--single-node",
            "--data-dir",
            "/tmp/danube-single-node",
        ])
        .expect("parse args");

        assert!(args.single_node);
        assert_eq!(args.data_dir.as_deref(), Some("/tmp/danube-single-node"));
        assert!(args.config_file.is_none());
    }

    #[test]
    fn rejects_single_node_without_data_dir() {
        let err = Args::parse_from(["danube-broker", "--single-node"])
            .expect_err("single-node without data-dir should fail");

        assert!(err.to_string().contains("--single-node requires --data-dir"));
    }

    #[test]
    fn rejects_single_node_with_config_file() {
        let err = Args::parse_from([
            "danube-broker",
            "--single-node",
            "--config-file",
            "config/danube_broker.yml",
            "--data-dir",
            "/tmp/danube-single-node",
        ])
        .expect_err("single-node with config-file should fail");

        assert!(err
            .to_string()
            .contains("--single-node cannot be used together with --config-file"));
    }
}
