use anyhow::Result;
use std::env;

/// Broker deployment mode.
///
/// - `Cluster`    — Full multi-node mode with Raft consensus, LoadManager, leader election.
/// - `Standalone` — Single-node mode: direct topic loading, no cluster orchestration.
/// - `Edge`       — Reserved for future edge-to-cloud mode (PR2).
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum BrokerMode {
    Cluster,
    Standalone,
    #[allow(dead_code)]
    Edge,
}

#[derive(Debug)]
pub(crate) struct Args {
    pub(crate) config_file: Option<String>,
    pub(crate) mode: BrokerMode,
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
        println!("  --config-file        Path to config file (required for cluster mode)");
        println!("  --mode <mode>        Broker mode: cluster, standalone, or edge (default: cluster)");
        println!("  --broker-addr        Danube Broker advertised address");
        println!("  --admin-addr         Danube Broker Admin address");
        println!("  --prom-exporter      Prometheus Exporter http address");
        println!("  --advertised-addr    Advertised address (fqdn)");
        println!("  --connect-url        External proxy/ingress address for clients");
        println!("  --raft-addr          Raft inter-node transport address (overrides config)");
        println!("  --data-dir           Base data directory (required for standalone mode)");
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
        let mut explicit_mode: Option<String> = None;
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
                "--mode" => {
                    explicit_mode = args_iter.next().map(|s| s.to_string());
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

        // Resolve BrokerMode
        let mode = if let Some(mode_str) = explicit_mode {
            match mode_str.to_lowercase().as_str() {
                "cluster" => BrokerMode::Cluster,
                "standalone" => BrokerMode::Standalone,
                "edge" => {
                    return Err(anyhow::anyhow!(
                        "Edge mode is not yet available. It will be implemented in a future release."
                    ));
                }
                _ => {
                    return Err(anyhow::anyhow!(
                        "Unknown mode '{}'. Valid modes: cluster, standalone",
                        mode_str
                    ));
                }
            }
        } else {
            // Default: cluster mode (requires --config-file)
            BrokerMode::Cluster
        };

        // Validation rules
        match &mode {
            BrokerMode::Standalone => {
                if config_file.is_some() {
                    return Err(anyhow::anyhow!(
                        "--mode standalone cannot be used together with --config-file"
                    ));
                }
                if join {
                    return Err(anyhow::anyhow!(
                        "--mode standalone cannot be used together with --join"
                    ));
                }
                if seed_nodes.is_some() {
                    return Err(anyhow::anyhow!(
                        "--mode standalone cannot be used together with --seed-nodes"
                    ));
                }
                if data_dir.is_none() {
                    return Err(anyhow::anyhow!(
                        "--mode standalone requires --data-dir"
                    ));
                }
            }
            BrokerMode::Cluster => {
                if config_file.is_none() {
                    return Err(anyhow::anyhow!(
                        "Missing required --config-file (or use --mode standalone --data-dir <path>)"
                    ));
                }
            }
            BrokerMode::Edge => {
                // Unreachable — rejected above. But for completeness:
                unreachable!("Edge mode is rejected at parse time");
            }
        }

        Ok(Args {
            config_file,
            mode,
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
    use super::{Args, BrokerMode};

    #[test]
    fn parses_config_file_mode() {
        let args = Args::parse_from(["danube-broker", "--config-file", "config/danube_broker.yml"])
            .expect("parse args");

        assert_eq!(args.config_file.as_deref(), Some("config/danube_broker.yml"));
        assert_eq!(args.mode, BrokerMode::Cluster);
    }

    #[test]
    fn parses_mode_standalone() {
        let args = Args::parse_from([
            "danube-broker",
            "--mode",
            "standalone",
            "--data-dir",
            "/tmp/danube-standalone",
        ])
        .expect("parse args");

        assert_eq!(args.mode, BrokerMode::Standalone);
        assert_eq!(args.data_dir.as_deref(), Some("/tmp/danube-standalone"));
        assert!(args.config_file.is_none());
    }

    #[test]
    fn parses_mode_cluster() {
        let args = Args::parse_from([
            "danube-broker",
            "--mode",
            "cluster",
            "--config-file",
            "config/danube_broker.yml",
        ])
        .expect("parse args");

        assert_eq!(args.mode, BrokerMode::Cluster);
        assert_eq!(args.config_file.as_deref(), Some("config/danube_broker.yml"));
    }

    #[test]
    fn rejects_mode_edge() {
        let err = Args::parse_from([
            "danube-broker",
            "--mode",
            "edge",
            "--data-dir",
            "/tmp/danube-edge",
        ])
        .expect_err("edge mode should be rejected");

        assert!(err.to_string().contains("Edge mode is not yet available"));
    }

    #[test]
    fn rejects_standalone_without_data_dir() {
        let err = Args::parse_from(["danube-broker", "--mode", "standalone"])
            .expect_err("standalone without data-dir should fail");

        assert!(err.to_string().contains("requires --data-dir"));
    }

    #[test]
    fn rejects_standalone_with_config_file() {
        let err = Args::parse_from([
            "danube-broker",
            "--mode",
            "standalone",
            "--config-file",
            "config/danube_broker.yml",
            "--data-dir",
            "/tmp/danube-standalone",
        ])
        .expect_err("standalone with config-file should fail");

        assert!(err
            .to_string()
            .contains("--mode standalone cannot be used together with --config-file"));
    }

    #[test]
    fn rejects_standalone_with_join() {
        let err = Args::parse_from([
            "danube-broker",
            "--mode",
            "standalone",
            "--data-dir",
            "/tmp/test",
            "--join",
        ])
        .expect_err("standalone with join should fail");

        assert!(err.to_string().contains("--mode standalone cannot be used together with --join"));
    }

    #[test]
    fn rejects_standalone_with_seed_nodes() {
        let err = Args::parse_from([
            "danube-broker",
            "--mode",
            "standalone",
            "--data-dir",
            "/tmp/test",
            "--seed-nodes",
            "node1:7650,node2:7650",
        ])
        .expect_err("standalone with seed-nodes should fail");

        assert!(err
            .to_string()
            .contains("--mode standalone cannot be used together with --seed-nodes"));
    }
}
