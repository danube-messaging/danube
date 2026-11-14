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
    Edge,
}

/// Broker node workload role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum NodeRole {
    Broker,
    Lakehouse,
    #[default]
    All,
}

impl NodeRole {
    pub(crate) fn from_str(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "broker" => Ok(Self::Broker),
            "lakehouse" => Ok(Self::Lakehouse),
            "all" => Ok(Self::All),
            other => Err(anyhow::anyhow!(
                "Unknown --node-role '{}'. Valid roles: broker, lakehouse, all",
                other
            )),
        }
    }
}

#[derive(Debug)]
pub(crate) struct Args {
    pub(crate) mode: BrokerMode,
    pub(crate) broker_addr: Option<String>,
    pub(crate) admin_addr: Option<String>,
    pub(crate) prom_exporter: Option<String>,
    pub(crate) advertised_addr: Option<String>,
    pub(crate) connect_url: Option<String>,
    pub(crate) raft_addr: Option<String>,
    pub(crate) data_dir: Option<String>,
    /// Per-node override for local WAL staging root directory (takes priority over cluster defaults)
    pub(crate) wal_dir: Option<String>,
    pub(crate) seed_nodes: Option<String>,
    pub(crate) join: bool,
    /// Edge mode: unique edge identifier
    pub(crate) edge_name: Option<String>,
    /// Edge mode: authentication token (can come from env)
    pub(crate) edge_token: Option<String>,
    /// Edge mode: zero-YAML cloud Danube endpoint (e.g. https://cloud:6650)
    pub(crate) cloud_endpoint: Option<String>,
    /// Edge mode: MQTT listener address (default 0.0.0.0:1883)
    pub(crate) mqtt_addr: Option<String>,
    /// Startup auth mode override: none, tls, or auto-tls
    pub(crate) auth_mode: Option<String>,
    /// TLS certificate path (for --auth-mode tls)
    pub(crate) tls_cert: Option<String>,
    /// TLS private key path (for --auth-mode tls)
    pub(crate) tls_key: Option<String>,
    /// TLS CA certificate path (for --auth-mode tls)
    pub(crate) ca_cert: Option<String>,
    /// JWT HMAC secret key (for --auth-mode tls)
    pub(crate) jwt_secret: Option<String>,
    /// Comma-separated list of super-admin JWT subjects
    pub(crate) super_admins: Option<String>,
    /// Local JSON/YAML recovery override file when Raft quorum is lost
    pub(crate) recovery_config: Option<String>,
    /// Workload role for this node (broker, lakehouse, or all)
    pub(crate) node_role: NodeRole,
}

impl Args {
    #[allow(dead_code)]
    fn show_usage() {
        println!("Danube Usage:");
        println!("  danube up [OPTIONS]          Start the broker (default when no subcommand)");
        println!("  danube ctl <COMMAND>         Control-plane and data-plane CLI");
        println!("  danube mcp [OPTIONS]         Start the MCP server");
        println!();
        println!("Broker options:");
        println!("  --mode <mode>        Broker mode: cluster, standalone, or edge (default: cluster)");
        println!("  --data-dir           Base data directory (default: ./danube-data)");
        println!("  --wal-dir            Per-node local WAL staging directory override");
        println!("  --broker-addr        Danube Broker listen/advertised address");
        println!("  --admin-addr         Optional separate Admin gRPC listen address");
        println!("  --prom-exporter      Prometheus Exporter http address");
        println!("  --advertised-addr    Advertised address (fqdn)");
        println!("  --connect-url        External proxy/ingress address for clients");
        println!("  --raft-addr          Raft inter-node transport address");
        println!("  --seed-nodes         Comma-separated Raft seed addresses");
        println!("  --join               Join an existing cluster");
        println!("  --auth-mode          Auth mode at startup: none, tls, or auto-tls");
        println!("  --tls-cert           Path to server TLS certificate PEM");
        println!("  --tls-key            Path to server TLS private key PEM");
        println!("  --ca-cert            Path to TLS CA certificate PEM");
        println!("  --jwt-secret         JWT HMAC secret key");
        println!("  --super-admins       Comma-separated super-admin JWT subjects");
        println!("  --node-role          Node workload role: broker, lakehouse, or all (default: all)");
        println!("  --recovery-config    Local JSON/YAML break-glass config override when Raft quorum is lost");
        println!();
        println!("Edge mode options:");
        println!("  --edge-name          Unique edge node name (default: HOSTNAME or edge-node)");
        println!("  --cloud-endpoint     Cloud Danube gRPC endpoint for edge replication");
        println!("  --mqtt-addr          MQTT listener address (default: 0.0.0.0:1883)");
        println!("  --edge-token         Authentication token override (optional)");
    }

    #[allow(dead_code)]
    pub(crate) fn parse() -> Result<Self> {
        Self::parse_from(env::args())
    }

    pub(crate) fn parse_from<I, S>(args: I) -> Result<Self>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let args: Vec<String> = args.into_iter().map(Into::into).collect();

        let mut explicit_mode: Option<String> = None;
        let mut broker_addr = None;
        let mut admin_addr = None;
        let mut prom_exporter = None;
        let mut advertised_addr = None;
        let mut connect_url = None;
        let mut raft_addr = None;
        let mut data_dir = None;
        let mut wal_dir = None;
        let mut seed_nodes = None;
        let mut join = false;
        let mut edge_name = None;
        let mut edge_token = None;
        let mut cloud_endpoint = None;
        let mut mqtt_addr = None;
        let mut auth_mode = None;
        let mut tls_cert = None;
        let mut tls_key = None;
        let mut ca_cert = None;
        let mut jwt_secret = None;
        let mut super_admins = None;
        let mut recovery_config = None;
        let mut node_role = NodeRole::All;

        let mut args_iter = args.iter().skip(1);
        while let Some(arg) = args_iter.next() {
            match arg.as_str() {
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
                "--wal-dir" => {
                    wal_dir = args_iter.next().map(|s| s.to_string());
                }
                "--seed-nodes" => {
                    seed_nodes = args_iter.next().map(|s| s.to_string());
                }
                "--join" => {
                    join = true;
                }
                "--edge-name" => {
                    edge_name = args_iter.next().map(|s| s.to_string());
                }
                "--edge-token" => {
                    edge_token = args_iter.next().map(|s| s.to_string());
                }
                "--cloud-endpoint" => {
                    cloud_endpoint = args_iter.next().map(|s| s.to_string());
                }
                "--mqtt-addr" => {
                    mqtt_addr = args_iter.next().map(|s| s.to_string());
                }
                "--auth-mode" => {
                    auth_mode = args_iter.next().map(|s| s.to_string());
                }
                "--tls-cert" => {
                    tls_cert = args_iter.next().map(|s| s.to_string());
                }
                "--tls-key" => {
                    tls_key = args_iter.next().map(|s| s.to_string());
                }
                "--ca-cert" => {
                    ca_cert = args_iter.next().map(|s| s.to_string());
                }
                "--jwt-secret" => {
                    jwt_secret = args_iter.next().map(|s| s.to_string());
                }
                "--super-admins" => {
                    super_admins = args_iter.next().map(|s| s.to_string());
                }
                "--recovery-config" => {
                    recovery_config = args_iter.next().map(|s| s.to_string());
                }
                "--node-role" => {
                    let val = args_iter
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("--node-role requires a value"))?;
                    node_role = NodeRole::from_str(val)?;
                }
                _ => return Err(anyhow::anyhow!("Unknown argument: {}", arg)),
            }
        }

        // Resolve BrokerMode
        let mode = if let Some(mode_str) = explicit_mode {
            match mode_str.to_lowercase().as_str() {
                "cluster" => BrokerMode::Cluster,
                "standalone" => BrokerMode::Standalone,
                "edge" => BrokerMode::Edge,
                _ => {
                    return Err(anyhow::anyhow!(
                        "Unknown mode '{}'. Valid modes: cluster, standalone, edge",
                        mode_str
                    ));
                }
            }
        } else {
            BrokerMode::Cluster
        };

        if let Some(ref mode_val) = auth_mode {
            match mode_val.to_ascii_lowercase().as_str() {
                "none" | "tls" | "auto-tls" => {}
                other => {
                    return Err(anyhow::anyhow!(
                        "Unknown --auth-mode '{}'. Valid options: none, tls, auto-tls",
                        other
                    ));
                }
            }
        }

        if data_dir.is_none() {
            data_dir = Some("./danube-data".to_string());
        }

        // Validation rules
        match &mode {
            BrokerMode::Standalone => {
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
            }
            BrokerMode::Cluster => {}
            BrokerMode::Edge => {
                if join {
                    return Err(anyhow::anyhow!(
                        "--mode edge cannot be used together with --join"
                    ));
                }
                if cloud_endpoint.is_none() && recovery_config.is_none() {
                    return Err(anyhow::anyhow!(
                        "--mode edge requires --cloud-endpoint (or --recovery-config)"
                    ));
                }
            }
        }

        Ok(Args {
            mode,
            broker_addr,
            admin_addr,
            prom_exporter,
            advertised_addr,
            connect_url,
            raft_addr,
            data_dir,
            wal_dir,
            seed_nodes,
            join,
            edge_name,
            edge_token,
            cloud_endpoint,
            mqtt_addr,
            auth_mode,
            tls_cert,
            tls_key,
            ca_cert,
            jwt_secret,
            super_admins,
            recovery_config,
            node_role,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{Args, BrokerMode};

    #[test]
    fn parses_bare_zero_yaml_defaults() {
        let args = Args::parse_from(["danube"]).expect("bare danube should parse");

        assert_eq!(args.mode, BrokerMode::Cluster);
        assert_eq!(args.data_dir.as_deref(), Some("./danube-data"));
    }

    #[test]
    fn parses_mode_standalone() {
        let args = Args::parse_from([
            "danube",
            "--mode",
            "standalone",
            "--data-dir",
            "/tmp/danube-standalone",
        ])
        .expect("parse args");

        assert_eq!(args.mode, BrokerMode::Standalone);
        assert_eq!(args.data_dir.as_deref(), Some("/tmp/danube-standalone"));
    }

    #[test]
    fn parses_mode_cluster() {
        let args = Args::parse_from([
            "danube",
            "--mode",
            "cluster",
            "--data-dir",
            "/tmp/danube-cluster",
        ])
        .expect("parse args");

        assert_eq!(args.mode, BrokerMode::Cluster);
        assert_eq!(args.data_dir.as_deref(), Some("/tmp/danube-cluster"));
    }

    #[test]
    fn rejects_edge_without_cloud_endpoint() {
        let err = Args::parse_from([
            "danube",
            "--mode",
            "edge",
            "--data-dir",
            "/tmp/danube-edge",
        ])
        .expect_err("edge without cloud-endpoint should fail");

        assert!(err.to_string().contains("requires --cloud-endpoint"));
    }

    #[test]
    fn rejects_standalone_with_join() {
        let err = Args::parse_from([
            "danube",
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
            "danube",
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

    #[test]
    fn parses_zero_yaml_cluster_and_tls_flags() {
        let args = Args::parse_from([
            "danube",
            "--data-dir",
            "/tmp/danube-cluster",
            "--seed-nodes",
            "node1:7650,node2:7650",
            "--node-role",
            "lakehouse",
            "--auth-mode",
            "tls",
            "--tls-cert",
            "./cert/server-cert.pem",
            "--tls-key",
            "./cert/server-key.pem",
            "--ca-cert",
            "./cert/ca-cert.pem",
            "--jwt-secret",
            "my-secret",
            "--super-admins",
            "admin,ops",
        ])
        .expect("zero-YAML cluster mode with TLS flags should parse");

        assert_eq!(args.mode, BrokerMode::Cluster);
        assert_eq!(args.data_dir.as_deref(), Some("/tmp/danube-cluster"));
        assert_eq!(args.node_role, super::NodeRole::Lakehouse);
        assert_eq!(args.auth_mode.as_deref(), Some("tls"));
        assert_eq!(args.tls_cert.as_deref(), Some("./cert/server-cert.pem"));
        assert_eq!(args.tls_key.as_deref(), Some("./cert/server-key.pem"));
        assert_eq!(args.ca_cert.as_deref(), Some("./cert/ca-cert.pem"));
        assert_eq!(args.jwt_secret.as_deref(), Some("my-secret"));
        assert_eq!(args.super_admins.as_deref(), Some("admin,ops"));
    }

    #[test]
    fn parses_zero_yaml_edge_mode() {
        let args = Args::parse_from([
            "danube",
            "--mode",
            "edge",
            "--edge-name",
            "edge2",
            "--data-dir",
            "/tmp/danube-edge",
            "--cloud-endpoint",
            "https://cloud:6650",
            "--mqtt-addr",
            "0.0.0.0:1884",
            "--edge-token",
            "secret-token",
        ])
        .expect("zero-YAML edge mode should parse");

        assert_eq!(args.mode, BrokerMode::Edge);
        assert_eq!(args.edge_name.as_deref(), Some("edge2"));
        assert_eq!(args.cloud_endpoint.as_deref(), Some("https://cloud:6650"));
        assert_eq!(args.mqtt_addr.as_deref(), Some("0.0.0.0:1884"));
        assert_eq!(args.edge_token.as_deref(), Some("secret-token"));
    }

    #[test]
    fn parses_wal_dir_override() {
        let args = Args::parse_from(["danube", "--wal-dir", "/mnt/nvme-wal"])
            .expect("--wal-dir should parse");

        assert_eq!(args.wal_dir.as_deref(), Some("/mnt/nvme-wal"));
    }
}
