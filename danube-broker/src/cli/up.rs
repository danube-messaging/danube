//! `danube up` - start a Danube node in one of three modes.

use anyhow::{bail, Context, Result};
use clap::{ArgGroup, Args, Subcommand, ValueEnum, ValueHint};
use std::path::{Path, PathBuf};

/// Broker deployment mode.
///
/// - `Cluster`    - Raft-replicated metadata, LoadManager, leader election.
/// - `Standalone` - Single node: direct topic loading, no cluster orchestration.
/// - `Edge`       - Edge node: MQTT ingest + replication to a cloud cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BrokerMode {
    Cluster,
    Standalone,
    Edge,
}

impl BrokerMode {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            BrokerMode::Cluster => "cluster",
            BrokerMode::Standalone => "standalone",
            BrokerMode::Edge => "edge",
        }
    }
}

/// Broker node workload role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, ValueEnum)]
pub(crate) enum NodeRole {
    /// Serve producers and consumers only
    Broker,
    /// Run lakehouse (Iceberg) conversion only
    Lakehouse,
    /// Both broker and lakehouse workloads
    #[default]
    All,
}

impl NodeRole {
    fn as_str(&self) -> &'static str {
        match self {
            NodeRole::Broker => "broker",
            NodeRole::Lakehouse => "lakehouse",
            NodeRole::All => "all",
        }
    }
}

/// Startup authentication mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum AuthMode {
    /// Plaintext, no authentication
    None,
    /// TLS with your own certificates (requires --tls-cert, --tls-key, --ca-cert)
    Tls,
    /// Generate a cluster CA, node certificate and bootstrap admin token in --data-dir
    AutoTls,
}

pub(crate) const DEFAULT_DATA_DIR: &str = "./danube-data";

const UP_ABOUT: &str = "Start a Danube node (standalone | cluster | edge)";

const UP_LONG_ABOUT: &str = "\
Start a Danube node.

Pick the mode that matches where the node runs:

  standalone   One node, no consensus traffic.     Dev, CI, single box
  cluster      Raft-replicated metadata.           Production, HA
  edge         MQTT ingest + replicate to cloud.   Factories, gateways, IoT

Cluster-wide settings (storage, WAL, load manager, namespaces, topics) are not
startup flags: once the node is up, manage them with `danube cluster ...`,
`danube topic ...` or `danube apply -f desired_state.yaml`.";

const UP_EXAMPLES: &str = "\
Examples:
  danube up standalone
  danube up cluster --data-dir /var/lib/danube --single-node
  danube up cluster --data-dir /var/lib/danube --seed-nodes n1:7650,n2:7650
  danube up edge --data-dir /var/lib/edge --name plant-7 \\
                 --cloud-endpoint https://danube.example.com:6650

Run 'danube up <MODE> --help' to see every option of a mode.";

#[derive(Debug, Args)]
#[command(
    about = UP_ABOUT,
    long_about = UP_LONG_ABOUT,
    after_help = UP_EXAMPLES,
    subcommand_value_name = "MODE",
    subcommand_help_heading = "Modes",
    subcommand_required = true,
    arg_required_else_help = true
)]
pub(crate) struct UpArgs {
    #[command(subcommand)]
    pub(crate) mode: UpMode,
}

#[derive(Debug, Subcommand)]
pub(crate) enum UpMode {
    /// Single node, no consensus. Best for dev, CI and single-box deployments
    #[command(
        long_about = "Start a single standalone node.\n\n\
Metadata is kept in a local single-member Raft log; there is no cluster \
membership, leader election or load balancing. The data directory defaults to \
./danube-data relative to the current working directory.",
        after_help = "Examples:
  danube up standalone
  danube up standalone --data-dir ~/danube --listen 0.0.0.0:6650
  danube up standalone --auth auto-tls"
    )]
    Standalone(StandaloneArgs),

    /// Raft-replicated multi-node broker. Best for production and HA
    #[command(
        long_about = "Start a node of a Raft-replicated Danube cluster.\n\n\
Choose how the node joins (at most one):\n  \
--single-node  bootstrap a brand-new one-node cluster\n  \
--seed-nodes   discover peers; the lowest node id bootstraps\n  \
--join         wait to be added with `danube cluster node add`\n\n\
With none of them, a fresh data directory bootstraps a single-node cluster and \
a warning is printed. A data directory that already holds Raft state always \
resumes its existing identity and membership.",
        after_help = "Examples:
  danube up cluster --data-dir /var/lib/danube --single-node
  danube up cluster --data-dir /var/lib/danube --advertised-addr broker1:6650 \\
                    --seed-nodes broker1:7650,broker2:7650,broker3:7650
  danube up cluster --data-dir /var/lib/danube --join --admin-listen 0.0.0.0:50051"
    )]
    Cluster(ClusterArgs),

    /// Edge node: MQTT ingest and replication to a cloud cluster
    #[command(
        long_about = "Start an edge node.\n\n\
The node accepts MQTT publishes on --mqtt-listen, stores them locally and \
replicates them to the cloud Danube cluster at --cloud-endpoint.",
        after_help = "Examples:
  danube up edge --data-dir /var/lib/danube-edge --name plant-7 \\
                 --cloud-endpoint https://danube.example.com:6650
  danube up edge --data-dir ./edge --name dev-edge --cloud-endpoint http://127.0.0.1:6650 \\
                 --mqtt-listen 0.0.0.0:1884"
    )]
    Edge(EdgeArgs),
}

// ─── Shared option groups ───────────────────────────────────────────────────

/// Network listeners and advertised addresses.
#[derive(Debug, Args, Clone, Default)]
pub(crate) struct NetworkArgs {
    /// Client + admin gRPC listen address [default: 127.0.0.1:6650]
    #[arg(long, value_name = "ADDR", env = "DANUBE_LISTEN", help_heading = "Network")]
    pub(crate) listen: Option<String>,

    /// Serve the admin gRPC API on a separate address (default: shared with --listen)
    #[arg(long, value_name = "ADDR", help_heading = "Network")]
    pub(crate) admin_listen: Option<String>,

    /// Prometheus metrics exporter listen address
    #[arg(long, value_name = "ADDR", help_heading = "Network")]
    pub(crate) metrics_listen: Option<String>,

    /// Host[:port] clients and peers use to reach this node
    #[arg(long, value_name = "HOST:PORT", env = "DANUBE_ADVERTISED_ADDR", help_heading = "Network")]
    pub(crate) advertised_addr: Option<String>,

    /// External proxy / ingress URL handed to clients
    #[arg(long, value_name = "URL", help_heading = "Network")]
    pub(crate) connect_url: Option<String>,
}

/// Startup security.
#[derive(Debug, Args, Clone, Default)]
pub(crate) struct SecurityArgs {
    /// Authentication mode [default: none]
    #[arg(long, value_enum, value_name = "MODE", env = "DANUBE_AUTH", help_heading = "Security")]
    pub(crate) auth: Option<AuthMode>,

    /// Server TLS certificate (PEM)
    #[arg(long, value_name = "FILE", value_hint = ValueHint::FilePath,
          required_if_eq("auth", "tls"), help_heading = "Security")]
    pub(crate) tls_cert: Option<String>,

    /// Server TLS private key (PEM)
    #[arg(long, value_name = "FILE", value_hint = ValueHint::FilePath,
          required_if_eq("auth", "tls"), help_heading = "Security")]
    pub(crate) tls_key: Option<String>,

    /// CA certificate (PEM) used to verify peers and clients
    #[arg(long, value_name = "FILE", value_hint = ValueHint::FilePath,
          required_if_eq("auth", "tls"), help_heading = "Security")]
    pub(crate) ca_cert: Option<String>,

    /// JWT HMAC signing secret
    #[arg(long, value_name = "SECRET", env = "DANUBE_JWT_SECRET", hide_env_values = true,
          help_heading = "Security")]
    pub(crate) jwt_secret: Option<String>,

    /// JWT subject granted super-admin rights (repeatable or comma-separated)
    #[arg(long = "super-admin", value_name = "SUBJECT", value_delimiter = ',',
          help_heading = "Security")]
    pub(crate) super_admins: Vec<String>,
}

/// Node-local settings.
#[derive(Debug, Args, Clone, Default)]
pub(crate) struct NodeArgs {
    /// Put the write-ahead log on a different disk than --data-dir
    #[arg(long, value_name = "DIR", value_hint = ValueHint::DirPath, help_heading = "Node")]
    pub(crate) wal_dir: Option<String>,

    /// Workloads this node runs
    #[arg(long, value_enum, default_value_t = NodeRole::All, help_heading = "Node")]
    pub(crate) role: NodeRole,
}

/// Break-glass recovery + dry run.
#[derive(Debug, Args, Clone, Default)]
pub(crate) struct OpsArgs {
    /// Local recovery override (JSON/YAML) used when Raft quorum is lost
    #[arg(long, value_name = "FILE", value_hint = ValueHint::FilePath,
          help_heading = "Recovery (break-glass)")]
    pub(crate) recovery_config: Option<String>,

    /// Print the resolved node configuration and exit without starting
    #[arg(long)]
    pub(crate) dry_run: bool,
}

// ─── Mode-specific argument sets ────────────────────────────────────────────

#[derive(Debug, Args)]
pub(crate) struct StandaloneArgs {
    /// Data directory (Raft log, node identity, WAL) [default: ./danube-data]
    #[arg(long, value_name = "DIR", env = "DANUBE_DATA_DIR", value_hint = ValueHint::DirPath,
          help_heading = "Node")]
    pub(crate) data_dir: Option<String>,

    /// Raft transport port of the local metadata log
    #[arg(long, value_name = "PORT", default_value_t = 7650, help_heading = "Network")]
    pub(crate) raft_port: u16,

    #[command(flatten)]
    pub(crate) node: NodeArgs,
    #[command(flatten)]
    pub(crate) net: NetworkArgs,
    #[command(flatten)]
    pub(crate) security: SecurityArgs,
    #[command(flatten)]
    pub(crate) ops: OpsArgs,
}

#[derive(Debug, Args)]
#[command(group(
    ArgGroup::new("membership")
        .args(["single_node", "seed_nodes", "join"])
        .multiple(false)
))]
pub(crate) struct ClusterArgs {
    /// Data directory (Raft log, node identity, WAL). Required: reusing a
    /// directory resumes that node; a new directory creates a new node
    #[arg(long, value_name = "DIR", env = "DANUBE_DATA_DIR", value_hint = ValueHint::DirPath,
          help_heading = "Node")]
    pub(crate) data_dir: String,

    /// Bootstrap a brand-new single-node cluster
    #[arg(long, help_heading = "Membership")]
    pub(crate) single_node: bool,

    /// Raft addresses of the initial members; the lowest node id bootstraps
    #[arg(long, value_name = "HOST:PORT,...", value_delimiter = ',', env = "DANUBE_SEED_NODES",
          help_heading = "Membership")]
    pub(crate) seed_nodes: Vec<String>,

    /// Wait to be added via `danube cluster node add` (pair with --admin-listen)
    #[arg(long, help_heading = "Membership")]
    pub(crate) join: bool,

    /// Raft transport port
    #[arg(long, value_name = "PORT", default_value_t = 7650, help_heading = "Network")]
    pub(crate) raft_port: u16,

    #[command(flatten)]
    pub(crate) node: NodeArgs,
    #[command(flatten)]
    pub(crate) net: NetworkArgs,
    #[command(flatten)]
    pub(crate) security: SecurityArgs,
    #[command(flatten)]
    pub(crate) ops: OpsArgs,
}

#[derive(Debug, Args)]
pub(crate) struct EdgeArgs {
    /// Data directory (local buffer, node identity, WAL). Required
    #[arg(long, value_name = "DIR", env = "DANUBE_DATA_DIR", value_hint = ValueHint::DirPath,
          help_heading = "Node")]
    pub(crate) data_dir: String,

    /// Unique name of this edge node
    #[arg(long, value_name = "NAME", env = "DANUBE_EDGE_NAME", help_heading = "Edge")]
    pub(crate) name: String,

    /// Cloud Danube cluster endpoint, e.g. https://danube.example.com:6650
    #[arg(long, value_name = "URL", env = "DANUBE_CLOUD_ENDPOINT",
          required_unless_present = "recovery_config", help_heading = "Edge")]
    pub(crate) cloud_endpoint: Option<String>,

    /// Token used to authenticate to the cloud cluster
    #[arg(long, value_name = "TOKEN", env = "DANUBE_CLOUD_TOKEN", hide_env_values = true,
          help_heading = "Edge")]
    pub(crate) cloud_token: Option<String>,

    /// MQTT listen address [default: 0.0.0.0:1883]
    #[arg(long, value_name = "ADDR", help_heading = "Edge")]
    pub(crate) mqtt_listen: Option<String>,

    /// Raft transport port of the local metadata log
    #[arg(long, value_name = "PORT", default_value_t = 7650, help_heading = "Network")]
    pub(crate) raft_port: u16,

    #[command(flatten)]
    pub(crate) node: NodeArgs,
    #[command(flatten)]
    pub(crate) net: NetworkArgs,
    #[command(flatten)]
    pub(crate) security: SecurityArgs,
    #[command(flatten)]
    pub(crate) ops: OpsArgs,
}

// ─── Resolved options consumed by the boot sequence ─────────────────────────

/// How cluster membership was chosen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Membership {
    /// Not a cluster node (standalone / edge).
    NotApplicable,
    /// Explicit `--single-node`.
    SingleNode,
    /// No membership flag given - falls back to single-node bootstrap.
    Implicit,
    /// `--seed-nodes`.
    Seeds,
    /// `--join`.
    Join,
}

/// Fully resolved `danube up` options.
#[derive(Debug)]
pub(crate) struct UpOptions {
    pub(crate) mode: BrokerMode,
    pub(crate) data_dir: String,
    pub(crate) data_dir_defaulted: bool,
    pub(crate) wal_dir: Option<String>,
    pub(crate) node_role: NodeRole,
    pub(crate) broker_addr: Option<String>,
    pub(crate) admin_addr: Option<String>,
    pub(crate) prom_exporter: Option<String>,
    pub(crate) advertised_addr: Option<String>,
    pub(crate) connect_url: Option<String>,
    pub(crate) raft_port: u16,
    pub(crate) membership: Membership,
    pub(crate) seed_nodes: Vec<String>,
    pub(crate) join: bool,
    pub(crate) edge_name: Option<String>,
    pub(crate) cloud_token: Option<String>,
    pub(crate) cloud_endpoint: Option<String>,
    pub(crate) mqtt_addr: Option<String>,
    pub(crate) auth: Option<AuthMode>,
    pub(crate) tls_cert: Option<String>,
    pub(crate) tls_key: Option<String>,
    pub(crate) ca_cert: Option<String>,
    pub(crate) jwt_secret: Option<String>,
    pub(crate) super_admins: Vec<String>,
    pub(crate) recovery_config: Option<String>,
    pub(crate) dry_run: bool,
}

impl UpOptions {
    fn base(
        mode: BrokerMode,
        data_dir: String,
        data_dir_defaulted: bool,
        raft_port: u16,
        node: NodeArgs,
        net: NetworkArgs,
        security: SecurityArgs,
        ops: OpsArgs,
    ) -> Self {
        Self {
            mode,
            data_dir,
            data_dir_defaulted,
            wal_dir: node.wal_dir,
            node_role: node.role,
            broker_addr: net.listen,
            admin_addr: net.admin_listen,
            prom_exporter: net.metrics_listen,
            advertised_addr: net.advertised_addr,
            connect_url: net.connect_url,
            raft_port,
            membership: Membership::NotApplicable,
            seed_nodes: Vec::new(),
            join: false,
            edge_name: None,
            cloud_token: None,
            cloud_endpoint: None,
            mqtt_addr: None,
            auth: security.auth,
            tls_cert: security.tls_cert,
            tls_key: security.tls_key,
            ca_cert: security.ca_cert,
            jwt_secret: security.jwt_secret,
            super_admins: security
                .super_admins
                .into_iter()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
            recovery_config: ops.recovery_config,
            dry_run: ops.dry_run,
        }
    }
}

impl From<UpArgs> for UpOptions {
    fn from(args: UpArgs) -> Self {
        match args.mode {
            UpMode::Standalone(a) => {
                let defaulted = a.data_dir.is_none();
                let data_dir = a.data_dir.unwrap_or_else(|| DEFAULT_DATA_DIR.to_string());
                UpOptions::base(
                    BrokerMode::Standalone,
                    data_dir,
                    defaulted,
                    a.raft_port,
                    a.node,
                    a.net,
                    a.security,
                    a.ops,
                )
            }
            UpMode::Cluster(a) => {
                let membership = if a.join {
                    Membership::Join
                } else if !a.seed_nodes.is_empty() {
                    Membership::Seeds
                } else if a.single_node {
                    Membership::SingleNode
                } else {
                    Membership::Implicit
                };
                let mut opts = UpOptions::base(
                    BrokerMode::Cluster,
                    a.data_dir,
                    false,
                    a.raft_port,
                    a.node,
                    a.net,
                    a.security,
                    a.ops,
                );
                opts.membership = membership;
                opts.seed_nodes = a
                    .seed_nodes
                    .into_iter()
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                opts.join = a.join;
                opts
            }
            UpMode::Edge(a) => {
                let mut opts = UpOptions::base(
                    BrokerMode::Edge,
                    a.data_dir,
                    false,
                    a.raft_port,
                    a.node,
                    a.net,
                    a.security,
                    a.ops,
                );
                opts.edge_name = Some(a.name);
                opts.cloud_endpoint = a.cloud_endpoint;
                opts.cloud_token = a.cloud_token;
                opts.mqtt_addr = a.mqtt_listen;
                opts
            }
        }
    }
}

// ─── Data-directory safety ──────────────────────────────────────────────────

/// Marker written at the root of every data directory recording the mode it
/// was initialised with, so a directory is never silently reused by another mode.
pub(crate) const NODE_MARKER_FILE: &str = "danube-node.json";

/// What we found in the data directory before starting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DataDirState {
    /// Nothing there yet: a new node identity will be created.
    Fresh,
    /// Previously initialised by the same mode: the node resumes.
    Existing,
    /// Holds Danube state from a version that predates the marker file.
    LegacyUnmarked,
}

/// Raft log subdirectory used by every mode (see `ServiceConfiguration`).
fn raft_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("raft")
}

fn dir_has_entries(path: &Path) -> bool {
    std::fs::read_dir(path)
        .map(|mut it| it.next().is_some())
        .unwrap_or(false)
}

/// Inspect `data_dir` and refuse to start if it belongs to a different mode.
pub(crate) fn inspect_data_dir(data_dir: &Path, mode: BrokerMode) -> Result<DataDirState> {
    let marker = data_dir.join(NODE_MARKER_FILE);
    if marker.exists() {
        let raw = std::fs::read_to_string(&marker)
            .with_context(|| format!("failed to read {}", marker.display()))?;
        let doc: serde_json::Value = serde_json::from_str(&raw)
            .with_context(|| format!("failed to parse {}", marker.display()))?;
        let recorded = doc.get("mode").and_then(|v| v.as_str()).unwrap_or("");
        if recorded != mode.as_str() {
            bail!(
                "data directory {} was initialised by a `danube up {}` node; refusing to start it as `{}`.\n\
                 Reusing it would mix incompatible metadata. Either run `danube up {} --data-dir {}` \
                 or choose a different --data-dir.",
                data_dir.display(),
                recorded,
                mode.as_str(),
                recorded,
                data_dir.display()
            );
        }
        return Ok(DataDirState::Existing);
    }
    if dir_has_entries(&raft_dir(data_dir)) {
        return Ok(DataDirState::LegacyUnmarked);
    }
    Ok(DataDirState::Fresh)
}

/// Record the mode in the data directory (creating it if needed).
pub(crate) fn write_node_marker(data_dir: &Path, mode: BrokerMode) -> Result<()> {
    std::fs::create_dir_all(data_dir)
        .with_context(|| format!("failed to create data directory {}", data_dir.display()))?;
    let doc = serde_json::json!({
        "mode": mode.as_str(),
        "created_by": format!("danube {}", env!("CARGO_PKG_VERSION")),
    });
    let marker = data_dir.join(NODE_MARKER_FILE);
    std::fs::write(&marker, serde_json::to_vec_pretty(&doc)?)
        .with_context(|| format!("failed to write {}", marker.display()))
}

// ─── Startup summary ────────────────────────────────────────────────────────

/// Values only known after `ServiceConfiguration` has been resolved.
pub(crate) struct ResolvedView<'a> {
    pub(crate) listen: String,
    pub(crate) admin: String,
    pub(crate) advertised: &'a str,
    pub(crate) connect_url: &'a str,
    pub(crate) metrics: Option<String>,
    pub(crate) auth: String,
    pub(crate) wal_root: String,
}

fn absolute(path: &str) -> String {
    std::path::absolute(path)
        .map(|p| p.display().to_string())
        .unwrap_or_else(|_| path.to_string())
}

/// Human-readable startup summary (printed to stderr) plus safety warnings.
pub(crate) fn render_summary(opts: &UpOptions, state: &DataDirState, view: &ResolvedView) -> String {
    let mut out = String::new();
    let line = |out: &mut String, k: &str, v: &str| out.push_str(&format!("  {:<14}{}\n", k, v));

    out.push_str(&format!(
        "\nDanube {} | {} node{}\n",
        env!("CARGO_PKG_VERSION"),
        opts.mode.as_str(),
        if opts.dry_run { " (dry run)" } else { "" }
    ));
    let state_txt = match state {
        DataDirState::Fresh => "new node",
        DataDirState::Existing => "resuming existing node",
        DataDirState::LegacyUnmarked => "resuming existing node (pre-marker data)",
    };
    line(
        &mut out,
        "data dir",
        &format!(
            "{}{}  [{}]",
            absolute(&opts.data_dir),
            if opts.data_dir_defaulted { " (default)" } else { "" },
            state_txt
        ),
    );
    line(&mut out, "wal dir", &view.wal_root);
    line(&mut out, "role", opts.node_role.as_str());
    line(&mut out, "listen", &view.listen);
    line(&mut out, "admin", &view.admin);
    line(&mut out, "advertised", view.advertised);
    if view.connect_url != view.advertised {
        line(&mut out, "connect url", view.connect_url);
    }
    line(&mut out, "raft port", &opts.raft_port.to_string());
    if let Some(m) = &view.metrics {
        line(&mut out, "metrics", m);
    }
    line(&mut out, "auth", &view.auth);
    match opts.membership {
        Membership::NotApplicable => {}
        Membership::SingleNode => line(&mut out, "membership", "single-node"),
        Membership::Implicit => line(&mut out, "membership", "single-node (implicit)"),
        Membership::Seeds => line(
            &mut out,
            "membership",
            &format!("seed nodes {}", opts.seed_nodes.join(",")),
        ),
        Membership::Join => line(&mut out, "membership", "join existing cluster"),
    }
    if opts.mode == BrokerMode::Edge {
        line(&mut out, "edge name", opts.edge_name.as_deref().unwrap_or("-"));
        line(
            &mut out,
            "cloud",
            opts.cloud_endpoint.as_deref().unwrap_or("(from recovery config)"),
        );
        line(
            &mut out,
            "mqtt",
            opts.mqtt_addr.as_deref().unwrap_or("0.0.0.0:1883"),
        );
    }
    if let Some(rc) = &opts.recovery_config {
        line(&mut out, "recovery", &format!("{} (break-glass override)", rc));
    }

    for warning in warnings(opts, state) {
        out.push_str(&format!("\n  warning: {}\n", warning));
    }
    out
}

/// Safety warnings shown before the node starts.
pub(crate) fn warnings(opts: &UpOptions, state: &DataDirState) -> Vec<String> {
    let mut w = Vec::new();
    if opts.data_dir_defaulted && *state == DataDirState::Fresh {
        w.push(format!(
            "no --data-dir given; creating a new node in {} (relative to the current directory).\n           \
             Start from the same directory or pass --data-dir to keep this node's data.",
            absolute(&opts.data_dir)
        ));
    }
    if opts.membership == Membership::Implicit && *state == DataDirState::Fresh {
        w.push(
            "no membership flag given; bootstrapping a NEW single-node cluster.\n           \
             Pass --seed-nodes to form a multi-node cluster, --join to join a running one,\n           \
             or --single-node to confirm and silence this warning."
                .to_string(),
        );
    }
    if opts.membership == Membership::Join && opts.admin_addr.is_none() && *state == DataDirState::Fresh {
        w.push(
            "--join without --admin-listen: the shared --listen port only opens after this node \
             becomes a member,\n           so `danube cluster node add --node-addr ...` cannot reach it. \
             Add --admin-listen <ADDR> (e.g. 0.0.0.0:50051)."
                .to_string(),
        );
    }
    if *state != DataDirState::Fresh && opts.membership == Membership::Seeds {
        w.push(
            "data directory already holds Raft state; --seed-nodes is only used on first boot \
             and the node resumes its existing membership."
                .to_string(),
        );
    }
    w
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::Cli;
    use clap::error::ErrorKind;
    use clap::Parser;

    fn up(args: &[&str]) -> std::result::Result<UpOptions, clap::Error> {
        let mut argv = vec!["danube", "up"];
        argv.extend_from_slice(args);
        let cli = Cli::try_parse_from(argv)?;
        match cli.command {
            Some(crate::cli::Command::Up(up)) => Ok(up.into()),
            other => panic!("expected up command, got {:?}", other.is_some()),
        }
    }

    #[test]
    fn standalone_defaults_data_dir() {
        let opts = up(&["standalone"]).unwrap();
        assert_eq!(opts.mode, BrokerMode::Standalone);
        assert_eq!(opts.data_dir, DEFAULT_DATA_DIR);
        assert!(opts.data_dir_defaulted);
        assert_eq!(opts.raft_port, 7650);
        assert_eq!(opts.node_role, NodeRole::All);
    }

    #[test]
    fn standalone_rejects_cluster_flags() {
        let err = up(&["standalone", "--seed-nodes", "a:7650"]).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnknownArgument);
        let err = up(&["standalone", "--join"]).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::UnknownArgument);
    }

    #[test]
    fn cluster_requires_data_dir() {
        let err = up(&["cluster", "--single-node"]).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::MissingRequiredArgument);
        assert!(err.to_string().contains("--data-dir"));
    }

    #[test]
    fn cluster_membership_variants() {
        let o = up(&["cluster", "--data-dir", "/d"]).unwrap();
        assert_eq!(o.membership, Membership::Implicit);
        let o = up(&["cluster", "--data-dir", "/d", "--single-node"]).unwrap();
        assert_eq!(o.membership, Membership::SingleNode);
        let o = up(&["cluster", "--data-dir", "/d", "--seed-nodes", "n1:7650, n2:7650"]).unwrap();
        assert_eq!(o.membership, Membership::Seeds);
        assert_eq!(o.seed_nodes, vec!["n1:7650", "n2:7650"]);
        let o = up(&["cluster", "--data-dir", "/d", "--join"]).unwrap();
        assert_eq!(o.membership, Membership::Join);
        assert!(o.join);
    }

    #[test]
    fn cluster_membership_flags_conflict() {
        let err = up(&["cluster", "--data-dir", "/d", "--join", "--seed-nodes", "a:1"]).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ArgumentConflict);
        let err = up(&["cluster", "--data-dir", "/d", "--single-node", "--join"]).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ArgumentConflict);
    }

    #[test]
    fn edge_requires_name_and_cloud_endpoint() {
        let err = up(&["edge", "--data-dir", "/d", "--name", "e1"]).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::MissingRequiredArgument);
        assert!(err.to_string().contains("--cloud-endpoint"));
        let err = up(&["edge", "--data-dir", "/d", "--cloud-endpoint", "http://c:6650"]).unwrap_err();
        assert!(err.to_string().contains("--name"));
        let o = up(&[
            "edge", "--data-dir", "/d", "--name", "e1", "--cloud-endpoint", "https://c:6650",
            "--mqtt-listen", "0.0.0.0:1884", "--cloud-token", "t",
        ])
        .unwrap();
        assert_eq!(o.mode, BrokerMode::Edge);
        assert_eq!(o.edge_name.as_deref(), Some("e1"));
        assert_eq!(o.mqtt_addr.as_deref(), Some("0.0.0.0:1884"));
        assert_eq!(o.cloud_token.as_deref(), Some("t"));
    }

    #[test]
    fn edge_cloud_endpoint_optional_with_recovery_config() {
        let o = up(&["edge", "--data-dir", "/d", "--name", "e1", "--recovery-config", "r.yaml"]).unwrap();
        assert!(o.cloud_endpoint.is_none());
    }

    #[test]
    fn tls_auth_requires_cert_files() {
        let err = up(&["standalone", "--auth", "tls"]).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::MissingRequiredArgument);
        let o = up(&[
            "cluster", "--data-dir", "/d", "--single-node", "--auth", "tls",
            "--tls-cert", "c.pem", "--tls-key", "k.pem", "--ca-cert", "ca.pem",
            "--jwt-secret", "s", "--super-admin", "admin,ops", "--super-admin", "root",
            "--role", "lakehouse",
        ])
        .unwrap();
        assert_eq!(o.auth, Some(AuthMode::Tls));
        assert_eq!(o.super_admins, vec!["admin", "ops", "root"]);
        assert_eq!(o.node_role, NodeRole::Lakehouse);
    }

    #[test]
    fn network_and_node_flags() {
        let o = up(&[
            "standalone", "--listen", "0.0.0.0:6650", "--admin-listen", "0.0.0.0:6651",
            "--metrics-listen", "0.0.0.0:9040", "--advertised-addr", "broker1:6650",
            "--connect-url", "https://proxy:443", "--wal-dir", "/mnt/nvme", "--raft-port", "7700",
            "--dry-run",
        ])
        .unwrap();
        assert_eq!(o.broker_addr.as_deref(), Some("0.0.0.0:6650"));
        assert_eq!(o.admin_addr.as_deref(), Some("0.0.0.0:6651"));
        assert_eq!(o.prom_exporter.as_deref(), Some("0.0.0.0:9040"));
        assert_eq!(o.advertised_addr.as_deref(), Some("broker1:6650"));
        assert_eq!(o.connect_url.as_deref(), Some("https://proxy:443"));
        assert_eq!(o.wal_dir.as_deref(), Some("/mnt/nvme"));
        assert_eq!(o.raft_port, 7700);
        assert!(o.dry_run);
    }

    #[test]
    fn bare_up_shows_modes() {
        let err = Cli::try_parse_from(["danube", "up"]).unwrap_err();
        assert_eq!(
            err.kind(),
            ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
        );
        let rendered = err.to_string();
        assert!(rendered.contains("standalone"));
        assert!(rendered.contains("cluster"));
        assert!(rendered.contains("edge"));
    }

    #[test]
    fn data_dir_marker_guards_mode() {
        let tmp = std::env::temp_dir().join(format!("danube-marker-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&tmp);
        assert_eq!(inspect_data_dir(&tmp, BrokerMode::Cluster).unwrap(), DataDirState::Fresh);
        write_node_marker(&tmp, BrokerMode::Cluster).unwrap();
        assert_eq!(inspect_data_dir(&tmp, BrokerMode::Cluster).unwrap(), DataDirState::Existing);
        let err = inspect_data_dir(&tmp, BrokerMode::Standalone).unwrap_err();
        assert!(err.to_string().contains("refusing to start"));
        let _ = std::fs::remove_dir_all(&tmp);

        // Legacy directory with Raft state but no marker
        std::fs::create_dir_all(tmp.join("raft")).unwrap();
        std::fs::write(tmp.join("raft").join("node_id"), "1").unwrap();
        assert_eq!(
            inspect_data_dir(&tmp, BrokerMode::Standalone).unwrap(),
            DataDirState::LegacyUnmarked
        );
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn implicit_single_node_warns_only_when_fresh() {
        let o = up(&["cluster", "--data-dir", "/d"]).unwrap();
        assert_eq!(warnings(&o, &DataDirState::Fresh).len(), 1);
        assert!(warnings(&o, &DataDirState::Existing).is_empty());
        let o = up(&["cluster", "--data-dir", "/d", "--single-node"]).unwrap();
        assert!(warnings(&o, &DataDirState::Fresh).is_empty());
    }
}
