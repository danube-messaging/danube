//! Command-line interface of the unified `danube` binary.
//!
//! ```text
//! danube
//! ├─ Server            up {standalone,cluster,edge} · mcp · console
//! ├─ Messaging         produce · consume
//! ├─ Resources         topic · namespace · schema
//! ├─ Cluster ops       cluster · broker · auth · apply
//! └─ Utilities         context · completions · version
//! ```

pub(crate) mod up;

use clap::builder::styling::{AnsiColor, Styles};
use clap::{Args, CommandFactory, FromArgMatches, Parser, Subcommand};
use clap_complete::Shell;

use danube_admin::cli as admin;

pub(crate) const STYLES: Styles = Styles::styled()
    .header(AnsiColor::Green.on_default().bold())
    .usage(AnsiColor::Green.on_default().bold())
    .literal(AnsiColor::Cyan.on_default().bold())
    .placeholder(AnsiColor::Cyan.on_default())
    .error(AnsiColor::Red.on_default().bold())
    .valid(AnsiColor::Cyan.on_default().bold())
    .invalid(AnsiColor::Yellow.on_default().bold());

const ROOT_ABOUT: &str = "Danube: lightweight, agent-native event streaming";

const ROOT_AFTER_HELP: &str = "\
Quick start:
  danube up standalone                                      # start a local node
  danube topic create /default/events --intent event_stream
  danube produce /default/events -m 'hello'
  danube consume /default/events

Run 'danube <COMMAND> --help' for details on a command.";

#[derive(Debug, Parser)]
#[command(
    name = "danube",
    version,
    about = ROOT_ABOUT,
    after_help = ROOT_AFTER_HELP,
    styles = STYLES,
    disable_help_subcommand = true,
    max_term_width = 100
)]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub(crate) command: Option<Command>,
}

#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    // ── Server ──
    Up(up::UpArgs),
    Mcp(danube_admin::McpArgs),
    Console(danube_admin::ConsoleArgs),

    // ── Messaging ──
    Produce(admin::produce::Produce),
    Consume(admin::consume::Consume),

    // ── Resources ──
    #[command(visible_alias = "topics")]
    Topic(admin::topics::Topics),
    #[command(visible_alias = "namespaces", visible_alias = "ns")]
    Namespace(admin::namespaces::Namespaces),
    #[command(visible_alias = "schemas")]
    Schema(admin::schemas::Schemas),

    // ── Cluster operations ──
    Cluster(admin::cluster::Cluster),
    #[command(visible_alias = "brokers")]
    Broker(admin::brokers::Brokers),
    Auth(admin::security::Security),
    Apply(admin::apply::Apply),

    // ── Utilities ──
    Context(admin::context::Context),
    Completions(CompletionsArgs),
    #[command(about = "Print version information")]
    Version,
}

/// Generate shell completion scripts
#[derive(Debug, Args)]
#[command(
    about = "Generate shell completion scripts",
    after_help = "Examples:
  danube completions bash > /etc/bash_completion.d/danube
  danube completions zsh  > \"${fpath[1]}/_danube\"
  danube completions fish > ~/.config/fish/completions/danube.fish"
)]
pub(crate) struct CompletionsArgs {
    /// Target shell
    #[arg(value_enum)]
    pub(crate) shell: Shell,
}

/// Root help sections: (heading, command names in display order).
pub(crate) const ROOT_GROUPS: &[(&str, &[&str])] = &[
    ("Server", &["up", "mcp", "console"]),
    ("Messaging", &["produce", "consume"]),
    ("Resources", &["topic", "namespace", "schema"]),
    ("Cluster operations", &["cluster", "broker", "auth", "apply"]),
    ("Utilities", &["context", "completions", "version"]),
];

/// Short descriptions shown in the grouped root help.
fn root_summary(name: &str) -> Option<&'static str> {
    Some(match name {
        "up" => "Start a Danube node (standalone | cluster | edge)",
        "mcp" => "Run the MCP server for AI agents",
        "console" => "Run the web admin console backend",
        "produce" => "Publish messages to a topic",
        "consume" => "Subscribe and stream messages from a topic",
        "topic" => "Create, inspect and operate topics and subscriptions",
        "namespace" => "Manage namespaces and their policies",
        "schema" => "Manage schema registry subjects",
        "cluster" => "Membership, storage, WAL and load-manager settings",
        "broker" => "Inspect, unload, activate and rebalance brokers",
        "auth" => "Roles, bindings and tokens",
        "apply" => "Reconcile a desired-state manifest",
        "context" => "Manage saved connection profiles",
        "completions" => "Generate shell completion scripts",
        "version" => "Print version information",
        _ => return None,
    })
}

/// Build the root help template with grouped command sections.
fn root_help_template() -> String {
    let header = STYLES.get_header();
    let literal = STYLES.get_literal();
    let width = ROOT_GROUPS
        .iter()
        .flat_map(|(_, names)| names.iter())
        .map(|n| n.len())
        .max()
        .unwrap_or(10)
        + 2;

    let mut out = String::from("{before-help}{about-with-newline}\n{usage-heading} {usage}\n");
    for (heading, names) in ROOT_GROUPS {
        out.push_str(&format!("\n{header}{heading}:{header:#}\n"));
        for name in names.iter() {
            let summary = root_summary(name).unwrap_or("");
            out.push_str(&format!(
                "  {literal}{name:<width$}{literal:#}{summary}\n",
                width = width
            ));
        }
    }
    out.push_str(&format!("\n{header}Options:{header:#}\n{{options}}{{after-help}}"));
    out
}

/// The fully configured root command (derive + grouped help template).
pub(crate) fn command() -> clap::Command {
    Cli::command().help_template(root_help_template())
}

/// Parse `std::env::args()` with the grouped root help.
pub(crate) fn parse() -> Cli {
    let mut matches = command().get_matches();
    Cli::from_arg_matches_mut(&mut matches).unwrap_or_else(|e| e.format(&mut command()).exit())
}

/// Print the root overview (used when `danube` is run without a command).
pub(crate) fn print_overview() {
    let _ = command().print_help();
}

/// `danube completions <SHELL>`
pub(crate) fn print_completions(shell: Shell) {
    let mut cmd = command();
    clap_complete::generate(shell, &mut cmd, "danube", &mut std::io::stdout());
}

/// `danube version`
pub(crate) fn print_version() {
    println!("danube {}", env!("CARGO_PKG_VERSION"));
    let mut features = Vec::new();
    if cfg!(feature = "lakehouse") {
        features.push("lakehouse");
    }
    if cfg!(feature = "lakehouse-aws") {
        features.push("lakehouse-aws");
    }
    if !features.is_empty() {
        println!("features: {}", features.join(", "));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_tree_is_valid() {
        command().debug_assert();
    }

    #[test]
    fn every_command_is_listed_in_root_help() {
        let grouped: Vec<&str> = ROOT_GROUPS.iter().flat_map(|(_, n)| n.iter().copied()).collect();
        for sub in command().get_subcommands() {
            if sub.is_hide_set() {
                continue;
            }
            let name = sub.get_name();
            assert!(grouped.contains(&name), "`{name}` missing from ROOT_GROUPS");
            assert!(root_summary(name).is_some(), "`{name}` missing a root summary");
        }
        for name in grouped {
            assert!(
                command().find_subcommand(name).is_some(),
                "ROOT_GROUPS lists unknown command `{name}`"
            );
        }
    }

    #[test]
    fn bare_danube_parses_to_overview() {
        let cli = Cli::try_parse_from(["danube"]).unwrap();
        assert!(cli.command.is_none());
    }

    #[test]
    fn root_help_is_grouped() {
        let help = command().render_help().to_string();
        for heading in ["Server:", "Messaging:", "Resources:", "Cluster operations:", "Utilities:"] {
            assert!(help.contains(heading), "missing {heading} in\n{help}");
        }
        assert!(help.contains("Quick start:"));
        assert!(!help.contains("Commands:"));
    }

    #[test]
    fn unknown_command_suggests() {
        let err = command().try_get_matches_from(["danube", "topc"]).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::InvalidSubcommand);
        assert!(err.to_string().contains("topic"));
    }

    #[test]
    fn plural_aliases_work() {
        for alias in ["topics", "namespaces", "ns", "schemas", "brokers"] {
            let res = command().try_get_matches_from(["danube", alias, "--help"]);
            let err = res.unwrap_err();
            assert_eq!(err.kind(), clap::error::ErrorKind::DisplayHelp, "{alias}");
        }
    }

    #[test]
    fn data_plane_commands_take_positional_topic() {
        let cli = Cli::try_parse_from([
            "danube", "produce", "/default/events", "-m", "hi", "--endpoint", "http://b:6650",
        ])
        .unwrap();
        match cli.command {
            Some(Command::Produce(p)) => {
                assert_eq!(p.topic, "/default/events");
                assert_eq!(p.client.endpoint.as_deref(), Some("http://b:6650"));
            }
            _ => panic!("expected produce"),
        }
        let err = Cli::try_parse_from(["danube", "produce", "/default/events"]).unwrap_err();
        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);

        let cli = Cli::try_parse_from(["danube", "consume", "/default/events", "-s", "sub1"]).unwrap();
        assert!(matches!(cli.command, Some(Command::Consume(_))));
    }

    #[test]
    fn mcp_defaults_to_stdio() {
        let cli = Cli::try_parse_from(["danube", "mcp"]).unwrap();
        match cli.command {
            Some(Command::Mcp(args)) => assert!(args.http.is_none()),
            _ => panic!("expected mcp"),
        }
    }
}
