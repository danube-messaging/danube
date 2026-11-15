//! `danube context` — manage saved client connection profiles.

use anyhow::{anyhow, Result};
use clap::{Args, Subcommand, ValueHint};

use super::connection::{ContextStore, Profile};

#[derive(Debug, Args)]
#[command(
    about = "Manage saved connection profiles (endpoint, TLS, token)",
    long_about = "Manage saved connection profiles (endpoint, TLS, token).\n\n\
Profiles are stored client-side in $DANUBE_CONFIG_DIR/contexts.toml \
(default: ~/.config/danube/contexts.toml). The server never reads this file.\n\n\
Every client command uses the current context unless --context, --endpoint \
or the DANUBE_* environment variables override it.",
    after_help = "Examples:
  danube context add local --endpoint http://127.0.0.1:6650 --use
  danube context add prod --endpoint https://danube.example.com:6650 --tls-ca ./ca.pem --token \"$TOKEN\"
  danube context use prod
  danube context list
  danube topic list --namespace default --context local",
    subcommand_required = true,
    arg_required_else_help = true
)]
pub struct Context {
    #[command(subcommand)]
    command: ContextCommands,
}

#[derive(Debug, Subcommand)]
enum ContextCommands {
    /// Add (or replace with --force) a connection profile
    Add {
        /// Profile name
        name: String,
        /// Broker endpoint, e.g. http://127.0.0.1:6650
        #[arg(long, value_name = "URL", value_hint = ValueHint::Url)]
        endpoint: String,
        /// JWT bearer token (stored in plain text; file mode is 0600)
        #[arg(long, value_name = "JWT")]
        token: Option<String>,
        /// Always connect with TLS
        #[arg(long)]
        tls: bool,
        /// CA certificate (PEM)
        #[arg(long, value_name = "FILE", value_hint = ValueHint::FilePath)]
        tls_ca: Option<String>,
        /// Client certificate (PEM) for mTLS
        #[arg(long, value_name = "FILE", value_hint = ValueHint::FilePath)]
        tls_cert: Option<String>,
        /// Client private key (PEM) for mTLS
        #[arg(long, value_name = "FILE", value_hint = ValueHint::FilePath)]
        tls_key: Option<String>,
        /// TLS server name to verify
        #[arg(long, value_name = "NAME")]
        tls_domain: Option<String>,
        /// Make this the current context
        #[arg(long = "use")]
        make_current: bool,
        /// Replace an existing profile with the same name
        #[arg(long)]
        force: bool,
    },
    /// Switch the current context
    Use {
        /// Profile name
        name: String,
    },
    /// List saved contexts (* marks the current one)
    #[command(visible_alias = "ls")]
    List,
    /// Print the current context name
    Current,
    /// Show a context's settings (token masked)
    Show {
        /// Profile name [default: current context]
        name: Option<String>,
    },
    /// Delete a context
    #[command(visible_alias = "rm")]
    Delete {
        /// Profile name
        name: String,
    },
}

pub async fn handle(cmd: Context) -> Result<()> {
    let mut store = ContextStore::load()?;
    match cmd.command {
        ContextCommands::Add {
            name,
            endpoint,
            token,
            tls,
            tls_ca,
            tls_cert,
            tls_key,
            tls_domain,
            make_current,
            force,
        } => {
            if store.contexts.contains_key(&name) && !force {
                return Err(anyhow!(
                    "context '{}' already exists; pass --force to replace it",
                    name
                ));
            }
            store.contexts.insert(
                name.clone(),
                Profile {
                    endpoint,
                    token,
                    tls,
                    tls_ca,
                    tls_cert,
                    tls_key,
                    tls_domain,
                },
            );
            let first = store.current.is_none();
            if make_current || first {
                store.current = Some(name.clone());
            }
            let path = store.save()?;
            println!("✅ Saved context '{}' to {}", name, path.display());
            if store.current.as_deref() == Some(name.as_str()) {
                println!("   '{}' is now the current context", name);
            }
        }
        ContextCommands::Use { name } => {
            if !store.contexts.contains_key(&name) {
                return Err(anyhow!(
                    "context '{}' not found (available: {})",
                    name,
                    store.names_or_none()
                ));
            }
            store.current = Some(name.clone());
            store.save()?;
            println!("✅ Switched to context '{}'", name);
        }
        ContextCommands::List => {
            if store.contexts.is_empty() {
                println!("No contexts saved. Add one with: danube context add <NAME> --endpoint <URL>");
                return Ok(());
            }
            println!("{:<3}{:<20}{}", "", "NAME", "ENDPOINT");
            for (name, profile) in &store.contexts {
                let marker = if store.current.as_deref() == Some(name.as_str()) {
                    "*"
                } else {
                    ""
                };
                println!("{:<3}{:<20}{}", marker, name, profile.endpoint);
            }
        }
        ContextCommands::Current => match &store.current {
            Some(name) => println!("{}", name),
            None => {
                return Err(anyhow!(
                    "no current context; set one with `danube context use <NAME>`"
                ))
            }
        },
        ContextCommands::Show { name } => {
            let name = name
                .or_else(|| store.current.clone())
                .ok_or_else(|| anyhow!("no context given and no current context set"))?;
            let profile = store
                .contexts
                .get(&name)
                .ok_or_else(|| anyhow!("context '{}' not found", name))?;
            println!("name:       {}", name);
            println!("endpoint:   {}", profile.endpoint);
            println!("tls:        {}", profile.tls);
            println!("tls_ca:     {}", profile.tls_ca.as_deref().unwrap_or("-"));
            println!("tls_cert:   {}", profile.tls_cert.as_deref().unwrap_or("-"));
            println!("tls_key:    {}", profile.tls_key.as_deref().unwrap_or("-"));
            println!("tls_domain: {}", profile.tls_domain.as_deref().unwrap_or("-"));
            println!(
                "token:      {}",
                if profile.token.is_some() { "<set>" } else { "-" }
            );
        }
        ContextCommands::Delete { name } => {
            if store.contexts.remove(&name).is_none() {
                return Err(anyhow!("context '{}' not found", name));
            }
            if store.current.as_deref() == Some(name.as_str()) {
                store.current = None;
            }
            store.save()?;
            println!("✅ Deleted context '{}'", name);
        }
    }
    Ok(())
}

