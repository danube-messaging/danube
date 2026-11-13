use anyhow::{Context, Result};
use clap::Args;
use danube_core::admin_proto::DesiredStateRequest;

use crate::core::{AdminGrpcClient, GrpcClientConfig};

#[derive(Debug, Args)]
#[command(about = "Plan or apply a declarative desired-state manifest (YAML or JSON)")]
pub struct Apply {
    /// Path to YAML or JSON desired-state manifest file (use '-' for stdin)
    #[arg(short = 'f', long = "file")]
    pub file: String,

    /// Show the computed plan without executing changes
    #[arg(long, default_value_t = false)]
    pub dry_run: bool,

    /// Opt-in deletion of user topics not present in the manifest
    #[arg(long, default_value_t = false)]
    pub prune: bool,

    /// Allow pruning non-empty or active topics when --prune is set
    #[arg(long, default_value_t = false)]
    pub force: bool,
}

pub async fn handle(cmd: Apply, endpoint: &str) -> Result<()> {
    let manifest = if cmd.file == "-" {
        use std::io::Read;
        let mut buf = String::new();
        std::io::stdin()
            .read_to_string(&mut buf)
            .context("failed to read manifest from stdin")?;
        buf
    } else {
        std::fs::read_to_string(&cmd.file)
            .with_context(|| format!("failed to read manifest '{}'", cmd.file))?
    };

    let client = AdminGrpcClient::connect(GrpcClientConfig {
        endpoint: endpoint.to_string(),
        ..Default::default()
    })
    .await?;

    let req = DesiredStateRequest {
        manifest,
        prune: cmd.prune,
        force_delete_non_empty: cmd.force,
    };

    let plan = client.plan_desired_state(req.clone()).await?;
    if plan.actions.is_empty() {
        println!("No changes required.");
        return Ok(());
    }

    println!("Planned actions ({}):", plan.actions.len());
    for action in &plan.actions {
        println!("  [{}] {} ({})", action.kind, action.resource, action.details);
    }

    if cmd.dry_run {
        println!("Dry-run complete (no changes applied).");
        return Ok(());
    }

    let res = client.apply_desired_state(req).await?;
    println!("{}", res.message);
    Ok(())
}

