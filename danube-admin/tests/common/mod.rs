use assert_cmd::prelude::*;
use rand::{distr::Alphanumeric, Rng};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn resolve_danube_binary() -> Option<PathBuf> {
    if let Ok(bin) = std::env::var("DANUBE_BIN") {
        let p = PathBuf::from(bin);
        if p.exists() {
            return Some(p);
        }
    }
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap_or_else(|| Path::new(env!("CARGO_MANIFEST_DIR")));
    for rel in ["./target/release/danube", "./target/debug/danube"] {
        let candidate = workspace_root.join(rel);
        if candidate.exists() {
            return Some(candidate);
        }
    }
    None
}

pub fn cli() -> Command {
    let mut cmd = if let Some(bin) = resolve_danube_binary() {
        let mut c = Command::new(bin);
        c.arg("ctl");
        c
    } else {
        let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap_or_else(|| Path::new(env!("CARGO_MANIFEST_DIR")));
        let mut c = Command::new("cargo");
        c.current_dir(workspace_root);
        c.args(["run", "-p", "danube-broker", "--bin", "danube", "--", "ctl"]);
        c
    };
    cmd.env(
        "DANUBE_ADMIN_ENDPOINT",
        std::env::var("DANUBE_ADMIN_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:6650".into()),
    );
    cmd.env(
        "DANUBE_BROKER_ENDPOINT",
        std::env::var("DANUBE_BROKER_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:6650".into()),
    );
    cmd
}

pub fn unique_ns() -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let rand: String = rand::rng()
        .sample_iter(&Alphanumeric)
        .take(6)
        .map(char::from)
        .collect();
    format!("ns-{}-{}", ts, rand)
}

pub fn create_ns(ns: &str) {
    let mut cmd = cli();
    cmd.args(["namespaces", "create", ns]).assert().success();
}

pub fn delete_ns(ns: &str) {
    let mut cmd = cli();
    let _ = cmd.args(["namespaces", "delete", ns]).output();
}
