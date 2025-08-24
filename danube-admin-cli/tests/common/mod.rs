use assert_cmd::prelude::*;
use rand::{distributions::Alphanumeric, Rng};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn cli() -> Command {
    let mut cmd = Command::cargo_bin("danube-admin-cli").expect("binary exists");
    cmd.env(
        "DANUBE_ADMIN_ENDPOINT",
        std::env::var("DANUBE_ADMIN_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:50051".into()),
    );
    cmd.env(
        "DANUBE_BROKER_ENDPOINT",
        std::env::var("DANUBE_BROKER_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:6650".into()),
    );
    cmd
}

pub fn unique_ns() -> String {
    let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    let rand: String = rand::thread_rng().sample_iter(&Alphanumeric).take(6).map(char::from).collect();
    format!("ns-{}-{}", ts, rand)
}

pub fn create_ns(ns: &str) {
    let mut cmd = cli();
    cmd.args(["namespaces", "create", ns])
        .assert()
        .success();
}

pub fn delete_ns(ns: &str) {
    let mut cmd = cli();
    let _ = cmd.args(["namespaces", "delete", ns]).output();
}

