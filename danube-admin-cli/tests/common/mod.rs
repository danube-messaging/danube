use assert_cmd::prelude::*;
use predicates::str::contains;
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

pub fn list_ns_contains(ns: &str) {
    let mut cmd = cli();
    cmd.args(["brokers", "namespaces", "--output", "json"]) // brokers namespaces aggregates cluster namespaces
        .assert()
        .success()
        .stdout(contains(ns));
}

pub fn create_topic(topic: &str, dispatch: &str) {
    let mut cmd = cli();
    cmd.args(["topic", "create", topic, "--dispatch-strategy", dispatch])
        .assert()
        .success();
}

pub fn create_partitioned_topic(base: &str, partitions: u32, dispatch: &str) {
    let mut cmd = cli();
    // partitions is positional in CLI: topic create-partitioned <TOPIC> <PARTITIONS>
    cmd.args([
        "topic",
        "create-partitioned",
        base,
        &partitions.to_string(),
        "--dispatch-strategy",
        dispatch,
    ])
    .assert()
    .success();
}

pub fn topic_list_contains(ns: &str, name_substr: &str) {
    let mut cmd = cli();
    cmd.args(["topic", "list", ns, "--output", "json"]) // pretty JSON
        .assert()
        .success()
        .stdout(contains(name_substr));
}

pub fn delete_topic(topic: &str) {
    let mut cmd = cli();
    cmd.args(["topic", "delete", topic]).assert().success();
}
