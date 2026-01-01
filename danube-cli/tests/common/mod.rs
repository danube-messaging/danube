use rand::{distr::Alphanumeric, Rng};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

pub fn cli() -> Command {
    Command::new(assert_cmd::cargo::cargo_bin!("danube-cli"))
}

pub fn service_addr() -> String {
    std::env::var("DANUBE_BROKER_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:6650".into())
}

#[allow(dead_code)]
pub fn unique_topic() -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let rand: String = rand::rng()
        .sample_iter(&Alphanumeric)
        .take(6)
        .map(char::from)
        .collect();
    format!("/default/test-{}-{}", ts, rand.to_lowercase())
}

#[allow(dead_code)]
pub fn unique_subject() -> String {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let rand: String = rand::rng()
        .sample_iter(&Alphanumeric)
        .take(6)
        .map(char::from)
        .collect();
    format!("schema-{}-{}", ts, rand.to_lowercase())
}
