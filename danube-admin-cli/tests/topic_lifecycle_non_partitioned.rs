mod common;
use assert_cmd::assert::OutputAssertExt;
use common::*;

#[test]
fn topic_lifecycle_non_partitioned() {
    let ns = unique_ns();
    // CLI uses leading '/ns/topic'; list output now includes leading '/ns/topic'
    let topic = format!("/{}/cli-e2e", ns);
    let list_name = format!("{}/cli-e2e", ns);

    // setup
    create_ns(&ns);

    // create topic non-reliable
    let mut cmd = cli();
    cmd.args(["topics", "create", &topic, "--dispatch-strategy", "non_reliable"])
        .assert()
        .success();

    // assert it appears in namespace list (parse JSON for robustness)
    let mut list = cli();
    let out = list
        .args(["topics", "list", "--namespace", &ns, "--output", "json"]) 
        .output()
        .expect("run topic list");
    assert!(out.status.success());
    let body = String::from_utf8(out.stdout).unwrap();
    let arr: Vec<serde_json::Value> = serde_json::from_str(&body).expect("valid JSON array");
    let names: Vec<String> = arr
        .iter()
        .filter_map(|v| v.get("name").and_then(|n| n.as_str()).map(|s| s.to_string()))
        .collect();
    let expected = format!("/{}", list_name);
    assert!(names.iter().any(|t| t == &expected), "missing {expected} in {names:?}");

    // describe topic (json path contains schema)
    let mut describe = cli();
    describe
        .args(["topics", "describe", &topic, "--output", "json"])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"schema\""));

    // subscriptions should be an array (we only assert success here)
    let mut subs = cli();
    subs.args(["topics", "subscriptions", &topic, "--output", "json"]).assert().success();

    // delete and verify removal
    let mut delete_cmd = cli();
    delete_cmd.args(["topics", "delete", &topic]).assert().success();
    let mut list2 = cli();
    let out2 = list2
        .args(["topics", "list", "--namespace", &ns, "--output", "json"]) 
        .output()
        .expect("run topic list after delete");
    assert!(out2.status.success());
    let body2 = String::from_utf8(out2.stdout).unwrap();
    let arr2: Vec<serde_json::Value> = serde_json::from_str(&body2).expect("valid JSON array");
    let names2: Vec<String> = arr2
        .iter()
        .filter_map(|v| v.get("name").and_then(|n| n.as_str()).map(|s| s.to_string()))
        .collect();
    assert!(!names2.iter().any(|t| t == &expected), "{expected} still present: {names2:?}");

    // teardown
    delete_ns(&ns);
}
