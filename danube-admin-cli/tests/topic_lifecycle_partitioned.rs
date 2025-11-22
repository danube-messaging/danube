mod common;
use assert_cmd::assert::OutputAssertExt;
use common::*;

#[test]
fn topic_lifecycle_partitioned() {
    let ns = unique_ns();
    // CLI expects leading '/namespace/topic'; list output now includes leading '/namespace/topic'
    let base_cli = format!("/{}/cli-e2e-p", ns);
    let base_list = format!("{}/cli-e2e-p", ns);

    // setup
    create_ns(&ns);

    // create partitioned topic (server-side)
    let mut cmd = cli();
    cmd.args([
        "topics",
        "create",
        &base_cli,
        "--partitions",
        "2",
        "--dispatch-strategy",
        "reliable",
    ])
    .assert()
    .success();

    // list topics should include both partitions (parse JSON for robustness)
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
    let part0 = format!("/{}-part-0", base_list);
    let part1 = format!("/{}-part-1", base_list);
    assert!(names.iter().any(|t| t == &part0), "missing {part0} in {names:?}");
    assert!(names.iter().any(|t| t == &part1), "missing {part1} in {names:?}");

    // subscriptions should be an array on part-0
    let mut subs = cli();
    let part0_cli = format!("{}-part-0", base_cli);
    subs.args(["topics", "subscriptions", &part0_cli, "--output", "json"]).assert().success();

    // delete both partitions
    let part1_cli = format!("{}-part-1", base_cli);
    let mut delete_cmd1 = cli();
    delete_cmd1.args(["topics", "delete", &part0_cli]).assert().success();
    let mut delete_cmd2 = cli();
    delete_cmd2.args(["topics", "delete", &part1_cli]).assert().success();

    // verify deletion: list should not include them anymore (parse JSON)
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
    assert!(!names2.iter().any(|t| t == &part0), "{part0} still present: {names2:?}");
    assert!(!names2.iter().any(|t| t == &part1), "{part1} still present: {names2:?}");

    // teardown: no explicit topic delete RPC for partitioned base name; remove namespace
    delete_ns(&ns);
}
