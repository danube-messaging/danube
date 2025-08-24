mod common;
use assert_cmd::assert::OutputAssertExt;
use common::*;

#[test]
fn topic_lifecycle_non_partitioned() {
    let ns = unique_ns();
    // CLI uses leading '/ns/topic'; list output uses 'ns/topic'
    let topic = format!("/{}/cli-e2e", ns);
    let list_name = format!("{}/cli-e2e", ns);

    // setup
    create_ns(&ns);

    // create topic non-reliable
    create_topic(&topic, "non_reliable");

    // assert it appears in namespace list (parse JSON for robustness)
    let mut list = cli();
    let out = list
        .args(["topic", "list", &ns, "--output", "json"]) 
        .output()
        .expect("run topic list");
    assert!(out.status.success());
    let body = String::from_utf8(out.stdout).unwrap();
    let topics: Vec<String> = serde_json::from_str(&body).expect("valid JSON array of strings");
    assert!(topics.iter().any(|t| t == &list_name), "missing {list_name} in {topics:?}");

    // describe topic (json path contains schema)
    // TODO: re-enable when Discovery schema fetch is stable in CI
    // let mut describe = cli();
    // describe
    //     .args(["topic", "describe", &topic, "--output", "json"])
    //     .assert()
    //     .success()
    //     .stdout(predicates::str::contains("\"schema\""));

    // subscriptions should be an array (we only assert success here)
    let mut subs = cli();
    subs.args(["topic", "subscriptions", &topic, "--output", "json"]).assert().success();

    // delete and verify removal
    delete_topic(&topic);
    let mut list2 = cli();
    let out2 = list2
        .args(["topic", "list", &ns, "--output", "json"]) 
        .output()
        .expect("run topic list after delete");
    assert!(out2.status.success());
    let body2 = String::from_utf8(out2.stdout).unwrap();
    let topics2: Vec<String> = serde_json::from_str(&body2).expect("valid JSON array of strings");
    assert!(!topics2.iter().any(|t| t == &list_name), "{list_name} still present: {topics2:?}");

    // teardown
    delete_ns(&ns);
}
