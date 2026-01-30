mod common;
use assert_cmd::assert::OutputAssertExt;
use common::*;

#[test]
fn namespace_lifecycle_creates_and_deletes() {
    let ns = unique_ns();

    // create namespace
    create_ns(&ns);

    // assert it appears in brokers namespaces
    let mut cmd = cli();
    cmd.args(["brokers", "namespaces", "--output", "json"])
        .assert()
        .success()
        .stdout(predicates::str::contains(&ns));

    // delete
    delete_ns(&ns);
}
