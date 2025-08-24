mod common;
use common::*;

#[test]
fn namespace_lifecycle_creates_and_deletes() {
    let ns = unique_ns();

    // create namespace
    create_ns(&ns);

    // assert it appears in brokers namespaces
    list_ns_contains(&ns);

    // delete
    delete_ns(&ns);
}
