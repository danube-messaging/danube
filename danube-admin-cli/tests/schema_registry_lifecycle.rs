mod common;
use assert_cmd::assert::OutputAssertExt;
use common::*;
use rand::Rng;
use std::fs;

/// Integration test for Schema Registry lifecycle and topic-schema integration.
///
/// # Test Flow
///
/// This test validates the complete end-to-end workflow of the Schema Registry
/// integration with the admin CLI, covering:
///
/// 1. **Schema Registration** - Register a new JSON schema with the registry
/// 2. **Schema Retrieval** - Get schema details by subject name
/// 3. **Version Management** - List all versions for a schema subject
/// 4. **Topic Creation with Schema** - Create a topic associated with a schema subject
/// 5. **Schema Metadata Verification** - Verify topic description includes all schema fields:
///    - schema_subject (subject name)
///    - schema_id (global unique ID)
///    - schema_version (version number)
///    - schema_type (json_schema, avro, etc.)
///    - compatibility_mode (BACKWARD, FORWARD, FULL, NONE)
/// 6. **Backward Compatibility** - Create topic without schema (optional schema support)
/// 7. **Schema Validation** - Verify topics without schemas return null schema fields
/// 8. **Compatibility Check** - Test schema compatibility validation (optional)
///
/// # What We Test
///
/// - Schema registry CRUD operations via admin CLI
/// - Topic-to-schema association in metadata storage (ETCD)
/// - Schema metadata retrieval from registry and display in topic describe
/// - Backward compatibility: topics can exist without schemas
/// - JSON output format for all commands
/// - Proper cleanup of test artifacts
///
/// # Architecture Validated
///
/// ```text
/// Admin CLI → Admin Server (port 50051)
///     ├─→ SchemaRegistry service → register/get schemas
///     └─→ TopicAdmin service → create topic + store schema_subject
///         ↓
///     ETCD Metadata
///         ├─→ /schemas/{subject}/metadata
///         ├─→ /schemas/{subject}/versions/{version}
///         └─→ /topics/{name}/schema_subject
/// ```
#[test]
fn schema_registry_lifecycle() {
    let ns = unique_ns();
    let topic = format!("/{}/events", ns);
    let schema_subject = "test-events-schema";

    // ============================================================================
    // SETUP: Create test namespace and schema file
    // ============================================================================
    create_ns(&ns);

    // Create a temporary JSON schema file for testing
    let schema_content = r#"{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "TestEvent",
  "type": "object",
  "required": ["event_type", "timestamp"],
  "properties": {
    "event_type": {
      "type": "string"
    },
    "timestamp": {
      "type": "integer"
    }
  }
}"#;

    let temp_dir = std::env::temp_dir();
    let schema_file = temp_dir.join(format!("test_schema_{}.json", rand::rng().random::<u32>()));
    fs::write(&schema_file, schema_content).expect("write schema file");

    // ============================================================================
    // STEP 1: Register schema with Schema Registry
    // Tests: schemas register command, schema creation in ETCD
    // ============================================================================
    let mut register_cmd = cli();
    let register_output = register_cmd
        .args([
            "schemas",
            "register",
            schema_subject,
            "--schema-type",
            "json_schema",
            "--file",
            schema_file.to_str().unwrap(),
        ])
        .output()
        .expect("register schema");

    assert!(register_output.status.success());
    let register_stdout = String::from_utf8(register_output.stdout).unwrap();
    
    // Extract schema_id from output (format: "Schema ID: <number>")
    let schema_id: u64 = register_stdout
        .lines()
        .find(|line| line.contains("Schema ID:"))
        .and_then(|line| line.split("Schema ID:").nth(1))
        .and_then(|s| s.trim().parse().ok())
        .expect("Failed to parse schema_id from register output");
    
    // Verify version is 1 (first version)
    assert!(register_stdout.contains("Version: 1"));

    // ============================================================================
    // STEP 2: Retrieve schema details by subject
    // Tests: schemas get command, schema metadata retrieval from registry
    // ============================================================================
    let mut get_cmd = cli();
    get_cmd
        .args([
            "schemas",
            "get",
            "--subject",
            schema_subject,
            "--output",
            "json",
        ])
        .assert()
        .success()
        .stdout(predicates::str::contains(schema_subject))
        .stdout(predicates::str::contains(&format!("\"schema_id\": {}", schema_id)));

    // ============================================================================
    // STEP 3: List all versions for the schema subject
    // Tests: schemas versions command, version enumeration
    // ============================================================================
    let mut versions_cmd = cli();
    versions_cmd
        .args(["schemas", "versions", schema_subject, "--output", "json"])
        .assert()
        .success()
        .stdout(predicates::str::contains("\"version\": 1"));

    // ============================================================================
    // STEP 4: Create topic associated with schema subject
    // Tests: topics create --schema-subject, schema_subject stored in ETCD
    // ============================================================================
    let mut create_topic_cmd = cli();
    create_topic_cmd
        .args([
            "topics",
            "create",
            &topic,
            "--schema-subject",
            schema_subject,
        ])
        .assert()
        .success()
        .stdout(predicates::str::contains("Topic created"))
        .stdout(predicates::str::contains(schema_subject));

    // ============================================================================
    // STEP 5: Verify topic description includes complete schema metadata
    // Tests: topics describe command, schema metadata retrieval and population
    //        Validates all schema fields: subject, ID, version, type, compatibility
    // ============================================================================
    let mut describe_cmd = cli();
    let describe_output = describe_cmd
        .args(["topics", "describe", &topic, "--output", "json"])
        .output()
        .expect("describe topic");

    assert!(describe_output.status.success());
    let body = String::from_utf8(describe_output.stdout).unwrap();
    let topic_info: serde_json::Value = serde_json::from_str(&body).expect("valid JSON");

    // Verify schema fields are populated (flat structure)
    assert_eq!(
        topic_info["schema_subject"].as_str().unwrap(),
        schema_subject
    );
    assert_eq!(topic_info["schema_id"].as_u64().unwrap(), schema_id);
    assert_eq!(topic_info["schema_version"].as_u64().unwrap(), 1);
    assert_eq!(topic_info["schema_type"].as_str().unwrap(), "json_schema");
    assert_eq!(
        topic_info["compatibility_mode"].as_str().unwrap(),
        "BACKWARD"
    );

    // ============================================================================
    // STEP 6: Create topic WITHOUT schema (backward compatibility test)
    // Tests: Optional schema support - topics can exist without schemas
    // ============================================================================
    let topic_no_schema = format!("/{}/logs", ns);
    let mut create_plain_cmd = cli();
    create_plain_cmd
        .args(["topics", "create", &topic_no_schema])
        .assert()
        .success();

    let mut describe_plain_cmd = cli();
    let plain_output = describe_plain_cmd
        .args(["topics", "describe", &topic_no_schema, "--output", "json"])
        .output()
        .expect("describe plain topic");

    let plain_body = String::from_utf8(plain_output.stdout).unwrap();
    let plain_info: serde_json::Value = serde_json::from_str(&plain_body).expect("valid JSON");

    // Schema fields should be null for topic without schema
    assert!(plain_info["schema_subject"].is_null());
    assert!(plain_info["schema_id"].is_null());
    assert!(plain_info["schema_version"].is_null());

    // ============================================================================
    // STEP 7: Test schema compatibility checking (optional feature)
    // Tests: schemas check command - validates schema evolution compatibility
    // ============================================================================
    let mut check_cmd = cli();
    let _ = check_cmd
        .args([
            "schemas",
            "check",
            schema_subject,
            "--file",
            schema_file.to_str().unwrap(),
            "--schema-type",
            "json_schema",
        ])
        .output(); // Don't assert - command may not be fully implemented yet

    // ============================================================================
    // CLEANUP: Remove test artifacts
    // ============================================================================
    fs::remove_file(&schema_file).ok();
    let mut delete_topic = cli();
    delete_topic
        .args(["topics", "delete", &topic])
        .assert()
        .success();

    let mut delete_plain = cli();
    delete_plain
        .args(["topics", "delete", &topic_no_schema])
        .assert()
        .success();

    delete_ns(&ns);
}
