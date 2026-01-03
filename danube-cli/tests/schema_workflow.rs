mod common;
use assert_cmd::assert::OutputAssertExt;
use common::*;
use predicates::prelude::*;
use rand::Rng;
use std::fs;
use std::process::Stdio;
use std::time::Duration;

/// Tests the complete schema registry workflow from registration to consumption
///
/// **What we're testing:**
/// - Schema can be registered with the schema registry
/// - Schema details can be retrieved (with JSON output validation)
/// - Schema versions can be listed
/// - Producer can send messages with schema validation
/// - Consumer automatically detects and validates against the schema
/// - JSON output format is correctly structured
///
/// **Why it's important:**
/// Schema registry is critical for data governance and contract enforcement.
/// This test validates the entire lifecycle: register → produce → consume,
/// ensuring that schemas are properly stored, referenced, and enforced across
/// the message pipeline. This prevents data quality issues in production.
#[test]
fn schema_register_and_produce_consume() {
    let subject = unique_subject();
    let topic = format!("/default/{}", subject);
    let addr = service_addr();

    // Create a schema file
    let schema_content = r#"{
  "type": "object",
  "properties": {
    "user_id": {"type": "string"},
    "action": {"type": "string"},
    "timestamp": {"type": "integer"}
  },
  "required": ["user_id", "action"]
}"#;

    let temp_dir = std::env::temp_dir();
    let schema_file = temp_dir.join(format!("test_schema_{}.json", rand::rng().random::<u32>()));
    fs::write(&schema_file, schema_content).expect("write schema file");

    // Step 1: Register schema
    let mut register = cli();
    register
        .args([
            "schema",
            "register",
            &subject,
            "--schema-type",
            "json_schema",
            "--file",
            schema_file.to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Schema registered successfully"));

    // Step 2: Get schema details with JSON output
    let mut get = cli();
    let output = get
        .args(["schema", "get", &subject, "--output", "json"])
        .output()
        .expect("get schema");

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();

    // Parse JSON and validate
    let json: serde_json::Value = serde_json::from_str(&stdout).expect("valid JSON");
    assert_eq!(json["subject"], subject);
    assert_eq!(json["schema_type"], "json_schema");
    assert!(json["schema_id"].as_u64().unwrap() > 0);

    // Step 3: List versions
    let mut versions = cli();
    versions
        .args(["schema", "versions", &subject])
        .assert()
        .success()
        .stdout(predicate::str::contains("v1"));

    // Step 4: Produce 1 message to create the topic
    let message = r#"{"user_id":"user_123","action":"login","timestamp":1234567890}"#;
    let mut produce_init = cli();
    produce_init
        .args([
            "produce",
            "-s",
            &addr,
            "-t",
            &topic,
            "--schema-subject",
            &subject,
            "-m",
            message,
            "-c",
            "1",
        ])
        .assert()
        .success();

    // Step 5: Start consumer with schema validation
    let mut consume = cli();
    let mut child = consume
        .args([
            "consume",
            "-s",
            &addr,
            "-t",
            &topic,
            "-m",
            "schema-test-sub",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn consume with schema");

    // Give consumer time to connect
    std::thread::sleep(Duration::from_millis(500));

    // Step 6: Produce more messages while consumer is running
    let mut produce = cli();
    produce
        .args([
            "produce",
            "-s",
            &addr,
            "-t",
            &topic,
            "--schema-subject",
            &subject,
            "-m",
            message,
            "-c",
            "3",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Success: 3"));

    std::thread::sleep(Duration::from_secs(2));
    let _ = child.kill();
    let output = child.wait_with_output().expect("wait for output");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = format!("{}{}", stdout, stderr);

    assert!(
        combined.contains("user_123"),
        "Expected 'user_123' not found. stdout: {}, stderr: {}",
        stdout,
        stderr
    );
    assert!(
        combined.contains("login"),
        "Expected 'login' not found. stdout: {}, stderr: {}",
        stdout,
        stderr
    );

    // Cleanup
    fs::remove_file(schema_file).ok();
}

/// Tests schema evolution and backward compatibility checking
///
/// **What we're testing:**
/// - Schema v1 can be successfully registered
/// - Schema v2 (with additional optional field) is backward compatible
/// - Compatibility check command validates schema evolution rules
/// - Compatible schema v2 can be registered as a new version
/// - Version history is maintained (versions 1 and 2 both exist)
///
/// **Why it's important:**
/// Schema evolution is essential for maintaining systems over time without
/// breaking existing consumers. This test validates that the compatibility
/// checking mechanism works correctly, preventing breaking changes from being
/// deployed. Backward compatibility ensures old consumers can still read new
/// messages, which is critical for zero-downtime upgrades.
#[test]
fn schema_compatibility_check() {
    let subject = unique_subject();
    let _addr = service_addr();

    // Create initial schema
    let schema_v1 = r#"{
  "type": "object",
  "properties": {
    "user_id": {"type": "string"}
  },
  "required": ["user_id"]
}"#;

    let temp_dir = std::env::temp_dir();
    let schema_file_v1 = temp_dir.join(format!("schema_v1_{}.json", rand::rng().random::<u32>()));
    fs::write(&schema_file_v1, schema_v1).expect("write schema v1");

    // Register v1
    let mut register = cli();
    register
        .args([
            "schema",
            "register",
            &subject,
            "--schema-type",
            "json_schema",
            "--file",
            schema_file_v1.to_str().unwrap(),
        ])
        .assert()
        .success();

    // Create v2 schema (backward compatible - adds optional field)
    let schema_v2 = r#"{
  "type": "object",
  "properties": {
    "user_id": {"type": "string"},
    "email": {"type": "string"}
  },
  "required": ["user_id"]
}"#;

    let schema_file_v2 = temp_dir.join(format!("schema_v2_{}.json", rand::rng().random::<u32>()));
    fs::write(&schema_file_v2, schema_v2).expect("write schema v2");

    // Check compatibility
    let mut check = cli();
    check
        .args([
            "schema",
            "check",
            &subject,
            "--schema-type",
            "json_schema",
            "--file",
            schema_file_v2.to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("COMPATIBLE"));

    // Register v2 if compatible
    let mut register_v2 = cli();
    register_v2
        .args([
            "schema",
            "register",
            &subject,
            "--schema-type",
            "json_schema",
            "--file",
            schema_file_v2.to_str().unwrap(),
        ])
        .assert()
        .success();

    // Verify we now have 2 versions
    let mut versions = cli();
    let output = versions
        .args(["schema", "versions", &subject])
        .output()
        .expect("list versions");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("v1"));
    assert!(stdout.contains("v2"));

    // Cleanup
    fs::remove_file(schema_file_v1).ok();
    fs::remove_file(schema_file_v2).ok();
}

/// Tests automatic schema registration during message production
///
/// **What we're testing:**
/// - Producer can auto-register a schema using `--schema-file` flag
/// - Schema is automatically created in the registry if it doesn't exist
/// - Subject name is auto-generated from topic name (e.g., `/default/test` → `default-test`)
/// - Auto-registered schema can be retrieved and validated
/// - Producer successfully sends messages after auto-registration
///
/// **Why it's important:**
/// Auto-registration simplifies the developer experience by eliminating the
/// need for a separate schema registration step. This is especially useful
/// during development and testing. The test validates that the subject naming
/// convention is correct and that the schema is properly registered before
/// messages are sent, maintaining data governance even with convenience features.
#[test]
fn auto_register_schema() {
    let subject = unique_subject();
    let topic = format!("/default/{}", subject);
    let addr = service_addr();

    // Create a schema file
    let schema_content = r#"{
  "type": "object",
  "properties": {
    "event": {"type": "string"},
    "data": {"type": "string"}
  }
}"#;

    let temp_dir = std::env::temp_dir();
    let schema_file = temp_dir.join(format!("auto_schema_{}.json", rand::rng().random::<u32>()));
    fs::write(&schema_file, schema_content).expect("write schema file");

    // Produce with auto-register (using --schema-file instead of --schema-subject)
    let message = r#"{"event":"test","data":"auto-registered"}"#;
    let mut produce = cli();
    produce
        .args([
            "produce",
            "-s",
            &addr,
            "-t",
            &topic,
            "--schema-file",
            schema_file.to_str().unwrap(),
            "--schema-type",
            "json_schema",
            "-m",
            message,
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Auto-registering schema"))
        .stdout(predicate::str::contains("Schema registered with ID"));

    // Verify schema was registered by checking it exists
    let expected_subject = topic.trim_start_matches('/').replace('/', "-");
    let mut get = cli();
    get.args(["schema", "get", &expected_subject])
        .assert()
        .success()
        .stdout(predicate::str::contains(&expected_subject));

    // Cleanup
    fs::remove_file(schema_file).ok();
}
