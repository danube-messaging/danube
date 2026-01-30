mod common;
use assert_cmd::assert::OutputAssertExt;
use common::*;
use rand::Rng;
use std::fs;

/// Integration test for Schema Registry admin-only operations.
///
/// # Test Flow
///
/// This test validates admin-only schema configuration operations including:
///
/// 1. **Schema Registration** - Setup: Register a schema for testing
/// 2. **Get Compatibility Mode** - Retrieve current compatibility mode for a subject
/// 3. **Set Compatibility Mode** - Change compatibility mode (admin-only)
/// 4. **Topic Schema Configuration** - Configure topic schema settings (admin-only):
///    - Assign schema subject to topic
///    - Set validation policy (none/warn/enforce)
///    - Enable/disable payload validation
/// 5. **Get Topic Schema Config** - Retrieve topic's schema configuration
/// 6. **Update Validation Policy** - Change validation policy without changing schema
/// 7. **Schema Deletion** - Delete specific schema version (admin-only)
///
/// # What We Test
///
/// - Admin-only schema configuration via CLI
/// - Compatibility mode management (get/set)
/// - Topic-level validation policy configuration
/// - Schema version deletion
/// - JSON output format for configuration commands
/// - Proper error handling for invalid operations
///
/// # Architecture Validated
///
/// ```text
/// Admin CLI → Schema Registry Service (admin operations)
///     ├─→ GetCompatibilityMode → fetch from subject metadata
///     ├─→ SetCompatibilityMode → update subject metadata (admin-only)
///     ├─→ ConfigureTopicSchema → configure topic schema settings (admin-only)
///     ├─→ UpdateTopicValidationPolicy → update validation policy (admin-only)
///     ├─→ GetTopicSchemaConfig → retrieve topic schema config
///     └─→ DeleteSchemaVersion → delete version (admin-only)
///         ↓
///     ETCD Metadata
///         ├─→ /schemas/{subject}/compatibility
///         ├─→ /schemas/{subject}/versions/{version}
///         └─→ /topics/{name}/schema_config
/// ```
#[test]
fn schema_admin_operations() {
    let ns = unique_ns();
    let topic = format!("/{}/admin-test-events", ns);
    let schema_subject = "admin-events-schema";

    // ============================================================================
    // SETUP: Create namespace and register test schema
    // ============================================================================
    create_ns(&ns);

    let schema_content = r#"{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "AdminTestEvent",
  "type": "object",
  "required": ["event_id", "action"],
  "properties": {
    "event_id": {
      "type": "string"
    },
    "action": {
      "type": "string"
    }
  }
}"#;

    let temp_dir = std::env::temp_dir();
    let schema_file = temp_dir.join(format!("admin_test_schema_{}.json", rand::rng().random::<u32>()));
    fs::write(&schema_file, schema_content).expect("write schema file");

    // Register initial schema
    let mut register_cmd = cli();
    register_cmd
        .args([
            "schemas",
            "register",
            schema_subject,
            "--schema-type",
            "json_schema",
            "--file",
            schema_file.to_str().unwrap(),
        ])
        .assert()
        .success();

    // ============================================================================
    // STEP 1: Get compatibility mode (should be default: BACKWARD)
    // Tests: schemas get-compatibility command
    // ============================================================================
    let mut get_compat_cmd = cli();
    get_compat_cmd
        .args([
            "schemas",
            "get-compatibility",
            schema_subject,
            "--output",
            "json",
        ])
        .assert()
        .success()
        .stdout(predicates::str::contains("BACKWARD"));

    // ============================================================================
    // STEP 2: Set compatibility mode to FULL (admin operation)
    // Tests: schemas set-compatibility command
    // ============================================================================
    let mut set_compat_cmd = cli();
    set_compat_cmd
        .args([
            "schemas",
            "set-compatibility",
            schema_subject,
            "--mode",
            "full",
        ])
        .assert()
        .success()
        .stdout(predicates::str::contains("Compatibility mode set"))
        .stdout(predicates::str::contains("FULL"));

    // Verify the change
    let mut verify_compat_cmd = cli();
    verify_compat_cmd
        .args([
            "schemas",
            "get-compatibility",
            schema_subject,
        ])
        .assert()
        .success()
        .stdout(predicates::str::contains("FULL"));

    // ============================================================================
    // STEP 3: Create topic and configure schema with validation policy
    // Tests: topics configure-schema command (admin-only)
    // ============================================================================
    let mut create_topic_cmd = cli();
    create_topic_cmd
        .args(["topics", "create", &topic])
        .assert()
        .success();

    let mut configure_cmd = cli();
    configure_cmd
        .args([
            "topics",
            "configure-schema",
            &topic,
            "--subject",
            schema_subject,
            "--validation-policy",
            "enforce",
            "--enable-payload-validation",
        ])
        .assert()
        .success()
        .stdout(predicates::str::contains("Schema configuration set"))
        .stdout(predicates::str::contains(schema_subject))
        .stdout(predicates::str::contains("ENFORCE"))
        .stdout(predicates::str::contains("ENABLED"));

    // ============================================================================
    // STEP 4: Get topic schema configuration
    // Tests: topics get-schema-config command
    // ============================================================================
    let mut get_config_cmd = cli();
    let config_output = get_config_cmd
        .args([
            "topics",
            "get-schema-config",
            &topic,
            "--output",
            "json",
        ])
        .output()
        .expect("get schema config");

    assert!(config_output.status.success());
    let config_body = String::from_utf8(config_output.stdout).unwrap();
    let config: serde_json::Value = serde_json::from_str(&config_body).expect("valid JSON");

    assert_eq!(config["schema_subject"].as_str().unwrap(), schema_subject);
    assert_eq!(config["validation_policy"].as_str().unwrap(), "ENFORCE");
    assert_eq!(config["enable_payload_validation"].as_bool().unwrap(), true);

    // ============================================================================
    // STEP 5: Update validation policy to WARN (without changing schema)
    // Tests: topics set-validation-policy command (admin-only)
    // ============================================================================
    let mut update_policy_cmd = cli();
    update_policy_cmd
        .args([
            "topics",
            "set-validation-policy",
            &topic,
            "--policy",
            "warn",
            "--enable-payload-validation",
        ])
        .assert()
        .success()
        .stdout(predicates::str::contains("Validation policy updated"))
        .stdout(predicates::str::contains("WARN"));

    // Verify the policy change
    let mut verify_policy_cmd = cli();
    let verify_output = verify_policy_cmd
        .args([
            "topics",
            "get-schema-config",
            &topic,
            "--output",
            "json",
        ])
        .output()
        .expect("verify policy");

    let verify_body = String::from_utf8(verify_output.stdout).unwrap();
    let verify_config: serde_json::Value = serde_json::from_str(&verify_body).expect("valid JSON");
    assert_eq!(verify_config["validation_policy"].as_str().unwrap(), "WARN");

    // ============================================================================
    // STEP 6: Test configure-schema on topic without --enable-payload-validation
    // Tests: Default behavior when flag is not provided
    // ============================================================================
    let topic2 = format!("/{}/admin-test-events-2", ns);
    let mut create_topic2_cmd = cli();
    create_topic2_cmd
        .args(["topics", "create", &topic2])
        .assert()
        .success();

    let mut configure2_cmd = cli();
    configure2_cmd
        .args([
            "topics",
            "configure-schema",
            &topic2,
            "--subject",
            schema_subject,
            "--validation-policy",
            "none",
        ])
        .assert()
        .success()
        .stdout(predicates::str::contains("DISABLED"));

    // ============================================================================
    // STEP 7: Register a second version and then delete it (admin operation)
    // Tests: schemas delete command with --confirm flag
    // ============================================================================
    let schema_v2 = r#"{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "AdminTestEvent",
  "type": "object",
  "required": ["event_id", "action"],
  "properties": {
    "event_id": {
      "type": "string"
    },
    "action": {
      "type": "string"
    },
    "metadata": {
      "type": "string"
    }
  }
}"#;

    let schema_v2_file = temp_dir.join(format!("admin_test_schema_v2_{}.json", rand::rng().random::<u32>()));
    fs::write(&schema_v2_file, schema_v2).expect("write schema v2 file");

    let mut register_v2_cmd = cli();
    register_v2_cmd
        .args([
            "schemas",
            "register",
            schema_subject,
            "--schema-type",
            "json_schema",
            "--file",
            schema_v2_file.to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicates::str::contains("Version: 2"));

    // Delete version 2
    let mut delete_cmd = cli();
    delete_cmd
        .args([
            "schemas",
            "delete",
            schema_subject,
            "--version",
            "2",
            "--confirm",
        ])
        .assert()
        .success()
        .stdout(predicates::str::contains("Deleted version 2"));

    // ============================================================================
    // STEP 8: Test error handling - delete without --confirm flag
    // Tests: Safety mechanism for dangerous operations
    // ============================================================================
    let mut delete_no_confirm_cmd = cli();
    delete_no_confirm_cmd
        .args([
            "schemas",
            "delete",
            schema_subject,
            "--version",
            "1",
        ])
        .assert()
        .failure(); // Should fail without --confirm

    // ============================================================================
    // CLEANUP: Remove test artifacts
    // ============================================================================
    fs::remove_file(&schema_file).ok();
    fs::remove_file(&schema_v2_file).ok();

    let mut delete_topic = cli();
    delete_topic
        .args(["topics", "delete", &topic])
        .assert()
        .success();

    let mut delete_topic2 = cli();
    delete_topic2
        .args(["topics", "delete", &topic2])
        .assert()
        .success();

    delete_ns(&ns);
}
