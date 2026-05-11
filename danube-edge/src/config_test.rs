use super::*;
use std::time::Duration;

const FULL_CONFIG: &str = r##"
edge:
  edge_name: "test-edge"
  cluster_url: "http://cluster:6650"
  token: "secret"
  heartbeat_interval_ms: 15000

replicator:
  batch_size: 50
  batch_timeout_ms: 2000

mqtt:
  listener: "0.0.0.0:1884"
  topic_mappings:
    - mqtt_pattern: "device/+/telemetry"
      danube_topic: "/test-edge/telemetry"
      schema_subject: "telemetry-events"
      extract_attributes:
        device_id: "$1"
    - mqtt_pattern: "#"
      danube_topic: "/test-edge/mqtt"
  ingestion:
    batch_size: 200
    batch_timeout_ms: 750
"##;

const MINIMAL_CONFIG: &str = r#"
edge:
  edge_name: "edge-minimal"
  cluster_url: "http://localhost:6650"
"#;

#[test]
fn parse_full_config() {
    let config = EdgeConfig::from_str(FULL_CONFIG).expect("parse full config");

    assert_eq!(config.edge.edge_name, "test-edge");
    assert_eq!(config.edge.cluster_url, "http://cluster:6650");
    assert_eq!(config.edge.token, "secret");
    assert_eq!(config.edge.heartbeat_interval_ms, 15000);
    assert_eq!(
        config.edge.heartbeat_interval(),
        Duration::from_millis(15000)
    );

    assert_eq!(config.replicator.batch_size, 50);
    assert_eq!(config.replicator.batch_timeout_ms, 2000);
    assert_eq!(
        config.replicator.batch_timeout(),
        Duration::from_millis(2000)
    );

    let mqtt = config.mqtt.as_ref().expect("mqtt section present");
    assert_eq!(mqtt.listener, "0.0.0.0:1884");
    assert_eq!(mqtt.topic_mappings.len(), 2);
    assert_eq!(mqtt.topic_mappings[0].mqtt_pattern, "device/+/telemetry");
    assert_eq!(mqtt.topic_mappings[0].danube_topic, "/test-edge/telemetry");
    assert_eq!(
        mqtt.topic_mappings[0].schema_subject,
        Some("telemetry-events".to_string())
    );
    assert_eq!(
        mqtt.topic_mappings[0].extract_attributes.get("device_id"),
        Some(&"$1".to_string())
    );
    // Catch-all has no schema
    assert_eq!(mqtt.topic_mappings[1].schema_subject, None);
    assert_eq!(mqtt.ingestion.batch_size, 200);
    assert_eq!(mqtt.ingestion.batch_timeout_ms, 750);
}

#[test]
fn parse_minimal_config_uses_defaults() {
    let config = EdgeConfig::from_str(MINIMAL_CONFIG).expect("parse minimal config");

    assert_eq!(config.edge.edge_name, "edge-minimal");
    assert_eq!(config.edge.cluster_url, "http://localhost:6650");
    assert_eq!(config.edge.token, ""); // default empty
    assert_eq!(config.edge.heartbeat_interval_ms, 30_000); // default

    // Replicator uses defaults
    assert_eq!(config.replicator.batch_size, 100);
    assert_eq!(config.replicator.batch_timeout_ms, 1000);

    // No MQTT section
    assert!(config.mqtt.is_none());
}

#[test]
fn mqtt_danube_topics_deduplicates() {
    let yaml = r#"
edge:
  edge_name: "e1"
  cluster_url: "http://localhost:6650"
mqtt:
  topic_mappings:
    - mqtt_pattern: "a/+"
      danube_topic: "/e1/data"
    - mqtt_pattern: "b/+"
      danube_topic: "/e1/data"
    - mqtt_pattern: "c/+"
      danube_topic: "/e1/other"
"#;
    let config = EdgeConfig::from_str(yaml).unwrap();
    let topics = config.mqtt_danube_topics();

    assert_eq!(topics, vec!["/e1/data", "/e1/other"]);
}

#[test]
fn mqtt_danube_topics_empty_without_mqtt() {
    let config = EdgeConfig::from_str(MINIMAL_CONFIG).unwrap();
    assert!(config.mqtt_danube_topics().is_empty());
}

#[test]
fn mqtt_defaults_applied() {
    let yaml = r##"
edge:
  edge_name: "e1"
  cluster_url: "http://localhost:6650"
mqtt:
  topic_mappings:
    - mqtt_pattern: "#"
      danube_topic: "/e1/all"
"##;
    let config = EdgeConfig::from_str(yaml).unwrap();
    let mqtt = config.mqtt.unwrap();

    assert_eq!(mqtt.listener, "0.0.0.0:1883"); // default
    assert_eq!(mqtt.ingestion.batch_size, 100); // default
    assert_eq!(mqtt.ingestion.batch_timeout_ms, 500); // default
}

#[test]
fn topic_declarations_deduplicates_with_schema() {
    let yaml = r##"
edge:
  edge_name: "e1"
  cluster_url: "http://localhost:6650"
mqtt:
  topic_mappings:
    - mqtt_pattern: "a/+"
      danube_topic: "/e1/data"
      schema_subject: "data-events"
    - mqtt_pattern: "b/+"
      danube_topic: "/e1/data"
      schema_subject: "data-events"
    - mqtt_pattern: "#"
      danube_topic: "/e1/raw"
"##;
    let config = EdgeConfig::from_str(yaml).unwrap();
    let decls = config.topic_declarations();

    // Deduplicated: 2 unique topics
    assert_eq!(decls.len(), 2);
    assert_eq!(decls[0].0, "/e1/data");
    assert_eq!(decls[0].1, Some("data-events".to_string()));
    assert_eq!(decls[1].0, "/e1/raw");
    assert_eq!(decls[1].1, None);
}

#[test]
fn topic_declarations_empty_without_mqtt() {
    let config = EdgeConfig::from_str(MINIMAL_CONFIG).unwrap();
    assert!(config.topic_declarations().is_empty());
}

#[test]
fn config_from_file_loads_actual_config() {
    // Test against the real config/edge.yaml in the repo
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let config_path = format!("{}/../config/edge.yaml", manifest_dir);
    let config = EdgeConfig::from_file(&config_path).expect("load config/edge.yaml");

    assert_eq!(config.edge.edge_name, "edge1");
    assert!(!config.edge.cluster_url.is_empty());
    assert!(config.mqtt.is_some());
    // Verify schema_subject is parsed from the real config
    let mqtt = config.mqtt.as_ref().unwrap();
    assert_eq!(
        mqtt.topic_mappings[0].schema_subject,
        Some("telemetry-events".to_string())
    );
}

// --- Namespace validation tests ---

#[test]
fn validate_namespaces_all_valid() {
    let yaml = r##"
edge:
  edge_name: "edge1"
  cluster_url: "http://localhost:6650"
mqtt:
  topic_mappings:
    - mqtt_pattern: "a/+"
      danube_topic: "/edge1/data"
    - mqtt_pattern: "#"
      danube_topic: "/edge1/raw"
"##;
    let config = EdgeConfig::from_str(yaml).unwrap();
    assert!(
        config.validate_namespaces().is_empty(),
        "all topics under /edge1/ should be valid"
    );
}

#[test]
fn validate_namespaces_rejects_wrong_namespace() {
    let yaml = r##"
edge:
  edge_name: "edge1"
  cluster_url: "http://localhost:6650"
mqtt:
  topic_mappings:
    - mqtt_pattern: "a/+"
      danube_topic: "/edge2/data"
    - mqtt_pattern: "#"
      danube_topic: "/default/sneaky"
"##;
    let config = EdgeConfig::from_str(yaml).unwrap();
    let violations = config.validate_namespaces();
    assert_eq!(violations.len(), 2);
    assert!(violations.contains(&"/edge2/data".to_string()));
    assert!(violations.contains(&"/default/sneaky".to_string()));
}

#[test]
fn validate_namespaces_mixed_valid_and_invalid() {
    let yaml = r##"
edge:
  edge_name: "factory-01"
  cluster_url: "http://localhost:6650"
mqtt:
  topic_mappings:
    - mqtt_pattern: "sensors/#"
      danube_topic: "/factory-01/telemetry"
    - mqtt_pattern: "alerts/#"
      danube_topic: "/other-edge/alerts"
    - mqtt_pattern: "#"
      danube_topic: "/factory-01/raw"
"##;
    let config = EdgeConfig::from_str(yaml).unwrap();
    let violations = config.validate_namespaces();
    assert_eq!(violations, vec!["/other-edge/alerts"]);
}

#[test]
fn validate_namespaces_no_mqtt_section() {
    let config = EdgeConfig::from_str(MINIMAL_CONFIG).unwrap();
    assert!(config.validate_namespaces().is_empty());
}
