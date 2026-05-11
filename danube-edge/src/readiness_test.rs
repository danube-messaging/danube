use super::*;

#[tokio::test]
async fn topic_not_ready_by_default() {
    let readiness = TopicReadiness::new();
    readiness.track_topic("/edge1/test").await;
    assert!(!readiness.is_ready("/edge1/test").await);
}

#[tokio::test]
async fn unknown_topic_is_not_ready() {
    let readiness = TopicReadiness::new();
    assert!(!readiness.is_ready("/edge1/unknown").await);
}

#[tokio::test]
async fn raw_bytes_topic_ready_after_local_and_cluster() {
    let readiness = TopicReadiness::new();
    readiness.track_topic("/edge1/raw").await;

    readiness.mark_local_provisioned("/edge1/raw").await;
    assert!(!readiness.is_ready("/edge1/raw").await);

    readiness.mark_cluster_registered("/edge1/raw").await;
    assert!(!readiness.is_ready("/edge1/raw").await);

    // Raw bytes topic: schema_resolved with None
    readiness
        .mark_schema_resolved("/edge1/raw", None, false)
        .await;
    assert!(readiness.is_ready("/edge1/raw").await);
    assert!(readiness.get_schema_info("/edge1/raw").await.is_none());
}

#[tokio::test]
async fn schema_topic_ready_after_all_three() {
    let readiness = TopicReadiness::new();
    readiness.track_topic("/edge1/telemetry").await;

    readiness.mark_local_provisioned("/edge1/telemetry").await;
    readiness.mark_cluster_registered("/edge1/telemetry").await;

    // Not ready yet — schema not resolved
    assert!(!readiness.is_ready("/edge1/telemetry").await);

    let schema = CachedSchema {
        subject: "telemetry-events".into(),
        schema_id: 42,
        schema_version: 1,
        schema_type: "json_schema".into(),
        fingerprint: "abc123".into(),
        schema_definition: b"{}".to_vec(),
    };
    readiness
        .mark_schema_resolved("/edge1/telemetry", Some(schema), false)
        .await;

    assert!(readiness.is_ready("/edge1/telemetry").await);
    let info = readiness.get_schema_info("/edge1/telemetry").await.unwrap();
    assert_eq!(info.schema_id, 42);
    assert_eq!(info.schema_version, 1);
}

#[tokio::test]
async fn schema_unresolved_makes_topic_not_ready() {
    let readiness = TopicReadiness::new();
    readiness.track_topic("/edge1/t").await;
    readiness.mark_local_provisioned("/edge1/t").await;
    readiness.mark_cluster_registered("/edge1/t").await;
    readiness
        .mark_schema_resolved(
            "/edge1/t",
            Some(CachedSchema {
                subject: "s".into(),
                schema_id: 1,
                schema_version: 1,
                schema_type: "json_schema".into(),
                fingerprint: "x".into(),
                schema_definition: vec![],
            }),
            false,
        )
        .await;
    assert!(readiness.is_ready("/edge1/t").await);

    // Schema removed on cluster
    readiness.mark_schema_unresolved("/edge1/t").await;
    assert!(!readiness.is_ready("/edge1/t").await);
}

#[tokio::test]
async fn not_ready_topics_returns_incomplete() {
    let readiness = TopicReadiness::new();
    readiness.track_topic("/edge1/ready").await;
    readiness.track_topic("/edge1/notready").await;

    // Make "ready" fully ready
    readiness.mark_local_provisioned("/edge1/ready").await;
    readiness.mark_cluster_registered("/edge1/ready").await;
    readiness
        .mark_schema_resolved("/edge1/ready", None, false)
        .await;

    // "notready" only has local provisioned
    readiness.mark_local_provisioned("/edge1/notready").await;

    let not_ready = readiness.not_ready_topics().await;
    assert_eq!(not_ready, vec!["/edge1/notready"]);
}

// --- Heartbeat simulation tests ---
// These simulate what run_heartbeat_loop does when it receives
// change events from the cluster.

fn make_schema(subject: &str, version: u32, fingerprint: &str) -> CachedSchema {
    CachedSchema {
        subject: subject.into(),
        schema_id: 42,
        schema_version: version,
        schema_type: "json_schema".into(),
        fingerprint: fingerprint.into(),
        schema_definition: b"{}".to_vec(),
    }
}

#[tokio::test]
async fn heartbeat_schema_update_keeps_topic_ready() {
    // Simulates: admin updates schema → heartbeat detects → edge updates cache
    let readiness = TopicReadiness::new();
    readiness.track_topic("/edge1/telemetry").await;
    readiness.mark_local_provisioned("/edge1/telemetry").await;
    readiness.mark_cluster_registered("/edge1/telemetry").await;
    readiness
        .mark_schema_resolved(
            "/edge1/telemetry",
            Some(make_schema("events", 1, "fp-v1")),
            false,
        )
        .await;
    assert!(readiness.is_ready("/edge1/telemetry").await);

    // Heartbeat: schema updated (new version, new fingerprint)
    readiness
        .update_schema("/edge1/telemetry", make_schema("events", 2, "fp-v2"))
        .await;

    // Topic should still be ready with updated schema
    assert!(readiness.is_ready("/edge1/telemetry").await);
    let info = readiness.get_schema_info("/edge1/telemetry").await.unwrap();
    assert_eq!(info.schema_version, 2);
    assert_eq!(info.fingerprint, "fp-v2");
}

#[tokio::test]
async fn fingerprint_tracks_schema_changes() {
    // Simulates: heartbeat uses fingerprint to detect schema content changes
    let readiness = TopicReadiness::new();
    readiness.track_topic("/edge1/t").await;
    readiness.mark_local_provisioned("/edge1/t").await;
    readiness.mark_cluster_registered("/edge1/t").await;
    readiness
        .mark_schema_resolved(
            "/edge1/t",
            Some(make_schema("events", 1, "fingerprint-aaa")),
            false,
        )
        .await;

    // Check fingerprint (used by heartbeat for change detection)
    assert_eq!(
        readiness.get_fingerprint("/edge1/t").await,
        Some("fingerprint-aaa".to_string())
    );

    // Update schema → fingerprint changes
    readiness
        .update_schema("/edge1/t", make_schema("events", 2, "fingerprint-bbb"))
        .await;
    assert_eq!(
        readiness.get_fingerprint("/edge1/t").await,
        Some("fingerprint-bbb".to_string())
    );
}

#[tokio::test]
async fn full_bootstrap_to_heartbeat_lifecycle() {
    // Simulates the complete edge lifecycle:
    // bootstrap → heartbeat (no change) → schema update → schema remove → recovery
    let readiness = TopicReadiness::new();

    // Phase 1: Bootstrap — track 2 topics (1 with schema, 1 raw)
    readiness.track_topic("/edge1/telemetry").await;
    readiness.track_topic("/edge1/raw").await;

    // Phase 2: Local provisioning
    readiness.mark_local_provisioned("/edge1/telemetry").await;
    readiness.mark_local_provisioned("/edge1/raw").await;

    // Phase 3: Cluster registration
    readiness.mark_cluster_registered("/edge1/telemetry").await;
    readiness.mark_cluster_registered("/edge1/raw").await;

    // Phase 4: Schema resolution
    readiness
        .mark_schema_resolved(
            "/edge1/telemetry",
            Some(make_schema("events", 1, "fp1")),
            false,
        )
        .await;
    readiness
        .mark_schema_resolved("/edge1/raw", None, false)
        .await;

    // All ready
    assert!(readiness.is_ready("/edge1/telemetry").await);
    assert!(readiness.is_ready("/edge1/raw").await);
    assert!(readiness.not_ready_topics().await.is_empty());

    // Heartbeat 1: no changes (fast path) — state unchanged
    assert!(readiness.is_ready("/edge1/telemetry").await);
    assert!(readiness.is_ready("/edge1/raw").await);

    // Heartbeat 2: schema updated for telemetry
    readiness
        .update_schema("/edge1/telemetry", make_schema("events", 2, "fp2"))
        .await;
    assert!(readiness.is_ready("/edge1/telemetry").await);
    assert!(readiness.is_ready("/edge1/raw").await);

    // Heartbeat 3: schema removed for telemetry
    readiness.mark_schema_unresolved("/edge1/telemetry").await;
    assert!(!readiness.is_ready("/edge1/telemetry").await);
    assert!(readiness.is_ready("/edge1/raw").await); // raw unaffected

    // Heartbeat 4: schema re-added
    readiness
        .mark_schema_resolved(
            "/edge1/telemetry",
            Some(make_schema("events", 3, "fp3")),
            false,
        )
        .await;
    assert!(readiness.is_ready("/edge1/telemetry").await);
    assert!(readiness.is_ready("/edge1/raw").await);
    assert!(readiness.not_ready_topics().await.is_empty());
}

// --- Payload validation tests ---

/// Helper: creates a CachedSchema with a real JSON Schema definition.
fn make_json_schema() -> CachedSchema {
    let json_schema = r#"{"type":"object","properties":{"temperature":{"type":"number"}},"required":["temperature"]}"#;
    CachedSchema {
        subject: "telemetry-events".into(),
        schema_id: 10,
        schema_version: 1,
        schema_type: "json_schema".into(),
        fingerprint: "fp-json".into(),
        schema_definition: json_schema.as_bytes().to_vec(),
    }
}

#[tokio::test]
async fn enforce_rejects_invalid_json_payload() {
    let readiness = TopicReadiness::new();
    readiness.track_topic("/edge1/telemetry").await;
    readiness.mark_local_provisioned("/edge1/telemetry").await;
    readiness.mark_cluster_registered("/edge1/telemetry").await;
    readiness
        .mark_schema_resolved(
            "/edge1/telemetry",
            Some(make_json_schema()),
            true, // enforce
        )
        .await;

    // Invalid: missing required "temperature" field
    let result = readiness
        .validate_payload("/edge1/telemetry", br#"{"humidity": 55}"#)
        .await;
    assert!(
        result.is_err(),
        "should reject payload missing required field"
    );

    // Invalid: not valid JSON at all
    let result = readiness
        .validate_payload("/edge1/telemetry", b"not json")
        .await;
    assert!(result.is_err(), "should reject non-JSON payload");
}

#[tokio::test]
async fn enforce_accepts_valid_json_payload() {
    let readiness = TopicReadiness::new();
    readiness.track_topic("/edge1/telemetry").await;
    readiness.mark_local_provisioned("/edge1/telemetry").await;
    readiness.mark_cluster_registered("/edge1/telemetry").await;
    readiness
        .mark_schema_resolved(
            "/edge1/telemetry",
            Some(make_json_schema()),
            true, // enforce
        )
        .await;

    // Valid: has required "temperature" field
    let result = readiness
        .validate_payload("/edge1/telemetry", br#"{"temperature": 22.5}"#)
        .await;
    assert!(result.is_ok(), "should accept valid payload");

    // Valid: extra fields are fine
    let result = readiness
        .validate_payload(
            "/edge1/telemetry",
            br#"{"temperature": 18, "humidity": 60}"#,
        )
        .await;
    assert!(result.is_ok(), "should accept payload with extra fields");
}

#[tokio::test]
async fn no_enforce_skips_validation() {
    let readiness = TopicReadiness::new();
    readiness.track_topic("/edge1/telemetry").await;
    readiness.mark_local_provisioned("/edge1/telemetry").await;
    readiness.mark_cluster_registered("/edge1/telemetry").await;
    readiness
        .mark_schema_resolved(
            "/edge1/telemetry",
            Some(make_json_schema()),
            false, // no enforcement
        )
        .await;

    // Invalid payload passes because enforce=false
    let result = readiness
        .validate_payload("/edge1/telemetry", b"totally invalid garbage")
        .await;
    assert!(result.is_ok(), "should skip validation when enforce=false");
}

#[tokio::test]
async fn validate_payload_no_schema_always_passes() {
    let readiness = TopicReadiness::new();
    readiness.track_topic("/edge1/raw").await;
    readiness.mark_local_provisioned("/edge1/raw").await;
    readiness.mark_cluster_registered("/edge1/raw").await;
    readiness
        .mark_schema_resolved("/edge1/raw", None, false)
        .await;

    // Any payload passes for raw-bytes topics
    let result = readiness
        .validate_payload("/edge1/raw", b"anything goes")
        .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn enforce_validator_recompiles_on_schema_update() {
    let readiness = TopicReadiness::new();
    readiness.track_topic("/edge1/t").await;
    readiness.mark_local_provisioned("/edge1/t").await;
    readiness.mark_cluster_registered("/edge1/t").await;

    // Start with schema requiring "temperature"
    readiness
        .mark_schema_resolved("/edge1/t", Some(make_json_schema()), true)
        .await;

    // payload with only "name" fails
    assert!(readiness
        .validate_payload("/edge1/t", br#"{"name": "sensor1"}"#)
        .await
        .is_err());

    // Update schema to require "name" instead
    let new_schema_def =
        r#"{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}"#;
    let new_schema = CachedSchema {
        subject: "telemetry-events".into(),
        schema_id: 10,
        schema_version: 2,
        schema_type: "json_schema".into(),
        fingerprint: "fp-v2".into(),
        schema_definition: new_schema_def.as_bytes().to_vec(),
    };
    readiness.update_schema("/edge1/t", new_schema).await;

    // Now payload with "name" should pass
    assert!(readiness
        .validate_payload("/edge1/t", br#"{"name": "sensor1"}"#)
        .await
        .is_ok());
    // And "temperature" alone should fail
    assert!(readiness
        .validate_payload("/edge1/t", br#"{"temperature": 22}"#)
        .await
        .is_err());
}
