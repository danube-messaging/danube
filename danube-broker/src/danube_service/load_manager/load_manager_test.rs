use super::*;
use danube_metadata_store::MemoryStore;
use serde_json::Value;

/// Test: Broker Failover Reallocation - Metadata Preservation
///
/// Purpose: Validates that when a broker fails, the failover reallocation process
/// correctly handles broker assignments while preserving all durable metadata.
#[tokio::test]
async fn test_delete_topic_allocation_preserves_metadata() -> Result<()> {
    // Setup in-memory metadata store
    let memory_store = MemoryStore::new().await?;
    let meta_store = MetadataStorage::InMemory(memory_store);
    // Use the same instance for both test and LoadManager to avoid clone issues
    let mut load_manager = LoadManager::new(1, meta_store.clone());

    let dead_broker_id = 999u64;
    let namespace = "test-ns";
    let topic = "test-topic";

    // Create broker assignment and metadata
    let broker_assignment_path = format!(
        "/cluster/brokers/{}/{}/{}",
        dead_broker_id, namespace, topic
    );
    let topic_metadata_path = format!("/topics/{}/{}/policy", namespace, topic);
    let unassigned_path = format!("/cluster/unassigned/{}/{}", namespace, topic);

    meta_store
        .put(&broker_assignment_path, Value::Null, MetaOptions::None)
        .await?;
    let topic_metadata = serde_json::json!({"retention": "7d", "dispatch": "reliable"});
    meta_store
        .put(&topic_metadata_path, topic_metadata.clone(), MetaOptions::None)
        .await?;

    // Verify initial state
    assert!(meta_store
        .get(&broker_assignment_path, MetaOptions::None)
        .await?
        .is_some());
    assert!(meta_store
        .get(&topic_metadata_path, MetaOptions::None)
        .await?
        .is_some());
    assert!(meta_store
        .get(&unassigned_path, MetaOptions::None)
        .await?
        .is_none());

    // Execute Phase 1 failover reallocation
    load_manager.delete_topic_allocation(dead_broker_id).await?;

    // Verification 1: Broker assignment should be deleted
    assert!(
        meta_store
            .get(&broker_assignment_path, MetaOptions::None)
            .await?
            .is_none(),
        "Broker assignment should be deleted after failover"
    );

    // Verification 2: Unassigned entry should be created
    assert!(
        meta_store
            .get(&unassigned_path, MetaOptions::None)
            .await?
            .is_some(),
        "Unassigned entry should be created for topic reassignment"
    );

    // Verification 3: Topic metadata should be preserved (CRITICAL)
    let preserved_topic_metadata = meta_store
        .get(&topic_metadata_path, MetaOptions::None)
        .await?;
    assert!(
        preserved_topic_metadata.is_some(),
        "Topic metadata must be preserved during broker failover"
    );
    assert_eq!(
        preserved_topic_metadata.unwrap(),
        topic_metadata,
        "Topic metadata should remain unchanged after failover"
    );

    Ok(())
}

/// Test: Multiple Topics Broker Failover Reallocation
///
/// Purpose: Validates that broker failover reallocation works correctly when
/// a single failed broker was managing multiple topics across different namespaces.
#[tokio::test]
async fn test_multiple_topics_failover_reallocation() -> Result<()> {
    let memory_store = MemoryStore::new().await?;
    let meta_store = MetadataStorage::InMemory(memory_store);
    let mut load_manager = LoadManager::new(1, meta_store.clone());

    let dead_broker_id = 888u64;
    let topics = vec![("ns1", "topic1"), ("ns1", "topic2"), ("ns2", "topic3")];

    // Setup initial assignments and metadata
    for (ns, topic) in &topics {
        let broker_assignment_path =
            format!("/cluster/brokers/{}/{}/{}", dead_broker_id, ns, topic);
        let topic_metadata_path = format!("/topics/{}/{}/policy", ns, topic);

        meta_store
            .put(&broker_assignment_path, Value::Null, MetaOptions::None)
            .await?;
        meta_store
            .put(
                &topic_metadata_path,
                serde_json::json!({"test": "data"}),
                MetaOptions::None,
            )
            .await?;
    }

    // Execute the failover reallocation process for all topics
    load_manager.delete_topic_allocation(dead_broker_id).await?;

    // Verification: Validate correct handling for each topic individually
    for (ns, topic) in &topics {
        let broker_assignment_path =
            format!("/cluster/brokers/{}/{}/{}", dead_broker_id, ns, topic);
        let unassigned_path = format!("/cluster/unassigned/{}/{}", ns, topic);
        let topic_metadata_path = format!("/topics/{}/{}/policy", ns, topic);

        // Check 1: Broker assignment should be deleted for this topic
        assert!(
            meta_store
                .get(&broker_assignment_path, MetaOptions::None)
                .await?
                .is_none(),
            "Broker assignment for {}/{} should be deleted",
            ns,
            topic
        );

        // Check 2: Unassigned entry should be created for this topic
        assert!(
            meta_store
                .get(&unassigned_path, MetaOptions::None)
                .await?
                .is_some(),
            "Unassigned entry for {}/{} should be created",
            ns,
            topic
        );

        // Check 3: Topic metadata should be preserved for this topic
        assert!(
            meta_store
                .get(&topic_metadata_path, MetaOptions::None)
                .await?
                .is_some(),
            "Topic metadata for {}/{} should be preserved",
            ns,
            topic
        );
    }

    Ok(())
}
