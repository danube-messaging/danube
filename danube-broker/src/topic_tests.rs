use std::collections::HashMap;

use danube_core::message::{MessageID, StreamMessage};
use danube_core::storage::StartPosition;
use danube_persistent_storage::wal::{Wal, WalConfig};
use danube_persistent_storage::WalStorage;
use futures::StreamExt;

use crate::policies::Policies;
use crate::subscription::SubscriptionOptions;
use crate::topic::Topic;
use crate::topic::TopicStore;
use anyhow::Result as AnyResult;
use danube_core::dispatch_strategy::ConfigDispatchStrategy;
use serde_json::{Number, Value};

fn mk_policies(entries: &[(&str, u32)]) -> Policies {
    let mut map: std::collections::HashMap<String, Value> = std::collections::HashMap::new();
    // Populate all required fields with defaults (0 means unlimited; message size default 10 MiB)
    map.insert(
        "max_producers_per_topic".to_string(),
        Value::Number(Number::from(0)),
    );
    map.insert(
        "max_subscriptions_per_topic".to_string(),
        Value::Number(Number::from(0)),
    );
    map.insert(
        "max_consumers_per_topic".to_string(),
        Value::Number(Number::from(0)),
    );
    map.insert(
        "max_consumers_per_subscription".to_string(),
        Value::Number(Number::from(0)),
    );
    map.insert(
        "max_publish_rate".to_string(),
        Value::Number(Number::from(0)),
    );
    map.insert(
        "max_dispatch_rate".to_string(),
        Value::Number(Number::from(0)),
    );
    map.insert(
        "max_subscription_dispatch_rate".to_string(),
        Value::Number(Number::from(0)),
    );
    map.insert(
        "max_message_size".to_string(),
        Value::Number(Number::from(10485760)),
    );
    // Override with provided entries
    for (k, v) in entries {
        map.insert((*k).to_string(), Value::Number(Number::from(*v as u64)));
    }
    Policies::from_hashmap(map).expect("valid policies map")
}

fn mk_topic(name: &str) -> Topic {
    Topic::new(name, ConfigDispatchStrategy::NonReliable, None, None)
}

fn sub_opts(sub: &str, consumer: &str, sub_type: i32) -> SubscriptionOptions {
    SubscriptionOptions {
        subscription_name: sub.to_string(),
        subscription_type: sub_type,
        consumer_id: None,
        consumer_name: consumer.to_string(),
    }
}

/// What this test validates
///
/// - Scenario: topic with `max_producers_per_topic = 2`.
/// - Expectation: creating two producers succeeds; the third creation fails with a policy error.
///
/// Why this matters
/// - Guards against producer fan-in over a single topic exhausting resources.
#[tokio::test]
async fn policy_limit_max_producers_per_topic() -> AnyResult<()> {
    let mut topic = mk_topic("/default/policy_producers");
    let pol = mk_policies(&[("max_producers_per_topic", 2)]);
    topic.policies_update(pol)?;

    let _ = topic.create_producer(1, "p1", 0).await?;
    let _ = topic.create_producer(2, "p2", 0).await?;
    let err = topic.create_producer(3, "p3", 0).await.unwrap_err();
    assert!(err.to_string().contains("Producer limit"));
    Ok(())
}

/// What this test validates
///
/// - Scenario: topic with `max_subscriptions_per_topic = 1`.
/// - Expectation: first subscription succeeds; second subscription creation is rejected.
///
/// Why this matters
/// - Caps the number of independent consumer groups on a topic.
#[tokio::test]
async fn policy_limit_max_subscriptions_per_topic() -> AnyResult<()> {
    let mut topic = mk_topic("/default/policy_subs");
    let pol = mk_policies(&[("max_subscriptions_per_topic", 1)]);
    topic.policies_update(pol)?;

    let _ = topic
        .subscribe("/default/policy_subs", sub_opts("s1", "c1", 1))
        .await?;
    let err = topic
        .subscribe("/default/policy_subs", sub_opts("s2", "c2", 1))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("Subscription limit"));
    Ok(())
}

/// What this test validates
///
/// - Scenario: topic with `max_consumers_per_subscription = 1`.
/// - Expectation: first consumer on a subscription succeeds; the second is rejected.
///
/// Why this matters
/// - Prevents accidental fan-out on a subscription intended to be limited (e.g., single active consumer).
#[tokio::test]
async fn policy_limit_max_consumers_per_subscription() -> AnyResult<()> {
    let mut topic = mk_topic("/default/policy_cons_per_sub");
    let pol = mk_policies(&[("max_consumers_per_subscription", 1)]);
    topic.policies_update(pol)?;

    let _ = topic
        .subscribe("/default/policy_cons_per_sub", sub_opts("s", "c1", 1))
        .await?;
    let err = topic
        .subscribe("/default/policy_cons_per_sub", sub_opts("s", "c2", 1))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("Consumer limit per subscription"));
    Ok(())
}

/// What this test validates
///
/// - Scenario: topic with `max_consumers_per_topic = 2` total across subscriptions.
/// - Expectation: two consumers across any subs succeed; the third consumer is rejected.
///
/// Why this matters
/// - Protects topic-level dispatch and state from excessive concurrent consumers.
#[tokio::test]
async fn policy_limit_max_consumers_per_topic() -> AnyResult<()> {
    let mut topic = mk_topic("/default/policy_cons_per_topic");
    let pol = mk_policies(&[("max_consumers_per_topic", 2)]);
    topic.policies_update(pol)?;

    let _ = topic
        .subscribe("/default/policy_cons_per_topic", sub_opts("s1", "c1", 1))
        .await?;
    let _ = topic
        .subscribe("/default/policy_cons_per_topic", sub_opts("s2", "c2", 1))
        .await?;
    let err = topic
        .subscribe("/default/policy_cons_per_topic", sub_opts("s3", "c3", 1))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("Consumer limit per topic"));
    Ok(())
}

/// What this test validates
///
/// - Scenario: topic with `max_message_size = 8` bytes.
/// - Expectation: publish of a 9-byte payload is rejected before dispatch/persist.
///
/// Why this matters
/// - Prevents oversize messages from consuming bandwidth/storage and violating limits.
#[tokio::test]
async fn policy_limit_max_message_size() -> AnyResult<()> {
    use danube_core::message::MessageID;

    let mut topic = mk_topic("/default/policy_msg_size");
    let pol = mk_policies(&[("max_message_size", 8)]); // 8 bytes
    topic.policies_update(pol)?;

    // Need a producer attached for publish
    let _ = topic.create_producer(42, "p", 0).await?;

    // Build a message exceeding size
    let msg = StreamMessage {
        request_id: 1,
        msg_id: MessageID {
            producer_id: 42,
            topic_name: "/default/policy_msg_size".to_string(),
            broker_addr: "127.0.0.1:0".to_string(),
            topic_offset: 0,
        },
        payload: b"too-large".to_vec(), // 9 bytes
        publish_time: 0,
        producer_name: "p".to_string(),
        subscription_name: None,
        attributes: HashMap::new(),
    };

    let err = topic.publish_message_async(msg).await.unwrap_err();
    assert!(err.to_string().contains("Message size"));
    Ok(())
}

fn make_msg(i: u64, topic: &str) -> StreamMessage {
    StreamMessage {
        request_id: i,
        msg_id: MessageID {
            producer_id: 1,
            topic_name: topic.to_string(),
            broker_addr: "127.0.0.1:8080".to_string(),
            topic_offset: i,
        },
        payload: format!("wal-hello-{}", i).into_bytes(),
        publish_time: 0,
        producer_name: "producer-wal".to_string(),
        subscription_name: None,
        attributes: HashMap::new(),
    }
}

/// What this test validates
///
/// - Scenario: append three messages and read from offset 1.
/// - Expectation: reader yields only messages at offsets 1 and 2.
///
/// Why this matters
/// - Ensures TopicStore correctly addresses WAL by absolute offsets.
#[tokio::test]
async fn topic_store_wal_store_and_read_from_offset() {
    // Temp WAL (file-backed in temp dir)
    let wal = Wal::with_config(WalConfig::default())
        .await
        .expect("create wal");
    let wal_storage = WalStorage::from_wal(wal);

    let topic = "/default/topic_store_offset";
    let ts = TopicStore::new(topic.to_string(), wal_storage);

    // Store messages offsets 0..2
    ts.store_message(make_msg(0, topic)).await.unwrap();
    ts.store_message(make_msg(1, topic)).await.unwrap();
    ts.store_message(make_msg(2, topic)).await.unwrap();

    // Read from offset 1, expect messages 1 and 2
    let mut stream = ts
        .create_reader(StartPosition::Offset(1))
        .await
        .expect("reader");

    let m1 = stream.next().await.expect("msg1").expect("ok");
    let m2 = stream.next().await.expect("msg2").expect("ok");

    assert_eq!(m1.payload, b"wal-hello-1");
    assert_eq!(m2.payload, b"wal-hello-2");
}

/// What this test validates
///
/// - Scenario: start a reader at `Latest`, then append two messages.
/// - Expectation: reader yields only messages appended after the reader was created.
///
/// Why this matters
/// - Confirms tailing semantics required by subscribers joining an active topic.
#[tokio::test]
async fn topic_store_wal_latest_tailing() {
    // Temp WAL (file-backed in temp dir)
    let wal = Wal::with_config(WalConfig::default())
        .await
        .expect("create wal");
    let wal_storage = WalStorage::from_wal(wal);

    let topic = "/default/topic_store_latest";
    let ts = TopicStore::new(topic.to_string(), wal_storage);

    // Tail from Latest; append afterwards
    let mut stream = ts
        .create_reader(StartPosition::Latest)
        .await
        .expect("reader");

    // Append after subscribing
    ts.store_message(make_msg(10, topic)).await.unwrap();
    ts.store_message(make_msg(11, topic)).await.unwrap();

    let m1 = stream.next().await.expect("msg1").expect("ok");
    let m2 = stream.next().await.expect("msg2").expect("ok");

    assert_eq!(m1.payload, b"wal-hello-10");
    assert_eq!(m2.payload, b"wal-hello-11");
}
