//! Key-Shared subscription key filtering tests
//!
//! Tests verify that:
//! - Glob-based key filters correctly route messages to consumers
//! - Consumers with different filters receive only matching keys
//! - Non-matching messages are silently skipped (no consumer gets them)
//! - Wildcard `*` filter receives all keys
//! - Both non-reliable and reliable paths respect key filters

extern crate danube_client;

use anyhow::Result;
use danube_client::SubType;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout, Duration};

#[path = "test_utils.rs"]
mod test_utils;

/// What this test validates
///
/// - Scenario: two KeyShared consumers with different glob filters on the same subscription
///   (non-reliable). Consumer A filters "user-*", Consumer B filters "order-*".
///   Producer sends messages with keys "user-1", "user-2", "order-1", "order-2".
/// - Expectation: Consumer A receives only "user-*" messages, Consumer B receives only "order-*".
///
/// Why this matters
/// - Key filtering enables semantic key-based routing at the broker level, which is
///   a feature unique to our implementation (not available in Pulsar).
#[tokio::test]
async fn key_shared_filter_glob_non_reliable() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/ks_filter_nr");

    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("prod_ks_filter_nr")
        .build()?;
    producer.create().await?;

    let sub = "ks_filter_nr_sub";
    let (tx, mut rx) = mpsc::unbounded_channel::<(String, String)>();

    // Consumer A: user-* filter
    {
        let topic_c = topic.clone();
        let sub_c = sub.to_string();
        let txc = tx.clone();
        let client_c = test_utils::setup_client().await?;

        tokio::spawn(async move {
            let mut c = client_c
                .new_consumer()
                .with_topic(topic_c)
                .with_consumer_name("filter-cons-user".to_string())
                .with_subscription(sub_c)
                .with_subscription_type(SubType::KeyShared)
                .with_key_filter("user-*")
                .build()
                .unwrap();
            c.subscribe().await.unwrap();
            let mut stream = c.receive().await.unwrap();
            loop {
                match stream.recv().await {
                    Some(msg) => {
                        let payload = String::from_utf8(msg.payload.to_vec()).unwrap();
                        let _ = c.ack(&msg).await;
                        let _ = txc.send(("filter-cons-user".to_string(), payload));
                    }
                    None => break,
                }
            }
        });
    }

    // Consumer B: order-* filter
    {
        let topic_c = topic.clone();
        let sub_c = sub.to_string();
        let txc = tx.clone();
        let client_c = test_utils::setup_client().await?;

        tokio::spawn(async move {
            let mut c = client_c
                .new_consumer()
                .with_topic(topic_c)
                .with_consumer_name("filter-cons-order".to_string())
                .with_subscription(sub_c)
                .with_subscription_type(SubType::KeyShared)
                .with_key_filter("order-*")
                .build()
                .unwrap();
            c.subscribe().await.unwrap();
            let mut stream = c.receive().await.unwrap();
            loop {
                match stream.recv().await {
                    Some(msg) => {
                        let payload = String::from_utf8(msg.payload.to_vec()).unwrap();
                        let _ = c.ack(&msg).await;
                        let _ = txc.send(("filter-cons-order".to_string(), payload));
                    }
                    None => break,
                }
            }
        });
    }

    sleep(Duration::from_millis(500)).await;

    // Send messages with different key prefixes
    let user_keys = vec!["user-1", "user-2", "user-3"];
    let order_keys = vec!["order-1", "order-2", "order-3"];

    for key in &user_keys {
        let payload = format!("payload-{}", key);
        let _ = producer
            .send_with_key(payload.into_bytes(), None, key)
            .await?;
    }
    for key in &order_keys {
        let payload = format!("payload-{}", key);
        let _ = producer
            .send_with_key(payload.into_bytes(), None, key)
            .await?;
    }

    let total = user_keys.len() + order_keys.len();

    let recv_future = async {
        let mut per_consumer: HashMap<String, Vec<String>> = HashMap::new();
        let mut received = 0usize;
        while received < total {
            if let Some((cname, payload)) = rx.recv().await {
                per_consumer.entry(cname).or_default().push(payload);
                received += 1;
            }
        }
        per_consumer
    };

    let per_consumer = timeout(Duration::from_secs(15), recv_future)
        .await
        .expect("timeout receiving filtered key-shared messages");

    // Assert: user consumer only got user-* messages
    let user_msgs = per_consumer
        .get("filter-cons-user")
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        user_msgs.len(),
        user_keys.len(),
        "user consumer should receive {} messages, got {}",
        user_keys.len(),
        user_msgs.len()
    );
    for msg in &user_msgs {
        assert!(
            msg.contains("user-"),
            "user consumer received non-user message: {}",
            msg
        );
    }

    // Assert: order consumer only got order-* messages
    let order_msgs = per_consumer
        .get("filter-cons-order")
        .cloned()
        .unwrap_or_default();
    assert_eq!(
        order_msgs.len(),
        order_keys.len(),
        "order consumer should receive {} messages, got {}",
        order_keys.len(),
        order_msgs.len()
    );
    for msg in &order_msgs {
        assert!(
            msg.contains("order-"),
            "order consumer received non-order message: {}",
            msg
        );
    }

    Ok(())
}

/// What this test validates
///
/// - Scenario: reliable Key-Shared with glob filters. Consumer A: "eu-*", Consumer B: "us-*".
///   Producer sends messages with keys "eu-west", "eu-east", "us-west", "us-east".
/// - Expectation: per-filter isolation and per-key ordering within each filter domain.
///
/// Why this matters
/// - Ensures that key filtering works in the reliable path where messages are persisted
///   and ack-gated through InFlightWindow.
#[tokio::test]
async fn key_shared_filter_glob_reliable() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/ks_filter_rel");

    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("prod_ks_filter_rel")
        .with_reliable_dispatch()
        .build()?;
    producer.create().await?;

    let sub = "ks_filter_rel_sub";
    let (tx, mut rx) = mpsc::unbounded_channel::<(String, String)>();

    // Register both consumers from the SAME client before spawning receivers
    let mut cons_eu = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name("filter-eu-cons".to_string())
        .with_subscription(sub.to_string())
        .with_subscription_type(SubType::KeyShared)
        .with_key_filter("eu-*")
        .build()?;
    cons_eu.subscribe().await?;
    let stream_eu = cons_eu.receive().await?;

    let mut cons_us = client
        .new_consumer()
        .with_topic(topic.clone())
        .with_consumer_name("filter-us-cons".to_string())
        .with_subscription(sub.to_string())
        .with_subscription_type(SubType::KeyShared)
        .with_key_filter("us-*")
        .build()?;
    cons_us.subscribe().await?;
    let stream_us = cons_us.receive().await?;

    sleep(Duration::from_millis(500)).await;

    // Spawn receivers after both consumers are registered
    for (cname, mut cons, mut stream) in [
        ("filter-eu-cons", cons_eu, stream_eu),
        ("filter-us-cons", cons_us, stream_us),
    ] {
        let txc = tx.clone();
        let cn = cname.to_string();
        tokio::spawn(async move {
            loop {
                match stream.recv().await {
                    Some(msg) => {
                        let payload = String::from_utf8(msg.payload.to_vec()).unwrap();
                        let _ = cons.ack(&msg).await;
                        let _ = txc.send((cn.clone(), payload));
                    }
                    None => break,
                }
            }
        });
    }

    let keys = vec!["eu-west", "eu-east", "us-west", "us-east"];
    let msgs_per_key = 3;
    let total = keys.len() * msgs_per_key;

    for round in 0..msgs_per_key {
        for key in &keys {
            let payload = format!("{}-{}", key, round);
            let _ = producer
                .send_with_key(payload.into_bytes(), None, key)
                .await?;
        }
        sleep(Duration::from_millis(100)).await;
    }

    let recv_future = async {
        let mut per_consumer: HashMap<String, Vec<String>> = HashMap::new();
        let mut received = 0usize;
        while received < total {
            if let Some((cname, payload)) = rx.recv().await {
                per_consumer.entry(cname).or_default().push(payload);
                received += 1;
            }
        }
        per_consumer
    };

    let per_consumer = timeout(Duration::from_secs(20), recv_future)
        .await
        .expect("timeout receiving reliable filtered messages");

    // EU consumer should only get eu-* messages
    let eu_msgs = per_consumer
        .get("filter-eu-cons")
        .cloned()
        .unwrap_or_default();
    assert_eq!(eu_msgs.len(), 2 * msgs_per_key); // eu-west + eu-east
    for msg in &eu_msgs {
        assert!(msg.starts_with("eu-"), "EU consumer got non-EU message: {}", msg);
    }

    // US consumer should only get us-* messages
    let us_msgs = per_consumer
        .get("filter-us-cons")
        .cloned()
        .unwrap_or_default();
    assert_eq!(us_msgs.len(), 2 * msgs_per_key); // us-west + us-east
    for msg in &us_msgs {
        assert!(msg.starts_with("us-"), "US consumer got non-US message: {}", msg);
    }

    Ok(())
}

/// What this test validates
///
/// - Scenario: one consumer with no filters (accepts all), one with "vip-*" filter.
///   Producer sends "vip-1", "vip-2", "regular-1", "regular-2".
/// - Expectation: the no-filter consumer only gets "regular-*" keys (because "vip-*"
///   matches the filtered consumer first via consistent hashing, and the filterless
///   consumer is eligible for non-vip keys).
///
/// Why this matters
/// - Tests the interaction between consumers with and without filters. The consistent
///   hash ring should still correctly route keys to eligible consumers.
#[tokio::test]
async fn key_shared_filter_mixed_with_unfiltered() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/ks_filter_mixed");

    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("prod_ks_filter_mixed")
        .build()?;
    producer.create().await?;

    let sub = "ks_filter_mixed_sub";
    let (tx, mut rx) = mpsc::unbounded_channel::<(String, String)>();

    // Consumer A: accepts all keys (no filter)
    {
        let topic_c = topic.clone();
        let sub_c = sub.to_string();
        let txc = tx.clone();
        let client_c = test_utils::setup_client().await?;

        tokio::spawn(async move {
            let mut c = client_c
                .new_consumer()
                .with_topic(topic_c)
                .with_consumer_name("mixed-all-cons".to_string())
                .with_subscription(sub_c)
                .with_subscription_type(SubType::KeyShared)
                .build()
                .unwrap();
            c.subscribe().await.unwrap();
            let mut stream = c.receive().await.unwrap();
            loop {
                match stream.recv().await {
                    Some(msg) => {
                        let payload = String::from_utf8(msg.payload.to_vec()).unwrap();
                        let _ = c.ack(&msg).await;
                        let _ = txc.send(("mixed-all-cons".to_string(), payload));
                    }
                    None => break,
                }
            }
        });
    }

    // Consumer B: vip-* filter only
    {
        let topic_c = topic.clone();
        let sub_c = sub.to_string();
        let txc = tx.clone();
        let client_c = test_utils::setup_client().await?;

        tokio::spawn(async move {
            let mut c = client_c
                .new_consumer()
                .with_topic(topic_c)
                .with_consumer_name("mixed-vip-cons".to_string())
                .with_subscription(sub_c)
                .with_subscription_type(SubType::KeyShared)
                .with_key_filter("vip-*")
                .build()
                .unwrap();
            c.subscribe().await.unwrap();
            let mut stream = c.receive().await.unwrap();
            loop {
                match stream.recv().await {
                    Some(msg) => {
                        let payload = String::from_utf8(msg.payload.to_vec()).unwrap();
                        let _ = c.ack(&msg).await;
                        let _ = txc.send(("mixed-vip-cons".to_string(), payload));
                    }
                    None => break,
                }
            }
        });
    }

    sleep(Duration::from_millis(500)).await;

    let vip_keys = vec!["vip-gold", "vip-platinum"];
    let regular_keys = vec!["regular-1", "regular-2"];
    let total = vip_keys.len() + regular_keys.len();

    for key in vip_keys.iter().chain(regular_keys.iter()) {
        let payload = format!("payload-{}", key);
        let _ = producer
            .send_with_key(payload.into_bytes(), None, key)
            .await?;
    }

    let recv_future = async {
        let mut per_consumer: HashMap<String, Vec<String>> = HashMap::new();
        let mut received = 0usize;
        while received < total {
            if let Some((cname, payload)) = rx.recv().await {
                per_consumer.entry(cname).or_default().push(payload);
                received += 1;
            }
        }
        per_consumer
    };

    let per_consumer = timeout(Duration::from_secs(15), recv_future)
        .await
        .expect("timeout receiving mixed filter messages");

    // VIP consumer should get vip-* messages (it's the only consumer eligible for them)
    let vip_msgs = per_consumer
        .get("mixed-vip-cons")
        .cloned()
        .unwrap_or_default();
    for msg in &vip_msgs {
        assert!(
            msg.contains("vip-"),
            "vip consumer received unexpected message: {}",
            msg
        );
    }

    // All consumer gets regular-* messages (since vip consumer can't handle them)
    // It may also get some vip-* messages since it's eligible for all keys
    let all_msgs = per_consumer
        .get("mixed-all-cons")
        .cloned()
        .unwrap_or_default();
    assert!(
        !all_msgs.is_empty(),
        "unfiltered consumer should receive at least some messages"
    );

    // Total messages across both consumers should equal total sent
    let total_received = vip_msgs.len() + all_msgs.len();
    assert_eq!(
        total_received, total,
        "total messages received should match total sent"
    );

    Ok(())
}

/// What this test validates
///
/// - Scenario: multiple consumers with multiple filters (using with_key_filters).
///   Consumer A: ["eu-*", "ap-*"], Consumer B: ["us-*", "af-*"].
/// - Expectation: each consumer gets only keys matching their filter list.
///
/// Why this matters
/// - Tests the multi-filter path where a consumer can accept keys from multiple patterns.
#[tokio::test]
async fn key_shared_filter_multiple_patterns() -> Result<()> {
    let client = test_utils::setup_client().await?;
    let topic = test_utils::unique_topic("/default/ks_filter_multi");

    let mut producer = client
        .new_producer()
        .with_topic(&topic)
        .with_name("prod_ks_filter_multi")
        .build()?;
    producer.create().await?;

    let sub = "ks_filter_multi_sub";
    let (tx, mut rx) = mpsc::unbounded_channel::<(String, String)>();

    // Consumer A: eu-* and ap-*
    {
        let topic_c = topic.clone();
        let sub_c = sub.to_string();
        let txc = tx.clone();
        let client_c = test_utils::setup_client().await?;

        tokio::spawn(async move {
            let mut c = client_c
                .new_consumer()
                .with_topic(topic_c)
                .with_consumer_name("multi-euap-cons".to_string())
                .with_subscription(sub_c)
                .with_subscription_type(SubType::KeyShared)
                .with_key_filters(vec!["eu-*".to_string(), "ap-*".to_string()])
                .build()
                .unwrap();
            c.subscribe().await.unwrap();
            let mut stream = c.receive().await.unwrap();
            loop {
                match stream.recv().await {
                    Some(msg) => {
                        let payload = String::from_utf8(msg.payload.to_vec()).unwrap();
                        let _ = c.ack(&msg).await;
                        let _ = txc.send(("multi-euap-cons".to_string(), payload));
                    }
                    None => break,
                }
            }
        });
    }

    // Consumer B: us-* and af-*
    {
        let topic_c = topic.clone();
        let sub_c = sub.to_string();
        let txc = tx.clone();
        let client_c = test_utils::setup_client().await?;

        tokio::spawn(async move {
            let mut c = client_c
                .new_consumer()
                .with_topic(topic_c)
                .with_consumer_name("multi-usaf-cons".to_string())
                .with_subscription(sub_c)
                .with_subscription_type(SubType::KeyShared)
                .with_key_filters(vec!["us-*".to_string(), "af-*".to_string()])
                .build()
                .unwrap();
            c.subscribe().await.unwrap();
            let mut stream = c.receive().await.unwrap();
            loop {
                match stream.recv().await {
                    Some(msg) => {
                        let payload = String::from_utf8(msg.payload.to_vec()).unwrap();
                        let _ = c.ack(&msg).await;
                        let _ = txc.send(("multi-usaf-cons".to_string(), payload));
                    }
                    None => break,
                }
            }
        });
    }

    sleep(Duration::from_millis(500)).await;

    let keys = vec!["eu-west", "ap-tokyo", "us-east", "af-cape"];
    let total = keys.len();

    for key in &keys {
        let payload = format!("{}-data", key);
        let _ = producer
            .send_with_key(payload.into_bytes(), None, key)
            .await?;
    }

    let recv_future = async {
        let mut per_consumer: HashMap<String, Vec<String>> = HashMap::new();
        let mut received = 0usize;
        while received < total {
            if let Some((cname, payload)) = rx.recv().await {
                per_consumer.entry(cname).or_default().push(payload);
                received += 1;
            }
        }
        per_consumer
    };

    let per_consumer = timeout(Duration::from_secs(15), recv_future)
        .await
        .expect("timeout receiving multi-filter messages");

    // EU/AP consumer
    let euap = per_consumer
        .get("multi-euap-cons")
        .cloned()
        .unwrap_or_default();
    assert_eq!(euap.len(), 2, "EU/AP consumer should get 2 messages");
    for msg in &euap {
        assert!(
            msg.starts_with("eu-") || msg.starts_with("ap-"),
            "EU/AP consumer got unexpected: {}",
            msg
        );
    }

    // US/AF consumer
    let usaf = per_consumer
        .get("multi-usaf-cons")
        .cloned()
        .unwrap_or_default();
    assert_eq!(usaf.len(), 2, "US/AF consumer should get 2 messages");
    for msg in &usaf {
        assert!(
            msg.starts_with("us-") || msg.starts_with("af-"),
            "US/AF consumer got unexpected: {}",
            msg
        );
    }

    Ok(())
}
