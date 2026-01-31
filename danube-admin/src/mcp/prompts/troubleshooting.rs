//! Troubleshooting prompts for Danube
//!
//! Contains prompt definitions and message builders for common
//! troubleshooting workflows.

use rmcp::model::{
    GetPromptRequestParams, GetPromptResult, Prompt, PromptArgument, PromptMessage,
    PromptMessageContent, PromptMessageRole,
};

/// Get all troubleshooting prompt definitions
pub fn prompts() -> Vec<Prompt> {
    vec![
        Prompt {
            name: "diagnose_consumer_lag".to_string(),
            title: Some("Diagnose Consumer Lag".to_string()),
            description: Some(
                "Step-by-step guide to diagnose why consumers are lagging behind producers"
                    .to_string(),
            ),
            arguments: Some(vec![
                PromptArgument {
                    name: "topic".to_string(),
                    title: None,
                    description: Some("Topic name to diagnose".to_string()),
                    required: Some(true),
                },
                PromptArgument {
                    name: "subscription".to_string(),
                    title: None,
                    description: Some("Subscription name (optional)".to_string()),
                    required: Some(false),
                },
            ]),
            icons: None,
            meta: None,
        },
        Prompt {
            name: "diagnose_broker_issues".to_string(),
            title: Some("Diagnose Broker Issues".to_string()),
            description: Some(
                "Investigate broker health, load distribution, and potential problems".to_string(),
            ),
            arguments: Some(vec![PromptArgument {
                name: "broker_id".to_string(),
                title: None,
                description: Some(
                    "Broker ID to investigate (optional, diagnoses all if omitted)".to_string(),
                ),
                required: Some(false),
            }]),
            icons: None,
            meta: None,
        },
        Prompt {
            name: "analyze_topic_performance".to_string(),
            title: Some("Analyze Topic Performance".to_string()),
            description: Some(
                "Deep dive into topic metrics including throughput, latency, and errors"
                    .to_string(),
            ),
            arguments: Some(vec![PromptArgument {
                name: "topic".to_string(),
                title: None,
                description: Some("Topic name to analyze".to_string()),
                required: Some(true),
            }]),
            icons: None,
            meta: None,
        },
        Prompt {
            name: "cluster_health_check".to_string(),
            title: Some("Cluster Health Check".to_string()),
            description: Some(
                "Comprehensive cluster health assessment with actionable recommendations"
                    .to_string(),
            ),
            arguments: None,
            icons: None,
            meta: None,
        },
    ]
}

/// Try to get a troubleshooting prompt by name
pub fn get_prompt(params: &GetPromptRequestParams) -> Option<GetPromptResult> {
    let args = params.arguments.as_ref();

    match params.name.as_str() {
        "diagnose_consumer_lag" => {
            let topic = args
                .and_then(|a| a.get("topic"))
                .and_then(|v| v.as_str())
                .unwrap_or("<TOPIC_NAME>");
            let subscription = args
                .and_then(|a| a.get("subscription"))
                .and_then(|v| v.as_str());

            Some(GetPromptResult {
                description: Some(format!("Diagnose consumer lag for topic {}", topic)),
                messages: vec![PromptMessage {
                    role: PromptMessageRole::User,
                    content: PromptMessageContent::Text {
                        text: build_lag_diagnosis_prompt(topic, subscription),
                    },
                }],
            })
        }

        "diagnose_broker_issues" => {
            let broker_id = args
                .and_then(|a| a.get("broker_id"))
                .and_then(|v| v.as_str());

            Some(GetPromptResult {
                description: Some("Diagnose broker health and issues".to_string()),
                messages: vec![PromptMessage {
                    role: PromptMessageRole::User,
                    content: PromptMessageContent::Text {
                        text: build_broker_diagnosis_prompt(broker_id),
                    },
                }],
            })
        }

        "analyze_topic_performance" => {
            let topic = args
                .and_then(|a| a.get("topic"))
                .and_then(|v| v.as_str())
                .unwrap_or("<TOPIC_NAME>");

            Some(GetPromptResult {
                description: Some(format!("Analyze performance for topic {}", topic)),
                messages: vec![PromptMessage {
                    role: PromptMessageRole::User,
                    content: PromptMessageContent::Text {
                        text: build_topic_analysis_prompt(topic),
                    },
                }],
            })
        }

        "cluster_health_check" => Some(GetPromptResult {
            description: Some("Comprehensive cluster health check".to_string()),
            messages: vec![PromptMessage {
                role: PromptMessageRole::User,
                content: PromptMessageContent::Text {
                    text: build_cluster_health_prompt(),
                },
            }],
        }),

        _ => None,
    }
}

fn build_lag_diagnosis_prompt(topic: &str, subscription: Option<&str>) -> String {
    let sub_filter = subscription
        .map(|s| format!(", subscription=\"{}\"", s))
        .unwrap_or_default();

    format!(
        r#"I need help diagnosing consumer lag for topic "{}".

Please follow these steps:

## Step 1: Check Current Lag
Use the `diagnose_consumer_lag` tool with topic="{}" {} to get the current lag status.

## Step 2: Analyze Topic Metrics
Use `get_topic_metrics` with topic="{}" to understand:
- Current publish rate vs dispatch rate
- Number of active consumers
- Any producer errors

## Step 3: Check Broker Health
If lag is high, check if the broker hosting this topic is overloaded:
- Use `list_brokers` to find which broker owns the topic
- Use `get_broker_metrics` to check broker load

## Step 4: Custom Queries (if needed)
Use `query_prometheus` with these queries:
- Lag trend: `danube_subscription_lag{{topic="{}"{}}}`
- Publish vs dispatch rate comparison:
  - `rate(danube_topic_messages_in_total{{topic="{}"}}[5m])`
  - `rate(danube_topic_messages_out_total{{topic="{}"}}[5m])`

## Step 5: Recommendations
Based on findings, suggest:
- Add more consumers if dispatch rate < publish rate
- Check consumer application logs for errors
- Consider topic partitioning for parallelism
- Verify network connectivity between consumers and brokers"#,
        topic,
        topic,
        subscription
            .map(|s| format!("and subscription=\"{}\"", s))
            .unwrap_or_default(),
        topic,
        topic,
        sub_filter,
        topic,
        topic
    )
}

fn build_broker_diagnosis_prompt(broker_id: Option<&str>) -> String {
    match broker_id {
        Some(id) => format!(
            r#"I need help diagnosing issues with broker "{}".

Please follow these steps:

## Step 1: Check Broker Status
Use `list_brokers` to verify the broker is online and check its role (leader/follower).

## Step 2: Get Broker Metrics
Use `get_broker_metrics` with broker_id="{}" to check:
- Topics owned (is it overloaded?)
- RPC count (traffic level)
- Active producers/consumers
- Bytes in/out

## Step 3: Compare with Other Brokers
Use `get_cluster_balance` to see if load is evenly distributed.
Check the imbalance coefficient - values > 0.3 indicate poor distribution.

## Step 4: Check Broker Logs
Use `get_broker_logs` with broker_id="{}" to look for:
- Error messages
- Connection issues
- Resource warnings

## Step 5: Recommendations
Based on findings:
- If overloaded: trigger rebalance with `trigger_rebalance`
- If errors in logs: address specific issues
- If network issues: check connectivity"#,
            id, id, id
        ),
        None => r#"I need help diagnosing the health of all brokers in the cluster.

Please follow these steps:

## Step 1: List All Brokers
Use `list_brokers` to get the complete broker list with status and roles.

## Step 2: Check Cluster Balance
Use `get_cluster_balance` to assess load distribution:
- Coefficient of variation (CV) indicates balance quality
- CV < 0.1 = excellent, CV > 0.3 = needs rebalancing

## Step 3: Get Cluster Metrics
Use `get_cluster_metrics` to understand overall health:
- Total topics, producers, consumers
- Message rates across cluster
- Any anomalies in throughput

## Step 4: Identify Problem Brokers
For any brokers with issues, use `get_broker_metrics` to deep dive.

## Step 5: Recommendations
Use `get_recommendations` to get AI-generated suggestions for:
- Rebalancing needs
- Capacity planning
- Configuration optimizations"#
            .to_string(),
    }
}

fn build_topic_analysis_prompt(topic: &str) -> String {
    format!(
        r#"I need a comprehensive performance analysis for topic "{}".

Please follow these steps:

## Step 1: Get Topic Metrics
Use `get_topic_metrics` with topic="{}" to gather:
- Message counts (in/out)
- Byte throughput
- Active producers/consumers
- Publish and dispatch rates
- Latency percentiles (p50, p95, p99)
- Subscription lag

## Step 2: Describe Topic Configuration
Use `describe_topic` with name="{}" to understand:
- Delivery strategy (reliable vs non-reliable)
- Partition count
- Schema association

## Step 3: Historical Trend Analysis
Use `query_prometheus` for trend queries:
```promql
# Publish rate over last hour
rate(danube_topic_messages_in_total{{topic="{}"}}[1h])

# Latency trend
histogram_quantile(0.99, rate(danube_producer_send_latency_bucket{{topic="{}"}}[5m]))

# Error rate
rate(danube_producer_send_errors_total{{topic="{}"}}[5m])
```

## Step 4: Performance Assessment
Evaluate:
- Is latency acceptable? (p99 < 100ms is typically good)
- Is throughput meeting expectations?
- Are there any errors?
- Is consumer lag growing?

## Step 5: Recommendations
Suggest optimizations:
- Batching configuration for higher throughput
- Consumer scaling for lag issues
- Schema optimization for message size"#,
        topic, topic, topic, topic, topic, topic
    )
}

fn build_cluster_health_prompt() -> String {
    r#"I need a comprehensive health check of the Danube cluster.

Please follow these steps:

## Step 1: Cluster Overview
Use `get_cluster_metrics` to get:
- Total brokers, topics, producers, consumers
- Cluster-wide message rates
- Balance coefficient

## Step 2: Broker Health
Use `list_brokers` and check:
- All brokers are online
- Leader is elected
- No brokers in error state

## Step 3: Load Distribution
Use `get_cluster_balance` to verify:
- Topics are evenly distributed
- No single broker is overloaded
- CV (coefficient of variation) < 0.2 is healthy

## Step 4: Check for Issues
Use `health_check` tool for automated diagnostics:
- Connectivity issues
- Resource constraints
- Configuration problems

## Step 5: Get Recommendations
Use `get_recommendations` for:
- Actionable improvements
- Capacity planning advice
- Best practice suggestions

## Step 6: Summary Report
Provide a summary with:
- Overall health status (healthy/warning/critical)
- Key metrics snapshot
- Top 3 action items if any issues found"#
        .to_string()
}
