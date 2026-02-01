//! Topic operational workflow prompts
//!
//! Contains prompts for topic creation and configuration.

use rmcp::model::{
    GetPromptRequestParams, GetPromptResult, Prompt, PromptArgument, PromptMessage,
    PromptMessageContent, PromptMessageRole,
};

/// Get topic operational prompt definitions
pub fn prompts() -> Vec<Prompt> {
    vec![Prompt {
        name: "setup_new_topic".to_string(),
        title: Some("Setup New Topic".to_string()),
        description: Some(
            "Guided workflow to create a new topic with best-practice configuration".to_string(),
        ),
        arguments: Some(vec![
            PromptArgument {
                name: "namespace".to_string(),
                title: None,
                description: Some("Namespace where the topic will be created".to_string()),
                required: Some(true),
            },
            PromptArgument {
                name: "use_case".to_string(),
                title: None,
                description: Some(
                    "Use case for optimization: high-throughput, low-latency, or persistent (optional)"
                        .to_string(),
                ),
                required: Some(false),
            },
        ]),
        icons: None,
        meta: None,
    }]
}

/// Try to get a topic operational prompt by name
pub fn get_prompt(params: &GetPromptRequestParams) -> Option<GetPromptResult> {
    let args = params.arguments.as_ref();

    match params.name.as_str() {
        "setup_new_topic" => {
            let namespace = args
                .and_then(|a| a.get("namespace"))
                .and_then(|v| v.as_str())
                .unwrap_or("<NAMESPACE>");
            let use_case = args
                .and_then(|a| a.get("use_case"))
                .and_then(|v| v.as_str());

            Some(GetPromptResult {
                description: Some(format!("Setup new topic in namespace {}", namespace)),
                messages: vec![PromptMessage {
                    role: PromptMessageRole::User,
                    content: PromptMessageContent::Text {
                        text: build_setup_topic_prompt(namespace, use_case),
                    },
                }],
            })
        }
        _ => None,
    }
}

fn build_setup_topic_prompt(namespace: &str, use_case: Option<&str>) -> String {
    let use_case_guidance = match use_case {
        Some("high-throughput") => {
            r#"
**Optimized for High Throughput:**
- Partitions: 4-8 (enables parallelism for high load)
- Delivery: non_reliable (faster, in-memory)
- Schema: Optional, use if structured data needs validation"#
        }
        Some("low-latency") => {
            r#"
**Optimized for Low Latency:**
- Partitions: 0 (non-partitioned, no partition overhead)
- Delivery: non_reliable (lowest latency)
- Schema: Optional, adds validation overhead"#
        }
        Some("persistent") => {
            r#"
**Optimized for Durability:**
- Partitions: 0 (non-partitioned, durability not related to partitioning)
- Delivery: reliable (WAL + cloud storage)
- Schema: Recommended for data governance and validation"#
        }
        _ => {
            r#"
**General Purpose Defaults:**
- Partitions: 0 (non-partitioned topic)
- Delivery: non_reliable (can change later)
- Schema: Optional, depends on data validation needs"#
        }
    };

    format!(
        r#"I need to create a new topic in namespace "{}".

Please guide me through the following steps:

## Step 1: Verify Namespace Exists
Use `list_namespaces` to check if namespace "{}" exists.
If not, create it first with `create_namespace`.

## Step 2: Choose Configuration{}

## Step 3: Create Topic
Use `create_topic` with:
- name: /{}/<topic-name>
- partitions: <based on use case above>
- dispatch_strategy: reliable or non_reliable
- schema_subject: <optional, see Step 4>

Ask me for the topic name and any custom configuration preferences.

## Step 4: Schema Subject (Optional but Recommended)
The `schema_subject` parameter links this topic to a schema for validation:

**When to use schemas:**
- Structured data (String, Number, JSON, Avro, Protobuf)
- Multiple producers need consistent format
- Data governance and validation required
- Schema evolution/versioning needed

**How to set it up:**
1. Register schema first using `create_schema`:
   - subject: "my-topic-value" (convention: topic-name + "-value")
   - schema: Your schema definition
   - schema_type: "json_schema", "avro", "protobuf", "string", "number", or "bytes"
2. Then use that subject name in `create_topic`:
   - schema_subject: "my-topic-value"

**Skip if:**
- Unstructured/freeform data
- Simple use cases without validation needs

## Step 5: Verify Setup
Use `describe_topic` to confirm:
- Topic created successfully
- Configuration matches requirements
- Broker assignment is appropriate

## Step 6: Next Steps
Provide guidance on:
- Creating subscriptions (if needed)
- Setting up producers/consumers
- Monitoring topic metrics with `get_topic_metrics`"#,
        namespace, namespace, use_case_guidance, namespace
    )
}
