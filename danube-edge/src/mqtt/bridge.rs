//! MQTT-to-Danube topic mapping and StreamMessage construction.
//!
//! Evaluates incoming MQTT topic strings against the ordered list of
//! `TopicMapping` rules from the config. On first match, extracts wildcard
//! captures and builds a Danube `StreamMessage` ready for WAL ingestion.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use danube_core::message::{MessageID, StreamMessage};

use crate::mqtt::config::TopicMapping;

/// Compiled topic matcher built from the config at startup.
///
/// Holds the ordered list of mapping rules. `resolve()` evaluates them
/// in order and returns the first match.
#[derive(Debug, Clone)]
pub struct TopicRouter {
    rules: Vec<CompiledRule>,
}

/// A pre-processed mapping rule with the pattern split into segments.
#[derive(Debug, Clone)]
struct CompiledRule {
    /// Original pattern segments (split by `/`).
    /// Each segment is either a literal, `+`, or `#`.
    segments: Vec<String>,
    /// Target Danube topic.
    danube_topic: String,
    /// Attribute extraction templates: attr_name → "$1", "$2", etc.
    extract_attributes: HashMap<String, String>,
}

/// Result of a successful topic match.
#[derive(Debug, Clone)]
pub struct MatchResult {
    /// The resolved Danube topic name.
    pub danube_topic: String,
    /// Extracted attributes (wildcard captures applied).
    pub attributes: HashMap<String, String>,
}

impl TopicRouter {
    /// Build a router from the config's topic mapping rules.
    pub fn new(mappings: &[TopicMapping]) -> Self {
        let rules = mappings
            .iter()
            .map(|m| CompiledRule {
                segments: m.mqtt_pattern.split('/').map(String::from).collect(),
                danube_topic: m.danube_topic.clone(),
                extract_attributes: m.extract_attributes.clone(),
            })
            .collect();
        Self { rules }
    }

    /// Match an MQTT topic against the routing rules.
    ///
    /// Returns `Some(MatchResult)` on the first match, `None` if no rule matches.
    pub fn resolve(&self, mqtt_topic: &str) -> Option<MatchResult> {
        let topic_segments: Vec<&str> = mqtt_topic.split('/').collect();

        for rule in &self.rules {
            if let Some(captures) = Self::try_match(&rule.segments, &topic_segments) {
                // Apply captures to attribute templates
                let mut attributes = HashMap::new();
                for (attr_name, template) in &rule.extract_attributes {
                    if let Some(idx) = template.strip_prefix('$') {
                        if let Ok(i) = idx.parse::<usize>() {
                            if i >= 1 && i <= captures.len() {
                                attributes
                                    .insert(attr_name.clone(), captures[i - 1].to_string());
                            }
                        }
                    }
                }

                return Some(MatchResult {
                    danube_topic: rule.danube_topic.clone(),
                    attributes,
                });
            }
        }

        None
    }

    /// Try to match topic segments against a pattern. Returns captured
    /// wildcard values on success.
    ///
    /// - `+` matches exactly one segment (captured)
    /// - `#` matches zero or more remaining segments (not captured individually)
    fn try_match<'a>(
        pattern: &[String],
        topic: &[&'a str],
    ) -> Option<Vec<&'a str>> {
        let mut captures = Vec::new();
        let mut pi = 0; // pattern index
        let mut ti = 0; // topic index

        while pi < pattern.len() {
            let seg = &pattern[pi];

            if seg == "#" {
                // Multi-level wildcard — matches everything remaining.
                // Must be the last segment in the pattern.
                return Some(captures);
            }

            if ti >= topic.len() {
                // Topic is shorter than pattern (and pattern isn't `#`)
                return None;
            }

            if seg == "+" {
                // Single-level wildcard — capture this segment
                captures.push(topic[ti]);
            } else if seg != topic[ti] {
                // Literal mismatch
                return None;
            }

            pi += 1;
            ti += 1;
        }

        // Pattern fully consumed — topic must also be fully consumed
        if ti == topic.len() {
            Some(captures)
        } else {
            None
        }
    }
}

/// Build a `StreamMessage` from an MQTT publish payload and a match result.
///
/// Uses a synthetic producer identity (`mqtt-gateway`) since MQTT devices
/// are not registered as Danube producers.
pub fn build_stream_message(
    danube_topic: &str,
    payload: Bytes,
    attributes: HashMap<String, String>,
) -> StreamMessage {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    StreamMessage {
        request_id: 0,
        msg_id: MessageID {
            producer_id: 0, // synthetic: no registered producer
            topic_name: danube_topic.to_string(),
            broker_addr: String::new(),
            topic_offset: 0, // assigned by WAL on append
        },
        payload,
        publish_time: now,
        producer_name: "mqtt-gateway".to_string(),
        subscription_name: None,
        attributes,
        schema_id: None,
        schema_version: None,
        routing_key: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mqtt::config::TopicMapping;

    fn make_mapping(pattern: &str, topic: &str, attrs: Vec<(&str, &str)>) -> TopicMapping {
        TopicMapping {
            mqtt_pattern: pattern.to_string(),
            danube_topic: topic.to_string(),
            extract_attributes: attrs
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    #[test]
    fn test_exact_match() {
        let router = TopicRouter::new(&[make_mapping("sensors/temp", "/default/temp", vec![])]);
        let result = router.resolve("sensors/temp").unwrap();
        assert_eq!(result.danube_topic, "/default/temp");
        assert!(result.attributes.is_empty());
    }

    #[test]
    fn test_single_wildcard() {
        let router = TopicRouter::new(&[make_mapping(
            "device/+/telemetry",
            "/default/telemetry",
            vec![("device_id", "$1")],
        )]);
        let result = router.resolve("device/sensor-42/telemetry").unwrap();
        assert_eq!(result.danube_topic, "/default/telemetry");
        assert_eq!(result.attributes.get("device_id").unwrap(), "sensor-42");
    }

    #[test]
    fn test_multi_wildcard() {
        let router = TopicRouter::new(&[make_mapping(
            "device/+/+",
            "/default/device",
            vec![("id", "$1"), ("type", "$2")],
        )]);
        let result = router.resolve("device/abc/temp").unwrap();
        assert_eq!(result.attributes.get("id").unwrap(), "abc");
        assert_eq!(result.attributes.get("type").unwrap(), "temp");
    }

    #[test]
    fn test_hash_wildcard() {
        let router = TopicRouter::new(&[make_mapping("#", "/default/mqtt", vec![])]);
        let result = router.resolve("any/topic/here").unwrap();
        assert_eq!(result.danube_topic, "/default/mqtt");
    }

    #[test]
    fn test_no_match() {
        let router = TopicRouter::new(&[make_mapping(
            "sensors/temp",
            "/default/temp",
            vec![],
        )]);
        assert!(router.resolve("other/topic").is_none());
    }

    #[test]
    fn test_first_match_wins() {
        let router = TopicRouter::new(&[
            make_mapping("device/+/telemetry", "/default/telemetry", vec![]),
            make_mapping("#", "/default/catchall", vec![]),
        ]);
        let result = router.resolve("device/x/telemetry").unwrap();
        assert_eq!(result.danube_topic, "/default/telemetry");
    }
}
