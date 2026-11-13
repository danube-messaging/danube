use std::fmt;

use thiserror::Error;

/// Canonical normalized Danube topic identifier (`/{namespace}/{topic}`).
///
/// Normalization uses `/` as the **only** structural separator (never `.`),
/// preserving dots in names like `orders.v1` or `events.v2.raw`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicPath {
    namespace: String,
    topic: String,
    full_path: String,
    key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TopicPathError {
    #[error("topic name cannot be empty")]
    Empty,
    #[error("topic name '{0}' contains invalid characters or empty segments")]
    InvalidFormat(String),
}

impl TopicPath {
    /// Parse and normalize a user-supplied or canonical topic name.
    ///
    /// Examples:
    /// - `"orders.v1"` -> `namespace = "default"`, `topic = "orders.v1"`, `full_path = "/default/orders.v1"`
    /// - `"/default/orders.v1"` -> `namespace = "default"`, `topic = "orders.v1"`
    /// - `"iot/sensors/temp"` -> `namespace = "iot"`, `topic = "sensors/temp"`
    pub fn parse(raw: &str) -> Result<Self, TopicPathError> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(TopicPathError::Empty);
        }

        let stripped = trimmed.strip_prefix('/').unwrap_or(trimmed);
        if stripped.is_empty() || stripped.ends_with('/') {
            return Err(TopicPathError::InvalidFormat(raw.to_string()));
        }

        let segments: Vec<&str> = stripped.split('/').collect();
        if segments.iter().any(|seg| !is_valid_segment(seg)) {
            return Err(TopicPathError::InvalidFormat(raw.to_string()));
        }

        let (namespace, topic) = if segments.len() == 1 {
            ("default".to_string(), segments[0].to_string())
        } else {
            (segments[0].to_string(), segments[1..].join("/"))
        };

        let key = format!("{}/{}", namespace, topic);
        let full_path = format!("/{}", key);

        Ok(Self {
            namespace,
            topic,
            full_path,
            key,
        })
    }

    /// Strict validator for canonical `/{namespace}/{topic}` strings (already normalized).
    pub fn is_valid_canonical(raw: &str) -> bool {
        if !raw.starts_with('/') {
            return false;
        }
        let stripped = &raw[1..];
        if stripped.is_empty() || stripped.ends_with('/') {
            return false;
        }
        let segments: Vec<&str> = stripped.split('/').collect();
        segments.len() >= 2 && segments.iter().all(|seg| is_valid_segment(seg))
    }

    /// Extract namespace name (`"default"` or `"iot"`).
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Extract topic portion after namespace (`"orders.v1"` or `"sensors/temp"`).
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Full canonical path with leading `/` (`"/default/orders.v1"`).
    pub fn full_path(&self) -> &str {
        &self.full_path
    }

    /// Key without leading `/` (`"default/orders.v1"`), matching Raft key subpaths.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// If this topic is a partition (`...-part-{N}`), returns `Some(N)`.
    pub fn partition_index(&self) -> Option<usize> {
        let (_, idx_str) = self.topic.rsplit_once("-part-")?;
        if idx_str.is_empty() || !idx_str.chars().all(|c| c.is_ascii_digit()) {
            return None;
        }
        idx_str.parse::<usize>().ok()
    }

    /// Returns the base canonical path without any trailing `-part-{N}` suffix.
    pub fn base_full_path(&self) -> String {
        match self.topic.rsplit_once("-part-") {
            Some((base, idx_str))
                if !base.is_empty()
                    && !idx_str.is_empty()
                    && idx_str.chars().all(|c| c.is_ascii_digit()) =>
            {
                format!("/{}/{}", self.namespace, base)
            }
            _ => self.full_path.clone(),
        }
    }

    /// Returns the base key without any leading `/` or trailing `-part-{N}` suffix.
    pub fn base_key(&self) -> String {
        self.base_full_path()
            .strip_prefix('/')
            .unwrap_or(&self.key)
            .to_string()
    }
}

fn is_valid_segment(seg: &str) -> bool {
    !seg.is_empty()
        && seg != "."
        && seg != ".."
        && seg
            .chars()
            .all(|c| c.is_alphanumeric() || matches!(c, '_' | '-' | '.'))
}

impl fmt::Display for TopicPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.full_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_short_and_dotted_names() {
        let tp = TopicPath::parse("orders.v1").unwrap();
        assert_eq!(tp.namespace(), "default");
        assert_eq!(tp.topic(), "orders.v1");
        assert_eq!(tp.full_path(), "/default/orders.v1");
        assert_eq!(tp.key(), "default/orders.v1");

        let tp2 = TopicPath::parse("/default/orders.v1").unwrap();
        assert_eq!(tp, tp2);
    }

    #[test]
    fn preserves_hierarchical_slashes_without_dot_conversion() {
        let tp = TopicPath::parse("iot/sensors/temp.c").unwrap();
        assert_eq!(tp.namespace(), "iot");
        assert_eq!(tp.topic(), "sensors/temp.c");
        assert_eq!(tp.full_path(), "/iot/sensors/temp.c");
        assert!(TopicPath::is_valid_canonical(tp.full_path()));
    }

    #[test]
    fn parses_partition_suffixes() {
        let tp = TopicPath::parse("/payments/orders.v1-part-3").unwrap();
        assert_eq!(tp.partition_index(), Some(3));
        assert_eq!(tp.base_full_path(), "/payments/orders.v1");
        assert_eq!(tp.base_key(), "payments/orders.v1");

        let non_part = TopicPath::parse("/payments/orders-v1").unwrap();
        assert_eq!(non_part.partition_index(), None);
        assert_eq!(non_part.base_full_path(), "/payments/orders-v1");
    }

    #[test]
    fn rejects_invalid_topic_paths() {
        assert!(TopicPath::parse("").is_err());
        assert!(TopicPath::parse("/").is_err());
        assert!(TopicPath::parse("default/").is_err());
        assert!(TopicPath::parse("/default//foo").is_err());
        assert!(TopicPath::parse("ns/..").is_err());
        assert!(TopicPath::parse("ns/has space").is_err());
        assert!(!TopicPath::is_valid_canonical("orders.v1"));
    }
}

