use crate::schema::metadata::{CompatibilityResult, ProtobufDefinition};
use crate::schema::types::CompatibilityMode;
use anyhow::Result;

/// Protobuf-specific compatibility checker
#[derive(Debug)]
pub struct ProtobufCompatibilityChecker;

impl ProtobufCompatibilityChecker {
    pub fn new() -> Self {
        Self
    }

    /// Check Protobuf schema compatibility
    pub fn check(
        &self,
        _old: &ProtobufDefinition,
        _new: &ProtobufDefinition,
        mode: CompatibilityMode,
    ) -> Result<CompatibilityResult> {
        // TODO: Implement Protobuf compatibility checking
        // For now, we'll be permissive except for None mode
        match mode {
            CompatibilityMode::None => Ok(CompatibilityResult::compatible()),
            _ => {
                // TODO: Implement actual protobuf compatibility rules:
                // - Field number changes (breaking)
                // - Field type changes (some are compatible, e.g., int32 -> int64)
                // - Required/optional/repeated changes
                // - Message nesting changes
                Ok(CompatibilityResult::compatible())
            }
        }
    }
}

impl Default for ProtobufCompatibilityChecker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_protobuf_none_mode_always_compatible() {
        let checker = ProtobufCompatibilityChecker::new();

        let old = ProtobufDefinition::new(
            "syntax = \"proto3\"; message Old {}".to_string(),
            "Old".to_string(),
            "fp1".to_string(),
        );

        let new = ProtobufDefinition::new(
            "syntax = \"proto3\"; message New {}".to_string(),
            "New".to_string(),
            "fp2".to_string(),
        );

        let result = checker.check(&old, &new, CompatibilityMode::None).unwrap();
        assert!(result.is_compatible);
    }

    // TODO: Add tests once Protobuf compatibility is implemented
}
