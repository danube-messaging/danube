use crate::metadata::{CompatibilityResult, ProtobufDefinition};
use crate::types::CompatibilityMode;
use anyhow::Result;

#[derive(Debug)]
pub struct ProtobufCompatibilityChecker;

impl ProtobufCompatibilityChecker {
    pub fn new() -> Self { Self }

    pub fn check(&self, _old: &ProtobufDefinition, _new: &ProtobufDefinition, mode: CompatibilityMode) -> Result<CompatibilityResult> {
        match mode {
            CompatibilityMode::None => Ok(CompatibilityResult::compatible()),
            _ => Ok(CompatibilityResult::compatible()), // TODO: Implement
        }
    }
}

impl Default for ProtobufCompatibilityChecker {
    fn default() -> Self { Self::new() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_protobuf_none_mode_always_compatible() {
        let checker = ProtobufCompatibilityChecker::new();
        let old = ProtobufDefinition::new("syntax = \"proto3\"; message Old {}".to_string(), "Old".to_string(), "fp1".to_string());
        let new = ProtobufDefinition::new("syntax = \"proto3\"; message New {}".to_string(), "New".to_string(), "fp2".to_string());
        let result = checker.check(&old, &new, CompatibilityMode::None).unwrap();
        assert!(result.is_compatible);
    }
}
