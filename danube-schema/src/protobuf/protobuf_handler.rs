use crate::metadata::ProtobufDefinition;
use anyhow::{anyhow, Result};
use sha2::{Digest, Sha256};

pub struct ProtobufHandler;

impl ProtobufHandler {
    pub fn parse(raw_schema_bytes: &[u8], message_name: String) -> Result<ProtobufDefinition> {
        let raw_proto = std::str::from_utf8(raw_schema_bytes)
            .map_err(|e| anyhow!("Invalid UTF-8 in Protobuf schema: {}", e))?
            .to_string();
        let fingerprint = Self::compute_fingerprint(&raw_proto);
        Ok(ProtobufDefinition::new(raw_proto, message_name, fingerprint))
    }

    pub fn compute_fingerprint(raw_proto: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(raw_proto.as_bytes());
        let result = hasher.finalize();
        format!("sha256:{}", hex::encode(result))
    }

    #[allow(dead_code)]
    pub fn validate(_raw_proto: &str) -> Result<()> { Ok(()) }

    #[allow(dead_code)]
    pub fn extract_message_names(_raw_proto: &str) -> Result<Vec<String>> { Ok(Vec::new()) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fingerprint_consistency() {
        let proto = "syntax = \"proto3\";\nmessage User { string name = 1; int32 age = 2; }";
        let fp1 = ProtobufHandler::compute_fingerprint(proto);
        let fp2 = ProtobufHandler::compute_fingerprint(proto);
        assert_eq!(fp1, fp2);
    }
}
