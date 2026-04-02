// Claims are now defined in danube-core for sharing across crates.
// Re-export here for backward compatibility within the broker.
pub(crate) use danube_core::jwt::Claims;
