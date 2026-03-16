mod durable_store;
mod storage_config;
mod storage;

pub use durable_store::OpendalDurableStore;
pub use storage::OpendalStore;
pub use storage_config::{BackendConfig, CloudBackend, LocalBackend};

#[cfg(test)]
mod storage_test;
