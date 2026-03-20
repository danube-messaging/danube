mod durable_store;
mod storage_config;
mod storage;

pub use durable_store::OpendalDurableStore;
pub use storage::OpendalStore;
pub(crate) use storage_config::BackendConfig;
pub use storage_config::ObjectStoreBackend;

#[cfg(test)]
mod storage_test;
