mod config;
mod durable_store;
mod store;

pub use config::ObjectStoreBackend;
pub(crate) use config::BackendConfig;
pub use durable_store::ObjStoreDurable;
pub(crate) use store::ObjStore;

#[cfg(test)]
mod storage_test;
