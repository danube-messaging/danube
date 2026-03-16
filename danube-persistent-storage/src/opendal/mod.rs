mod storage_config;
mod storage;

pub use storage::{OpendalRangeReader, OpendalStore};
pub use storage_config::{BackendConfig, CloudBackend, LocalBackend};

#[cfg(test)]
mod storage_test;
