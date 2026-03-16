// Top-level cloud module entry: flat submodules and public re-exports

mod storage_config;
mod storage;
mod reader;

// Re-export the public API used by the rest of the crate
pub use storage::{CloudRangeReader, CloudStore};
pub use storage_config::{BackendConfig, CloudBackend, LocalBackend};
pub use reader::DurableHistoryReader;

// Local test modules for cloud components
#[cfg(test)]
mod storage_test;
#[cfg(test)]
mod reader_test;
