// Top-level cloud module entry: flat submodules and public re-exports

pub mod storage_config;
pub mod storage;
pub mod reader;
pub mod uploader;
pub mod uploader_stream;

// Re-export the public API used by the rest of the crate
pub use storage::{CloudRangeReader, CloudStore, CloudWriter};
pub use storage_config::{BackendConfig, CloudBackend, LocalBackend};
pub use reader::CloudReader;
pub use uploader::{Uploader, UploaderBaseConfig, UploaderConfig};

// Local test modules for cloud components
#[cfg(test)]
mod storage_test;
#[cfg(test)]
mod reader_test;
#[cfg(test)]
mod uploader_test;
