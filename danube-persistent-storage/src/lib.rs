mod errors;

mod local_disk;
pub use local_disk::DiskStorage;

mod managed_storage;
pub use managed_storage::RemoteStorage;

mod connection;
