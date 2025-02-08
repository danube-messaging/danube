pub mod dispatch_strategy;
pub mod message;
pub mod storage;

pub mod proto {
    include!("proto/danube.rs");
}

pub mod admin_proto {
    include!("proto/danube_admin.rs");
}

pub mod managed_storage_proto {
    include!("proto/managed_storage.rs");
}
