pub mod dispatch_strategy;
pub mod message;
pub mod storage;

pub mod proto {
    include!("proto/danube.v23.rs");
}

pub mod admin_proto {
    include!("proto/danube_admin.rs");
}
