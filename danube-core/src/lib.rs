pub mod message;
pub mod storage;

pub mod proto {
    include!("proto/danube.rs");
}

pub mod admin_proto {
    include!("proto/danube_admin.rs");
}
