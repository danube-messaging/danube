pub mod dispatch_strategy;
pub mod message;
pub mod metadata;
pub mod storage;

pub mod proto {
    include!("proto/danube.rs");

    // Schema Registry proto module
    pub mod danube_schema {
        include!("proto/danube_schema.rs");
    }
}

pub mod admin_proto {
    include!("proto/danube_admin.rs");
}

pub mod raft_proto {
    include!("proto/danube.raft.rs");
}
