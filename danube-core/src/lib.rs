pub mod dispatch_strategy;
pub mod jwt;
pub mod message;
pub mod metadata;
pub mod storage;

pub mod proto {
    include!("proto/danube.rs");

    // Schema Registry proto module
    pub mod danube_schema {
        include!("proto/danube_schema.rs");
    }

    // Edge Replicator proto module — nested under `proto` (package `danube`)
    // so that `super::StreamMessage` in the generated code resolves to
    // the `danube.StreamMessage` proto type defined in `danube.rs`.
    pub mod edge {
        include!("proto/danube.edge.rs");
    }
}

pub mod admin_proto {
    include!("proto/danube_admin.rs");
}

pub mod raft_proto {
    include!("proto/danube.raft.rs");
}

/// Re-export for backward compatibility.
pub use proto::edge as edge_proto;

