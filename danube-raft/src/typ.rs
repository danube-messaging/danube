//! Openraft type configuration for Danube.
//!
//! Defines the concrete types that parameterize all openraft generics.
//! Uses `declare_raft_types!` to satisfy all required supertraits automatically.

use std::io::Cursor;

use crate::commands::{RaftCommand, RaftResponse};

openraft::declare_raft_types!(
    /// The openraft type configuration for Danube.
    pub TypeConfig:
        D = RaftCommand,
        R = RaftResponse,
        NodeId = u64,
        Node = openraft::BasicNode,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);
