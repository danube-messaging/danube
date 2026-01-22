//! # Danube Load Manager
//!
//! Distributed broker load balancing and cluster orchestration for Danube messaging.
//!
//! ## Core Responsibilities
//!
//! - **Load Distribution**: Monitors broker resource usage and distributes topic assignments
//! - **Dynamic Rebalancing**: Redistributes topics when brokers join or leave the cluster
//! - **Failover Management**: Handles broker failures by reallocating topics to healthy brokers
//! - **Resource Optimization**: Uses ranking algorithms to optimize broker utilization
//!
//! ## Architecture
//!
//! The LoadManager operates as a centralized coordinator that:
//! 1. Watches broker load metrics via metadata store events
//! 2. Calculates broker rankings based on resource utilization
//! 3. Assigns new topics to the least loaded brokers
//! 4. Handles broker failures by cleaning up assignments and marking topics for reassignment

pub mod load_report;
pub mod rankings;
mod manager;

// Re-export main types
pub use load_report::{LoadReport, ResourceType, SystemLoad, generate_load_report};
pub use manager::{LoadManager, LeaderStateProvider, LeaderState};
pub use rankings::{rankings_simple, rankings_composite};
