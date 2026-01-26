//! Unit Tests for Rebalancing Data Structures
//!
//! Tests complex behavior of rebalancing history tracking (ring buffer, time-based filtering).
//! These tests validate the rate limiting infrastructure that prevents rebalancing storms.

use super::rebalancing::*;
use std::time::SystemTime;

/// **Test:** Ring Buffer Behavior - FIFO with Capacity Limit
///
/// **Reason:** RebalancingHistory uses a ring buffer to track recent moves. This is complex
/// data structure logic (overwrites oldest when full, maintains insertion order).
///
/// **Expectation:** With capacity=3, adding 5 items should keep only the last 3 in FIFO order.
/// Validates the ring buffer correctly discards old entries and retrieves in reverse order.
#[test]
fn test_rebalancing_history_record() {
    let mut history = RebalancingHistory::new(3);

    // Add moves
    for i in 0..5 {
        let mv = RebalancingMove::new(
            format!("/default/topic-{}", i),
            1,
            2,
            RebalancingReason::LoadImbalance,
            10.0,
        );
        history.record_move(mv);
    }

    // Should only keep last 3 (ring buffer)
    assert_eq!(history.total_moves(), 3);

    let recent = history.get_recent_moves(10);
    assert_eq!(recent.len(), 3);
    // Most recent first
    assert_eq!(recent[0].topic_name, "/default/topic-4");
    assert_eq!(recent[1].topic_name, "/default/topic-3");
    assert_eq!(recent[2].topic_name, "/default/topic-2");
}

/// **Test:** Time-Based Filtering for Rate Limiting
///
/// **Reason:** The hourly rate limit is a critical safety feature that prevents rebalancing storms.
/// This test validates the time-based filtering logic that counts only recent moves.
///
/// **Expectation:** Moves older than 1 hour (7200 seconds) should NOT be counted in the
/// hourly limit. This ensures rate limiting works correctly across time boundaries.
#[test]
fn test_rebalancing_history_hourly_count() {
    let mut history = RebalancingHistory::new(100);

    // Add a move with current timestamp (should be counted)
    let mv1 = RebalancingMove::new(
        "/default/topic-1".to_string(),
        1,
        2,
        RebalancingReason::LoadImbalance,
        10.0,
    );
    history.record_move(mv1);

    // Add a move with old timestamp (should not be counted)
    let old_timestamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .saturating_sub(7200); // 2 hours ago

    let mut mv2 = RebalancingMove::new(
        "/default/topic-2".to_string(),
        1,
        2,
        RebalancingReason::LoadImbalance,
        10.0,
    );
    mv2.timestamp = old_timestamp;
    history.record_move(mv2);

    // Only the recent move should be counted
    assert_eq!(history.count_moves_in_last_hour(), 1);
    assert_eq!(history.total_moves(), 2);
}
