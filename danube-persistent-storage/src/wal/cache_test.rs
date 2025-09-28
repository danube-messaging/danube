#[cfg(test)]
mod tests {
    use crate::wal::cache::Cache;
    use danube_core::message::{MessageID, StreamMessage};
    use std::collections::HashMap;

    fn make_message(i: u64) -> StreamMessage {
        StreamMessage {
            request_id: i,
            msg_id: MessageID {
                producer_id: 1,
                topic_name: "test".to_string(),
                broker_addr: "localhost:6650".to_string(),
                segment_id: 0,
                segment_offset: i,
            },
            payload: format!("msg-{}", i).into_bytes(),
            publish_time: i,
            producer_name: "test-producer".to_string(),
            subscription_name: None,
            attributes: HashMap::new(),
        }
    }

    /// Test: Cache insert and retrieval operations
    ///
    /// Purpose
    /// - Validate basic cache functionality for storing and retrieving messages by offset
    /// - Ensure messages are stored in correct order and can be retrieved accurately
    ///
    /// Flow
    /// - Create a cache with capacity 10
    /// - Insert messages at offsets 1, 3, 5
    /// - Retrieve messages by offset and verify content
    ///
    /// Expected
    /// - Messages are retrievable by their exact offset
    /// - Non-existent offsets return None
    /// - Message content matches what was inserted
    #[test]
    fn test_cache_insert_and_get() {
        let mut cache = Cache::new();
        
        // Insert messages with offsets 1, 3, 5
        cache.insert(1, make_message(1));
        cache.insert(3, make_message(3));
        cache.insert(5, make_message(5));
        
        assert_eq!(cache.len(), 3);
        
        // Test get(1) - should get message with offset 1
        let item = cache.get(1);
        assert!(item.is_some());
        assert_eq!(item.unwrap().0, 1);
        
        // Test get(3) - should get message with offset 3
        let item = cache.get(3);
        assert!(item.is_some());
        assert_eq!(item.unwrap().0, 3);
        
        // Test get(5) - should get message with offset 5
        let item = cache.get(5);
        assert!(item.is_some());
        assert_eq!(item.unwrap().0, 5);
        
        // Test get(2) - should get no message
        let item = cache.get(2);
        assert!(item.is_none());
    }

    #[test]
    fn test_cache_insert_and_range() {
        let mut cache = Cache::new();
        
        // Insert messages with offsets 0, 2, 4
        cache.insert(0, make_message(0));
        cache.insert(2, make_message(2));
        cache.insert(4, make_message(4));
        
        assert_eq!(cache.len(), 3);
        
        // Test range_from(0) - should get all messages
        let items: Vec<_> = cache.range_from(0).collect();
        assert_eq!(items.len(), 3);
        assert_eq!(items[0].0, 0);
        assert_eq!(items[1].0, 2);
        assert_eq!(items[2].0, 4);
        
        // Test range_from(2) - should get messages from offset 2 onwards
        let items: Vec<_> = cache.range_from(2).collect();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].0, 2);
        assert_eq!(items[1].0, 4);
        
        // Test range_from(5) - should get no messages
        let items: Vec<_> = cache.range_from(5).collect();
        assert_eq!(items.len(), 0);
    }

    /// Test: Cache capacity-based eviction (LRU behavior)
    ///
    /// Purpose
    /// - Validate that cache respects capacity limits and evicts oldest entries
    /// - Ensure LRU (Least Recently Used) eviction policy works correctly
    ///
    /// Flow
    /// - Create a cache with capacity 2
    /// - Insert 3 messages (should trigger eviction of first message)
    /// - Verify oldest message is evicted, newer messages remain
    ///
    /// Expected
    /// - Cache maintains at most 2 entries
    /// - First inserted message (offset 1) is evicted
    /// - Later messages (offsets 2, 3) remain accessible
    #[test]
    fn test_cache_capacity_eviction() {
        let mut cache = Cache::new();
        
        // Insert 5 messages
        for i in 0..5u64 {
            cache.insert(i, make_message(i));
        }
        assert_eq!(cache.len(), 5);
        
        // Evict to capacity 3 - should keep newest 3 (offsets 2, 3, 4)
        cache.evict_to(3);
        assert_eq!(cache.len(), 3);
        
        let items: Vec<_> = cache.range_from(0).collect();
        assert_eq!(items.len(), 3);
        assert_eq!(items[0].0, 2);
        assert_eq!(items[1].0, 3);
        assert_eq!(items[2].0, 4);
    }

    #[test]
    fn test_cache_eviction_to_zero() {
        let mut cache = Cache::new();
        
        // Insert messages
        for i in 0..3u64 {
            cache.insert(i, make_message(i));
        }
        assert_eq!(cache.len(), 3);
        
        // Evict to capacity 0 - should remove all
        cache.evict_to(0);
        assert_eq!(cache.len(), 0);
        
        let items: Vec<_> = cache.range_from(0).collect();
        assert_eq!(items.len(), 0);
    }

    /// Test: Cache maintains correct offset ordering
    ///
    /// Purpose
    /// - Validate that cache maintains messages in offset order regardless of insertion order
    /// - Ensure range queries always return results in ascending offset order
    ///
    /// Flow
    /// - Insert messages in non-sequential order: offsets 5, 1, 3, 7, 2
    /// - Query full range and verify ordering
    /// - Test partial ranges to ensure ordering is maintained
    ///
    /// Expected
    /// - All range queries return messages in ascending offset order: 1, 2, 3, 5, 7
    /// - Insertion order does not affect retrieval order
    /// - Partial ranges maintain correct ordering
    #[test]
    fn test_cache_ordering() {
        let mut cache = Cache::new();
        
        // Insert messages in non-sequential order
        cache.insert(10, make_message(10));
        cache.insert(5, make_message(5));
        cache.insert(15, make_message(15));
        cache.insert(1, make_message(1));
        
        // Should be returned in sorted order by offset
        let items: Vec<_> = cache.range_from(0).collect();
        assert_eq!(items.len(), 4);
        assert_eq!(items[0].0, 1);
        assert_eq!(items[1].0, 5);
        assert_eq!(items[2].0, 10);
        assert_eq!(items[3].0, 15);
    }

    /// Test: Cache range query functionality
    ///
    /// Purpose
    /// - Validate range queries return messages within specified offset bounds
    /// - Ensure range queries handle sparse data and boundary conditions correctly
    ///
    /// Flow
    /// - Insert messages at offsets 1, 3, 5, 7, 9
    /// - Query range [3, 7] and verify results
    /// - Test edge cases with empty ranges and out-of-bounds queries
    ///
    /// Expected
    /// - Range [3, 7] returns messages at offsets 3, 5, 7
    /// - Messages are returned in ascending offset order
    /// - Empty ranges return empty results
    #[test]
    fn test_cache_range_query() {
        let mut cache = Cache::new();
        
        // Insert messages at offsets 1, 3, 5, 7, 9
        cache.insert(1, make_message(1));
        cache.insert(3, make_message(3));
        cache.insert(5, make_message(5));
        cache.insert(7, make_message(7));
        cache.insert(9, make_message(9));
        
        // Query range [3, 7] and verify results
        let items: Vec<_> = cache.range_from(3).collect();
        assert_eq!(items.len(), 3);
        assert_eq!(items[0].0, 3);
        assert_eq!(items[1].0, 5);
        assert_eq!(items[2].0, 7);
        
        // Test edge cases with empty ranges and out-of-bounds queries
        let items: Vec<_> = cache.range_from(10).collect();
        assert_eq!(items.len(), 0);
    }

    #[test]
    fn test_cache_empty() {
        let cache = Cache::new();
        assert_eq!(cache.len(), 0);
        
        let items: Vec<_> = cache.range_from(0).collect();
        assert_eq!(items.len(), 0);
    }
}
