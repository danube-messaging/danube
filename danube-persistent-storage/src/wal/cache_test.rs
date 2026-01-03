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
                topic_offset: i,
            },
            payload: format!("msg-{}", i).into_bytes(),
            publish_time: i,
            producer_name: "test-producer".to_string(),
            subscription_name: None,
            attributes: HashMap::new(),
            schema_id: None,
            schema_version: None,
        }
    }

    /// Test: Cache basic behavior (insert, get, ordering, range)
    ///
    /// Purpose
    /// - Validate core behaviors without over-testing: insertion, retrieval, ordering, and range iteration
    ///
    /// Flow
    /// - Insert messages at non-sequential offsets
    /// - Validate: len(), get(), ordered range_from(), bounded range
    /// - Validate: non-existing get() returns None
    ///
    /// Expected
    /// - Items returned in ascending offset order
    /// - Exact-offset retrieval works; missing offsets return None
    /// - Bounded range filters correctly
    #[test]
    fn test_cache_basic_behavior() {
        let mut cache = Cache::new();

        // Insert in non-sequential order
        cache.insert(10, make_message(10));
        cache.insert(5, make_message(5));
        cache.insert(15, make_message(15));
        cache.insert(1, make_message(1));

        assert_eq!(cache.len(), 4);

        // get() exact offsets
        assert_eq!(cache.get(1).unwrap().0, 1);
        assert_eq!(cache.get(5).unwrap().0, 5);
        assert!(cache.get(2).is_none());

        // range_from ordered
        let items: Vec<_> = cache.range_from(0).collect();
        assert_eq!(items.len(), 4);
        assert_eq!(items[0].0, 1);
        assert_eq!(items[1].0, 5);
        assert_eq!(items[2].0, 10);
        assert_eq!(items[3].0, 15);

        // bounded range [5, 10]
        let bounded: Vec<_> = cache
            .range_from(5)
            .take_while(|(off, _)| *off <= 10)
            .collect();
        assert_eq!(bounded.len(), 2);
        assert_eq!(bounded[0].0, 5);
        assert_eq!(bounded[1].0, 10);
    }

    /// Test: Cache eviction behavior
    ///
    /// Purpose
    /// - Validate evict_to() preserves the newest items by offset and updates len()
    ///
    /// Flow
    /// - Insert offsets 0..5
    /// - Evict to capacity 3 (keep offsets 3,4,5)
    /// - Evict to capacity 0 (empty)
    ///
    /// Expected
    /// - After evict_to(3): len() == 3 and items are 3,4,5
    /// - After evict_to(0): len() == 0 and range is empty
    #[test]
    fn test_cache_eviction_behavior() {
        let mut cache = Cache::new();

        for i in 0..6u64 {
            cache.insert(i, make_message(i));
        }
        assert_eq!(cache.len(), 6);

        cache.evict_to(3);
        assert_eq!(cache.len(), 3);
        let items: Vec<_> = cache.range_from(0).collect();
        assert_eq!(
            items.iter().map(|(o, _)| *o).collect::<Vec<_>>(),
            vec![3, 4, 5]
        );

        cache.evict_to(0);
        assert_eq!(cache.len(), 0);
        let items: Vec<_> = cache.range_from(0).collect();
        assert!(items.is_empty());
    }
}
