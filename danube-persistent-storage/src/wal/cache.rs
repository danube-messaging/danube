use std::collections::BTreeMap;

use danube_core::message::StreamMessage;

#[derive(Debug, Default)]
pub(crate) struct Cache {
    map: BTreeMap<u64, StreamMessage>,
}

impl Cache {
    pub(crate) fn new() -> Self {
        Self {
            map: BTreeMap::new(),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn len(&self) -> usize {
        self.map.len()
    }

    pub(crate) fn insert(&mut self, offset: u64, msg: StreamMessage) {
        self.map.insert(offset, msg);
    }

    pub(crate) fn evict_to(&mut self, capacity: usize) {
        while self.map.len() > capacity {
            if let Some(oldest) = self.map.keys().next().cloned() {
                self.map.remove(&oldest);
            } else {
                break;
            }
        }
    }

    pub(crate) fn range_from(&self, from: u64) -> impl Iterator<Item = (u64, StreamMessage)> + '_ {
        self.map.range(from..).map(|(k, v)| (*k, v.clone()))
    }
}
