use metrics::gauge;
use serde::{Deserialize, Serialize};

use crate::broker_metrics::TOPIC_ACTIVE_PRODUCERS;

// Represents the connected producer
#[derive(Debug, Serialize, Deserialize)]
#[allow(dead_code)]
pub(crate) struct Producer {
    pub(crate) producer_id: u64,
    pub(crate) producer_name: String,
    pub(crate) topic_name: String,
    pub(crate) access_mode: i32, // should be ProducerAccessMode
    // status = true -> producer OK, status = false -> Close the producer
    pub(crate) status: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub(crate) enum ProducerAccessMode {
    Shared,
    Exclusive,
}

impl Producer {
    pub(crate) fn new(
        producer_id: u64,
        producer_name: String,
        topic_name: String,
        access_mode: i32,
    ) -> Self {
        Producer {
            producer_id,
            producer_name,
            topic_name,
            access_mode,
            status: true,
        }
    }

    // closes the producer from server-side and inform the client through health_check mechanism
    // to disconnect producer
    pub(crate) fn disconnect(&mut self) -> u64 {
        //metrics, number of producers per topic
        gauge!(TOPIC_ACTIVE_PRODUCERS.name, "topic" => self.topic_name.to_string()).decrement(1);

        self.status = false;
        self.producer_id
    }
}
