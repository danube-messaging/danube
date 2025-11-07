use serde::Serialize;

#[derive(Clone, Serialize)]
pub struct ClusterBrokerStatsDto {
    pub topics_owned: u64,
    pub rpc_total: u64,
    pub rpc_rate_1m: f64,
    pub active_connections: u64,
    pub errors_5xx_total: u64,
}

#[derive(Clone, Serialize)]
pub struct ClusterBrokerDto {
    pub broker_id: String,
    pub broker_addr: String,
    pub broker_role: String,
    pub stats: ClusterBrokerStatsDto,
}

#[derive(Clone, Serialize)]
pub struct ClusterTotalsDto {
    pub broker_count: u64,
    pub topics_total: u64,
    pub rpc_total: u64,
    pub active_connections: u64,
}

#[derive(Clone, Serialize)]
pub struct ClusterPageDto {
    pub timestamp: String,
    pub brokers: Vec<ClusterBrokerDto>,
    pub totals: ClusterTotalsDto,
    pub errors: Vec<String>,
}

#[derive(Clone, Serialize)]
pub struct BrokerTopicMiniDto {
    pub name: String,
    pub producers_connected: u64,
    pub consumers_connected: u64,
}

#[derive(Clone, Serialize)]
pub struct BrokerMetricsDto {
    pub rpc_total: u64,
    pub rpc_rate_1m: f64,
    pub topics_owned: u64,
    pub producers_connected: u64,
    pub consumers_connected: u64,
    pub inbound_bytes_total: u64,
    pub outbound_bytes_total: u64,
    pub errors_5xx_total: u64,
}

#[derive(Clone, Serialize)]
pub struct BrokerIdentityDto {
    pub broker_id: String,
    pub broker_addr: String,
    pub broker_role: String,
}

#[derive(Clone, Serialize)]
pub struct BrokerPageDto {
    pub timestamp: String,
    pub broker: BrokerIdentityDto,
    pub metrics: BrokerMetricsDto,
    pub topics: Vec<BrokerTopicMiniDto>,
    pub errors: Vec<String>,
}

#[derive(Clone, Serialize)]
pub struct TopicDto {
    pub name: String,
    pub type_schema: i32,
    pub schema_data: String,
    pub subscriptions: Vec<String>,
}

#[derive(Clone, Serialize)]
pub struct TopicMetricsDto {
    pub msg_in_total: u64,
    pub msg_out_total: u64,
    pub msg_backlog: u64,
    pub storage_bytes: u64,
    pub producers: u64,
    pub consumers: u64,
    pub publish_rate_1m: f64,
    pub dispatch_rate_1m: f64,
}

#[derive(Clone, Serialize)]
pub struct TopicPageDto {
    pub timestamp: String,
    pub topic: TopicDto,
    pub metrics: TopicMetricsDto,
    pub errors: Vec<String>,
}
