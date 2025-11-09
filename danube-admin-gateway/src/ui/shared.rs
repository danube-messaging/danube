use crate::app::AppState;

pub async fn fetch_brokers(
    state: &AppState,
) -> anyhow::Result<danube_core::admin_proto::BrokerListResponse> {
    let brokers = state.client.list_brokers().await?;
    Ok(brokers)
}

pub fn resolve_metrics_endpoint(
    state: &AppState,
    br: &danube_core::admin_proto::BrokerInfo,
) -> (String, u16) {
    if !br.metrics_addr.is_empty() {
        let maddr = br
            .metrics_addr
            .trim_start_matches("http://")
            .trim_start_matches("https://");
        let mut parts = maddr.split(':');
        let host = parts.next().unwrap_or("localhost").to_string();
        let port = parts
            .next()
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(state.metrics.base_port());
        return (host, port);
    }
    let addr = br
        .broker_addr
        .trim_start_matches("http://")
        .trim_start_matches("https://");
    let mut parts = addr.split(':');
    let host = parts.next().unwrap_or("localhost").to_string();
    (host, state.metrics.base_port())
}
