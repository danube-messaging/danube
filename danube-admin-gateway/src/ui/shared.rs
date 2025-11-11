use crate::app::AppState;

pub async fn fetch_brokers(
    state: &AppState,
) -> anyhow::Result<danube_core::admin_proto::BrokerListResponse> {
    let brokers = state.client.list_brokers().await?;
    Ok(brokers)
}
