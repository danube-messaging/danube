use danube_core::message::MessageID;

#[derive(Debug, Clone)]
pub(crate) struct AckMessage {
    pub(crate) request_id: u64,
    pub(crate) msg_id: MessageID,
    pub(crate) subscription_name: String,
}
