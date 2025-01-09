use danube_reliable_dispatch::ReliableDispatch;

#[derive(Debug)]
pub(crate) enum DispatchStrategy {
    // Does not store messages, sends them directly to the dispatcher
    NonReliable,
    // Stores messages for reliable delivery
    Reliable(ReliableDispatch),
}
