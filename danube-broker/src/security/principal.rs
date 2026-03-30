#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Principal {
    User { name: String },
    ServiceAccount { name: String },
    BrokerInternal { name: String },
    Anonymous,
}

impl Principal {
    pub(crate) fn principal_type(&self) -> &'static str {
        match self {
            Principal::User { .. } => "user",
            Principal::ServiceAccount { .. } => "service_account",
            Principal::BrokerInternal { .. } => "broker_internal",
            Principal::Anonymous => "anonymous",
        }
    }

    pub(crate) fn principal_name(&self) -> &str {
        match self {
            Principal::User { name } => name,
            Principal::ServiceAccount { name } => name,
            Principal::BrokerInternal { name } => name,
            Principal::Anonymous => "anonymous",
        }
    }
}
