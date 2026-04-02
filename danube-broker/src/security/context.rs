use crate::security::principal::Principal;
use tonic::{Request, Status};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AuthenticationMethod {
    None,
    Jwt,
    MutualTls,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SecurityContext {
    pub(crate) principal: Principal,
    pub(crate) authentication_method: AuthenticationMethod,
}

impl SecurityContext {
    pub(crate) fn anonymous() -> Self {
        Self {
            principal: Principal::Anonymous,
            authentication_method: AuthenticationMethod::None,
        }
    }

    pub(crate) fn authenticated(
        principal: Principal,
        authentication_method: AuthenticationMethod,
    ) -> Self {
        Self {
            principal,
            authentication_method,
        }
    }
}

pub(crate) fn get_security_context<T>(request: &Request<T>) -> Result<SecurityContext, Status> {
    request
        .extensions()
        .get::<SecurityContext>()
        .cloned()
        .ok_or_else(|| Status::unauthenticated("Security context missing"))
}
