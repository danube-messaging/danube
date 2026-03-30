use tonic::Status;

#[derive(Debug)]
pub(crate) enum SecurityError {
    InvalidApiKey,
    InvalidMetadata,
    InvalidToken,
    JwtNotConfigured,
    MissingCredentials,
}

impl SecurityError {
    pub(crate) fn into_status(self) -> Status {
        match self {
            SecurityError::InvalidApiKey => Status::unauthenticated("Invalid service account credential"),
            SecurityError::InvalidMetadata => Status::unauthenticated("Authentication metadata is invalid"),
            SecurityError::InvalidToken => Status::unauthenticated("Invalid token"),
            SecurityError::JwtNotConfigured => Status::failed_precondition("JWT support is not enabled"),
            SecurityError::MissingCredentials => Status::unauthenticated("Authentication credentials are missing"),
        }
    }
}
