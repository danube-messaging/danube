use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Claims {
    pub(crate) iss: String,
    pub(crate) exp: u64,
    pub(crate) sub: String,
    pub(crate) principal_type: String,
    pub(crate) principal_name: String,
}
