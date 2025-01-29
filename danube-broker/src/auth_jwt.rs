use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use tonic::{Request, Status};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Claims {
    pub(crate) iss: String,
    pub(crate) exp: u64,
}

/// this function intercepts the incoming request and validates the JWT token from the Authorization header
pub fn jwt_auth_interceptor(request: Request<()>, jwt_secret: &str) -> Result<Request<()>, Status> {
    if let Some(token) = request.metadata().get("authorization") {
        let token_str = token
            .to_str()
            .map_err(|_| Status::unauthenticated("Authorization token is not valid UTF-8"))?;
        validate_token(token_str, jwt_secret)
            .map_err(|_| Status::unauthenticated("Invalid token"))?;
    } else {
        return Err(Status::unauthenticated("Authorization token is missing"));
    }
    Ok(request)
}

/// create a JWT token with the given claims and secret key
pub fn create_token(
    claims: &Claims,
    jwt_secret: &str,
) -> Result<String, jsonwebtoken::errors::Error> {
    encode(
        &Header::default(),
        claims,
        &EncodingKey::from_secret(jwt_secret.as_ref()),
    )
}

/// validate the JWT token using the secret key and return the claims if the token is valid
pub fn validate_token(
    token: &str,
    jwt_secret: &str,
) -> Result<Claims, jsonwebtoken::errors::Error> {
    let validation = Validation::default();
    let token_data = decode::<Claims>(
        token,
        &DecodingKey::from_secret(jwt_secret.as_ref()),
        &validation,
    )?;
    Ok(token_data.claims)
}
