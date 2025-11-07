use axum::{http::StatusCode, Json};
use serde_json::Value;
use tonic::Status as GrpcStatus;

pub fn map_error(err: anyhow::Error) -> (StatusCode, Json<Value>) {
    if let Some(st) = err.downcast_ref::<GrpcStatus>() {
        let code = st.code();
        let status = match code {
            tonic::Code::InvalidArgument => StatusCode::BAD_REQUEST,
            tonic::Code::Unauthenticated => StatusCode::UNAUTHORIZED,
            tonic::Code::PermissionDenied => StatusCode::FORBIDDEN,
            tonic::Code::NotFound => StatusCode::NOT_FOUND,
            tonic::Code::DeadlineExceeded => StatusCode::GATEWAY_TIMEOUT,
            tonic::Code::Unavailable | tonic::Code::Unknown => StatusCode::BAD_GATEWAY,
            _ => StatusCode::BAD_GATEWAY,
        };
        return (
            status,
            Json(serde_json::json!({ "error": st.message(), "code": format!("{:?}", code) })),
        );
    }
    let msg = err.to_string();
    if msg == "upstream timeout" {
        return (
            StatusCode::GATEWAY_TIMEOUT,
            Json(serde_json::json!({ "error": msg })),
        );
    }
    (
        StatusCode::BAD_GATEWAY,
        Json(serde_json::json!({ "error": msg })),
    )
}
