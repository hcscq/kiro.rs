//! Anthropic API 请求提取与拒绝映射

use std::ops::{Deref, DerefMut};

use axum::{
    Json as JsonExtractor,
    extract::{
        FromRequest, Request,
        rejection::{BytesRejection, FailedToBufferBody, JsonRejection},
    },
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Json, Response},
};
use bytes::Bytes;
use serde::de::DeserializeOwned;

use super::types::ErrorResponse;

/// Anthropic 兼容消息接口允许的最大请求体大小。
pub(crate) const MAX_ANTHROPIC_BODY_SIZE_BYTES: usize = 50 * 1024 * 1024;

#[derive(Debug)]
pub(crate) struct AnthropicJson<T> {
    inner: T,
    body_len: usize,
    content_length_header: Option<u64>,
}

impl<T> AnthropicJson<T> {
    pub(crate) fn into_inner(self) -> T {
        self.inner
    }

    pub(crate) fn body_len(&self) -> usize {
        self.body_len
    }

    pub(crate) fn content_length_header(&self) -> Option<u64> {
        self.content_length_header
    }
}

impl<T> Deref for AnthropicJson<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> DerefMut for AnthropicJson<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<T, S> FromRequest<S> for AnthropicJson<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let content_length_header = content_length_header_value(req.headers());
        if !has_json_content_type(req.headers()) {
            return Err(missing_json_content_type_response());
        }

        let bytes = Bytes::from_request(req, state)
            .await
            .map_err(bytes_rejection_response)?;
        let body_len = bytes.len();

        match JsonExtractor::<T>::from_bytes(&bytes) {
            Ok(JsonExtractor(payload)) => Ok(Self {
                inner: payload,
                body_len,
                content_length_header,
            }),
            Err(rejection) => Err(json_rejection_response(rejection)),
        }
    }
}

pub(crate) fn content_length_header_value(headers: &HeaderMap) -> Option<u64> {
    let value = headers.get(header::CONTENT_LENGTH)?.to_str().ok()?.trim();
    if value.is_empty() {
        return None;
    }
    value.parse::<u64>().ok()
}

pub(crate) fn request_body_too_large_message() -> String {
    format!(
        "Request body exceeds the Anthropic API size limit ({} MiB).",
        MAX_ANTHROPIC_BODY_SIZE_BYTES / (1024 * 1024)
    )
}

pub(crate) fn request_body_too_large_response() -> Response {
    (
        StatusCode::PAYLOAD_TOO_LARGE,
        Json(ErrorResponse::new(
            "invalid_request_error",
            request_body_too_large_message(),
        )),
    )
        .into_response()
}

fn missing_json_content_type_response() -> Response {
    (
        StatusCode::UNSUPPORTED_MEDIA_TYPE,
        Json(ErrorResponse::new(
            "invalid_request_error",
            "Expected request with `Content-Type: application/json`",
        )),
    )
        .into_response()
}

fn has_json_content_type(headers: &HeaderMap) -> bool {
    let Some(content_type) = headers.get(header::CONTENT_TYPE) else {
        return false;
    };
    let Ok(content_type) = content_type.to_str() else {
        return false;
    };
    let mime = content_type
        .split(';')
        .next()
        .map(str::trim)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let Some((mime_type, mime_subtype)) = mime.split_once('/') else {
        return false;
    };
    mime_type == "application" && (mime_subtype == "json" || mime_subtype.ends_with("+json"))
}

fn bytes_rejection_response(rejection: BytesRejection) -> Response {
    let status = rejection.status();
    let message = match &rejection {
        BytesRejection::FailedToBufferBody(FailedToBufferBody::LengthLimitError(_)) => {
            request_body_too_large_message()
        }
        _ => rejection.body_text(),
    };
    let error_type = if status.is_client_error() {
        "invalid_request_error"
    } else {
        "api_error"
    };

    (status, Json(ErrorResponse::new(error_type, message))).into_response()
}

fn json_rejection_response(rejection: JsonRejection) -> Response {
    let status = rejection.status();
    let message = match &rejection {
        JsonRejection::BytesRejection(BytesRejection::FailedToBufferBody(
            FailedToBufferBody::LengthLimitError(_),
        )) => request_body_too_large_message(),
        _ => rejection.body_text(),
    };
    let error_type = if status.is_client_error() {
        "invalid_request_error"
    } else {
        "api_error"
    };

    (status, Json(ErrorResponse::new(error_type, message))).into_response()
}

#[cfg(test)]
mod tests {
    use super::{AnthropicJson, MAX_ANTHROPIC_BODY_SIZE_BYTES};
    use axum::{Router, extract::DefaultBodyLimit, http::StatusCode, routing::post};
    use serde::Deserialize;
    use tokio::net::TcpListener;

    #[derive(Debug, Deserialize)]
    struct EchoRequest {
        value: String,
    }

    async fn echo(payload: AnthropicJson<EchoRequest>) -> StatusCode {
        let _ = payload.body_len();
        let _ = payload.value.len();
        StatusCode::NO_CONTENT
    }

    #[tokio::test]
    async fn oversized_json_body_returns_anthropic_error_json() {
        let app = Router::new()
            .route("/v1/messages", post(echo))
            .layer(DefaultBodyLimit::max(MAX_ANTHROPIC_BODY_SIZE_BYTES));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let client = reqwest::Client::new();
        let payload = serde_json::json!({
            "value": "a".repeat(MAX_ANTHROPIC_BODY_SIZE_BYTES + 1024),
        });
        let response = client
            .post(format!("http://{addr}/v1/messages"))
            .header("content-type", "application/json")
            .json(&payload)
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);

        let json: serde_json::Value = response.json().await.unwrap();
        assert_eq!(json["error"]["type"], "invalid_request_error");
        assert_eq!(
            json["error"]["message"],
            "Request body exceeds the Anthropic API size limit (50 MiB)."
        );

        server.abort();
    }
}
