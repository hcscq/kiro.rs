//! Anthropic API 中间件

use std::sync::Arc;

use axum::{
    body::{Body, Bytes, to_bytes},
    extract::{OriginalUri, State},
    http::{
        HeaderMap, Method, Request, StatusCode, Uri,
        header::{ACCEPT_ENCODING, CONNECTION, CONTENT_ENCODING, CONTENT_LENGTH, HOST},
    },
    middleware::Next,
    response::{IntoResponse, Json, Response},
};
use futures::TryStreamExt;
use reqwest::Client;

use crate::common::auth;
use crate::kiro::provider::KiroProvider;

use super::types::ErrorResponse;

const ANTHROPIC_FORWARDED_HEADER: &str = "x-kiro-anthropic-forwarded";
const MAX_FORWARDED_ANTHROPIC_BODY_BYTES: usize = 50 * 1024 * 1024;

struct BufferedAnthropicRequest {
    method: Method,
    uri: Uri,
    original_uri: Option<Uri>,
    headers: HeaderMap,
    body: Bytes,
}

impl BufferedAnthropicRequest {
    fn from_request_parts(parts: axum::http::request::Parts, body: Bytes) -> Self {
        let original_uri = parts
            .extensions
            .get::<OriginalUri>()
            .map(|value| value.0.clone());
        Self {
            method: parts.method,
            uri: parts.uri,
            original_uri,
            headers: parts.headers,
            body,
        }
    }

    fn route_path(&self) -> &str {
        self.original_uri
            .as_ref()
            .map(Uri::path)
            .unwrap_or_else(|| self.uri.path())
    }

    fn target_url(&self, leader_http_base_url: &str) -> String {
        let target_uri = self.original_uri.as_ref().unwrap_or(&self.uri);
        let path_and_query = target_uri
            .path_and_query()
            .map(|value| value.as_str())
            .unwrap_or_else(|| target_uri.path());
        format!(
            "{}{}",
            leader_http_base_url.trim_end_matches('/'),
            path_and_query
        )
    }
}

/// 应用共享状态
#[derive(Clone)]
pub struct AppState {
    /// API 密钥
    pub api_key: String,
    /// Kiro Provider（可选，用于实际 API 调用）
    /// 内部使用 MultiTokenManager，已支持线程安全的多凭据管理
    pub kiro_provider: Option<Arc<KiroProvider>>,
    client: Client,
}

impl AppState {
    /// 创建新的应用状态
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            kiro_provider: None,
            client: Client::new(),
        }
    }

    /// 设置 KiroProvider
    pub fn with_kiro_provider(mut self, provider: KiroProvider) -> Self {
        self.kiro_provider = Some(Arc::new(provider));
        self
    }

    fn leader_message_forward_target(&self) -> anyhow::Result<Option<String>> {
        let Some(provider) = &self.kiro_provider else {
            return Ok(None);
        };
        provider.leader_message_forward_target()
    }

    async fn forward_messages_to_leader(
        &self,
        leader_http_base_url: &str,
        request: BufferedAnthropicRequest,
    ) -> Response {
        tracing::info!(
            leader_http_base_url = %leader_http_base_url,
            path = request.route_path(),
            "单共享账号模式命中 follower，转发 messages 请求到 leader"
        );
        let mut upstream_request = self
            .client
            .request(
                request.method.clone(),
                request.target_url(leader_http_base_url),
            )
            .header(ANTHROPIC_FORWARDED_HEADER, "1")
            .header(reqwest::header::ACCEPT_ENCODING, "identity");

        for (name, value) in &request.headers {
            if name == HOST
                || name == CONTENT_LENGTH
                || name == CONNECTION
                || name == ACCEPT_ENCODING
                || name
                    .as_str()
                    .eq_ignore_ascii_case(ANTHROPIC_FORWARDED_HEADER)
            {
                continue;
            }
            upstream_request = upstream_request.header(name, value);
        }

        if !request.body.is_empty() {
            upstream_request = upstream_request.body(request.body.clone());
        }

        let upstream_response = match upstream_request.send().await {
            Ok(response) => response,
            Err(err) => {
                tracing::warn!(
                    leader_http_base_url,
                    path = request.route_path(),
                    "转发 Anthropic messages 请求到 leader 失败: {}",
                    err
                );
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(ErrorResponse::new(
                        "service_unavailable",
                        "Current instance cannot serve this request locally and forwarding to the runtime leader failed. Retry later.",
                    )),
                )
                    .into_response();
            }
        };

        let status = StatusCode::from_u16(upstream_response.status().as_u16())
            .unwrap_or(StatusCode::BAD_GATEWAY);
        let headers = upstream_response.headers().clone();
        let is_sse = headers
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("text/event-stream"));
        let mut builder = Response::builder().status(status);

        for (name, value) in &headers {
            if name == CONNECTION
                || name == CONTENT_LENGTH
                || name == CONTENT_ENCODING
                || name.as_str().eq_ignore_ascii_case("transfer-encoding")
            {
                continue;
            }
            builder = builder.header(name, value);
        }

        if is_sse {
            let body_stream = upstream_response
                .bytes_stream()
                .map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err.to_string()));
            return builder
                .body(Body::from_stream(body_stream))
                .unwrap_or_else(|err| {
                    (
                        StatusCode::BAD_GATEWAY,
                        Json(ErrorResponse::new(
                            "api_error",
                            format!("Failed to build leader streaming response: {err}"),
                        )),
                    )
                        .into_response()
                });
        }

        let response_body = match upstream_response.bytes().await {
            Ok(body) => body,
            Err(err) => {
                return (
                    StatusCode::BAD_GATEWAY,
                    Json(ErrorResponse::new(
                        "api_error",
                        format!("Failed to read leader response body: {err}"),
                    )),
                )
                    .into_response();
            }
        };

        builder
            .body(Body::from(response_body))
            .unwrap_or_else(|err| {
                (
                    StatusCode::BAD_GATEWAY,
                    Json(ErrorResponse::new(
                        "api_error",
                        format!("Failed to build leader response: {err}"),
                    )),
                )
                    .into_response()
            })
    }
}

/// API Key 认证中间件
pub async fn auth_middleware(
    State(state): State<AppState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    match auth::extract_api_key(&request) {
        Some(key) if auth::constant_time_eq(&key, &state.api_key) => next.run(request).await,
        _ => {
            let error = ErrorResponse::authentication_error();
            (StatusCode::UNAUTHORIZED, Json(error)).into_response()
        }
    }
}

pub async fn message_routing_middleware(
    State(state): State<AppState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    if request.headers().contains_key(ANTHROPIC_FORWARDED_HEADER) {
        return next.run(request).await;
    }

    let path = request
        .extensions()
        .get::<OriginalUri>()
        .map(|value| value.0.path().to_string())
        .unwrap_or_else(|| request.uri().path().to_string());

    if !requires_leader_message_routing(request.method(), &path) {
        return next.run(request).await;
    }

    let leader_http_base_url = match state.leader_message_forward_target() {
        Ok(Some(url)) => url,
        Ok(None) => return next.run(request).await,
        Err(err) => {
            tracing::warn!("解析 Anthropic leader 转发目标失败: {}", err);
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse::new(
                    "service_unavailable",
                    "Current instance cannot confirm the runtime leader for shared credential refresh. Retry later.",
                )),
            )
                .into_response();
        }
    };

    let (parts, body) = request.into_parts();
    let body = match to_bytes(body, MAX_FORWARDED_ANTHROPIC_BODY_BYTES).await {
        Ok(body) => body,
        Err(err) => {
            return (
                StatusCode::PAYLOAD_TOO_LARGE,
                Json(ErrorResponse::new(
                    "invalid_request_error",
                    format!("Failed to read request body for leader forwarding: {err}"),
                )),
            )
                .into_response();
        }
    };

    state
        .forward_messages_to_leader(
            &leader_http_base_url,
            BufferedAnthropicRequest::from_request_parts(parts, body),
        )
        .await
}

fn requires_leader_message_routing(method: &Method, path: &str) -> bool {
    if *method != Method::POST {
        return false;
    }

    matches!(
        path.trim_end_matches('/'),
        "/messages" | "/v1/messages" | "/cc/v1/messages"
    )
}

/// CORS 中间件层
///
/// **安全说明**：当前配置允许所有来源（Any），这是为了支持公开 API 服务。
/// 如果需要更严格的安全控制，请根据实际需求配置具体的允许来源、方法和头信息。
///
/// # 配置说明
/// - `allow_origin(Any)`: 允许任何来源的请求
/// - `allow_methods(Any)`: 允许任何 HTTP 方法
/// - `allow_headers(Any)`: 允许任何请求头
pub fn cors_layer() -> tower_http::cors::CorsLayer {
    use tower_http::cors::{Any, CorsLayer};

    CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any)
}

#[cfg(test)]
mod tests {
    use super::requires_leader_message_routing;
    use axum::http::Method;

    #[test]
    fn requires_leader_message_routing_only_matches_message_posts() {
        assert!(requires_leader_message_routing(&Method::POST, "/messages"));
        assert!(requires_leader_message_routing(
            &Method::POST,
            "/v1/messages"
        ));
        assert!(requires_leader_message_routing(
            &Method::POST,
            "/cc/v1/messages"
        ));
        assert!(!requires_leader_message_routing(
            &Method::POST,
            "/v1/messages/count_tokens"
        ));
        assert!(!requires_leader_message_routing(
            &Method::GET,
            "/v1/messages"
        ));
    }
}
