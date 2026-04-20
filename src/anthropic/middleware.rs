//! Anthropic API 中间件

use std::{sync::Arc, time::Duration};

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

use super::{
    body_budget::{
        InflightBodyBudget, InflightBodyBudgetPermit, load_budget_from_env,
        request_body_budget_exhausted_response, request_body_budget_reservation_bytes,
    },
    extractor::{
        BufferedAnthropicBody, MAX_ANTHROPIC_BODY_SIZE_BYTES, content_length_header_value,
        request_body_too_large_response,
    },
    types::ErrorResponse,
};

const ANTHROPIC_FORWARDED_HEADER: &str = "x-kiro-anthropic-forwarded";
pub(crate) const ANTHROPIC_RUNTIME_LEADER_REQUIRED_HEADER: &str = "x-kiro-runtime-leader-required";

#[derive(Clone)]
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

    fn into_axum_request(self) -> Request<Body> {
        let buffered_body = BufferedAnthropicBody::new(self.body.clone());
        let mut request = Request::builder()
            .method(self.method)
            .uri(self.uri)
            .body(Body::from(self.body))
            .unwrap_or_else(|err| {
                panic!("failed to rebuild anthropic request for local handling: {err}");
            });
        *request.headers_mut() = self.headers;
        request.extensions_mut().insert(buffered_body);
        if let Some(original_uri) = self.original_uri {
            request.extensions_mut().insert(OriginalUri(original_uri));
        }
        request
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
    request_body_budget: Option<Arc<InflightBodyBudget>>,
}

impl AppState {
    /// 创建新的应用状态
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            kiro_provider: None,
            client: Client::builder()
                .connect_timeout(Duration::from_secs(5))
                .timeout(Duration::from_secs(600))
                .build()
                .expect("failed to build leader forwarding client"),
            request_body_budget: load_budget_from_env(),
        }
    }

    /// 设置 KiroProvider
    pub fn with_kiro_provider(mut self, provider: KiroProvider) -> Self {
        self.kiro_provider = Some(Arc::new(provider));
        self
    }

    #[cfg(test)]
    pub fn with_request_body_budget_limit_bytes(mut self, limit_bytes: u64) -> Self {
        self.request_body_budget = InflightBodyBudget::new(limit_bytes);
        self
    }

    fn leader_message_forward_target(&self) -> anyhow::Result<Option<String>> {
        let Some(provider) = &self.kiro_provider else {
            return Ok(None);
        };
        provider.leader_message_forward_target()
    }

    fn runtime_leader_http_base_url(&self) -> anyhow::Result<Option<String>> {
        let Some(provider) = &self.kiro_provider else {
            return Ok(None);
        };
        provider.runtime_leader_http_base_url()
    }

    async fn forward_messages_to_leader(
        &self,
        leader_http_base_url: &str,
        request: BufferedAnthropicRequest,
    ) -> Response {
        tracing::info!(
            leader_http_base_url = %leader_http_base_url,
            path = request.route_path(),
            "当前实例需由 runtime leader 处理，转发 messages 请求到 leader"
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
                || name
                    .as_str()
                    .eq_ignore_ascii_case(ANTHROPIC_RUNTIME_LEADER_REQUIRED_HEADER)
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

    async fn handle_locally_then_maybe_forward_to_leader(
        &self,
        leader_http_base_url: &str,
        request: Request<Body>,
        next: Next,
    ) -> Response {
        let (parts, body) = request.into_parts();
        let content_length_header = content_length_header_value(&parts.headers);
        let body = match to_bytes(body, MAX_ANTHROPIC_BODY_SIZE_BYTES).await {
            Ok(body) => body,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    body_limit_bytes = MAX_ANTHROPIC_BODY_SIZE_BYTES,
                    content_length_header = ?content_length_header,
                    "Anthropic request body exceeds size limit while buffering for leader fallback"
                );
                return request_body_too_large_response();
            }
        };
        let buffered_request = BufferedAnthropicRequest::from_request_parts(parts, body);
        let mut local_response = next.run(buffered_request.clone().into_axum_request()).await;

        if response_requires_runtime_leader_forwarding(&local_response) {
            return self
                .forward_messages_to_leader(leader_http_base_url, buffered_request)
                .await;
        }

        strip_internal_routing_headers(&mut local_response);
        local_response
    }

    fn acquire_request_body_budget(
        &self,
        path: &str,
        content_length_header: Option<u64>,
    ) -> Result<Option<InflightBodyBudgetPermit>, Response> {
        let Some(budget) = &self.request_body_budget else {
            return Ok(None);
        };
        let Some(reservation_bytes) =
            request_body_budget_reservation_bytes(path, content_length_header)
        else {
            return Ok(None);
        };

        match budget.try_acquire(reservation_bytes) {
            Some(permit) => Ok(Some(permit)),
            None => {
                tracing::warn!(
                    path,
                    budget_limit_bytes = budget.limit_bytes(),
                    budget_used_bytes = budget.used_bytes(),
                    budget_reservation_bytes = reservation_bytes,
                    content_length_header = ?content_length_header,
                    "Anthropic request rejected by in-flight body budget"
                );
                Err(request_body_budget_exhausted_response())
            }
        }
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
        let mut response = next.run(request).await;
        strip_internal_routing_headers(&mut response);
        return response;
    }

    let path = request
        .extensions()
        .get::<OriginalUri>()
        .map(|value| value.0.path().to_string())
        .unwrap_or_else(|| request.uri().path().to_string());
    let content_length_header = content_length_header_value(request.headers());

    let _budget_permit = match state.acquire_request_body_budget(&path, content_length_header) {
        Ok(permit) => permit,
        Err(response) => return response,
    };

    if !requires_leader_message_routing(request.method(), &path) {
        return next.run(request).await;
    }

    let preemptive_leader_http_base_url = match state.leader_message_forward_target() {
        Ok(url) => url,
        Err(err) => {
            tracing::warn!("解析 Anthropic leader 预转发目标失败: {}", err);
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
    let fallback_leader_http_base_url = match state.runtime_leader_http_base_url() {
        Ok(url) => url,
        Err(err) => {
            tracing::warn!(
                "解析 Anthropic leader 兜底转发目标失败，将继续本地处理: {}",
                err
            );
            None
        }
    };

    if let Some(leader_http_base_url) = preemptive_leader_http_base_url {
        let (parts, body) = request.into_parts();
        let content_length_header = content_length_header_value(&parts.headers);
        let body = match to_bytes(body, MAX_ANTHROPIC_BODY_SIZE_BYTES).await {
            Ok(body) => body,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    body_limit_bytes = MAX_ANTHROPIC_BODY_SIZE_BYTES,
                    content_length_header = ?content_length_header,
                    "Anthropic request body exceeds size limit while buffering for leader forwarding"
                );
                return request_body_too_large_response();
            }
        };

        return state
            .forward_messages_to_leader(
                &leader_http_base_url,
                BufferedAnthropicRequest::from_request_parts(parts, body),
            )
            .await;
    }

    match fallback_leader_http_base_url {
        Some(leader_http_base_url) => {
            state
                .handle_locally_then_maybe_forward_to_leader(&leader_http_base_url, request, next)
                .await
        }
        None => {
            let mut response = next.run(request).await;
            strip_internal_routing_headers(&mut response);
            response
        }
    }
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

fn response_requires_runtime_leader_forwarding(response: &Response) -> bool {
    response
        .headers()
        .contains_key(ANTHROPIC_RUNTIME_LEADER_REQUIRED_HEADER)
}

fn strip_internal_routing_headers(response: &mut Response) {
    response
        .headers_mut()
        .remove(ANTHROPIC_RUNTIME_LEADER_REQUIRED_HEADER);
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
    use super::{
        ANTHROPIC_RUNTIME_LEADER_REQUIRED_HEADER, AppState, auth_middleware,
        message_routing_middleware, requires_leader_message_routing,
        response_requires_runtime_leader_forwarding, strip_internal_routing_headers,
    };
    use axum::{
        Router,
        body::Body,
        http::{Method, Response, StatusCode},
        middleware,
        routing::post,
    };
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::{
        net::TcpListener,
        sync::Notify,
        time::{Duration, timeout},
    };

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

    #[test]
    fn runtime_leader_forwarding_marker_is_internal_only() {
        let mut response = Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .header(ANTHROPIC_RUNTIME_LEADER_REQUIRED_HEADER, "1")
            .body(Body::empty())
            .unwrap();

        assert!(response_requires_runtime_leader_forwarding(&response));
        strip_internal_routing_headers(&mut response);
        assert!(!response_requires_runtime_leader_forwarding(&response));
    }

    #[tokio::test]
    async fn request_body_budget_middleware_rejects_concurrent_large_requests() {
        let started = std::sync::Arc::new(Notify::new());
        let release = std::sync::Arc::new(Notify::new());
        let call_count = std::sync::Arc::new(AtomicUsize::new(0));
        let body = json!({
            "model": "claude-sonnet-4",
            "max_tokens": 32,
            "messages": [{
                "role": "user",
                "content": "x".repeat(512),
            }]
        })
        .to_string();
        let request_size = body.len() as u64;
        let state = AppState::new("test-key").with_request_body_budget_limit_bytes(request_size);
        let app = Router::new()
            .route(
                "/v1/messages",
                post({
                    let started = started.clone();
                    let release = release.clone();
                    let call_count = call_count.clone();
                    move || {
                        let started = started.clone();
                        let release = release.clone();
                        let call_count = call_count.clone();
                        async move {
                            let call_index = call_count.fetch_add(1, Ordering::SeqCst);
                            if call_index == 0 {
                                started.notify_one();
                                release.notified().await;
                            }
                            StatusCode::NO_CONTENT
                        }
                    }
                }),
            )
            .layer(middleware::from_fn_with_state(
                state.clone(),
                message_routing_middleware,
            ))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                auth_middleware,
            ))
            .with_state(state);

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let first = tokio::spawn({
            let body = body.clone();
            async move {
                reqwest::Client::new()
                    .post(format!("http://{addr}/v1/messages"))
                    .header("x-api-key", "test-key")
                    .header("content-type", "application/json")
                    .body(body)
                    .send()
                    .await
                    .unwrap()
            }
        });

        timeout(Duration::from_secs(5), started.notified())
            .await
            .expect("first request should reach handler");

        let second = reqwest::Client::new()
            .post(format!("http://{addr}/v1/messages"))
            .header("x-api-key", "test-key")
            .header("content-type", "application/json")
            .body(body.clone())
            .send()
            .await
            .unwrap();
        assert_eq!(second.status(), StatusCode::SERVICE_UNAVAILABLE);

        let json: serde_json::Value = second.json().await.unwrap();
        assert_eq!(json["error"]["type"], "service_unavailable");
        assert_eq!(
            json["error"]["message"],
            "Anthropic request buffering budget is exhausted. Retry later or reduce concurrent large uploads."
        );

        release.notify_waiters();

        let first = first.await.unwrap();
        assert_eq!(first.status(), StatusCode::NO_CONTENT);

        let third = reqwest::Client::new()
            .post(format!("http://{addr}/v1/messages"))
            .header("x-api-key", "test-key")
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(third.status(), StatusCode::NO_CONTENT);

        server.abort();
    }
}
