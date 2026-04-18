//! Admin API 中间件

use std::sync::Arc;

use axum::{
    body::{Body, Bytes, to_bytes},
    extract::{OriginalUri, State},
    http::{
        HeaderMap, Method, Request, StatusCode, Uri,
        header::{ACCEPT, CONTENT_TYPE},
    },
    middleware::Next,
    response::{IntoResponse, Json, Response},
};
use reqwest::Client;
use tokio::time::{Duration, sleep};

use super::service::{AdminService, AdminWriteRoute};
use super::types::AdminErrorResponse;
use crate::common::auth;

const ADMIN_FORWARDED_HEADER: &str = "x-kiro-admin-forwarded";
const MAX_FORWARDED_ADMIN_BODY_BYTES: usize = 1024 * 1024;
const ADMIN_FORWARD_RETRY_DELAY: Duration = Duration::from_millis(250);
const ADMIN_FORWARD_RETRY_ATTEMPTS: usize = 36;

enum ForwardAdminError {
    Retryable(String),
    Terminal(Response),
}

struct BufferedAdminRequest {
    method: Method,
    uri: Uri,
    original_uri: Option<Uri>,
    headers: HeaderMap,
    body: Bytes,
}

impl BufferedAdminRequest {
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
        let mut request = Request::builder()
            .method(self.method)
            .uri(self.uri)
            .body(Body::from(self.body))
            .unwrap_or_else(|err| {
                panic!("failed to rebuild admin request for local retry: {err}");
            });
        *request.headers_mut() = self.headers;
        if let Some(original_uri) = self.original_uri {
            request.extensions_mut().insert(OriginalUri(original_uri));
        }
        request
    }

    fn route_path(&self) -> &str {
        self.original_uri
            .as_ref()
            .map(Uri::path)
            .unwrap_or_else(|| self.uri.path())
    }

    fn is_retryable_write(&self) -> bool {
        is_retryable_admin_write_route(&self.method, self.route_path())
    }
}

/// Admin API 共享状态
#[derive(Clone)]
pub struct AdminState {
    /// Admin API 密钥
    pub admin_api_key: String,
    /// Admin 服务
    pub service: Arc<AdminService>,
    client: Client,
}

impl AdminState {
    pub fn new(admin_api_key: impl Into<String>, service: AdminService) -> Self {
        Self {
            admin_api_key: admin_api_key.into(),
            service: Arc::new(service),
            client: Client::new(),
        }
    }

    async fn forward_to_leader(
        &self,
        leader_http_base_url: &str,
        request: &BufferedAdminRequest,
    ) -> Result<Response, ForwardAdminError> {
        let mut upstream_request = self
            .client
            .request(
                request.method.clone(),
                request.target_url(leader_http_base_url),
            )
            .header("x-api-key", &self.admin_api_key)
            .header(ADMIN_FORWARDED_HEADER, "1");

        if let Some(value) = request.headers.get(CONTENT_TYPE).cloned() {
            upstream_request = upstream_request.header(CONTENT_TYPE, value);
        }
        if let Some(value) = request.headers.get(ACCEPT).cloned() {
            upstream_request = upstream_request.header(ACCEPT, value);
        }
        if !request.body.is_empty() {
            upstream_request = upstream_request.body(request.body.clone());
        }

        let upstream_response = match upstream_request.send().await {
            Ok(response) => response,
            Err(err) => {
                return Err(ForwardAdminError::Retryable(format!(
                    "转发 Admin 写请求到 leader 失败: {}",
                    err
                )));
            }
        };

        let status = StatusCode::from_u16(upstream_response.status().as_u16())
            .unwrap_or(StatusCode::BAD_GATEWAY);
        let content_type = upstream_response.headers().get(CONTENT_TYPE).cloned();
        let response_body = match upstream_response.bytes().await {
            Ok(body) => body,
            Err(err) => {
                return Err(ForwardAdminError::Terminal(
                    (
                        StatusCode::BAD_GATEWAY,
                        Json(AdminErrorResponse::api_error(format!(
                            "读取 leader 响应失败: {}",
                            err
                        ))),
                    )
                        .into_response(),
                ));
            }
        };

        let mut builder = Response::builder().status(status);
        if let Some(content_type) = content_type {
            builder = builder.header(CONTENT_TYPE, content_type);
        }

        builder.body(Body::from(response_body)).map_err(|err| {
            let response = (
                StatusCode::BAD_GATEWAY,
                Json(AdminErrorResponse::api_error(format!(
                    "构建 leader 响应失败: {}",
                    err
                ))),
            )
                .into_response();
            ForwardAdminError::Terminal(response)
        })
    }

    async fn retry_forward_or_handle_locally(
        &self,
        leader_http_base_url: String,
        request: Request<Body>,
        next: Next,
    ) -> Response {
        let (parts, body) = request.into_parts();
        let body = match to_bytes(body, MAX_FORWARDED_ADMIN_BODY_BYTES).await {
            Ok(body) => body,
            Err(err) => {
                return (
                    StatusCode::PAYLOAD_TOO_LARGE,
                    Json(AdminErrorResponse::invalid_request(format!(
                        "读取 Admin 请求体失败: {}",
                        err
                    ))),
                )
                    .into_response();
            }
        };
        let buffered_request = BufferedAdminRequest::from_request_parts(parts, body);
        let retryable = buffered_request.is_retryable_write();
        let max_attempts = if retryable {
            ADMIN_FORWARD_RETRY_ATTEMPTS
        } else {
            1
        };

        let mut current_route = AdminWriteRoute::Forward(leader_http_base_url);
        let mut last_retryable_error: Option<String> = None;

        for attempt in 0..max_attempts {
            match current_route {
                AdminWriteRoute::Local => {
                    return next.run(buffered_request.into_axum_request()).await;
                }
                AdminWriteRoute::Forward(ref leader_http_base_url) => {
                    match self
                        .forward_to_leader(leader_http_base_url, &buffered_request)
                        .await
                    {
                        Ok(response) => return response,
                        Err(ForwardAdminError::Terminal(response)) => return response,
                        Err(ForwardAdminError::Retryable(err)) => {
                            last_retryable_error = Some(err);
                        }
                    }
                }
            }

            if attempt + 1 >= max_attempts {
                break;
            }

            sleep(ADMIN_FORWARD_RETRY_DELAY).await;

            match self.service.resolve_write_route() {
                Ok(route) => current_route = route,
                Err(super::error::AdminServiceError::NotLeader { .. }) => continue,
                Err(err) => return (err.status_code(), Json(err.into_response())).into_response(),
            }
        }

        (
            StatusCode::BAD_GATEWAY,
            Json(AdminErrorResponse::api_error(if retryable {
                format!(
                    "转发 Admin 写请求到 leader 失败，{} 次重试后仍未恢复: {}",
                    max_attempts,
                    last_retryable_error.unwrap_or_else(|| "未知错误".to_string())
                )
            } else {
                format!(
                    "转发非幂等 Admin 写请求到 leader 失败，已停止自动重试以避免重复写入: {}",
                    last_retryable_error.unwrap_or_else(|| "未知错误".to_string())
                )
            })),
        )
            .into_response()
    }
}

/// Admin API 认证中间件
pub async fn admin_auth_middleware(
    State(state): State<AdminState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let api_key = auth::extract_api_key(&request);

    match api_key {
        Some(key) if auth::constant_time_eq(&key, &state.admin_api_key) => next.run(request).await,
        _ => {
            let error = AdminErrorResponse::authentication_error();
            (StatusCode::UNAUTHORIZED, Json(error)).into_response()
        }
    }
}

pub async fn admin_write_routing_middleware(
    State(state): State<AdminState>,
    request: Request<Body>,
    next: Next,
) -> Response {
    if !requires_leader_routing(request.method())
        || request.headers().contains_key(ADMIN_FORWARDED_HEADER)
    {
        return next.run(request).await;
    }

    match state.service.resolve_write_route() {
        Ok(AdminWriteRoute::Local) => next.run(request).await,
        Ok(AdminWriteRoute::Forward(leader_http_base_url)) => {
            state
                .retry_forward_or_handle_locally(leader_http_base_url, request, next)
                .await
        }
        Err(err) => (err.status_code(), Json(err.into_response())).into_response(),
    }
}

fn requires_leader_routing(method: &Method) -> bool {
    !matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS)
}

fn is_retryable_admin_write_route(method: &Method, path: &str) -> bool {
    let path = path
        .strip_prefix("/api/admin")
        .unwrap_or(path)
        .trim_end_matches('/');

    match *method {
        Method::PUT => path == "/config/load-balancing" || path == "/config/model-capabilities",
        Method::POST => retryable_credential_admin_action(path),
        _ => false,
    }
}

fn retryable_credential_admin_action(path: &str) -> bool {
    let segments: Vec<_> = path.trim_matches('/').split('/').collect();
    if segments.len() != 3 || segments.first() != Some(&"credentials") {
        return false;
    }
    if segments[1].parse::<u64>().is_err() {
        return false;
    }

    matches!(
        segments[2],
        "disabled" | "priority" | "max-concurrency" | "rate-limit-config" | "model-policy"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryable_admin_write_route_only_allows_whitelisted_paths() {
        assert!(is_retryable_admin_write_route(
            &Method::PUT,
            "/api/admin/config/load-balancing",
        ));
        assert!(is_retryable_admin_write_route(
            &Method::PUT,
            "/api/admin/config/model-capabilities",
        ));
        assert!(is_retryable_admin_write_route(
            &Method::POST,
            "/api/admin/credentials/12/disabled",
        ));
        assert!(is_retryable_admin_write_route(
            &Method::POST,
            "/api/admin/credentials/12/priority",
        ));
        assert!(is_retryable_admin_write_route(
            &Method::POST,
            "/api/admin/credentials/12/max-concurrency",
        ));
        assert!(is_retryable_admin_write_route(
            &Method::POST,
            "/api/admin/credentials/12/rate-limit-config",
        ));
        assert!(is_retryable_admin_write_route(
            &Method::POST,
            "/api/admin/credentials/12/model-policy",
        ));

        assert!(!is_retryable_admin_write_route(
            &Method::POST,
            "/api/admin/credentials",
        ));
        assert!(!is_retryable_admin_write_route(
            &Method::DELETE,
            "/api/admin/credentials/12",
        ));
        assert!(!is_retryable_admin_write_route(
            &Method::POST,
            "/api/admin/credentials/12/reset",
        ));
        assert!(!is_retryable_admin_write_route(
            &Method::POST,
            "/api/admin/credentials/12/refresh",
        ));
    }
}
