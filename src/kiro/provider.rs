//! Kiro API Provider
//!
//! 核心组件，负责与 Kiro API 通信
//! 支持流式和非流式请求
//! 支持多凭据故障转移和重试

use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt, stream};
use reqwest::Client;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use uuid::Uuid;

use crate::common::logging::summarize_upstream_error;
use crate::http_client::{ProxyConfig, build_client};
use crate::kiro::machine_id;
use crate::kiro::model::credentials::KiroCredentials;
use crate::kiro::token_manager::{CallLease, MultiTokenManager, RuntimeRefreshLeaderRequiredError};
use crate::model::config::{RequestWeightingConfig, TlsBackend};
use parking_lot::Mutex;

/// 每个凭据的最大重试次数
const MAX_RETRIES_PER_CREDENTIAL: usize = 3;

/// 总重试次数硬上限（避免无限重试）
const MAX_TOTAL_RETRIES: usize = 9;
const DEFAULT_UPSTREAM_TIMEOUT_SECS: u64 = 720;
const STREAM_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(240);
const STREAM_TOTAL_WALL_CLOCK_BUDGET: Duration = Duration::from_secs(540);
const SLOW_UPSTREAM_HEADERS_MS: u128 = 3_000;
const SLOW_FIRST_CHUNK_MS: u128 = 3_000;
const SLOW_HEADERS_TO_FIRST_CHUNK_MS: u128 = 1_000;
const ERROR_BODY_EXCERPT_CHARS: usize = 240;

/// Kiro API Provider
///
/// 核心组件，负责与 Kiro API 通信
/// 支持多凭据故障转移和重试机制
pub struct KiroProvider {
    token_manager: Arc<MultiTokenManager>,
    /// 全局代理配置（用于凭据无自定义代理时的回退）
    global_proxy: Option<ProxyConfig>,
    /// Client 缓存：key = effective proxy config, value = reqwest::Client
    /// 不同代理配置的凭据使用不同的 Client，共享相同代理的凭据复用 Client
    client_cache: Mutex<HashMap<Option<ProxyConfig>, Client>>,
    /// TLS 后端配置
    tls_backend: TlsBackend,
}

pub struct ManagedResponse {
    response: reqwest::Response,
    _lease: CallLease,
    trace: Option<ResponseTrace>,
}

#[derive(Debug, Clone)]
pub struct RequestOptions {
    pub omit_agent_mode_header: bool,
    pub request_id: Option<String>,
    pub request_weight: f64,
}

impl Default for RequestOptions {
    fn default() -> Self {
        Self {
            omit_agent_mode_header: false,
            request_id: None,
            request_weight: 1.0,
        }
    }
}

impl RequestOptions {
    fn normalized_request_weight(&self) -> f64 {
        if self.request_weight.is_finite() && self.request_weight > 0.0 {
            self.request_weight
        } else {
            1.0
        }
    }
}

#[derive(Debug, Clone)]
pub struct PublicProviderError {
    status_code: u16,
    error_type: &'static str,
    public_message: String,
    log_message: String,
}

impl PublicProviderError {
    pub fn invalid_request(
        log_message: impl Into<String>,
        public_message: impl Into<String>,
    ) -> Self {
        Self {
            status_code: 400,
            error_type: "invalid_request_error",
            public_message: public_message.into(),
            log_message: log_message.into(),
        }
    }

    pub fn request_too_large(
        log_message: impl Into<String>,
        public_message: impl Into<String>,
    ) -> Self {
        Self {
            status_code: 413,
            error_type: "invalid_request_error",
            public_message: public_message.into(),
            log_message: log_message.into(),
        }
    }

    pub fn unprocessable_entity(
        log_message: impl Into<String>,
        public_message: impl Into<String>,
    ) -> Self {
        Self {
            status_code: 422,
            error_type: "invalid_request_error",
            public_message: public_message.into(),
            log_message: log_message.into(),
        }
    }

    pub fn gateway_timeout(
        log_message: impl Into<String>,
        public_message: impl Into<String>,
    ) -> Self {
        Self {
            status_code: 504,
            error_type: "api_error",
            public_message: public_message.into(),
            log_message: log_message.into(),
        }
    }

    pub fn status_code(&self) -> u16 {
        self.status_code
    }

    pub fn error_type(&self) -> &'static str {
        self.error_type
    }

    pub fn public_message(&self) -> &str {
        &self.public_message
    }
}

impl fmt::Display for PublicProviderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.log_message)
    }
}

impl std::error::Error for PublicProviderError {}

#[derive(Debug, Clone)]
struct ResponseTrace {
    request_id: String,
    api_type: &'static str,
    model: Option<String>,
    request_body_bytes: usize,
    credential_id: u64,
    attempt: usize,
    max_retries: usize,
    region: String,
    status_code: u16,
    overall_started_at: Instant,
    upstream_request_started_at: Instant,
    response_headers_at: Instant,
}

impl ManagedResponse {
    fn new(response: reqwest::Response, lease: CallLease, trace: Option<ResponseTrace>) -> Self {
        Self {
            response,
            _lease: lease,
            trace,
        }
    }

    pub async fn bytes(self) -> reqwest::Result<Bytes> {
        let Self {
            response,
            _lease,
            trace,
        } = self;
        let bytes = response.bytes().await?;
        if let Some(trace) = trace {
            trace.log_body_complete(bytes.len());
        }
        Ok(bytes)
    }

    pub async fn text(self) -> reqwest::Result<String> {
        let Self {
            response,
            _lease,
            trace,
        } = self;
        let text = response.text().await?;
        if let Some(trace) = trace {
            trace.log_body_complete(text.len());
        }
        Ok(text)
    }

    pub fn into_bytes_stream(self) -> BoxStream<'static, Result<Bytes, reqwest::Error>> {
        let Self {
            response,
            _lease,
            trace,
        } = self;
        let body_stream = response.bytes_stream();

        stream::unfold(
            (body_stream, _lease, trace, false, 0usize, false),
            |(mut body_stream, lease, trace, seen_first_chunk, total_bytes, finished)| async move {
                if finished {
                    return None;
                }

                match body_stream.next().await {
                    Some(Ok(chunk)) => {
                        let chunk_len = chunk.len();
                        let next_total_bytes = total_bytes + chunk_len;
                        if !seen_first_chunk {
                            if let Some(trace) = trace.as_ref() {
                                trace.log_first_chunk(chunk_len);
                            }
                        }
                        Some((
                            Ok(chunk),
                            (body_stream, lease, trace, true, next_total_bytes, false),
                        ))
                    }
                    Some(Err(err)) => {
                        if let Some(trace) = trace.as_ref() {
                            trace.log_stream_error(seen_first_chunk, total_bytes, &err);
                        }
                        Some((
                            Err(err),
                            (
                                body_stream,
                                lease,
                                trace,
                                seen_first_chunk,
                                total_bytes,
                                true,
                            ),
                        ))
                    }
                    None => {
                        if let Some(trace) = trace.as_ref() {
                            trace.log_stream_complete(seen_first_chunk, total_bytes);
                        }
                        None
                    }
                }
            },
        )
        .boxed()
    }
}

impl ResponseTrace {
    fn model_label(&self) -> &str {
        self.model.as_deref().unwrap_or("unknown")
    }

    fn log_body_complete(&self, body_len: usize) {
        tracing::info!(
            request_id = %self.request_id,
            api_type = self.api_type,
            model = self.model_label(),
            request_body_bytes = self.request_body_bytes,
            credential_id = self.credential_id,
            attempt = self.attempt,
            max_retries = self.max_retries,
            region = %self.region,
            status_code = self.status_code,
            body_len,
            total_elapsed_ms = self.overall_started_at.elapsed().as_millis(),
            upstream_elapsed_ms = self.upstream_request_started_at.elapsed().as_millis(),
            "上游响应体读取完成"
        );
    }

    fn log_first_chunk(&self, chunk_len: usize) {
        let total_elapsed_ms = self.overall_started_at.elapsed().as_millis();
        let first_chunk_wait_ms = self.upstream_request_started_at.elapsed().as_millis();
        let headers_to_first_chunk_ms = self.response_headers_at.elapsed().as_millis();
        let log_slow = first_chunk_wait_ms >= SLOW_FIRST_CHUNK_MS
            || headers_to_first_chunk_ms >= SLOW_HEADERS_TO_FIRST_CHUNK_MS;

        let request_id = &self.request_id;
        let api_type = self.api_type;
        let model = self.model_label();
        let request_body_bytes = self.request_body_bytes;
        let credential_id = self.credential_id;
        let attempt = self.attempt;
        let max_retries = self.max_retries;
        let region = &self.region;
        let status_code = self.status_code;

        if log_slow {
            tracing::warn!(
                request_id = %request_id,
                api_type,
                model,
                request_body_bytes,
                credential_id,
                attempt,
                max_retries,
                region = %region,
                status_code,
                chunk_len,
                total_elapsed_ms,
                first_chunk_wait_ms,
                headers_to_first_chunk_ms,
                "上游流首包偏慢"
            );
        } else {
            tracing::info!(
                request_id = %request_id,
                api_type,
                model,
                request_body_bytes,
                credential_id,
                attempt,
                max_retries,
                region = %region,
                status_code,
                chunk_len,
                total_elapsed_ms,
                first_chunk_wait_ms,
                headers_to_first_chunk_ms,
                "上游流首包已到达"
            );
        }
    }

    fn log_stream_complete(&self, seen_first_chunk: bool, total_bytes: usize) {
        tracing::info!(
            request_id = %self.request_id,
            api_type = self.api_type,
            model = self.model_label(),
            request_body_bytes = self.request_body_bytes,
            credential_id = self.credential_id,
            attempt = self.attempt,
            max_retries = self.max_retries,
            region = %self.region,
            status_code = self.status_code,
            seen_first_chunk,
            total_bytes,
            total_elapsed_ms = self.overall_started_at.elapsed().as_millis(),
            stream_elapsed_ms = self.response_headers_at.elapsed().as_millis(),
            "上游流读取完成"
        );
    }

    fn log_stream_error(&self, seen_first_chunk: bool, total_bytes: usize, error: &reqwest::Error) {
        tracing::warn!(
            request_id = %self.request_id,
            api_type = self.api_type,
            model = self.model_label(),
            request_body_bytes = self.request_body_bytes,
            credential_id = self.credential_id,
            attempt = self.attempt,
            max_retries = self.max_retries,
            region = %self.region,
            status_code = self.status_code,
            seen_first_chunk,
            total_bytes,
            total_elapsed_ms = self.overall_started_at.elapsed().as_millis(),
            stream_elapsed_ms = self.response_headers_at.elapsed().as_millis(),
            error = %error,
            "上游流读取失败"
        );
    }
}

impl KiroProvider {
    /// 创建带代理配置的 KiroProvider 实例
    pub fn with_proxy(token_manager: Arc<MultiTokenManager>, proxy: Option<ProxyConfig>) -> Self {
        let tls_backend = token_manager.config().tls_backend;
        // 预热：构建全局代理对应的 Client
        let initial_client =
            build_client(proxy.as_ref(), DEFAULT_UPSTREAM_TIMEOUT_SECS, tls_backend)
                .expect("创建 HTTP 客户端失败");
        let mut cache = HashMap::new();
        cache.insert(proxy.clone(), initial_client);

        Self {
            token_manager,
            global_proxy: proxy,
            client_cache: Mutex::new(cache),
            tls_backend,
        }
    }

    pub fn request_weighting_config(&self) -> RequestWeightingConfig {
        self.token_manager.request_weighting_config_snapshot()
    }

    pub fn supports_model(&self, model: &str) -> bool {
        self.token_manager.supports_model(model)
    }

    pub fn leader_message_forward_target(&self) -> anyhow::Result<Option<String>> {
        self.token_manager
            .leader_http_base_url_for_single_shared_credential_mode()
    }

    pub fn runtime_leader_http_base_url(&self) -> anyhow::Result<Option<String>> {
        self.token_manager.runtime_leader_http_base_url()
    }

    /// 根据凭据的代理配置获取（或创建并缓存）对应的 reqwest::Client
    fn client_for(&self, credentials: &KiroCredentials) -> anyhow::Result<Client> {
        let effective = credentials.effective_proxy(self.global_proxy.as_ref());
        let mut cache = self.client_cache.lock();
        if let Some(client) = cache.get(&effective) {
            return Ok(client.clone());
        }
        let client = build_client(
            effective.as_ref(),
            DEFAULT_UPSTREAM_TIMEOUT_SECS,
            self.tls_backend,
        )?;
        cache.insert(effective, client.clone());
        Ok(client)
    }

    /// 获取凭据级 API 基础 URL
    fn base_url_for(&self, credentials: &KiroCredentials) -> String {
        format!(
            "https://q.{}.amazonaws.com/generateAssistantResponse",
            credentials.effective_api_region(self.token_manager.config())
        )
    }

    /// 获取凭据级 MCP API URL
    fn mcp_url_for(&self, credentials: &KiroCredentials) -> String {
        format!(
            "https://q.{}.amazonaws.com/mcp",
            credentials.effective_api_region(self.token_manager.config())
        )
    }

    /// 获取凭据级 API 基础域名
    fn base_domain_for(&self, credentials: &KiroCredentials) -> String {
        format!(
            "q.{}.amazonaws.com",
            credentials.effective_api_region(self.token_manager.config())
        )
    }

    /// 从请求体中提取模型信息
    ///
    /// 尝试解析 JSON 请求体，提取 conversationState.currentMessage.userInputMessage.modelId
    fn extract_model_from_request(request_body: &str) -> Option<String> {
        use serde_json::Value;

        let json: Value = serde_json::from_str(request_body).ok()?;

        // 尝试提取 conversationState.currentMessage.userInputMessage.modelId
        json.get("conversationState")?
            .get("currentMessage")?
            .get("userInputMessage")?
            .get("modelId")?
            .as_str()
            .map(|s| s.to_string())
    }

    /// 将凭据的 profile_arn 注入到请求体 JSON 中
    fn inject_profile_arn(request_body: &str, profile_arn: &Option<String>) -> String {
        if let Some(arn) = profile_arn {
            if let Ok(mut json) = serde_json::from_str::<serde_json::Value>(request_body) {
                json["profileArn"] = serde_json::Value::String(arn.clone());
                if let Ok(body) = serde_json::to_string(&json) {
                    return body;
                }
            }
        }
        request_body.to_string()
    }

    fn summarize_error_body(status: reqwest::StatusCode, body: &str) -> String {
        summarize_upstream_error(status.as_u16(), body, ERROR_BODY_EXCERPT_CHARS)
    }

    fn invalid_request_public_message(body: &str) -> String {
        if body.contains("CONTENT_LENGTH_EXCEEDS_THRESHOLD") {
            return "Context window is full. Reduce conversation history, system prompt, or tools."
                .to_string();
        }
        if body.contains("Input is too long") {
            return "Input is too long. Reduce the size of your messages.".to_string();
        }
        if body.contains("Improperly formed request") {
            return "Upstream rejected the request as malformed. Review message ordering, tool payloads, and oversized inputs.".to_string();
        }
        "Upstream rejected the request as invalid. Review the request payload and try again."
            .to_string()
    }

    fn request_too_large_public_message(body: &str) -> String {
        if body.contains("CONTENT_LENGTH_EXCEEDS_THRESHOLD") {
            return "Context window is full. Reduce conversation history, system prompt, or tools."
                .to_string();
        }
        if body.contains("Input is too long") {
            return "Input is too long. Reduce the size of your messages.".to_string();
        }
        "Upstream rejected the request because the payload is too large. Reduce conversation history, attachments, or tool payloads and try again.".to_string()
    }

    fn unprocessable_public_message(body: &str) -> String {
        if body.contains("Improperly formed request") {
            return "Upstream rejected the request as malformed. Review message ordering, tool payloads, and oversized inputs.".to_string();
        }
        "Upstream could not process the request payload. Review message ordering, tool payloads, and schema compatibility.".to_string()
    }

    fn public_client_error_for_status(
        status: reqwest::StatusCode,
        api_type: &str,
        error_summary: &str,
        body: &str,
    ) -> Option<PublicProviderError> {
        match status.as_u16() {
            413 => Some(PublicProviderError::request_too_large(
                format!(
                    "{} API 请求失败: status={} {}",
                    api_type,
                    status.as_u16(),
                    error_summary
                ),
                Self::request_too_large_public_message(body),
            )),
            422 => Some(PublicProviderError::unprocessable_entity(
                format!(
                    "{} API 请求失败: status={} {}",
                    api_type,
                    status.as_u16(),
                    error_summary
                ),
                Self::unprocessable_public_message(body),
            )),
            _ => None,
        }
    }

    fn stream_timeout_public_message() -> &'static str {
        "Upstream stream exceeded the retry time budget before a usable response was produced."
    }

    fn remaining_stream_budget(
        overall_started_at: Instant,
        api_type: &str,
        request_id: &str,
    ) -> anyhow::Result<Duration> {
        STREAM_TOTAL_WALL_CLOCK_BUDGET
            .checked_sub(overall_started_at.elapsed())
            .ok_or_else(|| {
                anyhow::Error::new(PublicProviderError::gateway_timeout(
                    format!(
                        "{} API 请求超时: request_id={} total_elapsed_ms={} exceeded stream budget {}ms",
                        api_type,
                        request_id,
                        overall_started_at.elapsed().as_millis(),
                        STREAM_TOTAL_WALL_CLOCK_BUDGET.as_millis()
                    ),
                    Self::stream_timeout_public_message(),
                ))
            })
    }

    /// 发送非流式 API 请求
    ///
    /// 支持多凭据故障转移：
    /// - 400 Bad Request: 直接返回错误，不计入凭据失败
    /// - 401/403: 视为凭据/权限问题，计入失败次数并允许故障转移
    /// - 402 MONTHLY_REQUEST_COUNT: 视为额度用尽，禁用凭据并切换
    /// - 429/5xx/网络等瞬态错误: 重试但不禁用或切换凭据（避免误把所有凭据锁死）
    ///
    /// # Arguments
    /// * `request_body` - JSON 格式的请求体字符串
    ///
    /// # Returns
    /// 返回原始的 HTTP Response，不做解析
    pub async fn call_api(&self, request_body: &str) -> anyhow::Result<ManagedResponse> {
        self.call_api_with_options(request_body, RequestOptions::default())
            .await
    }

    pub async fn call_api_with_options(
        &self,
        request_body: &str,
        options: RequestOptions,
    ) -> anyhow::Result<ManagedResponse> {
        self.call_api_with_retry(request_body, false, options).await
    }

    /// 发送流式 API 请求
    ///
    /// 支持多凭据故障转移：
    /// - 400 Bad Request: 直接返回错误，不计入凭据失败
    /// - 401/403: 视为凭据/权限问题，计入失败次数并允许故障转移
    /// - 402 MONTHLY_REQUEST_COUNT: 视为额度用尽，禁用凭据并切换
    /// - 429/5xx/网络等瞬态错误: 重试但不禁用或切换凭据（避免误把所有凭据锁死）
    ///
    /// # Arguments
    /// * `request_body` - JSON 格式的请求体字符串
    ///
    /// # Returns
    /// 返回原始的 HTTP Response，调用方负责处理流式数据
    pub async fn call_api_stream(&self, request_body: &str) -> anyhow::Result<ManagedResponse> {
        self.call_api_stream_with_options(request_body, RequestOptions::default())
            .await
    }

    pub async fn call_api_stream_with_options(
        &self,
        request_body: &str,
        options: RequestOptions,
    ) -> anyhow::Result<ManagedResponse> {
        self.call_api_with_retry(request_body, true, options).await
    }

    /// 发送 MCP API 请求
    ///
    /// 用于 WebSearch 等工具调用
    ///
    /// # Arguments
    /// * `request_body` - JSON 格式的 MCP 请求体字符串
    ///
    /// # Returns
    /// 返回原始的 HTTP Response
    pub async fn call_mcp(&self, request_body: &str) -> anyhow::Result<ManagedResponse> {
        self.call_mcp_with_retry(request_body).await
    }

    /// 内部方法：带重试逻辑的 MCP API 调用
    async fn call_mcp_with_retry(&self, request_body: &str) -> anyhow::Result<ManagedResponse> {
        let total_credentials = self.token_manager.total_count();
        let max_retries = (total_credentials * MAX_RETRIES_PER_CREDENTIAL).min(MAX_TOTAL_RETRIES);
        let mut last_error: Option<anyhow::Error> = None;
        let mut force_refreshed: HashSet<u64> = HashSet::new();

        for attempt in 0..max_retries {
            // 获取调用上下文
            // MCP 调用（WebSearch 等工具）不涉及模型选择，无需按模型过滤凭据
            let ctx = match self.token_manager.acquire_context(None).await {
                Ok(c) => c,
                Err(err) => {
                    if self
                        .retry_after_leader_refresh_handoff(attempt, max_retries, &err)
                        .await
                    {
                        last_error = Some(err);
                        continue;
                    }
                    return Err(err);
                }
            };
            let (ctx_id, credentials, token, lease) = ctx.into_parts();

            let config = self.token_manager.config();
            let machine_id = match machine_id::generate_from_credentials(&credentials, config) {
                Some(id) => id,
                None => {
                    last_error = Some(anyhow::anyhow!("无法生成 machine_id，请检查凭证配置"));
                    continue;
                }
            };

            let url = self.mcp_url_for(&credentials);
            let x_amz_user_agent = format!(
                "aws-sdk-js/1.0.34 KiroIDE-{}-{}",
                config.kiro_version, machine_id
            );
            let user_agent = format!(
                "aws-sdk-js/1.0.34 ua/2.1 os/{} lang/js md/nodejs#{} api/codewhispererstreaming#1.0.34 m/E KiroIDE-{}-{}",
                config.system_version, config.node_version, config.kiro_version, machine_id
            );

            // 发送请求
            let mut request = self
                .client_for(&credentials)?
                .post(&url)
                .body(request_body.to_string())
                .header("content-type", "application/json");

            // MCP 请求需要携带 profile ARN（如果凭据中存在）
            if let Some(ref arn) = credentials.profile_arn {
                request = request.header("x-amzn-kiro-profile-arn", arn);
            }

            let response = match request
                .header("x-amz-user-agent", &x_amz_user_agent)
                .header("user-agent", &user_agent)
                .header("host", &self.base_domain_for(&credentials))
                .header("amz-sdk-invocation-id", Uuid::new_v4().to_string())
                .header("amz-sdk-request", "attempt=1; max=3")
                .header("Authorization", format!("Bearer {}", token))
                .send()
                .await
            {
                Ok(resp) => resp,
                Err(e) => {
                    tracing::warn!(
                        "MCP 请求发送失败（尝试 {}/{}）: {}",
                        attempt + 1,
                        max_retries,
                        e
                    );
                    last_error = Some(e.into());
                    if attempt + 1 < max_retries {
                        sleep(Self::retry_delay(attempt)).await;
                    }
                    continue;
                }
            };

            let status = response.status();

            // 成功响应
            if status.is_success() {
                self.token_manager.report_success(ctx_id);
                return Ok(ManagedResponse::new(response, lease, None));
            }

            // 失败响应
            let body = response.text().await.unwrap_or_default();
            let error_summary = Self::summarize_error_body(status, &body);

            // 402 额度用尽
            if status.as_u16() == 402 && Self::is_monthly_request_limit(&body) {
                let has_available = self.token_manager.report_quota_exhausted(ctx_id);
                if !has_available {
                    anyhow::bail!("MCP 请求失败（所有凭据已用尽）: {}", error_summary);
                }
                last_error = Some(anyhow::anyhow!("MCP 请求失败: {}", error_summary));
                continue;
            }

            // 400 Bad Request
            if status.as_u16() == 400 {
                anyhow::bail!("MCP 请求失败: {}", error_summary);
            }

            // 401/403 凭据问题
            if matches!(status.as_u16(), 401 | 403) {
                // token 被上游失效：先尝试 force-refresh，每凭据仅一次机会
                if Self::is_bearer_token_invalid(&body) && !force_refreshed.contains(&ctx_id) {
                    force_refreshed.insert(ctx_id);
                    tracing::info!("凭据 #{} token 疑似被上游失效，尝试强制刷新", ctx_id);
                    match self.token_manager.force_refresh_token_for(ctx_id).await {
                        Ok(_) => {
                            tracing::info!("凭据 #{} token 强制刷新成功，重试请求", ctx_id);
                            continue;
                        }
                        Err(err) => {
                            if err
                                .downcast_ref::<RuntimeRefreshLeaderRequiredError>()
                                .is_some()
                            {
                                if let Err(sync_err) =
                                    self.token_manager.sync_external_state_if_changed()
                                {
                                    tracing::warn!(
                                        "凭据 #{} 需由 leader 刷新 token，主动同步外部状态失败: {}",
                                        ctx_id,
                                        sync_err
                                    );
                                }
                                self.token_manager.defer_shared_refresh_credential(
                                    ctx_id,
                                    Self::retry_delay(attempt),
                                );
                                tracing::warn!(
                                    "凭据 #{} 需要由 leader 刷新 token，当前请求稍后重试: {}",
                                    ctx_id,
                                    err
                                );
                                last_error = Some(err);
                                continue;
                            }
                            tracing::warn!(
                                "凭据 #{} token 强制刷新失败，计入失败: {}",
                                ctx_id,
                                err
                            );
                        }
                    }
                }

                let has_available = self.token_manager.report_failure(ctx_id);
                if !has_available {
                    anyhow::bail!("MCP 请求失败（所有凭据已用尽）: {}", error_summary);
                }
                last_error = Some(anyhow::anyhow!("MCP 请求失败: {}", error_summary));
                continue;
            }

            // 瞬态错误
            if matches!(status.as_u16(), 408 | 429) || status.is_server_error() {
                if status.as_u16() == 429 {
                    self.token_manager.report_rate_limited(ctx_id);
                }
                tracing::warn!(
                    credential_id = ctx_id,
                    attempt = attempt + 1,
                    max_retries,
                    status_code = status.as_u16(),
                    error_summary = %error_summary,
                    "MCP 请求失败（上游瞬态错误）"
                );
                last_error = Some(anyhow::anyhow!("MCP 请求失败: {}", error_summary));
                if attempt + 1 < max_retries {
                    sleep(Self::retry_delay(attempt)).await;
                }
                continue;
            }

            // 其他 4xx
            if status.is_client_error() {
                anyhow::bail!("MCP 请求失败: {}", error_summary);
            }

            // 兜底
            tracing::warn!(
                credential_id = ctx_id,
                attempt = attempt + 1,
                max_retries,
                status_code = status.as_u16(),
                error_summary = %error_summary,
                "MCP 请求失败（未知错误）"
            );
            last_error = Some(anyhow::anyhow!("MCP 请求失败: {}", error_summary));
            if attempt + 1 < max_retries {
                sleep(Self::retry_delay(attempt)).await;
            }
        }

        Err(last_error.unwrap_or_else(|| {
            anyhow::anyhow!("MCP 请求失败：已达到最大重试次数（{}次）", max_retries)
        }))
    }

    /// 内部方法：带重试逻辑的 API 调用
    ///
    /// 重试策略：
    /// - 每个凭据最多重试 MAX_RETRIES_PER_CREDENTIAL 次
    /// - 默认总重试次数 = min(凭据数量 × 每凭据重试次数, MAX_TOTAL_RETRIES)
    /// - 真实 Opus 4.7 允许把所有凭据至少探测一遍，避免被前几张不支持的账号截断
    async fn call_api_with_retry(
        &self,
        request_body: &str,
        is_stream: bool,
        options: RequestOptions,
    ) -> anyhow::Result<ManagedResponse> {
        let total_credentials = self.token_manager.total_count();
        let mut last_error: Option<anyhow::Error> = None;
        let mut force_refreshed: HashSet<u64> = HashSet::new();
        let api_type = if is_stream { "流式" } else { "非流式" };
        let request_id = options
            .request_id
            .clone()
            .unwrap_or_else(|| format!("kirors-{}", Uuid::new_v4().simple()));
        let request_weight = options.normalized_request_weight();
        let overall_started_at = Instant::now();

        // 尝试从请求体中提取模型信息
        let model = Self::extract_model_from_request(request_body);
        let retry_cap = if Self::is_real_opus_4_7_model(model.as_deref()) {
            MAX_TOTAL_RETRIES.max(total_credentials)
        } else {
            MAX_TOTAL_RETRIES
        };
        let max_retries = (total_credentials * MAX_RETRIES_PER_CREDENTIAL).min(retry_cap);

        for attempt in 0..max_retries {
            let stream_budget_remaining = if is_stream {
                Some(Self::remaining_stream_budget(
                    overall_started_at,
                    api_type,
                    &request_id,
                )?)
            } else {
                None
            };
            let attempt_started_at = Instant::now();
            // 获取调用上下文（绑定 index、credentials、token）
            let ctx = match self
                .token_manager
                .acquire_context_with_weight(model.as_deref(), request_weight)
                .await
            {
                Ok(c) => c,
                Err(err) => {
                    if self
                        .retry_after_leader_refresh_handoff(attempt, max_retries, &err)
                        .await
                    {
                        last_error = Some(err);
                        continue;
                    }
                    return Err(err);
                }
            };
            let (ctx_id, credentials, token, lease) = ctx.into_parts();

            let config = self.token_manager.config();
            let machine_id = match machine_id::generate_from_credentials(&credentials, config) {
                Some(id) => id,
                None => {
                    last_error = Some(anyhow::anyhow!("无法生成 machine_id，请检查凭证配置"));
                    continue;
                }
            };

            let url = self.base_url_for(&credentials);
            let x_amz_user_agent = format!(
                "aws-sdk-js/1.0.34 KiroIDE-{}-{}",
                config.kiro_version, machine_id
            );
            let user_agent = format!(
                "aws-sdk-js/1.0.34 ua/2.1 os/{} lang/js md/nodejs#{} api/codewhispererstreaming#1.0.34 m/E KiroIDE-{}-{}",
                config.system_version, config.node_version, config.kiro_version, machine_id
            );
            let region = credentials.effective_api_region(config).to_string();
            let acquire_context_ms = attempt_started_at.elapsed().as_millis();
            let invocation_id = Uuid::new_v4().to_string();

            // 注入实际凭据的 profile_arn 到请求体
            let body = Self::inject_profile_arn(request_body, &credentials.profile_arn);
            let request_body_bytes = body.len();

            // 发送请求
            let mut request = self
                .client_for(&credentials)?
                .post(&url)
                .body(body)
                .header("content-type", "application/json")
                .header("x-amzn-codewhisperer-optout", "true")
                .header("x-amz-user-agent", &x_amz_user_agent)
                .header("user-agent", &user_agent)
                .header("host", &self.base_domain_for(&credentials))
                .header("amz-sdk-invocation-id", &invocation_id)
                .header("amz-sdk-request", "attempt=1; max=3")
                .header("Authorization", format!("Bearer {}", token));

            if !options.omit_agent_mode_header {
                request = request.header("x-amzn-kiro-agent-mode", "vibe");
            }
            if let Some(remaining_budget) = stream_budget_remaining {
                let attempt_timeout = remaining_budget.min(STREAM_ATTEMPT_TIMEOUT);
                request = request.timeout(attempt_timeout);
            }

            tracing::info!(
                request_id = %request_id,
                api_type,
                model = model.as_deref().unwrap_or("unknown"),
                credential_id = ctx_id,
                attempt = attempt + 1,
                max_retries,
                region = %region,
                stream = is_stream,
                request_body_bytes,
                request_weight,
                acquire_context_ms,
                stream_budget_remaining_ms = stream_budget_remaining
                    .map(|value| value.as_millis())
                    .unwrap_or(0),
                stream_attempt_timeout_ms = stream_budget_remaining
                    .map(|value| value.min(STREAM_ATTEMPT_TIMEOUT).as_millis())
                    .unwrap_or(0),
                omit_agent_mode_header = options.omit_agent_mode_header,
                invocation_id = %invocation_id,
                "开始调用上游 Kiro API"
            );

            let upstream_request_started_at = Instant::now();
            let response = match request.send().await {
                Ok(resp) => resp,
                Err(e) => {
                    tracing::warn!(
                        request_id = %request_id,
                        api_type,
                        model = model.as_deref().unwrap_or("unknown"),
                        credential_id = ctx_id,
                        attempt = attempt + 1,
                        max_retries,
                        region = %region,
                        stream = is_stream,
                        request_body_bytes,
                        request_weight,
                        acquire_context_ms,
                        total_elapsed_ms = overall_started_at.elapsed().as_millis(),
                        upstream_wait_ms = upstream_request_started_at.elapsed().as_millis(),
                        error = %e,
                        "API 请求发送失败"
                    );
                    // 网络错误通常是上游/链路瞬态问题，不应导致"禁用凭据"或"切换凭据"
                    // （否则一段时间网络抖动会把所有凭据都误禁用，需要重启才能恢复）
                    if e.is_timeout() {
                        last_error = Some(anyhow::Error::new(
                            PublicProviderError::gateway_timeout(
                                format!(
                                    "{} API 请求超时: request_id={} attempt={} total_elapsed_ms={} upstream_wait_ms={} error={}",
                                    api_type,
                                    request_id,
                                    attempt + 1,
                                    overall_started_at.elapsed().as_millis(),
                                    upstream_request_started_at.elapsed().as_millis(),
                                    e
                                ),
                                Self::stream_timeout_public_message(),
                            ),
                        ));
                    } else {
                        last_error = Some(e.into());
                    }
                    if attempt + 1 < max_retries {
                        sleep(Self::retry_delay(attempt)).await;
                    }
                    continue;
                }
            };

            let status = response.status();
            let response_headers_at = Instant::now();
            let upstream_headers_ms = response_headers_at
                .duration_since(upstream_request_started_at)
                .as_millis();
            let total_elapsed_ms = overall_started_at.elapsed().as_millis();

            if upstream_headers_ms >= SLOW_UPSTREAM_HEADERS_MS {
                tracing::warn!(
                    request_id = %request_id,
                    api_type,
                    model = model.as_deref().unwrap_or("unknown"),
                    credential_id = ctx_id,
                    attempt = attempt + 1,
                    max_retries,
                    region = %region,
                    stream = is_stream,
                    request_body_bytes,
                    status_code = status.as_u16(),
                    acquire_context_ms,
                    upstream_headers_ms,
                    total_elapsed_ms,
                    "上游响应头返回偏慢"
                );
            } else {
                tracing::info!(
                    request_id = %request_id,
                    api_type,
                    model = model.as_deref().unwrap_or("unknown"),
                    credential_id = ctx_id,
                    attempt = attempt + 1,
                    max_retries,
                    region = %region,
                    stream = is_stream,
                    request_body_bytes,
                    status_code = status.as_u16(),
                    acquire_context_ms,
                    upstream_headers_ms,
                    total_elapsed_ms,
                    "已收到上游响应头"
                );
            }

            // 成功响应
            if status.is_success() {
                self.token_manager.report_success(ctx_id);
                let trace = ResponseTrace {
                    request_id: request_id.clone(),
                    api_type,
                    model: model.clone(),
                    request_body_bytes,
                    credential_id: ctx_id,
                    attempt: attempt + 1,
                    max_retries,
                    region,
                    status_code: status.as_u16(),
                    overall_started_at,
                    upstream_request_started_at,
                    response_headers_at,
                };
                return Ok(ManagedResponse::new(response, lease, Some(trace)));
            }

            // 失败响应：读取 body 用于日志/错误信息
            let body = response.text().await.unwrap_or_default();
            let error_summary = Self::summarize_error_body(status, &body);

            // 402 Payment Required 且额度用尽：禁用凭据并故障转移
            if status.as_u16() == 402 && Self::is_monthly_request_limit(&body) {
                tracing::warn!(
                    request_id = %request_id,
                    api_type,
                    model = model.as_deref().unwrap_or("unknown"),
                    credential_id = ctx_id,
                    attempt = attempt + 1,
                    max_retries,
                    region = %region,
                    stream = is_stream,
                    request_body_bytes,
                    status_code = status.as_u16(),
                    error_summary = %error_summary,
                    total_elapsed_ms = overall_started_at.elapsed().as_millis(),
                    "API 请求失败（额度已用尽，禁用凭据并切换）"
                );

                let has_available = self.token_manager.report_quota_exhausted(ctx_id);
                if !has_available {
                    anyhow::bail!(
                        "{} API 请求失败（所有凭据已用尽）: {}",
                        api_type,
                        error_summary
                    );
                }

                last_error = Some(anyhow::anyhow!(
                    "{} API 请求失败: {}",
                    api_type,
                    error_summary
                ));
                continue;
            }

            // INVALID_MODEL_ID 说明“当前凭据不支持该模型”或“该模型尚未对该账号开放”，
            // 记录模型族运行时限制后应切卡继续尝试，但不要对整个账号施加全局冷却。
            if status.as_u16() == 400 {
                if Self::should_failover_model_unsupported(model.as_deref(), &body) {
                    tracing::warn!(
                        request_id = %request_id,
                        api_type,
                        model = model.as_deref().unwrap_or("unknown"),
                        credential_id = ctx_id,
                        attempt = attempt + 1,
                        max_retries,
                        region = %region,
                        stream = is_stream,
                        request_body_bytes,
                        status_code = status.as_u16(),
                        error_summary = %error_summary,
                        total_elapsed_ms = overall_started_at.elapsed().as_millis(),
                        "API 请求失败（当前凭据不支持该模型）"
                    );

                    let has_available = self.token_manager.defer_model_unsupported_credential(
                        ctx_id,
                        model.as_deref().unwrap_or("unknown"),
                    );
                    if !has_available {
                        anyhow::bail!(
                            "{} API 请求失败（所有候选凭据当前均被上游拒绝模型 {}）: {}",
                            api_type,
                            model.as_deref().unwrap_or("unknown"),
                            error_summary
                        );
                    }

                    if let Some(err) = self
                        .token_manager
                        .runtime_leader_refresh_required_for_model_candidates(
                            model.as_deref().unwrap_or("unknown"),
                        )?
                    {
                        return Err(anyhow::Error::new(err));
                    }

                    last_error = Some(anyhow::anyhow!(
                        "{} API 请求失败: {}",
                        api_type,
                        error_summary
                    ));
                    continue;
                }

                return Err(anyhow::Error::new(PublicProviderError::invalid_request(
                    format!("{} API 请求失败: {}", api_type, error_summary),
                    Self::invalid_request_public_message(&body),
                )));
            }

            // 401/403 - 更可能是凭据/权限问题：计入失败并允许故障转移
            if matches!(status.as_u16(), 401 | 403) {
                tracing::warn!(
                    request_id = %request_id,
                    api_type,
                    model = model.as_deref().unwrap_or("unknown"),
                    credential_id = ctx_id,
                    attempt = attempt + 1,
                    max_retries,
                    region = %region,
                    stream = is_stream,
                    request_body_bytes,
                    status_code = status.as_u16(),
                    error_summary = %error_summary,
                    total_elapsed_ms = overall_started_at.elapsed().as_millis(),
                    "API 请求失败（可能为凭据错误）"
                );

                // token 被上游失效：先尝试 force-refresh，每凭据仅一次机会
                if Self::is_bearer_token_invalid(&body) && !force_refreshed.contains(&ctx_id) {
                    force_refreshed.insert(ctx_id);
                    tracing::info!("凭据 #{} token 疑似被上游失效，尝试强制刷新", ctx_id);
                    match self.token_manager.force_refresh_token_for(ctx_id).await {
                        Ok(_) => {
                            tracing::info!("凭据 #{} token 强制刷新成功，重试请求", ctx_id);
                            continue;
                        }
                        Err(err) => {
                            if err
                                .downcast_ref::<RuntimeRefreshLeaderRequiredError>()
                                .is_some()
                            {
                                if let Err(sync_err) =
                                    self.token_manager.sync_external_state_if_changed()
                                {
                                    tracing::warn!(
                                        "凭据 #{} 需由 leader 刷新 token，主动同步外部状态失败: {}",
                                        ctx_id,
                                        sync_err
                                    );
                                }
                                self.token_manager.defer_shared_refresh_credential(
                                    ctx_id,
                                    Self::retry_delay(attempt),
                                );
                                tracing::warn!(
                                    "凭据 #{} 需要由 leader 刷新 token，当前请求稍后重试: {}",
                                    ctx_id,
                                    err
                                );
                                last_error = Some(err);
                                continue;
                            }
                            tracing::warn!(
                                "凭据 #{} token 强制刷新失败，计入失败: {}",
                                ctx_id,
                                err
                            );
                        }
                    }
                }

                let has_available = self.token_manager.report_failure(ctx_id);
                if !has_available {
                    anyhow::bail!(
                        "{} API 请求失败（所有凭据已用尽）: {}",
                        api_type,
                        error_summary
                    );
                }

                last_error = Some(anyhow::anyhow!(
                    "{} API 请求失败: {}",
                    api_type,
                    error_summary
                ));
                continue;
            }

            // 429/408/5xx - 瞬态上游错误：重试但不禁用或切换凭据
            // （避免 429 high traffic / 502 high load 等瞬态错误把所有凭据锁死）
            if matches!(status.as_u16(), 408 | 429) || status.is_server_error() {
                if status.as_u16() == 429 {
                    self.token_manager.report_rate_limited(ctx_id);
                }
                tracing::warn!(
                    request_id = %request_id,
                    api_type,
                    model = model.as_deref().unwrap_or("unknown"),
                    credential_id = ctx_id,
                    attempt = attempt + 1,
                    max_retries,
                    region = %region,
                    stream = is_stream,
                    request_body_bytes,
                    status_code = status.as_u16(),
                    error_summary = %error_summary,
                    total_elapsed_ms = overall_started_at.elapsed().as_millis(),
                    "API 请求失败（上游瞬态错误）"
                );
                last_error = Some(anyhow::anyhow!(
                    "{} API 请求失败: {}",
                    api_type,
                    error_summary
                ));
                if attempt + 1 < max_retries {
                    sleep(Self::retry_delay(attempt)).await;
                }
                continue;
            }

            // 其他 4xx - 通常为请求/配置问题：直接返回，不计入凭据失败
            if let Some(public_error) =
                Self::public_client_error_for_status(status, api_type, &error_summary, &body)
            {
                tracing::warn!(
                    request_id = %request_id,
                    api_type,
                    model = model.as_deref().unwrap_or("unknown"),
                    credential_id = ctx_id,
                    attempt = attempt + 1,
                    max_retries,
                    region = %region,
                    stream = is_stream,
                    request_body_bytes,
                    status_code = status.as_u16(),
                    error_summary = %error_summary,
                    total_elapsed_ms = overall_started_at.elapsed().as_millis(),
                    "API 请求失败（明确映射的客户端错误）"
                );
                return Err(anyhow::Error::new(public_error));
            }

            if status.is_client_error() {
                anyhow::bail!("{} API 请求失败: {}", api_type, error_summary);
            }

            // 兜底：当作可重试的瞬态错误处理（不切换凭据）
            tracing::warn!(
                request_id = %request_id,
                api_type,
                model = model.as_deref().unwrap_or("unknown"),
                credential_id = ctx_id,
                attempt = attempt + 1,
                max_retries,
                region = %region,
                stream = is_stream,
                request_body_bytes,
                status_code = status.as_u16(),
                error_summary = %error_summary,
                total_elapsed_ms = overall_started_at.elapsed().as_millis(),
                "API 请求失败（未知错误）"
            );
            last_error = Some(anyhow::anyhow!(
                "{} API 请求失败: {}",
                api_type,
                error_summary
            ));
            if attempt + 1 < max_retries {
                sleep(Self::retry_delay(attempt)).await;
            }
        }

        // 所有重试都失败
        Err(last_error.unwrap_or_else(|| {
            anyhow::anyhow!(
                "{} API 请求失败：已达到最大重试次数（{}次）",
                api_type,
                max_retries
            )
        }))
    }

    async fn retry_after_leader_refresh_handoff(
        &self,
        attempt: usize,
        max_retries: usize,
        err: &anyhow::Error,
    ) -> bool {
        if err
            .downcast_ref::<RuntimeRefreshLeaderRequiredError>()
            .is_none()
        {
            return false;
        }

        if let Err(sync_err) = self.token_manager.sync_external_state_if_changed() {
            tracing::warn!("leader 刷新交接重试前同步外部状态失败: {}", sync_err);
        }

        if self.token_manager.should_fast_fail_runtime_leader_refresh() {
            tracing::warn!("单共享账号需由 leader 刷新凭据，停止本地等待重试: {}", err);
            return false;
        }

        if attempt + 1 >= max_retries {
            return false;
        }

        let delay = Self::retry_delay(attempt);
        tracing::warn!(
            "当前实例需等待运行时 leader 刷新共享凭据，{}ms 后重试: {}",
            delay.as_millis(),
            err
        );
        sleep(delay).await;
        true
    }

    fn retry_delay(attempt: usize) -> Duration {
        // 指数退避 + 少量抖动，避免上游抖动时放大故障
        const BASE_MS: u64 = 200;
        const MAX_MS: u64 = 2_000;
        let exp = BASE_MS.saturating_mul(2u64.saturating_pow(attempt.min(6) as u32));
        let backoff = exp.min(MAX_MS);
        let jitter_max = (backoff / 4).max(1);
        let jitter = fastrand::u64(0..=jitter_max);
        Duration::from_millis(backoff.saturating_add(jitter))
    }

    fn is_real_opus_4_7_model(model: Option<&str>) -> bool {
        model.is_some_and(|model| {
            let lower = model.to_ascii_lowercase();
            lower.contains("claude-opus-4.7") || lower.contains("claude-opus-4-7")
        })
    }

    fn is_invalid_model_id(body: &str) -> bool {
        if body.contains("INVALID_MODEL_ID") {
            return true;
        }

        let Ok(value) = serde_json::from_str::<serde_json::Value>(body) else {
            return false;
        };

        value
            .get("reason")
            .and_then(|v| v.as_str())
            .is_some_and(|v| v == "INVALID_MODEL_ID")
            || value
                .pointer("/error/reason")
                .and_then(|v| v.as_str())
                .is_some_and(|v| v == "INVALID_MODEL_ID")
    }

    fn should_failover_model_unsupported(model: Option<&str>, body: &str) -> bool {
        model.is_some() && Self::is_invalid_model_id(body)
    }

    fn is_monthly_request_limit(body: &str) -> bool {
        if body.contains("MONTHLY_REQUEST_COUNT") {
            return true;
        }

        let Ok(value) = serde_json::from_str::<serde_json::Value>(body) else {
            return false;
        };

        if value
            .get("reason")
            .and_then(|v| v.as_str())
            .is_some_and(|v| v == "MONTHLY_REQUEST_COUNT")
        {
            return true;
        }

        value
            .pointer("/error/reason")
            .and_then(|v| v.as_str())
            .is_some_and(|v| v == "MONTHLY_REQUEST_COUNT")
    }

    /// 检查响应体是否包含 bearer token 失效的特征消息
    ///
    /// 当上游已使 accessToken 失效但本地 expiresAt 未到期时，
    /// API 会返回 401/403 并携带此特征消息。
    fn is_bearer_token_invalid(body: &str) -> bool {
        body.contains("The bearer token included in the request is invalid")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::config::Config;

    fn create_test_provider(config: Config, credentials: KiroCredentials) -> KiroProvider {
        let tm = MultiTokenManager::new(config, vec![credentials], None, None, false).unwrap();
        KiroProvider::with_proxy(Arc::new(tm), None)
    }

    #[test]
    fn test_is_monthly_request_limit_detects_reason() {
        let body = r#"{"message":"You have reached the limit.","reason":"MONTHLY_REQUEST_COUNT"}"#;
        assert!(KiroProvider::is_monthly_request_limit(body));
    }

    #[test]
    fn test_is_monthly_request_limit_nested_reason() {
        let body = r#"{"error":{"reason":"MONTHLY_REQUEST_COUNT"}}"#;
        assert!(KiroProvider::is_monthly_request_limit(body));
    }

    #[test]
    fn test_is_monthly_request_limit_false() {
        let body = r#"{"message":"nope","reason":"DAILY_REQUEST_COUNT"}"#;
        assert!(!KiroProvider::is_monthly_request_limit(body));
    }

    #[test]
    fn test_is_invalid_model_id_detects_reason() {
        let body = r#"{"message":"Invalid model. Please select a different model to continue.","reason":"INVALID_MODEL_ID"}"#;
        assert!(KiroProvider::is_invalid_model_id(body));
    }

    #[test]
    fn test_should_failover_model_unsupported_detects_invalid_model_body() {
        let body = r#"{"message":"Invalid model. Please select a different model to continue.","reason":"INVALID_MODEL_ID"}"#;
        assert!(KiroProvider::should_failover_model_unsupported(
            Some("claude-opus-4.7"),
            body
        ));
        assert!(KiroProvider::should_failover_model_unsupported(
            Some("claude-opus-4-7"),
            body
        ));
        assert!(KiroProvider::should_failover_model_unsupported(
            Some("claude-opus-4.6"),
            body
        ));
        assert!(!KiroProvider::should_failover_model_unsupported(None, body));
    }

    #[test]
    fn test_inject_profile_arn_with_some() {
        let body = r#"{"conversationState":{"conversationId":"c1"}}"#;
        let arn = Some("arn:aws:codewhisperer:us-east-1:123:profile/ABC".to_string());
        let result = KiroProvider::inject_profile_arn(body, &arn);
        let json: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(
            json["profileArn"],
            "arn:aws:codewhisperer:us-east-1:123:profile/ABC"
        );
        // 原有字段保留
        assert_eq!(json["conversationState"]["conversationId"], "c1");
    }

    #[test]
    fn test_inject_profile_arn_with_none() {
        let body = r#"{"conversationState":{"conversationId":"c1"}}"#;
        let result = KiroProvider::inject_profile_arn(body, &None);
        // 不注入 profileArn，原样返回
        let json: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(json.get("profileArn").is_none());
        assert_eq!(json["conversationState"]["conversationId"], "c1");
    }

    #[test]
    fn test_inject_profile_arn_overwrites_existing() {
        let body = r#"{"conversationState":{},"profileArn":"old-arn"}"#;
        let arn = Some("new-arn".to_string());
        let result = KiroProvider::inject_profile_arn(body, &arn);
        let json: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(json["profileArn"], "new-arn");
    }

    #[test]
    fn test_inject_profile_arn_invalid_json() {
        let body = "not-valid-json";
        let arn = Some("arn:test".to_string());
        let result = KiroProvider::inject_profile_arn(body, &arn);
        // 解析失败时原样返回
        assert_eq!(result, "not-valid-json");
    }

    #[test]
    fn test_invalid_request_public_message_special_cases_improper_form() {
        let message = KiroProvider::invalid_request_public_message(
            r#"{"message":"Improperly formed request."}"#,
        );
        assert_eq!(
            message,
            "Upstream rejected the request as malformed. Review message ordering, tool payloads, and oversized inputs."
        );
    }

    #[test]
    fn test_request_too_large_public_message_special_cases_input_too_long() {
        let message =
            KiroProvider::request_too_large_public_message(r#"{"message":"Input is too long"}"#);
        assert_eq!(
            message,
            "Input is too long. Reduce the size of your messages."
        );
    }

    #[test]
    fn test_unprocessable_public_message_special_cases_improper_form() {
        let message = KiroProvider::unprocessable_public_message(
            r#"{"message":"Improperly formed request."}"#,
        );
        assert_eq!(
            message,
            "Upstream rejected the request as malformed. Review message ordering, tool payloads, and oversized inputs."
        );
    }

    #[test]
    fn test_public_client_error_for_status_maps_413() {
        let public_error = KiroProvider::public_client_error_for_status(
            reqwest::StatusCode::PAYLOAD_TOO_LARGE,
            "非流式",
            "body_excerpt=\"Input is too long\"",
            r#"{"message":"Input is too long"}"#,
        )
        .expect("expected mapped public error");
        assert_eq!(public_error.status_code(), 413);
        assert_eq!(public_error.error_type(), "invalid_request_error");
        assert_eq!(
            public_error.public_message(),
            "Input is too long. Reduce the size of your messages."
        );
        assert_eq!(
            public_error.to_string(),
            "非流式 API 请求失败: status=413 body_excerpt=\"Input is too long\""
        );
    }

    #[test]
    fn test_public_client_error_for_status_maps_422() {
        let public_error = KiroProvider::public_client_error_for_status(
            reqwest::StatusCode::UNPROCESSABLE_ENTITY,
            "流式",
            "body_excerpt=\"Improperly formed request.\"",
            r#"{"message":"Improperly formed request."}"#,
        )
        .expect("expected mapped public error");
        assert_eq!(public_error.status_code(), 422);
        assert_eq!(public_error.error_type(), "invalid_request_error");
        assert_eq!(
            public_error.public_message(),
            "Upstream rejected the request as malformed. Review message ordering, tool payloads, and oversized inputs."
        );
        assert_eq!(
            public_error.to_string(),
            "流式 API 请求失败: status=422 body_excerpt=\"Improperly formed request.\""
        );
    }

    #[test]
    fn test_remaining_stream_budget_exhausted_returns_public_timeout_error() {
        let started_at = Instant::now() - STREAM_TOTAL_WALL_CLOCK_BUDGET - Duration::from_millis(1);
        let err =
            KiroProvider::remaining_stream_budget(started_at, "流式", "kirors-test").unwrap_err();
        let public = err
            .downcast_ref::<PublicProviderError>()
            .expect("expected public provider error");
        assert_eq!(public.status_code(), 504);
        assert_eq!(public.error_type(), "api_error");
        assert_eq!(
            public.public_message(),
            "Upstream stream exceeded the retry time budget before a usable response was produced."
        );
    }
}
