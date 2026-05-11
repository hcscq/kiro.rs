//! Kiro API Provider
//!
//! 核心组件，负责与 Kiro API 通信
//! 支持流式和非流式请求
//! 支持多凭据故障转移和重试

use bytes::{Bytes, BytesMut};
use futures::stream::BoxStream;
use futures::{StreamExt, stream};
use reqwest::Client;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::{sleep, timeout};
use uuid::Uuid;

use crate::common::logging::summarize_upstream_error;
use crate::http_client::{ProxyConfig, build_client};
use crate::kiro::machine_id;
use crate::kiro::model::credentials::KiroCredentials;
use crate::kiro::model::events::Event;
use crate::kiro::parser::decoder::EventStreamDecoder;
use crate::kiro::token_manager::{
    CallLease, DisabledReason, MultiTokenManager, RuntimeRefreshLeaderRequiredError,
    RuntimeRefreshLeaseBusyError,
};
use crate::model::config::{RequestWeightingConfig, TlsBackend};
use parking_lot::Mutex;

/// 每个凭据的最大重试次数
const MAX_RETRIES_PER_CREDENTIAL: usize = 3;

/// 总重试次数硬上限（避免无限重试）
const MAX_TOTAL_RETRIES: usize = 9;
/// 同一请求内，同一优先级连续触发多少个 429 后开始下探低优先级兜底账号。
const MAX_RATE_LIMITS_PER_PRIORITY_BEFORE_SPILL: usize = 3;
const DEFAULT_UPSTREAM_TIMEOUT_SECS: u64 = 720;
const STREAM_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(240);
const STREAM_PRE_SSE_RESPONSE_BUDGET: Duration = Duration::from_secs(170);
const STREAM_TOTAL_WALL_CLOCK_BUDGET: Duration = Duration::from_secs(540);
const STREAM_FIRST_CONTENT_FAILOVER_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(60);
const STREAM_FIRST_CONTENT_FAILOVER_TOTAL_BUDGET: Duration = Duration::from_secs(150);
const STREAM_FIRST_CONTENT_FAILOVER_COOLDOWN: Duration = Duration::from_secs(120);
const MAX_SLOW_FIRST_CONTENT_FAILOVERS: usize = 1;
const MIN_STREAM_FIRST_CONTENT_FAILOVER_REMAINING: Duration = Duration::from_secs(15);
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
    body: ManagedResponseBody,
    _lease: CallLease,
    trace: Option<ResponseTrace>,
    stream_first_chunk_already_logged: bool,
}

enum ManagedResponseBody {
    Response(reqwest::Response),
    Stream(BoxStream<'static, Result<Bytes, reqwest::Error>>),
}

#[derive(Debug, Clone)]
pub struct RequestOptions {
    pub omit_agent_mode_header: bool,
    pub request_id: Option<String>,
    pub request_weight: f64,
    pub wait_for_stream_content_start: bool,
    pub stream_thinking_enabled: bool,
}

impl Default for RequestOptions {
    fn default() -> Self {
        Self {
            omit_agent_mode_header: false,
            request_id: None,
            request_weight: 1.0,
            wait_for_stream_content_start: false,
            stream_thinking_enabled: false,
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
            body: ManagedResponseBody::Response(response),
            _lease: lease,
            trace,
            stream_first_chunk_already_logged: false,
        }
    }

    fn new_stream(
        stream: BoxStream<'static, Result<Bytes, reqwest::Error>>,
        lease: CallLease,
        trace: Option<ResponseTrace>,
        first_chunk_already_logged: bool,
    ) -> Self {
        Self {
            body: ManagedResponseBody::Stream(stream),
            _lease: lease,
            trace,
            stream_first_chunk_already_logged: first_chunk_already_logged,
        }
    }

    pub async fn bytes(self) -> reqwest::Result<Bytes> {
        let Self {
            body,
            _lease,
            trace,
            stream_first_chunk_already_logged: _,
        } = self;
        let bytes = match body {
            ManagedResponseBody::Response(response) => response.bytes().await?,
            ManagedResponseBody::Stream(mut body_stream) => {
                let mut buffer = BytesMut::new();
                while let Some(chunk) = body_stream.next().await {
                    buffer.extend_from_slice(&chunk?);
                }
                buffer.freeze()
            }
        };
        if let Some(trace) = trace {
            trace.log_body_complete(bytes.len());
        }
        Ok(bytes)
    }

    pub async fn text(self) -> reqwest::Result<String> {
        let Self {
            body,
            _lease,
            trace,
            stream_first_chunk_already_logged: _,
        } = self;
        let text = match body {
            ManagedResponseBody::Response(response) => response.text().await?,
            ManagedResponseBody::Stream(mut body_stream) => {
                let mut buffer = BytesMut::new();
                while let Some(chunk) = body_stream.next().await {
                    buffer.extend_from_slice(&chunk?);
                }
                String::from_utf8_lossy(&buffer).into_owned()
            }
        };
        if let Some(trace) = trace {
            trace.log_body_complete(text.len());
        }
        Ok(text)
    }

    pub fn into_bytes_stream(self) -> BoxStream<'static, Result<Bytes, reqwest::Error>> {
        let Self {
            body,
            _lease,
            trace,
            stream_first_chunk_already_logged,
        } = self;
        let body_stream = match body {
            ManagedResponseBody::Response(response) => response.bytes_stream().boxed(),
            ManagedResponseBody::Stream(stream) => stream,
        };

        stream::unfold(
            (
                body_stream,
                _lease,
                trace,
                stream_first_chunk_already_logged,
                0usize,
                false,
            ),
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

struct StreamContentStartProbe {
    thinking_enabled: bool,
    buffer: String,
}

impl StreamContentStartProbe {
    fn new(thinking_enabled: bool) -> Self {
        Self {
            thinking_enabled,
            buffer: String::new(),
        }
    }

    fn observe(&mut self, event: &Event) -> bool {
        match event {
            Event::ToolUse(_) => true,
            Event::AssistantResponse(resp) => self.observe_assistant_content(&resp.content),
            _ => false,
        }
    }

    fn observe_assistant_content(&mut self, content: &str) -> bool {
        if content.is_empty() {
            return false;
        }
        if !self.thinking_enabled {
            return true;
        }

        self.buffer.push_str(content);
        if self.buffer.contains("<thinking>") {
            return true;
        }

        let safe_len = self
            .buffer
            .len()
            .saturating_sub("<thinking>".len())
            .min(self.buffer.len());
        safe_len > 0 && !self.buffer[..safe_len].trim().is_empty()
    }
}

enum StreamContentStartPrefetch {
    Ready {
        stream: BoxStream<'static, Result<Bytes, reqwest::Error>>,
        first_chunk_logged: bool,
        prefetched_bytes: usize,
        elapsed: Duration,
    },
    TimedOut {
        elapsed: Duration,
        prefetched_bytes: usize,
    },
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

    fn stream_pre_sse_timeout_public_message() -> &'static str {
        "Upstream stream did not produce a usable response before the retry budget was exhausted."
    }

    fn stream_pre_sse_timeout_error(
        overall_started_at: Instant,
        api_type: &str,
        request_id: &str,
    ) -> anyhow::Error {
        anyhow::Error::new(PublicProviderError::gateway_timeout(
            format!(
                "{} API 请求超时: request_id={} total_elapsed_ms={} exceeded pre-SSE stream budget {}ms",
                api_type,
                request_id,
                overall_started_at.elapsed().as_millis(),
                STREAM_PRE_SSE_RESPONSE_BUDGET.as_millis()
            ),
            Self::stream_pre_sse_timeout_public_message(),
        ))
    }

    fn remaining_stream_pre_sse_response_budget(
        overall_started_at: Instant,
        api_type: &str,
        request_id: &str,
    ) -> anyhow::Result<Duration> {
        STREAM_PRE_SSE_RESPONSE_BUDGET
            .checked_sub(overall_started_at.elapsed())
            .ok_or_else(|| {
                Self::stream_pre_sse_timeout_error(overall_started_at, api_type, request_id)
            })
    }

    fn remaining_stream_first_content_failover_budget(
        overall_started_at: Instant,
    ) -> Option<Duration> {
        STREAM_FIRST_CONTENT_FAILOVER_TOTAL_BUDGET.checked_sub(overall_started_at.elapsed())
    }

    async fn prefetch_until_stream_content_start(
        response: reqwest::Response,
        trace: &ResponseTrace,
        timeout_budget: Duration,
        thinking_enabled: bool,
    ) -> Result<StreamContentStartPrefetch, reqwest::Error> {
        let mut body_stream = response.bytes_stream().boxed();
        let mut prefetched = Vec::new();
        let mut prefetched_bytes = 0usize;
        let mut first_chunk_logged = false;
        let mut decoder = EventStreamDecoder::new();
        let mut probe = StreamContentStartProbe::new(thinking_enabled);
        let started_at = Instant::now();

        loop {
            let Some(remaining) = timeout_budget.checked_sub(started_at.elapsed()) else {
                return Ok(StreamContentStartPrefetch::TimedOut {
                    elapsed: started_at.elapsed(),
                    prefetched_bytes,
                });
            };

            match timeout(remaining, body_stream.next()).await {
                Err(_) => {
                    return Ok(StreamContentStartPrefetch::TimedOut {
                        elapsed: started_at.elapsed(),
                        prefetched_bytes,
                    });
                }
                Ok(Some(Ok(chunk))) => {
                    if !first_chunk_logged {
                        trace.log_first_chunk(chunk.len());
                        first_chunk_logged = true;
                    }
                    prefetched_bytes += chunk.len();
                    if let Err(err) = decoder.feed(&chunk) {
                        tracing::warn!(error = %err, "预读上游流时解码缓冲失败");
                    }
                    prefetched.push(chunk);

                    for result in decoder.decode_iter() {
                        match result {
                            Ok(frame) => {
                                if let Ok(event) = Event::from_frame(frame) {
                                    if probe.observe(&event) {
                                        let prefetched_stream =
                                            stream::iter(prefetched.into_iter().map(Ok));
                                        let stream = prefetched_stream.chain(body_stream).boxed();
                                        return Ok(StreamContentStartPrefetch::Ready {
                                            stream,
                                            first_chunk_logged,
                                            prefetched_bytes,
                                            elapsed: started_at.elapsed(),
                                        });
                                    }
                                }
                            }
                            Err(err) => {
                                tracing::warn!(error = %err, "预读上游流时解码事件失败");
                            }
                        }
                    }
                }
                Ok(Some(Err(err))) => return Err(err),
                Ok(None) => {
                    let prefetched_stream = stream::iter(prefetched.into_iter().map(Ok));
                    let stream = prefetched_stream.chain(body_stream).boxed();
                    return Ok(StreamContentStartPrefetch::Ready {
                        stream,
                        first_chunk_logged,
                        prefetched_bytes,
                        elapsed: started_at.elapsed(),
                    });
                }
            }
        }
    }

    async fn read_failure_body_before_sse(
        response: reqwest::Response,
        is_stream: bool,
        overall_started_at: Instant,
        api_type: &str,
        request_id: &str,
    ) -> anyhow::Result<String> {
        if !is_stream {
            return Ok(response.text().await.unwrap_or_default());
        }

        let remaining = Self::remaining_stream_pre_sse_response_budget(
            overall_started_at,
            api_type,
            request_id,
        )?;
        match timeout(remaining, response.text()).await {
            Ok(result) => Ok(result.unwrap_or_default()),
            Err(_) => Err(Self::stream_pre_sse_timeout_error(
                overall_started_at,
                api_type,
                request_id,
            )),
        }
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
    /// - 402 quota exhausted: 视为额度用尽，禁用凭据并切换
    /// - 429: 记录短冷却/退避并在当前请求内切换其他候选凭据
    /// - 5xx/网络等瞬态错误: 重试但不禁用凭据
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
    /// - 402 quota exhausted: 视为额度用尽，禁用凭据并切换
    /// - 429: 记录短冷却/退避并在当前请求内切换其他候选凭据
    /// - 5xx/网络等瞬态错误: 重试但不禁用凭据
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
        let mut max_retries = Self::base_retry_cap(total_credentials);
        let mut last_error: Option<anyhow::Error> = None;
        let mut force_refreshed: HashSet<u64> = HashSet::new();
        let mut request_scoped_rate_limited_credentials: HashSet<u64> = HashSet::new();
        let mut priority_rate_limit_hits: HashMap<u32, usize> = HashMap::new();

        let mut attempt_count = 0;
        while attempt_count < max_retries {
            let supported_candidate_count =
                self.token_manager.enabled_supported_credential_count(None);
            if Self::should_reset_rate_limited_exclusions_for_next_pass(
                request_scoped_rate_limited_credentials.len(),
                supported_candidate_count,
                attempt_count,
                max_retries,
            ) {
                request_scoped_rate_limited_credentials.clear();
            }

            let attempt = attempt_count;
            attempt_count += 1;
            // 获取调用上下文
            // MCP 调用（WebSearch 等工具）不涉及模型选择，无需按模型过滤凭据
            let ctx = match self
                .token_manager
                .acquire_context_with_weight_excluding(
                    None,
                    1.0,
                    &request_scoped_rate_limited_credentials,
                )
                .await
            {
                Ok(c) => c,
                Err(err) => {
                    if self
                        .retry_after_runtime_refresh_coordination(attempt, max_retries, &err)
                        .await
                    {
                        last_error = Some(err);
                        continue;
                    }
                    if !request_scoped_rate_limited_credentials.is_empty() && last_error.is_some() {
                        tracing::warn!(
                            attempt = attempt + 1,
                            max_retries,
                            rate_limited_credentials =
                                request_scoped_rate_limited_credentials.len(),
                            error = %err,
                            "MCP 请求已无未排除的 429 故障转移候选"
                        );
                        break;
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
            if status.as_u16() == 402 && Self::is_quota_exhausted(&body) {
                let has_available = self
                    .token_manager
                    .report_quota_exhausted_with_error(ctx_id, Some(&error_summary));
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
                                self.token_manager.defer_runtime_refresh_credential(
                                    ctx_id,
                                    self.token_manager.runtime_refresh_coordination_cooldown(),
                                );
                                if err.downcast_ref::<RuntimeRefreshLeaseBusyError>().is_some() {
                                    tracing::info!(
                                        "凭据 #{} 正等待其他实例刷新 token，当前请求稍后重试: {}",
                                        ctx_id,
                                        err
                                    );
                                } else {
                                    tracing::warn!(
                                        "凭据 #{} 需要由 leader 刷新 token，当前请求稍后重试: {}",
                                        ctx_id,
                                        err
                                    );
                                }
                                last_error = Some(err);
                                continue;
                            }
                            tracing::warn!(
                                "凭据 #{} token 强制刷新失败，将停调该凭据: {}",
                                ctx_id,
                                err
                            );
                        }
                    }
                }

                let disabled_reason = Self::disabled_reason_for_auth_status(status, &body);
                let has_available = self.token_manager.report_auth_or_permission_failure(
                    ctx_id,
                    disabled_reason,
                    status.as_u16(),
                    &error_summary,
                );
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
                    request_scoped_rate_limited_credentials.insert(ctx_id);
                    let empty_model_unsupported = HashSet::new();
                    let empty_slow_first_content = HashSet::new();
                    Self::maybe_spill_rate_limited_priority(
                        &self.token_manager,
                        None,
                        credentials.priority,
                        &mut priority_rate_limit_hits,
                        &empty_model_unsupported,
                        &mut request_scoped_rate_limited_credentials,
                        &empty_slow_first_content,
                    );
                    let supported_candidate_count =
                        self.token_manager.enabled_supported_credential_count(None);
                    max_retries = max_retries.max(Self::rate_limit_retry_cap(
                        total_credentials,
                        supported_candidate_count,
                    ));
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
    /// - 429 会扩展到当前支持该模型的候选数，并在请求内排除已 429 的凭据
    async fn call_api_with_retry(
        &self,
        request_body: &str,
        is_stream: bool,
        options: RequestOptions,
    ) -> anyhow::Result<ManagedResponse> {
        let total_credentials = self.token_manager.total_count();
        let mut last_error: Option<anyhow::Error> = None;
        let mut force_refreshed: HashSet<u64> = HashSet::new();
        let mut request_scoped_model_unsupported_credentials: HashSet<u64> = HashSet::new();
        let mut request_scoped_rate_limited_credentials: HashSet<u64> = HashSet::new();
        let mut request_scoped_slow_first_content_credentials: HashSet<u64> = HashSet::new();
        let mut priority_rate_limit_hits: HashMap<u32, usize> = HashMap::new();
        let mut slow_first_content_failovers = 0usize;
        let api_type = if is_stream { "流式" } else { "非流式" };
        let request_id = options
            .request_id
            .clone()
            .unwrap_or_else(|| format!("kirors-{}", Uuid::new_v4().simple()));
        let request_weight = options.normalized_request_weight();
        let overall_started_at = Instant::now();

        // 尝试从请求体中提取模型信息
        let model = Self::extract_model_from_request(request_body);
        let mut max_retries = Self::initial_api_retry_cap(total_credentials, model.as_deref());
        let mut attempt_count = 0;

        while attempt_count < max_retries {
            let supported_candidate_count = self
                .token_manager
                .enabled_supported_credential_count(model.as_deref());
            let retryable_candidate_count = Self::request_retryable_candidate_count(
                supported_candidate_count,
                request_scoped_model_unsupported_credentials.len(),
            );
            if Self::should_reset_rate_limited_exclusions_for_next_pass(
                request_scoped_rate_limited_credentials.len(),
                retryable_candidate_count,
                attempt_count,
                max_retries,
            ) {
                request_scoped_rate_limited_credentials.clear();
            }

            let attempt = attempt_count;
            attempt_count += 1;
            let attempt_started_at = Instant::now();
            // 获取调用上下文（绑定 index、credentials、token）
            let request_scoped_excluded_credentials = Self::combined_request_exclusions(
                &request_scoped_model_unsupported_credentials,
                &request_scoped_rate_limited_credentials,
                &request_scoped_slow_first_content_credentials,
            );
            let acquire_context = || {
                self.token_manager.acquire_context_with_weight_excluding(
                    model.as_deref(),
                    request_weight,
                    &request_scoped_excluded_credentials,
                )
            };
            let acquire_result = if is_stream {
                let remaining = Self::remaining_stream_pre_sse_response_budget(
                    overall_started_at,
                    api_type,
                    &request_id,
                )?;
                match timeout(remaining, acquire_context()).await {
                    Ok(result) => result,
                    Err(_) => {
                        return Err(Self::stream_pre_sse_timeout_error(
                            overall_started_at,
                            api_type,
                            &request_id,
                        ));
                    }
                }
            } else {
                acquire_context().await
            };
            let ctx = match acquire_result {
                Ok(c) => c,
                Err(err) => {
                    if self
                        .retry_after_runtime_refresh_coordination(attempt, max_retries, &err)
                        .await
                    {
                        last_error = Some(err);
                        continue;
                    }
                    if !request_scoped_rate_limited_credentials.is_empty() && last_error.is_some() {
                        tracing::warn!(
                            request_id = %request_id,
                            api_type,
                            model = model.as_deref().unwrap_or("unknown"),
                            attempt = attempt + 1,
                            max_retries,
                            rate_limited_credentials =
                                request_scoped_rate_limited_credentials.len(),
                            error = %err,
                            "当前请求已无未排除的 429 故障转移候选"
                        );
                        break;
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
            let stream_budget_remaining = if is_stream {
                Some(Self::remaining_stream_budget(
                    overall_started_at,
                    api_type,
                    &request_id,
                )?)
            } else {
                None
            };
            let stream_pre_sse_budget_remaining = if is_stream {
                Some(Self::remaining_stream_pre_sse_response_budget(
                    overall_started_at,
                    api_type,
                    &request_id,
                )?)
            } else {
                None
            };
            let stream_attempt_timeout = match stream_budget_remaining {
                Some(remaining_budget) => Some(remaining_budget.min(STREAM_ATTEMPT_TIMEOUT)),
                None => None,
            };
            if let Some(attempt_timeout) = stream_attempt_timeout {
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
                stream_pre_sse_budget_remaining_ms = stream_pre_sse_budget_remaining
                    .map(|value| value.as_millis())
                    .unwrap_or(0),
                stream_attempt_timeout_ms = stream_attempt_timeout
                    .map(|value| value.as_millis())
                    .unwrap_or(0),
                omit_agent_mode_header = options.omit_agent_mode_header,
                invocation_id = %invocation_id,
                "开始调用上游 Kiro API"
            );

            let upstream_request_started_at = Instant::now();
            let send_result = if let Some(pre_sse_budget) = stream_pre_sse_budget_remaining {
                match timeout(pre_sse_budget, request.send()).await {
                    Ok(result) => result,
                    Err(_) => {
                        return Err(Self::stream_pre_sse_timeout_error(
                            overall_started_at,
                            api_type,
                            &request_id,
                        ));
                    }
                }
            } else {
                request.send().await
            };
            let response = match send_result {
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
                let trace = ResponseTrace {
                    request_id: request_id.clone(),
                    api_type,
                    model: model.clone(),
                    request_body_bytes,
                    credential_id: ctx_id,
                    attempt: attempt + 1,
                    max_retries,
                    region: region.clone(),
                    status_code: status.as_u16(),
                    overall_started_at,
                    upstream_request_started_at,
                    response_headers_at,
                };

                let scoped_candidate_count = supported_candidate_count
                    .saturating_sub(request_scoped_model_unsupported_credentials.len())
                    .saturating_sub(request_scoped_rate_limited_credentials.len())
                    .saturating_sub(request_scoped_slow_first_content_credentials.len());
                let content_start_probe_budget =
                    Self::remaining_stream_first_content_failover_budget(overall_started_at);
                let should_probe_stream_content_start = is_stream
                    && options.wait_for_stream_content_start
                    && slow_first_content_failovers < MAX_SLOW_FIRST_CONTENT_FAILOVERS
                    && scoped_candidate_count > 1
                    && content_start_probe_budget.is_some_and(|remaining| {
                        remaining >= MIN_STREAM_FIRST_CONTENT_FAILOVER_REMAINING
                    });

                if should_probe_stream_content_start {
                    let prefetch_budget = content_start_probe_budget
                        .unwrap_or(STREAM_FIRST_CONTENT_FAILOVER_ATTEMPT_TIMEOUT)
                        .min(STREAM_FIRST_CONTENT_FAILOVER_ATTEMPT_TIMEOUT);
                    match Self::prefetch_until_stream_content_start(
                        response,
                        &trace,
                        prefetch_budget,
                        options.stream_thinking_enabled,
                    )
                    .await
                    {
                        Ok(StreamContentStartPrefetch::Ready {
                            stream,
                            first_chunk_logged,
                            prefetched_bytes,
                            elapsed,
                        }) => {
                            tracing::info!(
                                request_id = %request_id,
                                api_type,
                                model = model.as_deref().unwrap_or("unknown"),
                                credential_id = ctx_id,
                                attempt = attempt + 1,
                                max_retries,
                                region = %region,
                                request_body_bytes,
                                prefetched_bytes,
                                prefetch_elapsed_ms = elapsed.as_millis(),
                                "上游流预读已满足首内容块调度条件"
                            );
                            self.token_manager.report_success(ctx_id);
                            return Ok(ManagedResponse::new_stream(
                                stream,
                                lease,
                                Some(trace),
                                first_chunk_logged,
                            ));
                        }
                        Ok(StreamContentStartPrefetch::TimedOut {
                            elapsed,
                            prefetched_bytes,
                        }) => {
                            tracing::warn!(
                                request_id = %request_id,
                                api_type,
                                model = model.as_deref().unwrap_or("unknown"),
                                credential_id = ctx_id,
                                attempt = attempt + 1,
                                max_retries,
                                region = %region,
                                request_body_bytes,
                                prefetched_bytes,
                                prefetch_elapsed_ms = elapsed.as_millis(),
                                cooldown_ms = STREAM_FIRST_CONTENT_FAILOVER_COOLDOWN.as_millis(),
                                "上游流在预算内未产生可转换内容块，临时冷却凭据并切换"
                            );
                            request_scoped_slow_first_content_credentials.insert(ctx_id);
                            slow_first_content_failovers =
                                slow_first_content_failovers.saturating_add(1);
                            let _ = self.token_manager.defer_slow_first_content_credential(
                                ctx_id,
                                model.as_deref().unwrap_or("unknown"),
                                STREAM_FIRST_CONTENT_FAILOVER_COOLDOWN,
                            );
                            last_error = Some(anyhow::Error::new(
                                PublicProviderError::gateway_timeout(
                                    format!(
                                        "{} API 请求首内容块超时: request_id={} credential_id={} attempt={} prefetch_elapsed_ms={} prefetched_bytes={}",
                                        api_type,
                                        request_id,
                                        ctx_id,
                                        attempt + 1,
                                        elapsed.as_millis(),
                                        prefetched_bytes
                                    ),
                                    Self::stream_pre_sse_timeout_public_message(),
                                ),
                            ));
                            continue;
                        }
                        Err(err) => {
                            tracing::warn!(
                                request_id = %request_id,
                                api_type,
                                model = model.as_deref().unwrap_or("unknown"),
                                credential_id = ctx_id,
                                attempt = attempt + 1,
                                max_retries,
                                region = %region,
                                request_body_bytes,
                                error = %err,
                                "预读上游流首内容块失败"
                            );
                            last_error = Some(err.into());
                            if attempt + 1 < max_retries {
                                sleep(Self::retry_delay(attempt)).await;
                            }
                            continue;
                        }
                    }
                }

                self.token_manager.report_success(ctx_id);
                return Ok(ManagedResponse::new(response, lease, Some(trace)));
            }

            // 失败响应：读取 body 用于日志/错误信息
            let body = Self::read_failure_body_before_sse(
                response,
                is_stream,
                overall_started_at,
                api_type,
                &request_id,
            )
            .await?;
            let error_summary = Self::summarize_error_body(status, &body);

            // 402 Payment Required 且额度用尽：禁用凭据并故障转移
            if status.as_u16() == 402 && Self::is_quota_exhausted(&body) {
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

                let has_available = self
                    .token_manager
                    .report_quota_exhausted_with_error(ctx_id, Some(&error_summary));
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
                    request_scoped_model_unsupported_credentials.insert(ctx_id);
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

            // 401/403 - 更可能是凭据/权限问题：停调该凭据并故障转移
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
                                self.token_manager.defer_runtime_refresh_credential(
                                    ctx_id,
                                    self.token_manager.runtime_refresh_coordination_cooldown(),
                                );
                                if err.downcast_ref::<RuntimeRefreshLeaseBusyError>().is_some() {
                                    tracing::info!(
                                        "凭据 #{} 正等待其他实例刷新 token，当前请求稍后重试: {}",
                                        ctx_id,
                                        err
                                    );
                                } else {
                                    tracing::warn!(
                                        "凭据 #{} 需要由 leader 刷新 token，当前请求稍后重试: {}",
                                        ctx_id,
                                        err
                                    );
                                }
                                last_error = Some(err);
                                continue;
                            }
                            tracing::warn!(
                                "凭据 #{} token 强制刷新失败，将停调该凭据: {}",
                                ctx_id,
                                err
                            );
                        }
                    }
                }

                let disabled_reason = Self::disabled_reason_for_auth_status(status, &body);
                let has_available = self.token_manager.report_auth_or_permission_failure(
                    ctx_id,
                    disabled_reason,
                    status.as_u16(),
                    &error_summary,
                );
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

            // 429/408/5xx - 瞬态上游错误：重试但不禁用凭据。
            // 429 会进入短冷却/桶退避，并在当前请求内排除已 429 的候选以便切换。
            if matches!(status.as_u16(), 408 | 429) || status.is_server_error() {
                if status.as_u16() == 429 {
                    self.token_manager.report_rate_limited(ctx_id);
                    request_scoped_rate_limited_credentials.insert(ctx_id);
                    Self::maybe_spill_rate_limited_priority(
                        &self.token_manager,
                        model.as_deref(),
                        credentials.priority,
                        &mut priority_rate_limit_hits,
                        &request_scoped_model_unsupported_credentials,
                        &mut request_scoped_rate_limited_credentials,
                        &request_scoped_slow_first_content_credentials,
                    );
                    let supported_candidate_count = self
                        .token_manager
                        .enabled_supported_credential_count(model.as_deref());
                    max_retries = max_retries.max(Self::rate_limit_retry_cap(
                        total_credentials,
                        supported_candidate_count,
                    ));
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

    async fn retry_after_runtime_refresh_coordination(
        &self,
        attempt: usize,
        max_retries: usize,
        err: &anyhow::Error,
    ) -> bool {
        if err
            .downcast_ref::<RuntimeRefreshLeaderRequiredError>()
            .is_none()
            && err.downcast_ref::<RuntimeRefreshLeaseBusyError>().is_none()
        {
            return false;
        }

        if let Err(sync_err) = self.token_manager.sync_external_state_if_changed() {
            tracing::warn!("共享凭据刷新协调重试前同步外部状态失败: {}", sync_err);
        }

        if err
            .downcast_ref::<RuntimeRefreshLeaderRequiredError>()
            .is_some()
            && self.token_manager.should_fast_fail_runtime_leader_refresh()
        {
            tracing::warn!("单共享账号需由 leader 刷新凭据，停止本地等待重试: {}", err);
            return false;
        }

        if attempt + 1 >= max_retries {
            return false;
        }

        let delay = Self::retry_delay(attempt);
        if err.downcast_ref::<RuntimeRefreshLeaseBusyError>().is_some() {
            tracing::info!(
                "共享凭据正在由其他实例刷新，{}ms 后重试: {}",
                delay.as_millis(),
                err
            );
        } else {
            tracing::warn!(
                "当前实例需等待运行时 leader 刷新共享凭据，{}ms 后重试: {}",
                delay.as_millis(),
                err
            );
        }
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

    fn base_retry_cap(total_credentials: usize) -> usize {
        (total_credentials * MAX_RETRIES_PER_CREDENTIAL)
            .min(MAX_TOTAL_RETRIES)
            .max(1)
    }

    fn initial_api_retry_cap(total_credentials: usize, model: Option<&str>) -> usize {
        let base_cap = Self::base_retry_cap(total_credentials);
        if Self::is_real_opus_4_7_model(model) {
            base_cap.max(total_credentials).max(1)
        } else {
            base_cap
        }
    }

    fn rate_limit_retry_cap(total_credentials: usize, supported_candidate_count: usize) -> usize {
        Self::base_retry_cap(total_credentials)
            .max(supported_candidate_count)
            .max(1)
    }

    fn request_retryable_candidate_count(
        supported_candidate_count: usize,
        request_scoped_model_unsupported_count: usize,
    ) -> usize {
        supported_candidate_count.saturating_sub(request_scoped_model_unsupported_count)
    }

    fn should_reset_rate_limited_exclusions_for_next_pass(
        rate_limited_exclusion_count: usize,
        retryable_candidate_count: usize,
        attempted_count: usize,
        max_retries: usize,
    ) -> bool {
        rate_limited_exclusion_count > 0
            && retryable_candidate_count > 0
            && rate_limited_exclusion_count >= retryable_candidate_count
            && attempted_count < max_retries
    }

    fn combined_request_exclusions(
        model_unsupported_credentials: &HashSet<u64>,
        rate_limited_credentials: &HashSet<u64>,
        slow_first_content_credentials: &HashSet<u64>,
    ) -> HashSet<u64> {
        let mut excluded = model_unsupported_credentials.clone();
        excluded.extend(rate_limited_credentials.iter().copied());
        excluded.extend(slow_first_content_credentials.iter().copied());
        excluded
    }

    fn maybe_spill_rate_limited_priority(
        token_manager: &MultiTokenManager,
        model: Option<&str>,
        credential_priority: u32,
        priority_rate_limit_hits: &mut HashMap<u32, usize>,
        request_scoped_model_unsupported_credentials: &HashSet<u64>,
        request_scoped_rate_limited_credentials: &mut HashSet<u64>,
        request_scoped_slow_first_content_credentials: &HashSet<u64>,
    ) {
        let hits = priority_rate_limit_hits
            .entry(credential_priority)
            .or_insert(0);
        *hits = hits.saturating_add(1);

        if *hits < MAX_RATE_LIMITS_PER_PRIORITY_BEFORE_SPILL {
            return;
        }

        let excluded = Self::combined_request_exclusions(
            request_scoped_model_unsupported_credentials,
            request_scoped_rate_limited_credentials,
            request_scoped_slow_first_content_credentials,
        );
        if !token_manager.has_enabled_supported_credential_below_priority(
            model,
            credential_priority,
            &excluded,
        ) {
            return;
        }

        let skipped_ids =
            token_manager.enabled_supported_credential_ids_at_priority(model, credential_priority);
        let skipped_count = skipped_ids.len();
        request_scoped_rate_limited_credentials.extend(skipped_ids);
        tracing::warn!(
            priority = credential_priority,
            skipped_count,
            "同一优先级连续触发上游 429，当前请求跳过该优先级剩余候选并下探低优先级兜底账号"
        );
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

    fn is_quota_exhausted(body: &str) -> bool {
        const QUOTA_EXHAUSTED_REASONS: &[&str] =
            &["MONTHLY_REQUEST_COUNT", "OVERAGE_REQUEST_LIMIT_EXCEEDED"];

        if QUOTA_EXHAUSTED_REASONS
            .iter()
            .any(|reason| body.contains(reason))
        {
            return true;
        }

        let Ok(value) = serde_json::from_str::<serde_json::Value>(body) else {
            return false;
        };

        if value
            .get("reason")
            .and_then(|v| v.as_str())
            .is_some_and(|v| QUOTA_EXHAUSTED_REASONS.contains(&v))
        {
            return true;
        }

        value
            .pointer("/error/reason")
            .and_then(|v| v.as_str())
            .is_some_and(|v| QUOTA_EXHAUSTED_REASONS.contains(&v))
    }

    /// 检查响应体是否包含 bearer token 失效的特征消息
    ///
    /// 当上游已使 accessToken 失效但本地 expiresAt 未到期时，
    /// API 会返回 401/403 并携带此特征消息。
    fn is_bearer_token_invalid(body: &str) -> bool {
        body.contains("The bearer token included in the request is invalid")
    }

    fn is_account_suspended(body: &str) -> bool {
        let lower = body.to_ascii_lowercase();
        lower.contains("temporarily is suspended")
            || lower.contains("locked your account as a security precaution")
            || lower.contains("account is suspended")
    }

    fn disabled_reason_for_auth_status(status: reqwest::StatusCode, body: &str) -> DisabledReason {
        if status.as_u16() == 403 && Self::is_account_suspended(body) {
            DisabledReason::AccountSuspended
        } else if Self::is_bearer_token_invalid(body) || status.as_u16() == 401 {
            DisabledReason::AuthInvalid
        } else {
            DisabledReason::PermissionDenied
        }
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
    fn test_is_quota_exhausted_detects_monthly_reason() {
        let body = r#"{"message":"You have reached the limit.","reason":"MONTHLY_REQUEST_COUNT"}"#;
        assert!(KiroProvider::is_quota_exhausted(body));
    }

    #[test]
    fn test_is_quota_exhausted_detects_nested_monthly_reason() {
        let body = r#"{"error":{"reason":"MONTHLY_REQUEST_COUNT"}}"#;
        assert!(KiroProvider::is_quota_exhausted(body));
    }

    #[test]
    fn test_is_quota_exhausted_detects_overage_reason() {
        let body = r#"{"message":"You have reached the limit for overages.","reason":"OVERAGE_REQUEST_LIMIT_EXCEEDED"}"#;
        assert!(KiroProvider::is_quota_exhausted(body));
    }

    #[test]
    fn test_is_quota_exhausted_detects_nested_overage_reason() {
        let body = r#"{"error":{"reason":"OVERAGE_REQUEST_LIMIT_EXCEEDED"}}"#;
        assert!(KiroProvider::is_quota_exhausted(body));
    }

    #[test]
    fn test_is_quota_exhausted_false() {
        let body = r#"{"message":"nope","reason":"DAILY_REQUEST_COUNT"}"#;
        assert!(!KiroProvider::is_quota_exhausted(body));
    }

    #[test]
    fn test_disabled_reason_for_suspended_403() {
        let body = r#"{"message":"Your User ID temporarily is suspended. We've locked your account as a security precaution."}"#;
        assert_eq!(
            KiroProvider::disabled_reason_for_auth_status(reqwest::StatusCode::FORBIDDEN, body),
            DisabledReason::AccountSuspended
        );
    }

    #[test]
    fn test_disabled_reason_for_invalid_bearer_token() {
        let body = "The bearer token included in the request is invalid";
        assert_eq!(
            KiroProvider::disabled_reason_for_auth_status(reqwest::StatusCode::FORBIDDEN, body),
            DisabledReason::AuthInvalid
        );
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
    fn test_base_retry_cap_keeps_small_pool_per_credential_budget() {
        assert_eq!(KiroProvider::base_retry_cap(1), 3);
        assert_eq!(KiroProvider::base_retry_cap(2), 6);
        assert_eq!(KiroProvider::base_retry_cap(3), 9);
        assert_eq!(KiroProvider::base_retry_cap(4), 9);
    }

    #[test]
    fn test_rate_limit_retry_cap_scales_to_supported_candidate_count() {
        assert_eq!(KiroProvider::rate_limit_retry_cap(40, 40), 40);
        assert_eq!(KiroProvider::rate_limit_retry_cap(40, 12), 12);
        assert_eq!(KiroProvider::rate_limit_retry_cap(2, 2), 6);
    }

    #[test]
    fn test_initial_opus_retry_cap_scales_to_total_credentials() {
        assert_eq!(
            KiroProvider::initial_api_retry_cap(25, Some("claude-opus-4.7")),
            25
        );
        assert_eq!(
            KiroProvider::initial_api_retry_cap(25, Some("claude-sonnet-4.5")),
            9
        );
    }

    #[test]
    fn test_rate_limited_exclusions_reset_after_one_pass_when_budget_remains() {
        assert!(KiroProvider::should_reset_rate_limited_exclusions_for_next_pass(2, 2, 2, 6));
        assert!(!KiroProvider::should_reset_rate_limited_exclusions_for_next_pass(2, 2, 6, 6));
        assert!(!KiroProvider::should_reset_rate_limited_exclusions_for_next_pass(1, 2, 1, 6));
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

    #[test]
    fn test_remaining_stream_pre_sse_response_budget_exhausted_returns_public_timeout_error() {
        let started_at = Instant::now() - STREAM_PRE_SSE_RESPONSE_BUDGET - Duration::from_millis(1);
        let err = KiroProvider::remaining_stream_pre_sse_response_budget(
            started_at,
            "流式",
            "kirors-test",
        )
        .unwrap_err();
        let public = err
            .downcast_ref::<PublicProviderError>()
            .expect("expected public provider error");
        assert_eq!(public.status_code(), 504);
        assert_eq!(public.error_type(), "api_error");
        assert_eq!(
            public.public_message(),
            "Upstream stream did not produce a usable response before the retry budget was exhausted."
        );
    }

    #[test]
    fn test_stream_content_start_probe_non_thinking_accepts_first_text() {
        let mut probe = StreamContentStartProbe::new(false);
        assert!(probe.observe_assistant_content("hello"));
    }

    #[test]
    fn test_stream_content_start_probe_thinking_waits_for_real_start() {
        let mut probe = StreamContentStartProbe::new(true);
        assert!(!probe.observe_assistant_content("\n\n<th"));
        assert!(!probe.observe_assistant_content("inking"));
        assert!(probe.observe_assistant_content(">step 1"));
    }

    #[test]
    fn test_stream_content_start_probe_thinking_accepts_plain_text_prefix() {
        let mut probe = StreamContentStartProbe::new(true);
        assert!(probe.observe_assistant_content("This is normal text before thinking."));
    }
}
