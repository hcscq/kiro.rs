//! Kiro API Provider
//!
//! 核心组件，负责与 Kiro API 通信
//! 支持流式和非流式请求
//! 支持多凭据故障转移和重试

use bytes::{Bytes, BytesMut};
use futures::stream::BoxStream;
use futures::{StreamExt, stream};
use reqwest::{Client, header::HeaderMap};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::error::Error as StdError;
use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::time::{sleep, timeout};
use uuid::Uuid;

use crate::common::logging::{summarize_text_for_log, summarize_upstream_error};
use crate::http_client::{ProxyConfig, build_client};
use crate::kiro::machine_id;
use crate::kiro::model::credentials::KiroCredentials;
use crate::kiro::model::events::Event;
use crate::kiro::parser::decoder::EventStreamDecoder;
use crate::kiro::token_manager::{
    CallLease, DisabledReason, MultiTokenManager, RuntimeRefreshLeaderRequiredError,
    RuntimeRefreshLeaseBusyError,
};
use crate::model::config::{
    Config, RequestWeightingConfig, ServerWebToolsMode, StreamPreSseFailoverConfig,
    ThinkingSignatureValidationMode, TlsBackend,
};
use parking_lot::Mutex;

/// 每个凭据的最大重试次数
const MAX_RETRIES_PER_CREDENTIAL: usize = 3;

/// 总重试次数硬上限（避免无限重试）
const MAX_TOTAL_RETRIES: usize = 9;
/// 429/容量不足时最多探测的候选凭据数量，避免上游容量抖动时把全池扫穿。
const MAX_RATE_LIMIT_RETRY_CANDIDATES: usize = 24;
/// 真实 Opus 4.7/4.8 模型也只做有限扇出探测，避免大凭据池下单请求长期占用本地资源。
const MAX_OPUS_4_7_RETRY_CANDIDATES: usize = 24;
/// 同一请求内，同一优先级连续触发多少个 429 后开始下探低优先级兜底账号。
const MAX_RATE_LIMITS_PER_PRIORITY_BEFORE_SPILL: usize = 3;
const DEFAULT_UPSTREAM_TIMEOUT_SECS: u64 = 720;
const STREAM_PRE_SSE_RESPONSE_BUDGET: Duration = Duration::from_secs(170);
const STREAM_TOTAL_WALL_CLOCK_BUDGET: Duration = Duration::from_secs(540);
const STREAM_FIRST_CONTENT_FAILOVER_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(60);
const STREAM_FIRST_CONTENT_FAILOVER_TOTAL_BUDGET: Duration = Duration::from_secs(165);
const STREAM_FIRST_CONTENT_FAILOVER_COOLDOWN: Duration = Duration::from_secs(15);
const STREAM_CONTENT_START_ACTIVITY_READY_BYTES: usize = 8 * 1024;
const INSUFFICIENT_CAPACITY_COOLDOWN: Duration = Duration::from_secs(10);
const MAX_SLOW_FIRST_CONTENT_FAILOVERS: usize = 2;
const MIN_STREAM_FIRST_CONTENT_FAILOVER_REMAINING: Duration = Duration::from_secs(15);
const SLOW_FIRST_CONTENT_SHARED_COOLDOWN_MAX_REQUEST_BODY_BYTES: usize = 1024 * 1024;
const OPUS_4_7_SLOW_MODEL_COOLDOWN: Duration = Duration::from_secs(120);
const OPUS_4_7_SLOW_MODEL_HEADERS_MS: u128 = 20_000;
const OPUS_4_7_SLOW_MODEL_FIRST_CHUNK_MS: u128 = 20_000;
const SLOW_UPSTREAM_HEADERS_MS: u128 = 3_000;
const SLOW_FIRST_CHUNK_MS: u128 = 3_000;
const SLOW_HEADERS_TO_FIRST_CHUNK_MS: u128 = 1_000;
const ERROR_BODY_EXCERPT_CHARS: usize = 240;
const LARGE_PROVIDER_REQUEST_WARN_THRESHOLD_BYTES: usize = 16 * 1024 * 1024;
const AMAZON_EVENTSTREAM_CONTENT_TYPE: &str = "application/vnd.amazon.eventstream";
const MAX_NON_STREAM_EVENTSTREAM_STALL_FAILOVERS: usize = 1;
const CONTEXT_LENGTH_EXCEEDED_CODE: &str = "context_length_exceeded";
const CONTEXT_LENGTH_EXCEEDED_PUBLIC_MESSAGE: &str = "prompt is too long: context window is full. Reduce conversation history, system prompt, or tools.";
const DEFAULT_CAPTURE_400_DIR: &str = "/app/diagnostics/400-bodies";
const DEFAULT_CAPTURE_400_MAX_PER_CLASS: usize = 3;
const DEFAULT_CAPTURE_400_MAX_BODY_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_CAPTURE_400_TTL_HOURS: u64 = 24;
const DEFAULT_CAPTURE_400_MAX_TOTAL_BYTES: u64 = 256 * 1024 * 1024;

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
    _lease: Option<CallLease>,
    trace: Option<ResponseTrace>,
    stream_first_chunk_already_logged: bool,
}

enum ManagedResponseBody {
    Response(reqwest::Response),
    Stream(BoxStream<'static, Result<Bytes, reqwest::Error>>),
    Bytes(Bytes),
}

#[derive(Debug, Clone)]
pub struct RequestOptions {
    pub omit_agent_mode_header: bool,
    pub request_id: Option<String>,
    pub model_id: Option<String>,
    pub session_affinity_key: Option<String>,
    pub request_weight: f64,
    pub wait_for_stream_content_start: bool,
    pub stream_thinking_enabled: bool,
}

impl Default for RequestOptions {
    fn default() -> Self {
        Self {
            omit_agent_mode_header: false,
            request_id: None,
            model_id: None,
            session_affinity_key: None,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StreamPreSseAttemptTimeout {
    timeout: Duration,
    fast_failover: bool,
    configured_timeout_ms: u64,
}

#[derive(Debug, Clone)]
pub struct PublicProviderError {
    status_code: u16,
    error_type: &'static str,
    error_code: Option<&'static str>,
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
            error_code: None,
            public_message: public_message.into(),
            log_message: log_message.into(),
        }
    }

    pub fn context_length_exceeded(status_code: u16, log_message: impl Into<String>) -> Self {
        Self {
            status_code,
            error_type: "invalid_request_error",
            error_code: Some(CONTEXT_LENGTH_EXCEEDED_CODE),
            public_message: CONTEXT_LENGTH_EXCEEDED_PUBLIC_MESSAGE.to_string(),
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
            error_code: None,
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
            error_code: None,
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
            error_code: None,
            public_message: public_message.into(),
            log_message: log_message.into(),
        }
    }

    pub fn service_unavailable(
        log_message: impl Into<String>,
        public_message: impl Into<String>,
    ) -> Self {
        Self {
            status_code: 503,
            error_type: "service_unavailable",
            error_code: None,
            public_message: public_message.into(),
            log_message: log_message.into(),
        }
    }

    pub fn bad_gateway(log_message: impl Into<String>, public_message: impl Into<String>) -> Self {
        Self {
            status_code: 502,
            error_type: "api_error",
            error_code: None,
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

    pub fn error_code(&self) -> Option<&'static str> {
        self.error_code
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

#[derive(Clone)]
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
    response_content_type: Option<String>,
    response_content_encoding: Option<String>,
    response_content_length: Option<String>,
    response_transfer_encoding: Option<String>,
    slow_model_cooldown: Option<SlowModelCooldownTrace>,
}

#[derive(Clone)]
struct SlowModelCooldownTrace {
    token_manager: Arc<MultiTokenManager>,
}

impl ManagedResponse {
    fn new(response: reqwest::Response, lease: CallLease, trace: Option<ResponseTrace>) -> Self {
        Self {
            body: ManagedResponseBody::Response(response),
            _lease: Some(lease),
            trace,
            stream_first_chunk_already_logged: false,
        }
    }

    fn new_stream(
        stream: BoxStream<'static, Result<Bytes, reqwest::Error>>,
        lease: Option<CallLease>,
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

    fn new_bytes(bytes: Bytes, lease: CallLease) -> Self {
        Self {
            body: ManagedResponseBody::Bytes(bytes),
            _lease: Some(lease),
            trace: None,
            stream_first_chunk_already_logged: false,
        }
    }

    pub async fn bytes(self) -> reqwest::Result<Bytes> {
        let Self {
            body,
            _lease,
            trace,
            stream_first_chunk_already_logged: _,
        } = self;
        match body {
            ManagedResponseBody::Response(response) => {
                let Some(trace) = trace.as_ref() else {
                    return response.bytes().await;
                };
                read_response_body_with_trace(response, trace).await
            }
            ManagedResponseBody::Stream(mut body_stream) => {
                let mut buffer = BytesMut::new();
                while let Some(chunk) = body_stream.next().await {
                    buffer.extend_from_slice(&chunk?);
                }
                let bytes = buffer.freeze();
                if let Some(trace) = trace {
                    trace.log_body_complete(bytes.len());
                }
                Ok(bytes)
            }
            ManagedResponseBody::Bytes(bytes) => Ok(bytes),
        }
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
            ManagedResponseBody::Bytes(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
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
            ManagedResponseBody::Bytes(bytes) => stream::once(async move { Ok(bytes) }).boxed(),
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

    fn defer_slow_model_cooldown_if_needed(
        &self,
        observed_ms: u128,
        threshold_ms: u128,
        reason: &str,
    ) -> bool {
        if observed_ms < threshold_ms {
            return false;
        }

        let Some(cooldown) = self.slow_model_cooldown.as_ref() else {
            return false;
        };
        let Some(model) = self.model.as_deref() else {
            return false;
        };

        cooldown.token_manager.defer_slow_model_credential(
            self.credential_id,
            model,
            OPUS_4_7_SLOW_MODEL_COOLDOWN,
            reason,
        )
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

    fn log_body_error(
        &self,
        partial_body_len: usize,
        partial_body: &BytesMut,
        error: &reqwest::Error,
    ) {
        let error_sources = summarize_error_sources(error);
        let _ = partial_body;
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
            response_content_type = self.response_content_type.as_deref().unwrap_or("unknown"),
            response_content_encoding = self.response_content_encoding.as_deref().unwrap_or("unknown"),
            response_content_length = self.response_content_length.as_deref().unwrap_or("unknown"),
            response_transfer_encoding = self.response_transfer_encoding.as_deref().unwrap_or("unknown"),
            partial_body_len,
            total_elapsed_ms = self.overall_started_at.elapsed().as_millis(),
            upstream_elapsed_ms = self.upstream_request_started_at.elapsed().as_millis(),
            body_read_elapsed_ms = self.response_headers_at.elapsed().as_millis(),
            error = %error,
            error_debug = ?error,
            error_is_timeout = error.is_timeout(),
            error_is_connect = error.is_connect(),
            error_is_request = error.is_request(),
            error_status = error.status().map(|status| status.as_u16()).unwrap_or(0),
            error_sources = %error_sources,
            "上游响应体读取失败"
        );
    }

    fn log_body_timeout(&self, partial_body_len: usize, partial_body: &BytesMut, timeout_ms: u64) {
        let _ = partial_body;
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
            response_content_type = self.response_content_type.as_deref().unwrap_or("unknown"),
            response_content_encoding = self.response_content_encoding.as_deref().unwrap_or("unknown"),
            response_content_length = self.response_content_length.as_deref().unwrap_or("unknown"),
            response_transfer_encoding = self.response_transfer_encoding.as_deref().unwrap_or("unknown"),
            partial_body_len,
            total_elapsed_ms = self.overall_started_at.elapsed().as_millis(),
            upstream_elapsed_ms = self.upstream_request_started_at.elapsed().as_millis(),
            body_read_elapsed_ms = self.response_headers_at.elapsed().as_millis(),
            timeout_ms,
            "上游响应体读取超时"
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
        let slow_model_cooldown_applied = self.defer_slow_model_cooldown_if_needed(
            first_chunk_wait_ms,
            OPUS_4_7_SLOW_MODEL_FIRST_CHUNK_MS,
            "slow_first_chunk",
        );

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
                slow_model_cooldown_applied,
                slow_model_cooldown_threshold_ms = OPUS_4_7_SLOW_MODEL_FIRST_CHUNK_MS,
                slow_model_cooldown_ms = OPUS_4_7_SLOW_MODEL_COOLDOWN.as_millis(),
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
        let error_sources = summarize_error_sources(error);
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
            error_debug = ?error,
            error_is_timeout = error.is_timeout(),
            error_is_connect = error.is_connect(),
            error_is_request = error.is_request(),
            error_status = error.status().map(|status| status.as_u16()).unwrap_or(0),
            error_sources = %error_sources,
            "上游流读取失败"
        );
    }
}

fn summarize_error_sources(error: &reqwest::Error) -> String {
    let mut sources = Vec::new();
    let mut current = StdError::source(error);
    while let Some(source) = current {
        sources.push(source.to_string());
        if sources.len() >= 4 {
            break;
        }
        current = source.source();
    }
    sources.join(" | ")
}

fn response_header_for_log(headers: &HeaderMap, name: &'static str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(|value| summarize_text_for_log(value, 160))
        .filter(|value| value != "<empty>")
}

async fn read_response_body_with_trace(
    response: reqwest::Response,
    trace: &ResponseTrace,
) -> reqwest::Result<Bytes> {
    let mut body_stream = response.bytes_stream();
    let mut buffer = BytesMut::new();

    while let Some(chunk_result) = body_stream.next().await {
        match chunk_result {
            Ok(chunk) => buffer.extend_from_slice(&chunk),
            Err(err) => {
                trace.log_body_error(buffer.len(), &buffer, &err);
                return Err(err);
            }
        }
    }

    let bytes = buffer.freeze();
    trace.log_body_complete(bytes.len());
    Ok(bytes)
}

#[derive(Debug)]
enum ResponseBodyReadFailure {
    Upstream(reqwest::Error),
    Timeout {
        timeout_ms: u64,
        reason: &'static str,
        eventstream_diagnostics: Option<NonStreamEventStreamReadDiagnostics>,
    },
}

#[derive(Clone, Debug, Default)]
struct NonStreamEventStreamReadDiagnostics {
    observed_frames: usize,
    event_frames: usize,
    assistant_events: usize,
    assistant_content_bytes: usize,
    reasoning_events: usize,
    reasoning_text_bytes: usize,
    tool_use_events: usize,
    tool_use_stop_events: usize,
    metering_events: usize,
    context_usage_events: usize,
    unknown_events: usize,
    error_events: usize,
    exception_events: usize,
    payload_parse_errors: usize,
    decoder_errors: usize,
    last_message_type: String,
    last_event_type: String,
}

impl NonStreamEventStreamReadDiagnostics {
    fn observe_frame(&mut self, frame: crate::kiro::parser::frame::Frame) {
        self.observed_frames = self.observed_frames.saturating_add(1);
        self.last_message_type = frame.message_type().unwrap_or("unknown").to_string();
        self.last_event_type = frame.event_type().unwrap_or("unknown").to_string();
        if frame.message_type() == Some("event") {
            self.event_frames = self.event_frames.saturating_add(1);
        }

        match Event::from_frame(frame) {
            Ok(Event::AssistantResponse(resp)) => {
                self.assistant_events = self.assistant_events.saturating_add(1);
                self.assistant_content_bytes = self
                    .assistant_content_bytes
                    .saturating_add(resp.content.len());
            }
            Ok(Event::ReasoningContent(reasoning)) => {
                self.reasoning_events = self.reasoning_events.saturating_add(1);
                self.reasoning_text_bytes = self
                    .reasoning_text_bytes
                    .saturating_add(reasoning.text.len());
            }
            Ok(Event::ToolUse(tool_use)) => {
                self.tool_use_events = self.tool_use_events.saturating_add(1);
                if tool_use.stop {
                    self.tool_use_stop_events = self.tool_use_stop_events.saturating_add(1);
                }
            }
            Ok(Event::Metering(())) => {
                self.metering_events = self.metering_events.saturating_add(1);
            }
            Ok(Event::ContextUsage(_)) => {
                self.context_usage_events = self.context_usage_events.saturating_add(1);
            }
            Ok(Event::Unknown {}) => {
                self.unknown_events = self.unknown_events.saturating_add(1);
            }
            Ok(Event::Error { .. }) => {
                self.error_events = self.error_events.saturating_add(1);
            }
            Ok(Event::Exception { .. }) => {
                self.exception_events = self.exception_events.saturating_add(1);
            }
            Err(_) => {
                self.payload_parse_errors = self.payload_parse_errors.saturating_add(1);
            }
        }
    }

    fn record_decoder_error(&mut self) {
        self.decoder_errors = self.decoder_errors.saturating_add(1);
    }

    fn has_usable_output(&self) -> bool {
        self.assistant_content_bytes > 0
            || self.reasoning_text_bytes > 0
            || self.tool_use_events > 0
    }

    fn safe_to_retry_stall(&self) -> bool {
        self.observed_frames > 0
            && !self.has_usable_output()
            && self.error_events == 0
            && self.exception_events == 0
            && self.payload_parse_errors == 0
            && self.decoder_errors == 0
    }
}

fn is_amazon_eventstream_content_type(content_type: Option<&str>) -> bool {
    content_type
        .map(|value| {
            value
                .to_ascii_lowercase()
                .contains(AMAZON_EVENTSTREAM_CONTENT_TYPE)
        })
        .unwrap_or(false)
}

async fn read_response_body_with_trace_timeout(
    response: reqwest::Response,
    trace: &ResponseTrace,
    timeout_ms: u64,
    eventstream_idle_timeout_ms: u64,
) -> Result<Bytes, ResponseBodyReadFailure> {
    let mut body_stream = response.bytes_stream();
    let mut buffer = BytesMut::new();
    let timeout_duration = Duration::from_millis(timeout_ms);
    let eventstream_idle_timeout = Duration::from_millis(eventstream_idle_timeout_ms);
    let eventstream_response =
        is_amazon_eventstream_content_type(trace.response_content_type.as_deref());
    let mut eventstream_decoder = eventstream_response.then(EventStreamDecoder::new);
    let mut eventstream_diagnostics =
        eventstream_response.then(NonStreamEventStreamReadDiagnostics::default);
    let mut last_eventstream_chunk_at: Option<Instant> = None;
    let started_at = Instant::now();

    loop {
        let elapsed = started_at.elapsed();
        if elapsed >= timeout_duration {
            trace.log_body_timeout(buffer.len(), &buffer, timeout_ms);
            return Err(ResponseBodyReadFailure::Timeout {
                timeout_ms,
                reason: "total_body_read_timeout",
                eventstream_diagnostics,
            });
        }
        let mut wait_for_next_chunk = timeout_duration - elapsed;
        let mut timeout_reason = "total_body_read_timeout";
        let mut reported_timeout_ms = timeout_ms;
        if eventstream_response {
            if let Some(last_chunk_at) = last_eventstream_chunk_at {
                let idle_elapsed = last_chunk_at.elapsed();
                if idle_elapsed >= eventstream_idle_timeout {
                    trace.log_body_timeout(buffer.len(), &buffer, eventstream_idle_timeout_ms);
                    return Err(ResponseBodyReadFailure::Timeout {
                        timeout_ms: eventstream_idle_timeout_ms,
                        reason: "eventstream_idle_timeout",
                        eventstream_diagnostics,
                    });
                }
                let remaining_idle = eventstream_idle_timeout - idle_elapsed;
                if remaining_idle < wait_for_next_chunk {
                    wait_for_next_chunk = remaining_idle;
                    timeout_reason = "eventstream_idle_timeout";
                    reported_timeout_ms = eventstream_idle_timeout_ms;
                }
            }
        }

        match timeout(wait_for_next_chunk, body_stream.next()).await {
            Ok(Some(Ok(chunk))) => {
                if eventstream_response {
                    last_eventstream_chunk_at = Some(Instant::now());
                    if let Some(decoder) = eventstream_decoder.as_mut() {
                        if let Err(err) = decoder.feed(&chunk) {
                            if let Some(diagnostics) = eventstream_diagnostics.as_mut() {
                                diagnostics.record_decoder_error();
                            }
                            tracing::warn!(
                                request_id = %trace.request_id,
                                api_type = trace.api_type,
                                model = trace.model_label(),
                                credential_id = trace.credential_id,
                                attempt = trace.attempt,
                                max_retries = trace.max_retries,
                                region = %trace.region,
                                status_code = trace.status_code,
                                chunk_len = chunk.len(),
                                decoder_buffer_len = decoder.buffer_len(),
                                error = %err,
                                "非流式上游 eventstream 读取诊断解码缓冲失败"
                            );
                        }

                        loop {
                            match decoder.decode() {
                                Ok(Some(frame)) => {
                                    if let Some(diagnostics) = eventstream_diagnostics.as_mut() {
                                        diagnostics.observe_frame(frame);
                                    }
                                }
                                Ok(None) => break,
                                Err(err) => {
                                    if let Some(diagnostics) = eventstream_diagnostics.as_mut() {
                                        diagnostics.record_decoder_error();
                                    }
                                    tracing::warn!(
                                        request_id = %trace.request_id,
                                        api_type = trace.api_type,
                                        model = trace.model_label(),
                                        credential_id = trace.credential_id,
                                        attempt = trace.attempt,
                                        max_retries = trace.max_retries,
                                        region = %trace.region,
                                        status_code = trace.status_code,
                                        decoder_buffer_len = decoder.buffer_len(),
                                        decoder_error_count = decoder.error_count(),
                                        decoder_bytes_skipped = decoder.bytes_skipped(),
                                        error = %err,
                                        "非流式上游 eventstream 读取诊断解码事件失败"
                                    );
                                    if err.is_fatal_stream_error() {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                buffer.extend_from_slice(&chunk);
            }
            Ok(Some(Err(err))) => {
                trace.log_body_error(buffer.len(), &buffer, &err);
                return Err(ResponseBodyReadFailure::Upstream(err));
            }
            Ok(None) => {
                if let Some(diagnostics) = eventstream_diagnostics.as_ref() {
                    tracing::info!(
                        request_id = %trace.request_id,
                        api_type = trace.api_type,
                        model = trace.model_label(),
                        request_body_bytes = trace.request_body_bytes,
                        credential_id = trace.credential_id,
                        attempt = trace.attempt,
                        max_retries = trace.max_retries,
                        region = %trace.region,
                        status_code = trace.status_code,
                        body_len = buffer.len(),
                        eventstream_observed_frames = diagnostics.observed_frames,
                        eventstream_event_frames = diagnostics.event_frames,
                        eventstream_assistant_events = diagnostics.assistant_events,
                        eventstream_assistant_content_bytes = diagnostics.assistant_content_bytes,
                        eventstream_reasoning_events = diagnostics.reasoning_events,
                        eventstream_reasoning_text_bytes = diagnostics.reasoning_text_bytes,
                        eventstream_tool_use_events = diagnostics.tool_use_events,
                        eventstream_tool_use_stop_events = diagnostics.tool_use_stop_events,
                        eventstream_metering_events = diagnostics.metering_events,
                        eventstream_context_usage_events = diagnostics.context_usage_events,
                        eventstream_unknown_events = diagnostics.unknown_events,
                        eventstream_error_events = diagnostics.error_events,
                        eventstream_exception_events = diagnostics.exception_events,
                        eventstream_payload_parse_errors = diagnostics.payload_parse_errors,
                        eventstream_decoder_errors = diagnostics.decoder_errors,
                        eventstream_last_message_type = diagnostics.last_message_type.as_str(),
                        eventstream_last_event_type = diagnostics.last_event_type.as_str(),
                        total_elapsed_ms = trace.overall_started_at.elapsed().as_millis(),
                        upstream_elapsed_ms = trace.upstream_request_started_at.elapsed().as_millis(),
                        body_read_elapsed_ms = trace.response_headers_at.elapsed().as_millis(),
                        "非流式上游 eventstream 响应体读取完成"
                    );
                }
                let bytes = buffer.freeze();
                trace.log_body_complete(bytes.len());
                return Ok(bytes);
            }
            Err(_) => {
                trace.log_body_timeout(buffer.len(), &buffer, reported_timeout_ms);
                return Err(ResponseBodyReadFailure::Timeout {
                    timeout_ms: reported_timeout_ms,
                    reason: timeout_reason,
                    eventstream_diagnostics,
                });
            }
        }
    }
}

const THINKING_START_TAG: &str = "<thinking>";

struct StreamContentStartProbe {
    thinking_enabled: bool,
    buffer: String,
    observed_events: usize,
    non_error_events: usize,
    assistant_events: usize,
    assistant_content_bytes: usize,
    reasoning_events: usize,
    reasoning_text_bytes: usize,
    tool_use_events: usize,
    error_events: usize,
}

impl StreamContentStartProbe {
    fn new(thinking_enabled: bool) -> Self {
        Self {
            thinking_enabled,
            buffer: String::new(),
            observed_events: 0,
            non_error_events: 0,
            assistant_events: 0,
            assistant_content_bytes: 0,
            reasoning_events: 0,
            reasoning_text_bytes: 0,
            tool_use_events: 0,
            error_events: 0,
        }
    }

    fn observe(&mut self, event: &Event) -> bool {
        self.observed_events = self.observed_events.saturating_add(1);
        match event {
            Event::ToolUse(_) => {
                self.non_error_events = self.non_error_events.saturating_add(1);
                self.tool_use_events = self.tool_use_events.saturating_add(1);
                true
            }
            Event::AssistantResponse(resp) => {
                self.non_error_events = self.non_error_events.saturating_add(1);
                self.assistant_events = self.assistant_events.saturating_add(1);
                self.assistant_content_bytes = self
                    .assistant_content_bytes
                    .saturating_add(resp.content.len());
                self.observe_assistant_content(&resp.content)
            }
            Event::ReasoningContent(reasoning) => {
                self.non_error_events = self.non_error_events.saturating_add(1);
                self.reasoning_events = self.reasoning_events.saturating_add(1);
                self.reasoning_text_bytes = self
                    .reasoning_text_bytes
                    .saturating_add(reasoning.text.len());
                self.thinking_enabled
                    && (!reasoning.text.is_empty() || reasoning.signature.is_some())
            }
            Event::Error { .. } | Event::Exception { .. } => {
                self.error_events = self.error_events.saturating_add(1);
                false
            }
            _ => {
                self.non_error_events = self.non_error_events.saturating_add(1);
                false
            }
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
        if self.buffer.contains(THINKING_START_TAG) {
            return true;
        }

        let safe_len = stream_content_probe_safe_prefix_len(&self.buffer);
        let safe_prefix = &self.buffer[..safe_len];
        if !safe_prefix.trim().is_empty() {
            return true;
        }
        if safe_len > 0 {
            self.buffer.drain(..safe_len);
        }
        false
    }

    fn should_release_after_stream_activity(&self, prefetched_bytes: usize) -> bool {
        self.thinking_enabled
            && prefetched_bytes >= STREAM_CONTENT_START_ACTIVITY_READY_BYTES
            && self.non_error_events > 0
            && self.error_events == 0
    }

    fn diagnostics(&self) -> StreamContentStartProbeDiagnostics {
        StreamContentStartProbeDiagnostics {
            observed_events: self.observed_events,
            non_error_events: self.non_error_events,
            assistant_events: self.assistant_events,
            assistant_content_bytes: self.assistant_content_bytes,
            reasoning_events: self.reasoning_events,
            reasoning_text_bytes: self.reasoning_text_bytes,
            tool_use_events: self.tool_use_events,
            error_events: self.error_events,
        }
    }
}

fn stream_content_probe_safe_prefix_len(buffer: &str) -> usize {
    let mut boundary = buffer.len().saturating_sub(THINKING_START_TAG.len());
    while boundary > 0 && !buffer.is_char_boundary(boundary) {
        boundary -= 1;
    }
    boundary
}

#[derive(Clone, Copy, Debug, Default)]
struct StreamContentStartProbeDiagnostics {
    observed_events: usize,
    non_error_events: usize,
    assistant_events: usize,
    assistant_content_bytes: usize,
    reasoning_events: usize,
    reasoning_text_bytes: usize,
    tool_use_events: usize,
    error_events: usize,
}

enum StreamContentStartPrefetch {
    Ready {
        stream: BoxStream<'static, Result<Bytes, reqwest::Error>>,
        first_chunk_logged: bool,
        prefetched_bytes: usize,
        elapsed: Duration,
        ready_reason: &'static str,
        probe_diagnostics: StreamContentStartProbeDiagnostics,
    },
    TimedOut {
        elapsed: Duration,
        prefetched_bytes: usize,
        probe_diagnostics: StreamContentStartProbeDiagnostics,
    },
}

#[derive(Debug, Default, PartialEq)]
struct KiroRequestBodyDiagnostics {
    body_bytes: usize,
    profile_arn_present: bool,
    conversation_id_present: bool,
    agent_task_type_present: bool,
    chat_trigger_type: Option<String>,
    current_model_id: Option<String>,
    current_origin: Option<String>,
    current_content_bytes: usize,
    current_image_count: usize,
    current_document_count: usize,
    current_tool_count: usize,
    current_tool_schemas: Vec<KiroToolSchemaDiagnostics>,
    current_tool_result_count: usize,
    current_tool_result_error_count: usize,
    history_count: usize,
    history_user_count: usize,
    history_assistant_count: usize,
    history_user_image_count: usize,
    history_user_document_count: usize,
    history_tool_result_count: usize,
    history_tool_use_count: usize,
    tail_history_roles: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct KiroToolSchemaDiagnostics {
    index: usize,
    name: String,
    root_type: String,
}

const KIRO_REQUEST_DIAGNOSTICS_MAX_BODY_BYTES: usize = 2 * 1024 * 1024;
const KIRO_REQUEST_DIAGNOSTICS_MAX_TOOL_SCHEMAS: usize = 64;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Capture400BodiesConfig {
    enabled: bool,
    dir: PathBuf,
    max_per_class: usize,
    max_body_bytes: usize,
    ttl: Duration,
    max_total_bytes: u64,
}

impl Capture400BodiesConfig {
    fn disabled() -> Self {
        Self {
            enabled: false,
            dir: PathBuf::from(DEFAULT_CAPTURE_400_DIR),
            max_per_class: DEFAULT_CAPTURE_400_MAX_PER_CLASS,
            max_body_bytes: DEFAULT_CAPTURE_400_MAX_BODY_BYTES,
            ttl: Duration::from_secs(DEFAULT_CAPTURE_400_TTL_HOURS * 60 * 60),
            max_total_bytes: DEFAULT_CAPTURE_400_MAX_TOTAL_BYTES,
        }
    }

    fn from_env() -> Self {
        let enabled = std::env::var("KIRO_CAPTURE_400_BODIES")
            .ok()
            .is_some_and(|value| env_bool_enabled(&value));
        let mut config = Self::disabled();
        config.enabled = enabled;
        config.dir = std::env::var("KIRO_CAPTURE_400_DIR")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_CAPTURE_400_DIR));
        config.max_per_class = env_usize_or(
            "KIRO_CAPTURE_400_MAX_PER_CLASS",
            DEFAULT_CAPTURE_400_MAX_PER_CLASS,
        )
        .min(50);
        config.max_body_bytes = env_usize_or(
            "KIRO_CAPTURE_400_MAX_BODY_BYTES",
            DEFAULT_CAPTURE_400_MAX_BODY_BYTES,
        );
        let ttl_hours = env_u64_or("KIRO_CAPTURE_400_TTL_HOURS", DEFAULT_CAPTURE_400_TTL_HOURS);
        config.ttl = Duration::from_secs(ttl_hours.saturating_mul(60 * 60));
        config.max_total_bytes = env_u64_or(
            "KIRO_CAPTURE_400_MAX_TOTAL_BYTES",
            DEFAULT_CAPTURE_400_MAX_TOTAL_BYTES,
        );
        config
    }
}

#[derive(Debug)]
struct Capture400Request<'a> {
    request_id: &'a str,
    api_type: &'static str,
    model: Option<&'a str>,
    credential_id: u64,
    attempt: usize,
    max_retries: usize,
    region: &'a str,
    stream: bool,
    status_code: u16,
    error_summary: &'a str,
    upstream_error_body: &'a str,
    request_body: &'a str,
}

#[derive(Debug, Clone)]
struct CaptureFilePair {
    meta_path: PathBuf,
    body_path: PathBuf,
    modified: SystemTime,
    bytes: u64,
}

fn env_bool_enabled(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

fn env_usize_or(name: &str, default_value: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(default_value)
}

fn env_u64_or(name: &str, default_value: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(default_value)
}

fn capture_400_error_class(body: &str, error_summary: &str) -> &'static str {
    if KiroProvider::is_invalid_thinking_signature_error(body, error_summary) {
        "invalid_thinking_signature"
    } else if KiroProvider::is_context_length_exceeded_body(body)
        || body.contains("Input is too long")
        || error_summary.contains("CONTENT_LENGTH_EXCEEDS_THRESHOLD")
        || error_summary.contains("context_length_exceeded")
    {
        "context_length_exceeded"
    } else if KiroProvider::is_tool_schema_invalid_body(body) {
        "tool_schema_invalid"
    } else if body.contains("TOOL_USE_RESULT_MISMATCH")
        || body.contains("toolResult blocks")
        || body.contains("toolUse blocks of previous turn")
    {
        "tool_use_result_mismatch"
    } else if body.contains("REQUEST_BODY_INVALID")
        || error_summary.contains("REQUEST_BODY_INVALID")
        || body.contains("Improperly formed request")
    {
        "request_body_invalid"
    } else {
        "invalid_request"
    }
}

fn set_private_dir_permissions(path: &Path) {
    #[cfg(unix)]
    {
        let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o700));
    }
}

fn set_private_file_permissions(path: &Path) {
    #[cfg(unix)]
    {
        let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o600));
    }
}

fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn sha256_hex_str(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    hex::encode(hasher.finalize())
}

fn sanitize_capture_component(value: &str, limit: usize) -> String {
    let mut sanitized = String::with_capacity(value.len().min(limit));
    for ch in value.chars().take(limit) {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
            sanitized.push(ch);
        } else {
            sanitized.push('_');
        }
    }
    if sanitized.is_empty() {
        "unknown".to_string()
    } else {
        sanitized
    }
}

fn write_private_file(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);

    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    set_private_file_permissions(path);
    Ok(())
}

fn capture_400_request_body_for_diagnostics(
    config: &Capture400BodiesConfig,
    req: Capture400Request<'_>,
) {
    if !config.enabled || config.max_per_class == 0 {
        return;
    }

    if let Err(err) = capture_400_request_body_for_diagnostics_inner(config, req) {
        tracing::warn!(error = %err, "保存上游 400 请求体诊断失败");
    }
}

fn capture_400_request_body_for_diagnostics_inner(
    config: &Capture400BodiesConfig,
    req: Capture400Request<'_>,
) -> anyhow::Result<()> {
    let class = capture_400_error_class(req.upstream_error_body, req.error_summary);
    let base_dir = &config.dir;
    let class_dir = base_dir.join(class);
    fs::create_dir_all(&class_dir)?;
    set_private_dir_permissions(base_dir);
    set_private_dir_permissions(&class_dir);

    let request_hash = sha256_hex_str(req.request_body);
    let name = format!(
        "{}-{}-{}",
        now_millis(),
        sanitize_capture_component(req.request_id, 96),
        &request_hash[..16]
    );
    let body_path = class_dir.join(format!("{name}.body.json"));
    let meta_path = class_dir.join(format!("{name}.meta.json"));
    let body_saved = req.request_body.len() <= config.max_body_bytes;

    if body_saved {
        write_private_file(&body_path, req.request_body.as_bytes())?;
    }

    let metadata = serde_json::json!({
        "captured_at": chrono::Utc::now().to_rfc3339(),
        "request_id": req.request_id,
        "api_type": req.api_type,
        "model": req.model.unwrap_or("unknown"),
        "credential_id": req.credential_id,
        "attempt": req.attempt,
        "max_retries": req.max_retries,
        "region": req.region,
        "stream": req.stream,
        "status_code": req.status_code,
        "error_class": class,
        "error_summary": req.error_summary,
        "upstream_error_body_bytes": req.upstream_error_body.len(),
        "upstream_error_body_excerpt": truncate_log_string(req.upstream_error_body, 4096),
        "request_body_bytes": req.request_body.len(),
        "request_sha256": request_hash,
        "body_saved": body_saved,
        "body_file": if body_saved { Some(body_path.file_name().and_then(|name| name.to_str()).unwrap_or_default()) } else { None },
        "body_omitted_reason": if body_saved { None } else { Some("request body exceeds KIRO_CAPTURE_400_MAX_BODY_BYTES") },
        "max_body_bytes": config.max_body_bytes,
        "max_per_class": config.max_per_class,
        "ttl_seconds": config.ttl.as_secs(),
        "max_total_bytes": config.max_total_bytes
    });
    let metadata_bytes = serde_json::to_vec_pretty(&metadata)?;
    if let Err(err) = write_private_file(&meta_path, &metadata_bytes) {
        if body_saved {
            let _ = fs::remove_file(&body_path);
        }
        return Err(err.into());
    }

    prune_capture_class(&class_dir, config.max_per_class, config.ttl);
    prune_capture_expired_in_tree(base_dir, config.ttl);
    prune_capture_total(base_dir, config.max_total_bytes);

    tracing::warn!(
        request_id = req.request_id,
        error_class = class,
        body_saved,
        request_body_bytes = req.request_body.len(),
        capture_dir = %class_dir.display(),
        "已保存上游 400 请求体诊断样本"
    );

    Ok(())
}

fn capture_body_path_for_meta(meta_path: &Path) -> PathBuf {
    let Some(file_name) = meta_path.file_name().and_then(|name| name.to_str()) else {
        return meta_path.with_extension("body.json");
    };
    if let Some(stem) = file_name.strip_suffix(".meta.json") {
        meta_path.with_file_name(format!("{stem}.body.json"))
    } else {
        meta_path.with_extension("body.json")
    }
}

fn capture_file_pair_from_meta(meta_path: PathBuf) -> Option<CaptureFilePair> {
    let body_path = capture_body_path_for_meta(&meta_path);
    let meta = fs::metadata(&meta_path).ok()?;
    let modified = meta.modified().unwrap_or(UNIX_EPOCH);
    let mut bytes = meta.len();
    if let Ok(body_meta) = fs::metadata(&body_path) {
        bytes = bytes.saturating_add(body_meta.len());
    }
    Some(CaptureFilePair {
        meta_path,
        body_path,
        modified,
        bytes,
    })
}

fn capture_pairs_in_dir(dir: &Path) -> Vec<CaptureFilePair> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Vec::new();
    };
    entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(".meta.json"))
        })
        .filter_map(capture_file_pair_from_meta)
        .collect()
}

fn remove_capture_pair(pair: &CaptureFilePair) {
    let _ = fs::remove_file(&pair.body_path);
    let _ = fs::remove_file(&pair.meta_path);
}

fn prune_capture_class(class_dir: &Path, max_per_class: usize, ttl: Duration) {
    let mut pairs = capture_pairs_in_dir(class_dir);
    let now = SystemTime::now();
    for pair in &pairs {
        if ttl.as_secs() > 0 && now.duration_since(pair.modified).is_ok_and(|age| age > ttl) {
            remove_capture_pair(pair);
        }
    }

    pairs = capture_pairs_in_dir(class_dir);
    pairs.sort_by(|a, b| b.modified.cmp(&a.modified));
    for pair in pairs.into_iter().skip(max_per_class) {
        remove_capture_pair(&pair);
    }
}

fn capture_pairs_in_tree(base_dir: &Path) -> Vec<CaptureFilePair> {
    let Ok(class_dirs) = fs::read_dir(base_dir) else {
        return Vec::new();
    };
    let mut pairs = Vec::new();
    for class_dir in class_dirs.filter_map(Result::ok).map(|entry| entry.path()) {
        if class_dir.is_dir() {
            pairs.extend(capture_pairs_in_dir(&class_dir));
        }
    }
    pairs
}

fn prune_capture_expired_in_tree(base_dir: &Path, ttl: Duration) {
    if ttl.as_secs() == 0 {
        return;
    }

    let now = SystemTime::now();
    for pair in capture_pairs_in_tree(base_dir) {
        if now.duration_since(pair.modified).is_ok_and(|age| age > ttl) {
            remove_capture_pair(&pair);
        }
    }
}

fn prune_capture_total(base_dir: &Path, max_total_bytes: u64) {
    if max_total_bytes == 0 {
        return;
    }
    let mut pairs = capture_pairs_in_tree(base_dir);
    let mut total_bytes = pairs
        .iter()
        .fold(0_u64, |total, pair| total.saturating_add(pair.bytes));
    if total_bytes <= max_total_bytes {
        return;
    }

    pairs.sort_by(|a, b| a.modified.cmp(&b.modified));
    for pair in pairs {
        if total_bytes <= max_total_bytes {
            break;
        }
        total_bytes = total_bytes.saturating_sub(pair.bytes);
        remove_capture_pair(&pair);
    }
}

fn append_bounded_tail(mut values: Vec<String>, value: &str, limit: usize) -> Vec<String> {
    if limit == 0 {
        return values;
    }
    values.push(truncate_log_string(value.trim(), 64));
    if values.len() > limit {
        let drain_count = values.len() - limit;
        values.drain(0..drain_count);
    }
    values
}

fn truncate_log_string(value: &str, limit: usize) -> String {
    if limit == 0 || value.len() <= limit {
        return value.to_string();
    }
    if limit <= 3 {
        let mut end = limit;
        while end > 0 && !value.is_char_boundary(end) {
            end -= 1;
        }
        return value[..end].to_string();
    }

    let mut end = limit - 3;
    while end > 0 && !value.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &value[..end])
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

    pub fn thinking_signature_validation_mode(&self) -> ThinkingSignatureValidationMode {
        self.token_manager.thinking_signature_validation_mode()
    }

    pub fn response_thinking_signature_compat_enabled(&self) -> bool {
        self.token_manager
            .response_thinking_signature_compat_enabled()
    }

    pub fn server_web_tools_mode(&self) -> ServerWebToolsMode {
        self.token_manager.config().server_web_tools_mode
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
        let base = self.runtime_endpoint_base_for(credentials);
        format!("{}/generateAssistantResponse", base)
    }

    fn runtime_endpoint_base_for(&self, credentials: &KiroCredentials) -> String {
        let config = self.token_manager.config();
        let api_region = credentials.effective_api_region(config);
        if credentials.detected_auth_account_type().as_deref() == Some("enterprise") {
            Config::q_endpoint_base(api_region)
        } else {
            config.effective_runtime_endpoint_base(api_region)
        }
    }

    /// 获取凭据级 MCP API URL
    fn mcp_url_for(&self, credentials: &KiroCredentials) -> String {
        let base = self.runtime_endpoint_base_for(credentials);
        format!("{}/mcp", base)
    }

    /// 获取凭据级 API 基础域名
    fn base_domain_for(&self, credentials: &KiroCredentials) -> String {
        let base = self.runtime_endpoint_base_for(credentials);
        Config::endpoint_host(&base)
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

    fn summarize_kiro_request_body_for_log(
        request_body: &str,
    ) -> Option<KiroRequestBodyDiagnostics> {
        if request_body.len() > KIRO_REQUEST_DIAGNOSTICS_MAX_BODY_BYTES {
            return None;
        }

        let json: serde_json::Value = serde_json::from_str(request_body).ok()?;
        let conversation = json.get("conversationState")?;
        let current = conversation
            .get("currentMessage")?
            .get("userInputMessage")?;
        let current_context = current.get("userInputMessageContext");

        let mut diagnostics = KiroRequestBodyDiagnostics {
            body_bytes: request_body.len(),
            profile_arn_present: json.get("profileArn").is_some(),
            conversation_id_present: conversation
                .get("conversationId")
                .and_then(|value| value.as_str())
                .is_some_and(|value| !value.trim().is_empty()),
            agent_task_type_present: conversation
                .get("agentTaskType")
                .and_then(|value| value.as_str())
                .is_some_and(|value| !value.trim().is_empty()),
            chat_trigger_type: Self::bounded_json_string(conversation.get("chatTriggerType"), 32),
            current_model_id: Self::bounded_json_string(current.get("modelId"), 96),
            current_origin: Self::bounded_json_string(current.get("origin"), 64),
            current_content_bytes: current
                .get("content")
                .and_then(|value| value.as_str())
                .map(str::len)
                .unwrap_or_default(),
            current_image_count: Self::json_array_len(current.get("images")),
            current_document_count: Self::json_array_len(current.get("documents")),
            current_tool_result_count: Self::json_array_len(
                current_context.and_then(|ctx| ctx.get("toolResults")),
            ),
            ..Default::default()
        };

        if let Some(tools) = current_context
            .and_then(|ctx| ctx.get("tools"))
            .and_then(|value| value.as_array())
        {
            diagnostics.current_tool_count = tools.len();
            diagnostics.current_tool_schemas = tools
                .iter()
                .enumerate()
                .take(KIRO_REQUEST_DIAGNOSTICS_MAX_TOOL_SCHEMAS)
                .map(|(index, tool)| Self::summarize_kiro_tool_schema_for_log(index, tool))
                .collect();
        }

        if let Some(tool_results) = current_context
            .and_then(|ctx| ctx.get("toolResults"))
            .and_then(|value| value.as_array())
        {
            diagnostics.current_tool_result_error_count = tool_results
                .iter()
                .filter(|result| {
                    result
                        .get("isError")
                        .and_then(|value| value.as_bool())
                        .unwrap_or(false)
                })
                .count();
        }

        if let Some(history) = conversation
            .get("history")
            .and_then(|value| value.as_array())
        {
            diagnostics.history_count = history.len();
            for entry in history {
                if let Some(user) = entry.get("userInputMessage") {
                    diagnostics.history_user_count += 1;
                    diagnostics.tail_history_roles =
                        append_bounded_tail(diagnostics.tail_history_roles, "user", 16);
                    diagnostics.history_user_image_count +=
                        Self::json_array_len(user.get("images"));
                    diagnostics.history_user_document_count +=
                        Self::json_array_len(user.get("documents"));
                    if let Some(ctx) = user.get("userInputMessageContext") {
                        diagnostics.history_tool_result_count +=
                            Self::json_array_len(ctx.get("toolResults"));
                    }
                    continue;
                }
                if let Some(assistant) = entry.get("assistantResponseMessage") {
                    diagnostics.history_assistant_count += 1;
                    diagnostics.tail_history_roles =
                        append_bounded_tail(diagnostics.tail_history_roles, "assistant", 16);
                    diagnostics.history_tool_use_count +=
                        Self::json_array_len(assistant.get("toolUses"));
                    continue;
                }
                diagnostics.tail_history_roles =
                    append_bounded_tail(diagnostics.tail_history_roles, "unknown", 16);
            }
        }

        Some(diagnostics)
    }

    fn summarize_kiro_tool_schema_for_log(
        index: usize,
        tool: &serde_json::Value,
    ) -> KiroToolSchemaDiagnostics {
        let tool_spec = tool
            .get("toolSpecification")
            .or_else(|| tool.get("toolSpec"));
        let name = tool_spec
            .and_then(|spec| spec.get("name"))
            .and_then(|value| value.as_str())
            .map(|value| truncate_log_string(value.trim(), 96))
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "<missing>".to_string());
        let root_type = tool_spec
            .and_then(|spec| spec.get("inputSchema"))
            .and_then(|schema| schema.get("json"))
            .and_then(|schema| schema.get("type"))
            .map(Self::summarize_json_schema_type_for_log)
            .unwrap_or_else(|| "<missing>".to_string());

        KiroToolSchemaDiagnostics {
            index,
            name,
            root_type,
        }
    }

    fn summarize_json_schema_type_for_log(value: &serde_json::Value) -> String {
        match value {
            serde_json::Value::String(value) => {
                let value = value.trim();
                if value.is_empty() {
                    "<empty>".to_string()
                } else {
                    truncate_log_string(value, 64)
                }
            }
            serde_json::Value::Array(values) => {
                let joined = values
                    .iter()
                    .map(|value| match value {
                        serde_json::Value::String(value) => truncate_log_string(value.trim(), 32),
                        other => format!("<{}>", Self::json_value_kind(other)),
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                truncate_log_string(&format!("[{joined}]"), 96)
            }
            other => format!("<{}>", Self::json_value_kind(other)),
        }
    }

    fn json_value_kind(value: &serde_json::Value) -> &'static str {
        match value {
            serde_json::Value::Null => "null",
            serde_json::Value::Bool(_) => "bool",
            serde_json::Value::Number(_) => "number",
            serde_json::Value::String(_) => "string",
            serde_json::Value::Array(_) => "array",
            serde_json::Value::Object(_) => "object",
        }
    }

    fn rejected_tool_index_from_error_body(body: &str) -> Option<usize> {
        for marker in ["toolConfig.tools.", "tools."] {
            if let Some(index) = Self::parse_index_after_marker(body, marker) {
                return Some(index);
            }
        }
        None
    }

    fn parse_index_after_marker(text: &str, marker: &str) -> Option<usize> {
        let mut search_from = 0usize;
        while search_from < text.len() {
            let Some(relative_marker_start) = text[search_from..].find(marker) else {
                return None;
            };
            let digits_start = search_from + relative_marker_start + marker.len();
            let digits = text[digits_start..]
                .chars()
                .take_while(|ch| ch.is_ascii_digit())
                .collect::<String>();
            if !digits.is_empty() {
                return digits.parse().ok();
            }
            search_from = digits_start;
        }
        None
    }

    fn json_array_len(value: Option<&serde_json::Value>) -> usize {
        value.and_then(|value| value.as_array()).map_or(0, Vec::len)
    }

    fn bounded_json_string(value: Option<&serde_json::Value>, limit: usize) -> Option<String> {
        let value = value?.as_str()?.trim();
        if value.is_empty() {
            return None;
        }
        Some(truncate_log_string(value, limit))
    }

    /// 将凭据的 profile_arn 注入到请求体 JSON 中。
    ///
    /// 返回 `None` 表示请求体保持原样，调用方可复用已缓存的 Bytes，避免重试时反复复制大请求体。
    fn inject_profile_arn(request_body: &str, profile_arn: &Option<String>) -> Option<String> {
        let Some(arn) = profile_arn else {
            return None;
        };

        if !request_body.contains("\"profileArn\"") {
            if let Some(insert_at) = request_body.rfind('}') {
                let arn_json = serde_json::to_string(arn).unwrap_or_else(|_| "\"\"".to_string());
                let prefix = &request_body[..insert_at];
                let suffix = &request_body[insert_at..];
                let separator = if prefix.trim_end().ends_with('{') {
                    ""
                } else {
                    ","
                };
                return Some(format!(
                    "{prefix}{separator}\"profileArn\":{arn_json}{suffix}"
                ));
            }
        }

        if let Ok(mut json) = serde_json::from_str::<serde_json::Value>(request_body) {
            json["profileArn"] = serde_json::Value::String(arn.clone());
            if let Ok(body) = serde_json::to_string(&json) {
                return Some(body);
            }
        }

        None
    }

    fn rewrite_model_ids(value: &mut serde_json::Value, model_id: &str) -> bool {
        match value {
            serde_json::Value::Object(map) => {
                let mut changed = false;
                for (key, value) in map {
                    if key == "modelId" {
                        if value.as_str() != Some(model_id) {
                            *value = serde_json::Value::String(model_id.to_string());
                            changed = true;
                        }
                    } else {
                        changed |= Self::rewrite_model_ids(value, model_id);
                    }
                }
                changed
            }
            serde_json::Value::Array(values) => values
                .iter_mut()
                .any(|value| Self::rewrite_model_ids(value, model_id)),
            _ => false,
        }
    }

    fn request_body_for_profile_arn_and_model(
        request_body: &str,
        original_body: &mut Option<Bytes>,
        profile_arn: &Option<String>,
        model_id: &Option<String>,
        strip_profile_arn: bool,
    ) -> Bytes {
        if model_id.is_some() || strip_profile_arn {
            if let Ok(mut json) = serde_json::from_str::<serde_json::Value>(request_body) {
                if let Some(arn) = profile_arn {
                    json["profileArn"] = serde_json::Value::String(arn.clone());
                } else if strip_profile_arn {
                    if let Some(object) = json.as_object_mut() {
                        object.remove("profileArn");
                    }
                }
                if let Some(model_id) = model_id {
                    Self::rewrite_model_ids(&mut json, model_id);
                }
                if let Ok(body) = serde_json::to_string(&json) {
                    return Bytes::from(body);
                }
            }
        }

        if let Some(body) = Self::inject_profile_arn(request_body, profile_arn) {
            return Bytes::from(body);
        }

        original_body
            .get_or_insert_with(|| Bytes::copy_from_slice(request_body.as_bytes()))
            .clone()
    }

    fn summarize_error_body(status: reqwest::StatusCode, body: &str) -> String {
        summarize_upstream_error(status.as_u16(), body, ERROR_BODY_EXCERPT_CHARS)
    }

    fn is_context_length_exceeded_body(body: &str) -> bool {
        body.contains("CONTENT_LENGTH_EXCEEDS_THRESHOLD")
    }

    fn invalid_request_public_message(body: &str) -> String {
        if Self::is_invalid_thinking_signature_error(body, "") {
            return "Upstream rejected a historical thinking signature. Retry with unmodified thinking blocks or start a new conversation."
                .to_string();
        }
        if Self::is_context_length_exceeded_body(body) {
            return CONTEXT_LENGTH_EXCEEDED_PUBLIC_MESSAGE.to_string();
        }
        if Self::is_tool_schema_invalid_body(body) {
            return "Upstream rejected one of the tool definitions as invalid. Review the request tools and try again."
                .to_string();
        }
        if body.contains("Input is too long") {
            return "Input is too long. Reduce the size of your messages.".to_string();
        }
        if body.contains("TOOL_USE_RESULT_MISMATCH")
            || body.contains("toolResult blocks")
            || body.contains("toolUse blocks of previous turn")
        {
            return "Upstream rejected mismatched tool history. Ensure each tool_result immediately follows the assistant tool_use it answers."
                .to_string();
        }
        if body.contains("Improperly formed request") {
            return "Upstream rejected the request as malformed. Review message ordering, tool payloads, and oversized inputs.".to_string();
        }
        "Upstream rejected the request as invalid. Review the request payload and try again."
            .to_string()
    }

    fn is_tool_schema_invalid_body(body: &str) -> bool {
        body.contains("TOOL_SCHEMA_INVALID")
            || (body.contains("custom.input_schema") && body.contains("JSON schema is invalid"))
            || (body.contains("input_schema") && body.contains("draft 2020-12"))
    }

    fn request_too_large_public_message(body: &str) -> String {
        if Self::is_context_length_exceeded_body(body) {
            return CONTEXT_LENGTH_EXCEEDED_PUBLIC_MESSAGE.to_string();
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

    fn is_insufficient_model_capacity(body: &str, error_summary: &str) -> bool {
        body.contains("INSUFFICIENT_MODEL_CAPACITY")
            || error_summary.contains("reason=INSUFFICIENT_MODEL_CAPACITY")
            || body.contains("MODEL_TEMPORARILY_UNAVAILABLE")
            || error_summary.contains("reason=MODEL_TEMPORARILY_UNAVAILABLE")
    }

    fn insufficient_model_capacity_public_message() -> &'static str {
        "Upstream model capacity is temporarily unavailable. Retry later or choose another model."
    }

    fn insufficient_model_capacity_error(
        api_type: &str,
        error_summary: &str,
    ) -> PublicProviderError {
        PublicProviderError::service_unavailable(
            format!("{} API 请求失败: {}", api_type, error_summary),
            Self::insufficient_model_capacity_public_message(),
        )
    }

    fn is_suspicious_activity_limited(body: &str, error_summary: &str) -> bool {
        let body = body.to_ascii_lowercase();
        let error_summary = error_summary.to_ascii_lowercase();
        body.contains("suspicious activity") || error_summary.contains("suspicious activity")
    }

    fn is_invalid_thinking_signature_error(body: &str, error_summary: &str) -> bool {
        let body = body.to_ascii_lowercase();
        let error_summary = error_summary.to_ascii_lowercase();
        body.contains("invalid thinking signature")
            || error_summary.contains("invalid thinking signature")
    }

    fn public_client_error_for_status(
        status: reqwest::StatusCode,
        api_type: &str,
        error_summary: &str,
        body: &str,
    ) -> Option<PublicProviderError> {
        match status.as_u16() {
            413 if Self::is_context_length_exceeded_body(body) => {
                Some(PublicProviderError::context_length_exceeded(
                    status.as_u16(),
                    format!(
                        "{} API 请求失败: status={} {}",
                        api_type,
                        status.as_u16(),
                        error_summary
                    ),
                ))
            }
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
        Self::stream_pre_sse_timeout_error_with_budget(
            overall_started_at,
            api_type,
            request_id,
            STREAM_PRE_SSE_RESPONSE_BUDGET,
        )
    }

    fn stream_pre_sse_timeout_error_with_budget(
        overall_started_at: Instant,
        api_type: &str,
        request_id: &str,
        budget: Duration,
    ) -> anyhow::Error {
        anyhow::Error::new(PublicProviderError::gateway_timeout(
            format!(
                "{} API 请求超时: request_id={} total_elapsed_ms={} exceeded pre-SSE stream budget {}ms",
                api_type,
                request_id,
                overall_started_at.elapsed().as_millis(),
                budget.as_millis()
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

    fn remaining_stream_pre_sse_response_budget_with_config(
        overall_started_at: Instant,
        api_type: &str,
        request_id: &str,
        config: &StreamPreSseFailoverConfig,
    ) -> anyhow::Result<Duration> {
        let budget = Duration::from_millis(config.total_budget_ms.max(1));
        budget
            .checked_sub(overall_started_at.elapsed())
            .ok_or_else(|| {
                Self::stream_pre_sse_timeout_error_with_budget(
                    overall_started_at,
                    api_type,
                    request_id,
                    budget,
                )
            })
    }

    fn stream_pre_sse_configured_timeout_ms(
        config: &StreamPreSseFailoverConfig,
        request_body_bytes: usize,
        model: Option<&str>,
    ) -> u64 {
        let mut timeout_ms = if request_body_bytes <= config.small_request_threshold_bytes {
            config.small_request_timeout_ms
        } else if request_body_bytes <= config.medium_request_threshold_bytes {
            config.medium_request_timeout_ms
        } else if request_body_bytes <= config.large_request_threshold_bytes {
            config.large_request_timeout_ms
        } else {
            config.huge_request_timeout_ms
        };

        if timeout_ms > 0 && Self::is_real_opus_4_7_model(model) {
            timeout_ms = timeout_ms.max(config.slow_model_min_timeout_ms);
        }

        timeout_ms
    }

    fn stream_pre_sse_attempt_timeout(
        config: &StreamPreSseFailoverConfig,
        request_body_bytes: usize,
        model: Option<&str>,
        fast_failovers_used: usize,
        attempts_remaining: bool,
        retryable_candidates_after_current: usize,
        remaining_budget: Duration,
    ) -> StreamPreSseAttemptTimeout {
        let remaining_ms = remaining_budget.as_millis().min(u128::from(u64::MAX)) as u64;
        let configured_timeout_ms =
            Self::stream_pre_sse_configured_timeout_ms(config, request_body_bytes, model);
        if !config.enabled
            || configured_timeout_ms == 0
            || fast_failovers_used >= config.max_fast_failovers
            || !attempts_remaining
            || retryable_candidates_after_current == 0
            || remaining_ms <= config.min_remaining_ms
        {
            return StreamPreSseAttemptTimeout {
                timeout: remaining_budget,
                fast_failover: false,
                configured_timeout_ms,
            };
        }

        let reserved_final_budget = config.min_remaining_ms;
        if configured_timeout_ms.saturating_add(reserved_final_budget) >= remaining_ms {
            return StreamPreSseAttemptTimeout {
                timeout: remaining_budget,
                fast_failover: false,
                configured_timeout_ms,
            };
        }

        StreamPreSseAttemptTimeout {
            timeout: Duration::from_millis(configured_timeout_ms),
            fast_failover: true,
            configured_timeout_ms,
        }
    }

    fn remaining_stream_first_content_failover_budget(
        overall_started_at: Instant,
    ) -> Option<Duration> {
        STREAM_FIRST_CONTENT_FAILOVER_TOTAL_BUDGET.checked_sub(overall_started_at.elapsed())
    }

    fn should_apply_slow_first_content_shared_cooldown(request_body_bytes: usize) -> bool {
        request_body_bytes <= SLOW_FIRST_CONTENT_SHARED_COOLDOWN_MAX_REQUEST_BODY_BYTES
    }

    async fn prefetch_until_stream_content_start(
        response: reqwest::Response,
        trace: &ResponseTrace,
        timeout_budget: Duration,
        thinking_enabled: bool,
    ) -> anyhow::Result<StreamContentStartPrefetch> {
        let mut body_stream = response.bytes_stream().boxed();
        let mut prefetched = Vec::new();
        let mut prefetched_bytes = 0usize;
        let mut first_chunk_logged = false;
        let mut decoder = EventStreamDecoder::new();
        let mut probe = StreamContentStartProbe::new(thinking_enabled);
        let started_at = Instant::now();

        loop {
            let Some(remaining) = timeout_budget.checked_sub(started_at.elapsed()) else {
                if probe.should_release_after_stream_activity(prefetched_bytes) {
                    let probe_diagnostics = probe.diagnostics();
                    let prefetched_stream = stream::iter(prefetched.into_iter().map(Ok));
                    let stream = prefetched_stream.chain(body_stream).boxed();
                    return Ok(StreamContentStartPrefetch::Ready {
                        stream,
                        first_chunk_logged,
                        prefetched_bytes,
                        elapsed: started_at.elapsed(),
                        ready_reason: "stream_activity_without_content_start",
                        probe_diagnostics,
                    });
                }
                return Ok(StreamContentStartPrefetch::TimedOut {
                    elapsed: started_at.elapsed(),
                    prefetched_bytes,
                    probe_diagnostics: probe.diagnostics(),
                });
            };

            match timeout(remaining, body_stream.next()).await {
                Err(_) => {
                    if probe.should_release_after_stream_activity(prefetched_bytes) {
                        let probe_diagnostics = probe.diagnostics();
                        let prefetched_stream = stream::iter(prefetched.into_iter().map(Ok));
                        let stream = prefetched_stream.chain(body_stream).boxed();
                        return Ok(StreamContentStartPrefetch::Ready {
                            stream,
                            first_chunk_logged,
                            prefetched_bytes,
                            elapsed: started_at.elapsed(),
                            ready_reason: "stream_activity_without_content_start",
                            probe_diagnostics,
                        });
                    }
                    return Ok(StreamContentStartPrefetch::TimedOut {
                        elapsed: started_at.elapsed(),
                        prefetched_bytes,
                        probe_diagnostics: probe.diagnostics(),
                    });
                }
                Ok(Some(Ok(chunk))) => {
                    if !first_chunk_logged {
                        trace.log_first_chunk(chunk.len());
                        first_chunk_logged = true;
                    }
                    prefetched_bytes += chunk.len();
                    if let Err(err) = decoder.feed(&chunk) {
                        tracing::warn!(
                            error = %err,
                            chunk_len = chunk.len(),
                            decoder_buffer_len = decoder.buffer_len(),
                            "预读上游流时解码缓冲失败"
                        );
                        if err.is_fatal_stream_error() {
                            return Err(anyhow::anyhow!(
                                "预读上游流时解码进入不可恢复状态: {}",
                                err
                            ));
                        }
                    }
                    prefetched.push(chunk);

                    loop {
                        match decoder.decode() {
                            Ok(Some(frame)) => {
                                let message_type =
                                    frame.message_type().unwrap_or("unknown").to_string();
                                let event_type =
                                    frame.event_type().unwrap_or("unknown").to_string();
                                let payload_len = frame.payload.len();
                                let payload_excerpt = summarize_text_for_log(
                                    &String::from_utf8_lossy(&frame.payload),
                                    ERROR_BODY_EXCERPT_CHARS,
                                );
                                match Event::from_frame(frame) {
                                    Ok(event) => {
                                        if probe.observe(&event) {
                                            let probe_diagnostics = probe.diagnostics();
                                            let prefetched_stream =
                                                stream::iter(prefetched.into_iter().map(Ok));
                                            let stream =
                                                prefetched_stream.chain(body_stream).boxed();
                                            return Ok(StreamContentStartPrefetch::Ready {
                                                stream,
                                                first_chunk_logged,
                                                prefetched_bytes,
                                                elapsed: started_at.elapsed(),
                                                ready_reason: "content_start_observed",
                                                probe_diagnostics,
                                            });
                                        }
                                    }
                                    Err(err) => {
                                        tracing::warn!(
                                            error = %err,
                                            message_type,
                                            event_type,
                                            payload_len,
                                            payload_excerpt = %payload_excerpt,
                                            "预读上游流时事件 payload 解析失败"
                                        );
                                    }
                                }
                            }
                            Ok(None) => break,
                            Err(err) => {
                                tracing::warn!(
                                    error = %err,
                                    decoder_buffer_len = decoder.buffer_len(),
                                    decoder_error_count = decoder.error_count(),
                                    decoder_bytes_skipped = decoder.bytes_skipped(),
                                    "预读上游流时解码事件失败"
                                );
                                if err.is_fatal_stream_error() {
                                    return Err(anyhow::anyhow!(
                                        "预读上游流时解码进入不可恢复状态: {}",
                                        err
                                    ));
                                }
                            }
                        }
                    }
                }
                Ok(Some(Err(err))) => return Err(err.into()),
                Ok(None) => {
                    let probe_diagnostics = probe.diagnostics();
                    let prefetched_stream = stream::iter(prefetched.into_iter().map(Ok));
                    let stream = prefetched_stream.chain(body_stream).boxed();
                    return Ok(StreamContentStartPrefetch::Ready {
                        stream,
                        first_chunk_logged,
                        prefetched_bytes,
                        elapsed: started_at.elapsed(),
                        ready_reason: "upstream_stream_ended",
                        probe_diagnostics,
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
    /// - 5xx/网络等瞬态错误: 重试并在当前请求内切换其他候选凭据，但不禁用账号
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
    /// - 5xx/网络等瞬态错误: 重试并在当前请求内切换其他候选凭据，但不禁用账号
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
        let mut profile_rediscovered: HashSet<u64> = HashSet::new();
        let mut request_scoped_rate_limited_credentials: HashSet<u64> = HashSet::new();
        let mut request_scoped_transient_error_credentials: HashSet<u64> = HashSet::new();
        let mut priority_rate_limit_hits: HashMap<u32, usize> = HashMap::new();

        let mut attempt_count = 0;
        while attempt_count < max_retries {
            let supported_candidate_count =
                self.token_manager.enabled_supported_credential_count(None);
            let retryable_exclusion_count = Self::request_scoped_retryable_exclusion_count(
                &request_scoped_rate_limited_credentials,
                &request_scoped_transient_error_credentials,
            );
            if Self::should_reset_retryable_exclusions_for_next_pass(
                retryable_exclusion_count,
                supported_candidate_count,
                attempt_count,
                max_retries,
            ) {
                request_scoped_rate_limited_credentials.clear();
                request_scoped_transient_error_credentials.clear();
            }

            let attempt = attempt_count;
            attempt_count += 1;
            // 获取调用上下文
            // MCP 调用（WebSearch 等工具）不涉及模型选择，无需按模型过滤凭据
            let empty_model_unsupported = HashSet::new();
            let empty_slow_first_content = HashSet::new();
            let empty_body = HashSet::new();
            let request_scoped_excluded_credentials = Self::combined_request_exclusions(
                &empty_model_unsupported,
                &request_scoped_rate_limited_credentials,
                &empty_slow_first_content,
                &empty_body,
                &request_scoped_transient_error_credentials,
            );
            let ctx = match self
                .token_manager
                .acquire_context_with_background_refresh(
                    None,
                    1.0,
                    &request_scoped_excluded_credentials,
                    None,
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
                    if (!request_scoped_rate_limited_credentials.is_empty()
                        || !request_scoped_transient_error_credentials.is_empty())
                        && last_error.is_some()
                    {
                        tracing::warn!(
                            attempt = attempt + 1,
                            max_retries,
                            rate_limited_credentials =
                                request_scoped_rate_limited_credentials.len(),
                            transient_error_credentials =
                                request_scoped_transient_error_credentials.len(),
                            error = %err,
                            "MCP 请求已无未排除的故障转移候选"
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

            // MCP 请求按账号类型携带 profile ARN。BuilderID 缺省时使用 Kiro 默认值；
            // Enterprise 仅使用导入或 ListAvailableProfiles 发现到的 profileArn。
            if let Some(arn) = credentials.effective_profile_arn_for_kiro_requests() {
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
                    request_scoped_transient_error_credentials.insert(ctx_id);
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
                if Self::should_failover_missing_profile_arn(&credentials, &body, &error_summary) {
                    tracing::warn!(
                        credential_id = ctx_id,
                        attempt = attempt + 1,
                        max_retries,
                        error_summary = %error_summary,
                        "MCP 请求失败（Enterprise 请求要求 profileArn，当前请求跳过该凭据并切换）"
                    );

                    request_scoped_transient_error_credentials.insert(ctx_id);
                    last_error = Some(anyhow::anyhow!("MCP 请求失败: {}", error_summary));
                    continue;
                }

                anyhow::bail!("MCP 请求失败: {}", error_summary);
            }

            // 401/403 凭据问题
            if matches!(status.as_u16(), 401 | 403) {
                if Self::should_rediscover_enterprise_profile(
                    &credentials,
                    status,
                    &body,
                    &error_summary,
                ) && !profile_rediscovered.contains(&ctx_id)
                {
                    profile_rediscovered.insert(ctx_id);
                    tracing::info!(
                        credential_id = ctx_id,
                        attempt = attempt + 1,
                        max_retries,
                        error_summary = %error_summary,
                        "MCP 请求遇到 Enterprise profileArn 授权失败，尝试重新发现 profileArn 后重试"
                    );
                    match self
                        .token_manager
                        .rediscover_enterprise_profile_for(ctx_id, &credentials, &token)
                        .await
                    {
                        Ok(Some(_)) => continue,
                        Ok(None) => {
                            tracing::warn!(
                                credential_id = ctx_id,
                                "MCP 请求重新发现 Enterprise profileArn 未得到新可用 profileArn"
                            );
                        }
                        Err(err) => {
                            tracing::warn!(
                                credential_id = ctx_id,
                                "MCP 请求重新发现 Enterprise profileArn 失败: {}",
                                err
                            );
                        }
                    }
                }

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
                    if Self::is_suspicious_activity_limited(&body, &error_summary) {
                        self.token_manager
                            .report_suspicious_activity_limited(ctx_id, Some(&error_summary));
                    } else {
                        self.token_manager.report_rate_limited(ctx_id);
                    }
                    request_scoped_rate_limited_credentials.insert(ctx_id);
                    let empty_model_unsupported = HashSet::new();
                    let empty_slow_first_content = HashSet::new();
                    let empty_body = HashSet::new();
                    Self::maybe_spill_rate_limited_priority(
                        &self.token_manager,
                        None,
                        credentials.priority,
                        &mut priority_rate_limit_hits,
                        &empty_model_unsupported,
                        &mut request_scoped_rate_limited_credentials,
                        &empty_slow_first_content,
                        &empty_body,
                        &request_scoped_transient_error_credentials,
                    );
                    let supported_candidate_count =
                        self.token_manager.enabled_supported_credential_count(None);
                    max_retries = max_retries.max(Self::rate_limit_retry_cap(
                        total_credentials,
                        supported_candidate_count,
                        None,
                    ));
                } else {
                    request_scoped_transient_error_credentials.insert(ctx_id);
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
            request_scoped_transient_error_credentials.insert(ctx_id);
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
    /// - 真实 Opus 4.7/4.8 做有限候选探测，避免大池下单请求长期占用本地资源
    /// - 429 会在有界范围内扩展候选，并在请求内排除已 429 的凭据
    async fn call_api_with_retry(
        &self,
        request_body: &str,
        is_stream: bool,
        options: RequestOptions,
    ) -> anyhow::Result<ManagedResponse> {
        let total_credentials = self.token_manager.total_count();
        let mut last_error: Option<anyhow::Error> = None;
        let mut force_refreshed: HashSet<u64> = HashSet::new();
        let mut profile_rediscovered: HashSet<u64> = HashSet::new();
        let mut request_scoped_model_unsupported_credentials: HashSet<u64> = HashSet::new();
        let mut request_scoped_rate_limited_credentials: HashSet<u64> = HashSet::new();
        let mut request_scoped_slow_first_content_credentials: HashSet<u64> = HashSet::new();
        let mut request_scoped_empty_body_credentials: HashSet<u64> = HashSet::new();
        let mut request_scoped_transient_error_credentials: HashSet<u64> = HashSet::new();
        let mut priority_rate_limit_hits: HashMap<u32, usize> = HashMap::new();
        let mut slow_first_content_failovers = 0usize;
        let mut pre_sse_fast_failovers = 0usize;
        let mut non_stream_eventstream_stall_failovers = 0usize;
        let mut capacity_unavailable_count = 0usize;
        let api_type = if is_stream { "流式" } else { "非流式" };
        let request_id = options
            .request_id
            .clone()
            .unwrap_or_else(|| format!("kirors-{}", Uuid::new_v4().simple()));
        let request_weight = options.normalized_request_weight();
        let overall_started_at = Instant::now();
        let mut original_request_body: Option<Bytes> = None;
        let stream_pre_sse_failover_config =
            self.token_manager.stream_pre_sse_failover_config_snapshot();
        let non_stream_body_read_timeout_config = self
            .token_manager
            .non_stream_body_read_timeout_config_snapshot();
        let stream_dispatch_lease_release_enabled =
            is_stream && self.token_manager.stream_dispatch_lease_release_enabled();

        // Anthropic handlers already know the mapped Kiro model. Fall back to JSON extraction for
        // direct provider callers so large requests avoid an extra full-body parse on the hot path.
        let model_extract_started_at = Instant::now();
        let model = options
            .model_id
            .clone()
            .or_else(|| Self::extract_model_from_request(request_body));
        let model_extract_ms = model_extract_started_at.elapsed().as_millis();
        if request_body.len() >= LARGE_PROVIDER_REQUEST_WARN_THRESHOLD_BYTES
            || model_extract_ms >= SLOW_HEADERS_TO_FIRST_CHUNK_MS
        {
            tracing::warn!(
                request_id = %request_id,
                api_type,
                model = model.as_deref().unwrap_or("unknown"),
                request_body_bytes = request_body.len(),
                model_extract_ms,
                model_id_from_options = options.model_id.is_some(),
                "Kiro provider request model resolution completed"
            );
        }
        let mut max_retries = Self::initial_api_retry_cap(total_credentials, model.as_deref());
        let mut attempt_count = 0;

        while attempt_count < max_retries {
            let supported_candidate_count = self
                .token_manager
                .enabled_supported_credential_count(model.as_deref());
            let retryable_candidate_count = Self::request_retryable_candidate_count(
                supported_candidate_count,
                &request_scoped_model_unsupported_credentials,
                &request_scoped_empty_body_credentials,
            );
            let retryable_exclusion_count = Self::request_scoped_retryable_exclusion_count(
                &request_scoped_rate_limited_credentials,
                &request_scoped_transient_error_credentials,
            );
            if Self::should_reset_retryable_exclusions_for_next_pass(
                retryable_exclusion_count,
                retryable_candidate_count,
                attempt_count,
                max_retries,
            ) {
                request_scoped_rate_limited_credentials.clear();
                request_scoped_transient_error_credentials.clear();
            }

            let attempt = attempt_count;
            attempt_count += 1;
            let attempt_started_at = Instant::now();
            // 获取调用上下文（绑定 index、credentials、token）
            let request_scoped_excluded_credentials = Self::combined_request_exclusions(
                &request_scoped_model_unsupported_credentials,
                &request_scoped_rate_limited_credentials,
                &request_scoped_slow_first_content_credentials,
                &request_scoped_empty_body_credentials,
                &request_scoped_transient_error_credentials,
            );
            let acquire_context = || {
                self.token_manager.acquire_context_with_background_refresh(
                    model.as_deref(),
                    request_weight,
                    &request_scoped_excluded_credentials,
                    options.session_affinity_key.as_deref(),
                )
            };
            let acquire_result = if is_stream {
                let remaining = Self::remaining_stream_pre_sse_response_budget_with_config(
                    overall_started_at,
                    api_type,
                    &request_id,
                    &stream_pre_sse_failover_config,
                )?;
                match timeout(remaining, acquire_context()).await {
                    Ok(result) => result,
                    Err(_) => {
                        return Err(Self::stream_pre_sse_timeout_error_with_budget(
                            overall_started_at,
                            api_type,
                            &request_id,
                            Duration::from_millis(stream_pre_sse_failover_config.total_budget_ms),
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
                    if (!request_scoped_rate_limited_credentials.is_empty()
                        || !request_scoped_empty_body_credentials.is_empty()
                        || !request_scoped_transient_error_credentials.is_empty())
                        && last_error.is_some()
                    {
                        tracing::warn!(
                            request_id = %request_id,
                            api_type,
                            model = model.as_deref().unwrap_or("unknown"),
                            attempt = attempt + 1,
                            max_retries,
                            rate_limited_credentials =
                                request_scoped_rate_limited_credentials.len(),
                            empty_body_credentials =
                                request_scoped_empty_body_credentials.len(),
                            transient_error_credentials =
                                request_scoped_transient_error_credentials.len(),
                            error = %err,
                            "当前请求已无未排除的故障转移候选"
                        );
                        break;
                    }
                    return Err(err);
                }
            };
            let (ctx_id, credentials, token, lease) = ctx.into_parts();
            let effective_model_id = model.as_deref().and_then(|requested_model| {
                credentials.effective_model_id_for_request(requested_model)
            });
            if let Some(requested_model) = model.as_deref() {
                if effective_model_id.is_none() && !credentials.available_model_ids.is_empty() {
                    self.token_manager
                        .trigger_available_models_refresh_after_model_signal(
                            ctx_id,
                            &token,
                            requested_model,
                            "cached-model-miss",
                        );
                    request_scoped_model_unsupported_credentials.insert(ctx_id);
                    let has_available = self
                        .token_manager
                        .defer_model_unsupported_credential(ctx_id, requested_model);
                    last_error = Some(anyhow::anyhow!(
                        "凭据 #{} 不支持模型 {}，且可用模型列表中没有可降级模型",
                        ctx_id,
                        requested_model
                    ));
                    if has_available {
                        continue;
                    }
                    break;
                }
            }

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

            // BuilderID 缺省时使用 Kiro 默认 profileArn；Enterprise 仅使用导入或发现到的 profileArn。
            let body_inject_started_at = Instant::now();
            let effective_profile_arn = credentials
                .effective_profile_arn_for_kiro_requests()
                .map(str::to_string);
            let strip_profile_arn =
                credentials.detected_auth_account_type().as_deref() == Some("enterprise");
            let model_rewrite = match (model.as_deref(), effective_model_id.as_deref()) {
                (Some(requested), Some(effective)) if requested != effective => {
                    Some(effective.to_string())
                }
                _ => None,
            };
            let body = Self::request_body_for_profile_arn_and_model(
                request_body,
                &mut original_request_body,
                &effective_profile_arn,
                &model_rewrite,
                strip_profile_arn,
            );
            let body_inject_ms = body_inject_started_at.elapsed().as_millis();
            let request_body_bytes = body.len();
            let request_body_guard = self.token_manager.kiro_request_body_guard_config_snapshot();
            if request_body_guard.should_reject(request_body_bytes) {
                tracing::warn!(
                    request_id = %request_id,
                    api_type,
                    model = model.as_deref().unwrap_or("unknown"),
                    credential_id = ctx_id,
                    attempt = attempt + 1,
                    request_body_bytes,
                    guard_limit_bytes = request_body_guard.max_bytes,
                    profile_arn_present = effective_profile_arn.is_some(),
                    "Kiro provider request body guard rejected oversized request before upstream dispatch"
                );
                return Err(anyhow::Error::new(
                    PublicProviderError::context_length_exceeded(
                        400,
                        format!(
                            "{} API 请求本地拦截: final Kiro request body {} bytes exceeds configured limit {} bytes",
                            api_type, request_body_bytes, request_body_guard.max_bytes
                        ),
                    ),
                ));
            }
            if request_body_bytes >= LARGE_PROVIDER_REQUEST_WARN_THRESHOLD_BYTES
                || body_inject_ms >= SLOW_HEADERS_TO_FIRST_CHUNK_MS
            {
                tracing::warn!(
                    request_id = %request_id,
                    api_type,
                    model = model.as_deref().unwrap_or("unknown"),
                    credential_id = ctx_id,
                    attempt = attempt + 1,
                    request_body_bytes,
                    body_inject_ms,
                    profile_arn_present = effective_profile_arn.is_some(),
                    effective_model = effective_model_id.as_deref().unwrap_or("unknown"),
                    "Kiro provider request body prepared"
                );
            }

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
                Some(Self::remaining_stream_pre_sse_response_budget_with_config(
                    overall_started_at,
                    api_type,
                    &request_id,
                    &stream_pre_sse_failover_config,
                )?)
            } else {
                None
            };
            let stream_pre_sse_retryable_candidates_after_current = if is_stream {
                let mut transient_after_current =
                    request_scoped_transient_error_credentials.clone();
                transient_after_current.insert(ctx_id);
                let retryable_exclusions_after_current =
                    Self::request_scoped_retryable_exclusion_count(
                        &request_scoped_rate_limited_credentials,
                        &transient_after_current,
                    );
                retryable_candidate_count.saturating_sub(retryable_exclusions_after_current)
            } else {
                0
            };
            let stream_pre_sse_attempt_timeout = stream_pre_sse_budget_remaining.map(|remaining| {
                Self::stream_pre_sse_attempt_timeout(
                    &stream_pre_sse_failover_config,
                    request_body_bytes,
                    model.as_deref(),
                    pre_sse_fast_failovers,
                    attempt + 1 < max_retries,
                    stream_pre_sse_retryable_candidates_after_current,
                    remaining,
                )
            });
            tracing::info!(
                request_id = %request_id,
                api_type,
                model = model.as_deref().unwrap_or("unknown"),
                effective_model = effective_model_id.as_deref().unwrap_or("unknown"),
                credential_id = ctx_id,
                attempt = attempt + 1,
                max_retries,
                region = %region,
                stream = is_stream,
                request_body_bytes,
                request_weight,
                acquire_context_ms,
                model_extract_ms,
                body_inject_ms,
                stream_budget_remaining_ms = stream_budget_remaining
                    .map(|value| value.as_millis())
                    .unwrap_or(0),
                stream_pre_sse_budget_remaining_ms = stream_pre_sse_budget_remaining
                    .map(|value| value.as_millis())
                    .unwrap_or(0),
                stream_pre_sse_failover_enabled = stream_pre_sse_failover_config.enabled,
                stream_pre_sse_configured_timeout_ms = stream_pre_sse_attempt_timeout
                    .map(|value| value.configured_timeout_ms)
                    .unwrap_or(0),
                stream_pre_sse_attempt_timeout_ms = stream_pre_sse_attempt_timeout
                    .map(|value| value.timeout.as_millis())
                    .unwrap_or(0),
                stream_pre_sse_fast_failover = stream_pre_sse_attempt_timeout
                    .map(|value| value.fast_failover)
                    .unwrap_or(false),
                stream_pre_sse_fast_failovers_used = pre_sse_fast_failovers,
                stream_pre_sse_max_fast_failovers =
                    stream_pre_sse_failover_config.max_fast_failovers,
                stream_pre_sse_retryable_candidates_after_current =
                    stream_pre_sse_retryable_candidates_after_current,
                stream_request_timeout_override_ms = 0u64,
                upstream_client_timeout_ms = u128::from(DEFAULT_UPSTREAM_TIMEOUT_SECS) * 1000,
                omit_agent_mode_header = options.omit_agent_mode_header,
                invocation_id = %invocation_id,
                "开始调用上游 Kiro API"
            );

            let upstream_request_started_at = Instant::now();
            let send_result = if let Some(pre_sse_attempt_timeout) = stream_pre_sse_attempt_timeout
            {
                match timeout(pre_sse_attempt_timeout.timeout, request.send()).await {
                    Ok(result) => result,
                    Err(_) => {
                        let will_retry = pre_sse_attempt_timeout.fast_failover;
                        tracing::warn!(
                            request_id = %request_id,
                            api_type,
                            model = model.as_deref().unwrap_or("unknown"),
                            credential_id = ctx_id,
                            attempt = attempt + 1,
                            max_retries,
                            region = %region,
                            request_body_bytes,
                            request_weight,
                            acquire_context_ms,
                            upstream_wait_ms = upstream_request_started_at.elapsed().as_millis(),
                            total_elapsed_ms = overall_started_at.elapsed().as_millis(),
                            stream_pre_sse_total_budget_ms =
                                stream_pre_sse_failover_config.total_budget_ms,
                            stream_pre_sse_budget_remaining_ms = stream_pre_sse_budget_remaining
                                .map(|value| value.as_millis())
                                .unwrap_or(0),
                            stream_pre_sse_configured_timeout_ms =
                                pre_sse_attempt_timeout.configured_timeout_ms,
                            stream_pre_sse_attempt_timeout_ms =
                                pre_sse_attempt_timeout.timeout.as_millis(),
                            stream_pre_sse_fast_failover = pre_sse_attempt_timeout.fast_failover,
                            stream_pre_sse_fast_failovers_used = pre_sse_fast_failovers,
                            stream_pre_sse_max_fast_failovers =
                                stream_pre_sse_failover_config.max_fast_failovers,
                            stream_pre_sse_retryable_candidates_after_current =
                                stream_pre_sse_retryable_candidates_after_current,
                            will_retry,
                            "等待上游响应头超时"
                        );
                        if will_retry {
                            pre_sse_fast_failovers = pre_sse_fast_failovers.saturating_add(1);
                            last_error = Some(anyhow::Error::new(
                                PublicProviderError::gateway_timeout(
                                    format!(
                                        "{} API 响应头等待超时: request_id={} attempt={} total_elapsed_ms={} upstream_wait_ms={} timeout_ms={}",
                                        api_type,
                                        request_id,
                                        attempt + 1,
                                        overall_started_at.elapsed().as_millis(),
                                        upstream_request_started_at.elapsed().as_millis(),
                                        pre_sse_attempt_timeout.timeout.as_millis()
                                    ),
                                    Self::stream_pre_sse_timeout_public_message(),
                                ),
                            ));
                            request_scoped_transient_error_credentials.insert(ctx_id);
                            if attempt + 1 < max_retries {
                                sleep(Self::retry_delay(attempt)).await;
                            }
                            continue;
                        }

                        return Err(Self::stream_pre_sse_timeout_error_with_budget(
                            overall_started_at,
                            api_type,
                            &request_id,
                            Duration::from_millis(stream_pre_sse_failover_config.total_budget_ms),
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
                    // 网络错误通常是上游/链路瞬态问题，不应禁用凭据；
                    // 仅在当前请求内跳过该候选，给低优先级账号兜底机会。
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
                    request_scoped_transient_error_credentials.insert(ctx_id);
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
                let is_real_opus_4_7 = Self::is_real_opus_4_7_model(model.as_deref());
                let slow_model_header_cooldown_applied = is_stream
                    && is_real_opus_4_7
                    && upstream_headers_ms >= OPUS_4_7_SLOW_MODEL_HEADERS_MS
                    && self.token_manager.defer_slow_model_credential(
                        ctx_id,
                        model.as_deref().unwrap_or("unknown"),
                        OPUS_4_7_SLOW_MODEL_COOLDOWN,
                        "slow_upstream_headers",
                    );
                if slow_model_header_cooldown_applied {
                    tracing::warn!(
                        request_id = %request_id,
                        api_type,
                        model = model.as_deref().unwrap_or("unknown"),
                        credential_id = ctx_id,
                        attempt = attempt + 1,
                        max_retries,
                        region = %region,
                        request_body_bytes,
                        status_code = status.as_u16(),
                        upstream_headers_ms,
                        slow_model_cooldown_threshold_ms = OPUS_4_7_SLOW_MODEL_HEADERS_MS,
                        slow_model_cooldown_ms = OPUS_4_7_SLOW_MODEL_COOLDOWN.as_millis(),
                        "真实高阶 Opus 响应头过慢，已触发模型级冷却"
                    );
                }
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
                    response_content_type: response_header_for_log(
                        response.headers(),
                        "content-type",
                    ),
                    response_content_encoding: response_header_for_log(
                        response.headers(),
                        "content-encoding",
                    ),
                    response_content_length: response_header_for_log(
                        response.headers(),
                        "content-length",
                    ),
                    response_transfer_encoding: response_header_for_log(
                        response.headers(),
                        "transfer-encoding",
                    ),
                    slow_model_cooldown: (is_stream && is_real_opus_4_7).then(|| {
                        SlowModelCooldownTrace {
                            token_manager: Arc::clone(&self.token_manager),
                        }
                    }),
                };

                if !is_stream {
                    let body_read_result = if non_stream_body_read_timeout_config.enabled {
                        read_response_body_with_trace_timeout(
                            response,
                            &trace,
                            non_stream_body_read_timeout_config.timeout_ms,
                            non_stream_body_read_timeout_config.eventstream_idle_timeout_ms,
                        )
                        .await
                    } else {
                        read_response_body_with_trace(response, &trace)
                            .await
                            .map_err(ResponseBodyReadFailure::Upstream)
                    };
                    let body_bytes = match body_read_result {
                        Ok(bytes) => bytes,
                        Err(ResponseBodyReadFailure::Upstream(err)) => {
                            last_error = Some(anyhow::Error::new(
                                PublicProviderError::bad_gateway(
                                    format!(
                                        "{} API 响应体读取失败: request_id={} credential_id={} attempt={} status={} total_elapsed_ms={} error={}",
                                        api_type,
                                        request_id,
                                        ctx_id,
                                        attempt + 1,
                                        status.as_u16(),
                                        overall_started_at.elapsed().as_millis(),
                                        err
                                    ),
                                    "Upstream response body could not be read. Retry later.",
                                ),
                            ));
                            request_scoped_transient_error_credentials.insert(ctx_id);
                            if attempt + 1 < max_retries {
                                sleep(Self::retry_delay(attempt)).await;
                            }
                            continue;
                        }
                        Err(ResponseBodyReadFailure::Timeout {
                            timeout_ms,
                            reason,
                            eventstream_diagnostics,
                        }) => {
                            let body_read_elapsed_ms = response_headers_at.elapsed().as_millis();
                            let eventstream_safe_retry_candidate = reason
                                == "eventstream_idle_timeout"
                                && eventstream_diagnostics
                                    .as_ref()
                                    .is_some_and(|diagnostics| diagnostics.safe_to_retry_stall())
                                && non_stream_eventstream_stall_failovers
                                    < MAX_NON_STREAM_EVENTSTREAM_STALL_FAILOVERS;
                            let will_retry = attempt + 1 < max_retries
                                && (non_stream_body_read_timeout_config.retry_on_timeout
                                    || (non_stream_body_read_timeout_config
                                        .eventstream_safe_retry_on_stall
                                        && eventstream_safe_retry_candidate));
                            let eventstream_observed_frames = eventstream_diagnostics
                                .as_ref()
                                .map(|diagnostics| diagnostics.observed_frames)
                                .unwrap_or(0);
                            let eventstream_assistant_events = eventstream_diagnostics
                                .as_ref()
                                .map(|diagnostics| diagnostics.assistant_events)
                                .unwrap_or(0);
                            let eventstream_assistant_content_bytes = eventstream_diagnostics
                                .as_ref()
                                .map(|diagnostics| diagnostics.assistant_content_bytes)
                                .unwrap_or(0);
                            let eventstream_reasoning_events = eventstream_diagnostics
                                .as_ref()
                                .map(|diagnostics| diagnostics.reasoning_events)
                                .unwrap_or(0);
                            let eventstream_reasoning_text_bytes = eventstream_diagnostics
                                .as_ref()
                                .map(|diagnostics| diagnostics.reasoning_text_bytes)
                                .unwrap_or(0);
                            let eventstream_tool_use_events = eventstream_diagnostics
                                .as_ref()
                                .map(|diagnostics| diagnostics.tool_use_events)
                                .unwrap_or(0);
                            let eventstream_unknown_events = eventstream_diagnostics
                                .as_ref()
                                .map(|diagnostics| diagnostics.unknown_events)
                                .unwrap_or(0);
                            let eventstream_error_events = eventstream_diagnostics
                                .as_ref()
                                .map(|diagnostics| diagnostics.error_events)
                                .unwrap_or(0);
                            let eventstream_exception_events = eventstream_diagnostics
                                .as_ref()
                                .map(|diagnostics| diagnostics.exception_events)
                                .unwrap_or(0);
                            let eventstream_decoder_errors = eventstream_diagnostics
                                .as_ref()
                                .map(|diagnostics| diagnostics.decoder_errors)
                                .unwrap_or(0);
                            let eventstream_payload_parse_errors = eventstream_diagnostics
                                .as_ref()
                                .map(|diagnostics| diagnostics.payload_parse_errors)
                                .unwrap_or(0);
                            let eventstream_last_message_type = eventstream_diagnostics
                                .as_ref()
                                .map(|diagnostics| diagnostics.last_message_type.as_str())
                                .unwrap_or("");
                            let eventstream_last_event_type = eventstream_diagnostics
                                .as_ref()
                                .map(|diagnostics| diagnostics.last_event_type.as_str())
                                .unwrap_or("");
                            let eventstream_timeout_output_state = eventstream_diagnostics
                                .as_ref()
                                .map(|diagnostics| {
                                    if diagnostics.has_usable_output() {
                                        "after_usable_output"
                                    } else if diagnostics.observed_frames > 0 {
                                        "before_usable_output"
                                    } else {
                                        "no_observed_frames"
                                    }
                                })
                                .unwrap_or("not_eventstream");
                            tracing::warn!(
                                request_id = %request_id,
                                api_type,
                                model = model.as_deref().unwrap_or("unknown"),
                                credential_id = ctx_id,
                                attempt = attempt + 1,
                                max_retries,
                                region = %region,
                                request_body_bytes,
                                status_code = status.as_u16(),
                                timeout_ms,
                                timeout_reason = reason,
                                body_read_elapsed_ms,
                                total_elapsed_ms = overall_started_at.elapsed().as_millis(),
                                retry_on_timeout =
                                    non_stream_body_read_timeout_config.retry_on_timeout,
                                eventstream_safe_retry_on_stall =
                                    non_stream_body_read_timeout_config
                                        .eventstream_safe_retry_on_stall,
                                eventstream_safe_retry_candidate,
                                non_stream_eventstream_stall_failovers,
                                max_non_stream_eventstream_stall_failovers =
                                    MAX_NON_STREAM_EVENTSTREAM_STALL_FAILOVERS,
                                eventstream_observed_frames,
                                eventstream_assistant_events,
                                eventstream_assistant_content_bytes,
                                eventstream_reasoning_events,
                                eventstream_reasoning_text_bytes,
                                eventstream_tool_use_events,
                                eventstream_unknown_events,
                                eventstream_error_events,
                                eventstream_exception_events,
                                eventstream_decoder_errors,
                                eventstream_payload_parse_errors,
                                eventstream_last_message_type,
                                eventstream_last_event_type,
                                eventstream_timeout_output_state,
                                will_retry,
                                "非流式上游响应体读取超时"
                            );
                            let timeout_error = anyhow::Error::new(
                                PublicProviderError::gateway_timeout(
                                    format!(
                                        "{} API 响应体读取超时: request_id={} credential_id={} attempt={} status={} timeout_ms={} timeout_reason={} body_read_elapsed_ms={} total_elapsed_ms={} retry_on_timeout={} eventstream_safe_retry_on_stall={} eventstream_safe_retry_candidate={} will_retry={}",
                                        api_type,
                                        request_id,
                                        ctx_id,
                                        attempt + 1,
                                        status.as_u16(),
                                        timeout_ms,
                                        reason,
                                        body_read_elapsed_ms,
                                        overall_started_at.elapsed().as_millis(),
                                        non_stream_body_read_timeout_config.retry_on_timeout,
                                        non_stream_body_read_timeout_config
                                            .eventstream_safe_retry_on_stall,
                                        eventstream_safe_retry_candidate,
                                        will_retry
                                    ),
                                    "Upstream response body timed out before a complete non-stream response was received.",
                                ),
                            );

                            if will_retry {
                                if eventstream_safe_retry_candidate {
                                    non_stream_eventstream_stall_failovers =
                                        non_stream_eventstream_stall_failovers.saturating_add(1);
                                }
                                last_error = Some(timeout_error);
                                request_scoped_transient_error_credentials.insert(ctx_id);
                                sleep(Self::retry_delay(attempt)).await;
                                continue;
                            }

                            return Err(timeout_error);
                        }
                    };
                    if body_bytes.is_empty() {
                        request_scoped_empty_body_credentials.insert(ctx_id);
                        let retryable_candidate_count_after_empty =
                            Self::request_retryable_candidate_count(
                                supported_candidate_count,
                                &request_scoped_model_unsupported_credentials,
                                &request_scoped_empty_body_credentials,
                            );
                        let will_retry_with_alternate_credential =
                            attempt + 1 < max_retries && retryable_candidate_count_after_empty > 0;
                        tracing::warn!(
                            request_id = %request_id,
                            api_type,
                            model = model.as_deref().unwrap_or("unknown"),
                            credential_id = ctx_id,
                            attempt = attempt + 1,
                            max_retries,
                            region = %region,
                            request_body_bytes,
                            status_code = status.as_u16(),
                            body_len = 0usize,
                            empty_body_credentials =
                                request_scoped_empty_body_credentials.len(),
                            supported_candidate_count,
                            retryable_candidate_count_after_empty,
                            will_retry_with_alternate_credential,
                            total_elapsed_ms = overall_started_at.elapsed().as_millis(),
                            "非流式上游成功响应体为空，当前请求排除该凭据"
                        );
                        last_error = Some(anyhow::Error::new(PublicProviderError::bad_gateway(
                            format!(
                                "{} API 上游成功响应体为空: request_id={} credential_id={} attempt={} status={} body_len=0 empty_body_credentials={} retryable_candidate_count_after_empty={} total_elapsed_ms={}",
                                api_type,
                                request_id,
                                ctx_id,
                                attempt + 1,
                                status.as_u16(),
                                request_scoped_empty_body_credentials.len(),
                                retryable_candidate_count_after_empty,
                                overall_started_at.elapsed().as_millis()
                            ),
                            "Upstream returned an empty response body for this request. Retry later.",
                        )));
                        continue;
                    }

                    self.token_manager.record_session_affinity(
                        model.as_deref(),
                        options.session_affinity_key.as_deref(),
                        ctx_id,
                    );
                    self.token_manager.report_success(ctx_id);
                    return Ok(ManagedResponse::new_bytes(body_bytes, lease));
                }

                let retryable_excluded_count = Self::request_scoped_retryable_exclusion_count(
                    &request_scoped_rate_limited_credentials,
                    &request_scoped_transient_error_credentials,
                );
                let scoped_candidate_count = supported_candidate_count
                    .saturating_sub(request_scoped_model_unsupported_credentials.len())
                    .saturating_sub(retryable_excluded_count)
                    .saturating_sub(request_scoped_slow_first_content_credentials.len());
                let content_start_probe_budget =
                    Self::remaining_stream_first_content_failover_budget(overall_started_at);
                let has_content_start_probe_budget =
                    content_start_probe_budget.is_some_and(|remaining| {
                        remaining >= MIN_STREAM_FIRST_CONTENT_FAILOVER_REMAINING
                    });
                let can_failover_slow_first_content = slow_first_content_failovers
                    < MAX_SLOW_FIRST_CONTENT_FAILOVERS
                    && scoped_candidate_count > 1;
                let should_guard_final_first_content_attempt = slow_first_content_failovers > 0;
                let should_probe_stream_content_start = is_stream
                    && options.wait_for_stream_content_start
                    && has_content_start_probe_budget
                    && (can_failover_slow_first_content
                        || should_guard_final_first_content_attempt);

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
                            ready_reason,
                            probe_diagnostics,
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
                                content_start_ready_reason = ready_reason,
                                prefetch_observed_events = probe_diagnostics.observed_events,
                                prefetch_non_error_events = probe_diagnostics.non_error_events,
                                prefetch_assistant_events = probe_diagnostics.assistant_events,
                                prefetch_assistant_content_bytes =
                                    probe_diagnostics.assistant_content_bytes,
                                prefetch_reasoning_events = probe_diagnostics.reasoning_events,
                                prefetch_reasoning_text_bytes =
                                    probe_diagnostics.reasoning_text_bytes,
                                prefetch_tool_use_events = probe_diagnostics.tool_use_events,
                                prefetch_error_events = probe_diagnostics.error_events,
                                slow_first_content_failovers,
                                max_slow_first_content_failovers = MAX_SLOW_FIRST_CONTENT_FAILOVERS,
                                "上游流预读已满足首内容块调度条件"
                            );
                            self.token_manager.record_session_affinity(
                                model.as_deref(),
                                options.session_affinity_key.as_deref(),
                                ctx_id,
                            );
                            self.token_manager.report_success(ctx_id);
                            let response_lease = if stream_dispatch_lease_release_enabled {
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
                                    stream_dispatch_lease_release_reason = ready_reason,
                                    "流式请求已开始产生可转发内容，提前释放调度 lease"
                                );
                                drop(lease);
                                None
                            } else {
                                Some(lease)
                            };
                            return Ok(ManagedResponse::new_stream(
                                stream,
                                response_lease,
                                Some(trace),
                                first_chunk_logged,
                            ));
                        }
                        Ok(StreamContentStartPrefetch::TimedOut {
                            elapsed,
                            prefetched_bytes,
                            probe_diagnostics,
                        }) => {
                            let remaining_after_prefetch =
                                Self::remaining_stream_first_content_failover_budget(
                                    overall_started_at,
                                );
                            let can_failover_after_timeout = slow_first_content_failovers
                                < MAX_SLOW_FIRST_CONTENT_FAILOVERS
                                && scoped_candidate_count > 1
                                && remaining_after_prefetch.is_some_and(|remaining| {
                                    remaining >= MIN_STREAM_FIRST_CONTENT_FAILOVER_REMAINING
                                });
                            let slow_model_cooldown_applied = can_failover_after_timeout
                                && is_real_opus_4_7
                                && self.token_manager.defer_slow_model_credential(
                                    ctx_id,
                                    model.as_deref().unwrap_or("unknown"),
                                    OPUS_4_7_SLOW_MODEL_COOLDOWN,
                                    "slow_first_content",
                                );
                            let shared_cooldown_applied = can_failover_after_timeout
                                && !slow_model_cooldown_applied
                                && Self::should_apply_slow_first_content_shared_cooldown(
                                    request_body_bytes,
                                );
                            let cooldown_skipped_reason = if shared_cooldown_applied {
                                "none"
                            } else if !can_failover_after_timeout {
                                "no_followup_failover"
                            } else if slow_model_cooldown_applied {
                                "model_cooldown_applied"
                            } else {
                                "request_body_too_large"
                            };
                            let applied_cooldown_ms = if shared_cooldown_applied {
                                STREAM_FIRST_CONTENT_FAILOVER_COOLDOWN.as_millis()
                            } else {
                                0
                            };
                            let first_content_timeout_reason = if can_failover_after_timeout {
                                "will_retry_with_alternate_credential"
                            } else if slow_first_content_failovers
                                >= MAX_SLOW_FIRST_CONTENT_FAILOVERS
                            {
                                "max_slow_first_content_failovers_reached"
                            } else if scoped_candidate_count <= 1 {
                                "no_alternate_candidate"
                            } else {
                                "first_content_failover_budget_exhausted"
                            };
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
                                prefetch_observed_events = probe_diagnostics.observed_events,
                                prefetch_non_error_events = probe_diagnostics.non_error_events,
                                prefetch_assistant_events = probe_diagnostics.assistant_events,
                                prefetch_assistant_content_bytes =
                                    probe_diagnostics.assistant_content_bytes,
                                prefetch_reasoning_events = probe_diagnostics.reasoning_events,
                                prefetch_reasoning_text_bytes =
                                    probe_diagnostics.reasoning_text_bytes,
                                prefetch_tool_use_events = probe_diagnostics.tool_use_events,
                                prefetch_error_events = probe_diagnostics.error_events,
                                slow_first_content_failovers,
                                max_slow_first_content_failovers = MAX_SLOW_FIRST_CONTENT_FAILOVERS,
                                scoped_candidate_count,
                                remaining_first_content_failover_budget_ms = remaining_after_prefetch
                                    .map(|value| value.as_millis())
                                    .unwrap_or(0),
                                will_failover = can_failover_after_timeout,
                                shared_cooldown_applied,
                                slow_model_cooldown_applied,
                                cooldown_ms = applied_cooldown_ms,
                                configured_cooldown_ms =
                                    STREAM_FIRST_CONTENT_FAILOVER_COOLDOWN.as_millis(),
                                slow_model_cooldown_ms = OPUS_4_7_SLOW_MODEL_COOLDOWN.as_millis(),
                                shared_cooldown_max_request_body_bytes =
                                    SLOW_FIRST_CONTENT_SHARED_COOLDOWN_MAX_REQUEST_BODY_BYTES,
                                cooldown_skipped_reason,
                                first_content_timeout_reason,
                                "上游流在预算内未产生可转换内容块"
                            );
                            let timeout_error = anyhow::Error::new(
                                PublicProviderError::gateway_timeout(
                                    format!(
                                        "{} API 请求首内容块超时: request_id={} credential_id={} attempt={} prefetch_elapsed_ms={} prefetched_bytes={} slow_first_content_failovers={} first_content_timeout_reason={}",
                                        api_type,
                                        request_id,
                                        ctx_id,
                                        attempt + 1,
                                        elapsed.as_millis(),
                                        prefetched_bytes,
                                        slow_first_content_failovers,
                                        first_content_timeout_reason
                                    ),
                                    Self::stream_pre_sse_timeout_public_message(),
                                ),
                            );
                            if can_failover_after_timeout {
                                request_scoped_slow_first_content_credentials.insert(ctx_id);
                                slow_first_content_failovers =
                                    slow_first_content_failovers.saturating_add(1);
                                if shared_cooldown_applied {
                                    let _ = self.token_manager.defer_slow_first_content_credential(
                                        ctx_id,
                                        model.as_deref().unwrap_or("unknown"),
                                        STREAM_FIRST_CONTENT_FAILOVER_COOLDOWN,
                                    );
                                }
                                last_error = Some(timeout_error);
                                continue;
                            }
                            return Err(timeout_error);
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
                            last_error = Some(err);
                            request_scoped_transient_error_credentials.insert(ctx_id);
                            if attempt + 1 < max_retries {
                                sleep(Self::retry_delay(attempt)).await;
                            }
                            continue;
                        }
                    }
                }

                self.token_manager.record_session_affinity(
                    model.as_deref(),
                    options.session_affinity_key.as_deref(),
                    ctx_id,
                );
                self.token_manager.report_success(ctx_id);
                if stream_dispatch_lease_release_enabled {
                    tracing::info!(
                        request_id = %request_id,
                        api_type,
                        model = model.as_deref().unwrap_or("unknown"),
                        credential_id = ctx_id,
                        attempt = attempt + 1,
                        max_retries,
                        region = %region,
                        request_body_bytes,
                        stream_dispatch_lease_release_reason =
                            "stream_headers_accepted_without_content_probe",
                        "流式请求上游响应已建立，提前释放调度 lease"
                    );
                    let stream = response.bytes_stream().boxed();
                    drop(lease);
                    return Ok(ManagedResponse::new_stream(
                        stream,
                        None,
                        Some(trace),
                        false,
                    ));
                }
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

                    self.token_manager
                        .trigger_available_models_refresh_after_model_signal(
                            ctx_id,
                            &token,
                            model.as_deref().unwrap_or("unknown"),
                            "invalid-model-id",
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

                if Self::should_failover_missing_profile_arn(&credentials, &body, &error_summary) {
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
                        "API 请求失败（Enterprise 请求要求 profileArn，当前请求跳过该凭据并切换）"
                    );

                    request_scoped_transient_error_credentials.insert(ctx_id);
                    last_error = Some(anyhow::anyhow!(
                        "{} API 请求失败: {}",
                        api_type,
                        error_summary
                    ));
                    continue;
                }

                if Self::is_invalid_thinking_signature_error(&body, &error_summary) {
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
                        upstream_invalid_thinking_signature = true,
                        total_elapsed_ms = overall_started_at.elapsed().as_millis(),
                        "API 请求失败（上游拒绝 thinking signature）"
                    );
                }

                let capture_config = Capture400BodiesConfig::from_env();
                capture_400_request_body_for_diagnostics(
                    &capture_config,
                    Capture400Request {
                        request_id: &request_id,
                        api_type,
                        model: model.as_deref(),
                        credential_id: ctx_id,
                        attempt: attempt + 1,
                        max_retries,
                        region: &region,
                        stream: is_stream,
                        status_code: status.as_u16(),
                        error_summary: &error_summary,
                        upstream_error_body: &body,
                        request_body,
                    },
                );

                if request_body.len() > KIRO_REQUEST_DIAGNOSTICS_MAX_BODY_BYTES {
                    let rejected_tool_index = Self::rejected_tool_index_from_error_body(&body)
                        .map(|index| index.to_string())
                        .unwrap_or_default();
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
                        kiro_body_bytes = request_body.len(),
                        kiro_diagnostic_body_limit_bytes = KIRO_REQUEST_DIAGNOSTICS_MAX_BODY_BYTES,
                        kiro_tool_error_index = %rejected_tool_index,
                        total_elapsed_ms = overall_started_at.elapsed().as_millis(),
                        "API 请求失败（上游 400，诊断摘要跳过：请求体超过诊断上限）"
                    );
                } else if let Some(diagnostics) =
                    Self::summarize_kiro_request_body_for_log(request_body)
                {
                    let rejected_tool_index = Self::rejected_tool_index_from_error_body(&body);
                    let rejected_tool = rejected_tool_index.and_then(|index| {
                        diagnostics
                            .current_tool_schemas
                            .iter()
                            .find(|tool| tool.index == index)
                    });
                    let rejected_tool_index = rejected_tool_index
                        .map(|index| index.to_string())
                        .unwrap_or_default();
                    let rejected_tool_name =
                        rejected_tool.map(|tool| tool.name.as_str()).unwrap_or("");
                    let rejected_tool_root_type = rejected_tool
                        .map(|tool| tool.root_type.as_str())
                        .unwrap_or("");
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
                        kiro_body_bytes = diagnostics.body_bytes,
                        kiro_profile_arn_present = diagnostics.profile_arn_present,
                        kiro_conversation_id_present = diagnostics.conversation_id_present,
                        kiro_agent_task_type_present = diagnostics.agent_task_type_present,
                        kiro_chat_trigger_type = diagnostics.chat_trigger_type.as_deref().unwrap_or(""),
                        kiro_current_model_id = diagnostics.current_model_id.as_deref().unwrap_or(""),
                        kiro_current_origin = diagnostics.current_origin.as_deref().unwrap_or(""),
                        kiro_current_content_bytes = diagnostics.current_content_bytes,
                        kiro_current_image_count = diagnostics.current_image_count,
                        kiro_current_document_count = diagnostics.current_document_count,
                        kiro_current_tool_count = diagnostics.current_tool_count,
                        kiro_tool_error_index = %rejected_tool_index,
                        kiro_tool_error_name = %rejected_tool_name,
                        kiro_tool_error_root_type = %rejected_tool_root_type,
                        kiro_current_tool_schemas = ?diagnostics.current_tool_schemas,
                        kiro_current_tool_result_count = diagnostics.current_tool_result_count,
                        kiro_current_tool_result_error_count = diagnostics.current_tool_result_error_count,
                        kiro_history_count = diagnostics.history_count,
                        kiro_history_user_count = diagnostics.history_user_count,
                        kiro_history_assistant_count = diagnostics.history_assistant_count,
                        kiro_history_user_image_count = diagnostics.history_user_image_count,
                        kiro_history_user_document_count = diagnostics.history_user_document_count,
                        kiro_history_tool_result_count = diagnostics.history_tool_result_count,
                        kiro_history_tool_use_count = diagnostics.history_tool_use_count,
                        kiro_tail_history_roles = ?diagnostics.tail_history_roles,
                        total_elapsed_ms = overall_started_at.elapsed().as_millis(),
                        "API 请求失败（上游 400 诊断摘要）"
                    );
                } else {
                    let rejected_tool_index = Self::rejected_tool_index_from_error_body(&body)
                        .map(|index| index.to_string())
                        .unwrap_or_default();
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
                        kiro_tool_error_index = %rejected_tool_index,
                        total_elapsed_ms = overall_started_at.elapsed().as_millis(),
                        "API 请求失败（上游 400，诊断摘要解析失败）"
                    );
                }

                let log_message = format!("{} API 请求失败: {}", api_type, error_summary);
                let public_error = if Self::is_context_length_exceeded_body(&body) {
                    PublicProviderError::context_length_exceeded(400, log_message)
                } else {
                    PublicProviderError::invalid_request(
                        log_message,
                        Self::invalid_request_public_message(&body),
                    )
                };
                return Err(anyhow::Error::new(public_error));
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

                // Enterprise profileArn 被上游判为不可用时，先重新发现可用 profile 并重试一次。
                if Self::should_rediscover_enterprise_profile(
                    &credentials,
                    status,
                    &body,
                    &error_summary,
                ) && !profile_rediscovered.contains(&ctx_id)
                {
                    profile_rediscovered.insert(ctx_id);
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
                        error_summary = %error_summary,
                        total_elapsed_ms = overall_started_at.elapsed().as_millis(),
                        "API 请求遇到 Enterprise profileArn 授权失败，尝试重新发现 profileArn 后重试"
                    );
                    match self
                        .token_manager
                        .rediscover_enterprise_profile_for(ctx_id, &credentials, &token)
                        .await
                    {
                        Ok(Some(_)) => continue,
                        Ok(None) => {
                            tracing::warn!(
                                request_id = %request_id,
                                credential_id = ctx_id,
                                "API 请求重新发现 Enterprise profileArn 未得到新可用 profileArn"
                            );
                        }
                        Err(err) => {
                            tracing::warn!(
                                request_id = %request_id,
                                credential_id = ctx_id,
                                "API 请求重新发现 Enterprise profileArn 失败: {}",
                                err
                            );
                        }
                    }
                }

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

            // 429/408/5xx - 瞬态上游错误：重试但不禁用凭据，并在当前请求内切换候选。
            // 429 会额外进入短冷却/桶退避。
            if matches!(status.as_u16(), 408 | 429) || status.is_server_error() {
                let insufficient_capacity =
                    Self::is_insufficient_model_capacity(&body, &error_summary);
                if insufficient_capacity {
                    capacity_unavailable_count = capacity_unavailable_count.saturating_add(1);
                    self.token_manager.defer_capacity_limited_credential(
                        ctx_id,
                        model.as_deref().unwrap_or("unknown"),
                        INSUFFICIENT_CAPACITY_COOLDOWN,
                    );
                    request_scoped_rate_limited_credentials.insert(ctx_id);
                    let supported_candidate_count = self
                        .token_manager
                        .enabled_supported_credential_count(model.as_deref());
                    max_retries = max_retries.max(Self::rate_limit_retry_cap(
                        total_credentials,
                        supported_candidate_count,
                        model.as_deref(),
                    ));
                } else if status.as_u16() == 429 {
                    if Self::is_suspicious_activity_limited(&body, &error_summary) {
                        self.token_manager
                            .report_suspicious_activity_limited(ctx_id, Some(&error_summary));
                    } else {
                        self.token_manager.report_rate_limited(ctx_id);
                    }
                    request_scoped_rate_limited_credentials.insert(ctx_id);
                    Self::maybe_spill_rate_limited_priority(
                        &self.token_manager,
                        model.as_deref(),
                        credentials.priority,
                        &mut priority_rate_limit_hits,
                        &request_scoped_model_unsupported_credentials,
                        &mut request_scoped_rate_limited_credentials,
                        &request_scoped_slow_first_content_credentials,
                        &request_scoped_empty_body_credentials,
                        &request_scoped_transient_error_credentials,
                    );
                } else {
                    request_scoped_transient_error_credentials.insert(ctx_id);
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
                    insufficient_capacity,
                    capacity_unavailable_count,
                    effective_retry_cap = max_retries,
                    total_elapsed_ms = overall_started_at.elapsed().as_millis(),
                    "API 请求失败（上游瞬态错误）"
                );
                last_error = Some(if insufficient_capacity {
                    anyhow::Error::new(Self::insufficient_model_capacity_error(
                        api_type,
                        &error_summary,
                    ))
                } else {
                    anyhow::anyhow!("{} API 请求失败: {}", api_type, error_summary)
                });
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

            // 兜底：当作可重试的瞬态错误处理，并在当前请求内切换候选凭据。
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
            request_scoped_transient_error_credentials.insert(ctx_id);
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
            base_cap
                .max(total_credentials.min(MAX_OPUS_4_7_RETRY_CANDIDATES))
                .max(1)
        } else {
            base_cap
        }
    }

    fn rate_limit_retry_cap(
        total_credentials: usize,
        supported_candidate_count: usize,
        model: Option<&str>,
    ) -> usize {
        let candidate_cap = if Self::is_real_opus_4_7_model(model) {
            MAX_OPUS_4_7_RETRY_CANDIDATES
        } else {
            MAX_RATE_LIMIT_RETRY_CANDIDATES
        };
        Self::base_retry_cap(total_credentials)
            .max(supported_candidate_count.min(candidate_cap))
            .max(1)
    }

    fn request_retryable_candidate_count(
        supported_candidate_count: usize,
        model_unsupported_credentials: &HashSet<u64>,
        empty_body_credentials: &HashSet<u64>,
    ) -> usize {
        if model_unsupported_credentials.is_empty() {
            return supported_candidate_count.saturating_sub(empty_body_credentials.len());
        }
        if empty_body_credentials.is_empty() {
            return supported_candidate_count.saturating_sub(model_unsupported_credentials.len());
        }

        let mut excluded = model_unsupported_credentials.clone();
        excluded.extend(empty_body_credentials.iter().copied());
        supported_candidate_count.saturating_sub(excluded.len())
    }

    fn should_reset_retryable_exclusions_for_next_pass(
        retryable_exclusion_count: usize,
        retryable_candidate_count: usize,
        attempted_count: usize,
        max_retries: usize,
    ) -> bool {
        retryable_exclusion_count > 0
            && retryable_candidate_count > 0
            && retryable_exclusion_count >= retryable_candidate_count
            && attempted_count < max_retries
    }

    fn combined_request_exclusions(
        model_unsupported_credentials: &HashSet<u64>,
        rate_limited_credentials: &HashSet<u64>,
        slow_first_content_credentials: &HashSet<u64>,
        empty_body_credentials: &HashSet<u64>,
        transient_error_credentials: &HashSet<u64>,
    ) -> HashSet<u64> {
        let mut excluded = model_unsupported_credentials.clone();
        excluded.extend(rate_limited_credentials.iter().copied());
        excluded.extend(slow_first_content_credentials.iter().copied());
        excluded.extend(empty_body_credentials.iter().copied());
        excluded.extend(transient_error_credentials.iter().copied());
        excluded
    }

    fn request_scoped_retryable_exclusion_count(
        rate_limited_credentials: &HashSet<u64>,
        transient_error_credentials: &HashSet<u64>,
    ) -> usize {
        if rate_limited_credentials.is_empty() {
            return transient_error_credentials.len();
        }
        if transient_error_credentials.is_empty() {
            return rate_limited_credentials.len();
        }

        let mut excluded = rate_limited_credentials.clone();
        excluded.extend(transient_error_credentials.iter().copied());
        excluded.len()
    }

    fn maybe_spill_rate_limited_priority(
        token_manager: &MultiTokenManager,
        model: Option<&str>,
        credential_priority: u32,
        priority_rate_limit_hits: &mut HashMap<u32, usize>,
        request_scoped_model_unsupported_credentials: &HashSet<u64>,
        request_scoped_rate_limited_credentials: &mut HashSet<u64>,
        request_scoped_slow_first_content_credentials: &HashSet<u64>,
        request_scoped_empty_body_credentials: &HashSet<u64>,
        request_scoped_transient_error_credentials: &HashSet<u64>,
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
            request_scoped_empty_body_credentials,
            request_scoped_transient_error_credentials,
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
            lower.contains("claude-opus-4.8")
                || lower.contains("claude-opus-4-8")
                || lower.contains("claude-opus-4.7")
                || lower.contains("claude-opus-4-7")
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

    fn is_profile_arn_required_error(body: &str, error_summary: &str) -> bool {
        let body = body.to_ascii_lowercase();
        let error_summary = error_summary.to_ascii_lowercase();
        body.contains("profilearn is required")
            || body.contains("profile arn is required")
            || error_summary.contains("profilearn is required")
            || error_summary.contains("profile arn is required")
    }

    fn should_failover_missing_profile_arn(
        credentials: &KiroCredentials,
        body: &str,
        error_summary: &str,
    ) -> bool {
        credentials.detected_auth_account_type().as_deref() == Some("enterprise")
            && credentials
                .effective_profile_arn_for_kiro_requests()
                .is_none()
            && Self::is_profile_arn_required_error(body, error_summary)
    }

    fn is_enterprise_profile_unauthorized_error(body: &str, error_summary: &str) -> bool {
        let body = body.to_ascii_lowercase();
        let error_summary = error_summary.to_ascii_lowercase();
        body.contains("user is not authorized to make this call")
            || body.contains("not authorized to make this call")
            || error_summary.contains("user is not authorized to make this call")
            || error_summary.contains("not authorized to make this call")
    }

    fn should_rediscover_enterprise_profile(
        credentials: &KiroCredentials,
        status: reqwest::StatusCode,
        body: &str,
        error_summary: &str,
    ) -> bool {
        credentials.detected_auth_account_type().as_deref() == Some("enterprise")
            && status.as_u16() == 403
            && !Self::is_bearer_token_invalid(body)
            && !Self::is_account_suspended(body)
            && Self::is_enterprise_profile_unauthorized_error(body, error_summary)
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
    use crate::kiro::parser::crc::crc32;
    use crate::kiro::parser::frame::PRELUDE_SIZE;
    use crate::model::config::Config;
    use axum::{Router, body::Body, response::Response, routing::get};
    use std::convert::Infallible;
    use tokio::net::TcpListener;

    fn create_test_provider(config: Config, credentials: KiroCredentials) -> KiroProvider {
        let tm = MultiTokenManager::new(config, vec![credentials], None, None, false).unwrap();
        KiroProvider::with_proxy(Arc::new(tm), None)
    }

    #[test]
    fn test_enterprise_runtime_endpoint_ignores_configured_kiro_runtime() {
        let mut config = Config::default();
        config.runtime_endpoint = Some("https://runtime.us-east-1.kiro.dev/".to_string());
        let credentials = KiroCredentials {
            provider: Some("Enterprise".to_string()),
            api_region: Some("us-east-1".to_string()),
            ..Default::default()
        };
        let provider = create_test_provider(config, credentials.clone());

        assert_eq!(
            provider.base_url_for(&credentials),
            "https://q.us-east-1.amazonaws.com/generateAssistantResponse"
        );
        assert_eq!(
            provider.mcp_url_for(&credentials),
            "https://q.us-east-1.amazonaws.com/mcp"
        );
        assert_eq!(
            provider.base_domain_for(&credentials),
            "q.us-east-1.amazonaws.com"
        );
    }

    #[test]
    fn test_builder_id_runtime_endpoint_uses_configured_kiro_runtime() {
        let mut config = Config::default();
        config.runtime_endpoint = Some("https://runtime.us-east-1.kiro.dev/".to_string());
        let credentials = KiroCredentials {
            provider: Some("BuilderId".to_string()),
            api_region: Some("us-east-1".to_string()),
            ..Default::default()
        };
        let provider = create_test_provider(config, credentials.clone());

        assert_eq!(
            provider.base_url_for(&credentials),
            "https://runtime.us-east-1.kiro.dev/generateAssistantResponse"
        );
        assert_eq!(
            provider.mcp_url_for(&credentials),
            "https://runtime.us-east-1.kiro.dev/mcp"
        );
        assert_eq!(
            provider.base_domain_for(&credentials),
            "runtime.us-east-1.kiro.dev"
        );
    }

    fn test_response_trace(content_type: Option<&str>) -> ResponseTrace {
        let now = Instant::now();
        ResponseTrace {
            request_id: "test-request".to_string(),
            api_type: "非流式",
            model: Some("claude-opus-4.8".to_string()),
            request_body_bytes: 128,
            credential_id: 1,
            attempt: 1,
            max_retries: 2,
            region: "us-east-1".to_string(),
            status_code: 200,
            overall_started_at: now,
            upstream_request_started_at: now,
            response_headers_at: now,
            response_content_type: content_type.map(ToOwned::to_owned),
            response_content_encoding: None,
            response_content_length: None,
            response_transfer_encoding: None,
            slow_model_cooldown: None,
        }
    }

    fn string_header(name: &str, value: &str) -> Vec<u8> {
        let mut header = Vec::new();
        header.push(name.len() as u8);
        header.extend_from_slice(name.as_bytes());
        header.push(7);
        header.extend_from_slice(&(value.len() as u16).to_be_bytes());
        header.extend_from_slice(value.as_bytes());
        header
    }

    fn event_frame(event_type: &str, payload: &[u8]) -> Vec<u8> {
        let mut headers = Vec::new();
        headers.extend(string_header(":message-type", "event"));
        headers.extend(string_header(":event-type", event_type));

        let total_length = (PRELUDE_SIZE + headers.len() + payload.len() + 4) as u32;
        let header_length = headers.len() as u32;
        let mut frame = Vec::new();
        frame.extend_from_slice(&total_length.to_be_bytes());
        frame.extend_from_slice(&header_length.to_be_bytes());
        let prelude_crc = crc32(&frame);
        frame.extend_from_slice(&prelude_crc.to_be_bytes());
        frame.extend_from_slice(&headers);
        frame.extend_from_slice(payload);
        let message_crc = crc32(&frame);
        frame.extend_from_slice(&message_crc.to_be_bytes());
        frame
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
    fn test_should_failover_missing_profile_arn_for_enterprise_without_arn() {
        let credentials = KiroCredentials {
            provider: Some("Enterprise".to_string()),
            start_url: Some("https://example.awsapps.com/start".to_string()),
            ..Default::default()
        };
        let body = r#"{"message":"profileArn is required for this request."}"#;

        assert!(KiroProvider::should_failover_missing_profile_arn(
            &credentials,
            body,
            "400 Bad Request"
        ));
    }

    #[test]
    fn test_should_not_failover_missing_profile_arn_for_enterprise_with_profile_arn() {
        let credentials = KiroCredentials {
            provider: Some("Enterprise".to_string()),
            start_url: Some("https://example.awsapps.com/start".to_string()),
            profile_arn: Some("arn:aws:codewhisperer:us-east-1:123:profile/test".to_string()),
            ..Default::default()
        };
        let body = r#"{"message":"profileArn is required for this request."}"#;

        assert!(!KiroProvider::should_failover_missing_profile_arn(
            &credentials,
            body,
            "400 Bad Request"
        ));
    }

    #[test]
    fn test_should_not_failover_missing_profile_arn_for_builder_id() {
        let credentials = KiroCredentials {
            provider: Some("BuilderId".to_string()),
            start_url: Some("https://view.awsapps.com/start/".to_string()),
            ..Default::default()
        };
        let body = r#"{"message":"profileArn is required for this request."}"#;

        assert!(!KiroProvider::should_failover_missing_profile_arn(
            &credentials,
            body,
            "400 Bad Request"
        ));
    }

    #[test]
    fn test_should_not_failover_missing_profile_arn_for_social() {
        let credentials = KiroCredentials {
            provider: Some("Google".to_string()),
            auth_method: Some("social".to_string()),
            ..Default::default()
        };
        let body = r#"{"message":"profileArn is required for this request."}"#;

        assert!(!KiroProvider::should_failover_missing_profile_arn(
            &credentials,
            body,
            "400 Bad Request"
        ));
    }

    #[test]
    fn test_should_rediscover_enterprise_profile_on_unauthorized_403() {
        let credentials = KiroCredentials {
            provider: Some("Enterprise".to_string()),
            start_url: Some("https://example.awsapps.com/start".to_string()),
            profile_arn: Some("arn:aws:codewhisperer:us-east-1:123:profile/old".to_string()),
            ..Default::default()
        };
        let body = r#"{"message":"User is not authorized to make this call."}"#;

        assert!(KiroProvider::should_rediscover_enterprise_profile(
            &credentials,
            reqwest::StatusCode::FORBIDDEN,
            body,
            "status=403 message=\"User is not authorized to make this call.\""
        ));
    }

    #[test]
    fn test_should_rediscover_enterprise_profile_without_existing_arn() {
        let credentials = KiroCredentials {
            provider: Some("Enterprise".to_string()),
            start_url: Some("https://example.awsapps.com/start".to_string()),
            ..Default::default()
        };
        let body = r#"{"message":"User is not authorized to make this call."}"#;

        assert!(KiroProvider::should_rediscover_enterprise_profile(
            &credentials,
            reqwest::StatusCode::FORBIDDEN,
            body,
            "status=403"
        ));
    }

    #[test]
    fn test_should_not_rediscover_enterprise_profile_for_builder_id() {
        let credentials = KiroCredentials {
            provider: Some("BuilderId".to_string()),
            start_url: Some("https://view.awsapps.com/start/".to_string()),
            ..Default::default()
        };
        let body = r#"{"message":"User is not authorized to make this call."}"#;

        assert!(!KiroProvider::should_rediscover_enterprise_profile(
            &credentials,
            reqwest::StatusCode::FORBIDDEN,
            body,
            "status=403"
        ));
    }

    #[test]
    fn test_should_not_rediscover_enterprise_profile_for_social() {
        let credentials = KiroCredentials {
            provider: Some("Google".to_string()),
            auth_method: Some("social".to_string()),
            ..Default::default()
        };
        let body = r#"{"message":"User is not authorized to make this call."}"#;

        assert!(!KiroProvider::should_rediscover_enterprise_profile(
            &credentials,
            reqwest::StatusCode::FORBIDDEN,
            body,
            "status=403"
        ));
    }

    #[test]
    fn test_should_not_rediscover_enterprise_profile_for_invalid_bearer_token() {
        let credentials = KiroCredentials {
            provider: Some("Enterprise".to_string()),
            start_url: Some("https://example.awsapps.com/start".to_string()),
            profile_arn: Some("arn:aws:codewhisperer:us-east-1:123:profile/old".to_string()),
            ..Default::default()
        };
        let body = "The bearer token included in the request is invalid";

        assert!(!KiroProvider::should_rediscover_enterprise_profile(
            &credentials,
            reqwest::StatusCode::FORBIDDEN,
            body,
            "status=403"
        ));
    }

    #[test]
    fn test_should_not_rediscover_enterprise_profile_for_suspended_account() {
        let credentials = KiroCredentials {
            provider: Some("Enterprise".to_string()),
            start_url: Some("https://example.awsapps.com/start".to_string()),
            profile_arn: Some("arn:aws:codewhisperer:us-east-1:123:profile/old".to_string()),
            ..Default::default()
        };
        let body = r#"{"message":"Your User ID temporarily is suspended. We've locked your account as a security precaution. User is not authorized to make this call."}"#;

        assert!(!KiroProvider::should_rediscover_enterprise_profile(
            &credentials,
            reqwest::StatusCode::FORBIDDEN,
            body,
            "status=403"
        ));
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
            Some("claude-opus-4.8"),
            body
        ));
        assert!(KiroProvider::should_failover_model_unsupported(
            Some("claude-opus-4-8"),
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
        let result = KiroProvider::inject_profile_arn(body, &arn).unwrap();
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
        let mut original_body = None;
        let result = KiroProvider::request_body_for_profile_arn_and_model(
            body,
            &mut original_body,
            &None,
            &None,
            false,
        );
        // 不注入 profileArn，原样返回
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert!(json.get("profileArn").is_none());
        assert_eq!(json["conversationState"]["conversationId"], "c1");
    }

    #[test]
    fn test_inject_profile_arn_overwrites_existing() {
        let body = r#"{"conversationState":{},"profileArn":"old-arn"}"#;
        let arn = Some("new-arn".to_string());
        let result = KiroProvider::inject_profile_arn(body, &arn).unwrap();
        let json: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(json["profileArn"], "new-arn");
    }

    #[test]
    fn test_enterprise_request_body_strips_existing_profile_arn() {
        let body = r#"{"conversationState":{},"profileArn":"old-arn","modelId":"claude-opus-4.7"}"#;
        let mut original_body = None;
        let result = KiroProvider::request_body_for_profile_arn_and_model(
            body,
            &mut original_body,
            &None,
            &Some("claude-sonnet-4.5".to_string()),
            true,
        );
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert!(json.get("profileArn").is_none());
        assert_eq!(json["modelId"], "claude-sonnet-4.5");
    }

    #[test]
    fn test_request_body_injects_profile_arn_when_effective_arn_present() {
        let body = r#"{"conversationState":{},"profileArn":"old-arn","modelId":"claude-opus-4.7"}"#;
        let mut original_body = None;
        let arn = Some("arn:aws:codewhisperer:us-east-1:123:profile/test".to_string());
        let result = KiroProvider::request_body_for_profile_arn_and_model(
            body,
            &mut original_body,
            &arn,
            &Some("claude-sonnet-4.5".to_string()),
            true,
        );
        let json: serde_json::Value = serde_json::from_slice(&result).unwrap();
        assert_eq!(
            json["profileArn"],
            "arn:aws:codewhisperer:us-east-1:123:profile/test"
        );
        assert_eq!(json["modelId"], "claude-sonnet-4.5");
    }

    #[test]
    fn test_inject_profile_arn_invalid_json() {
        let body = "not-valid-json";
        let arn = Some("arn:test".to_string());
        let mut original_body = None;
        let result = KiroProvider::request_body_for_profile_arn_and_model(
            body,
            &mut original_body,
            &arn,
            &None,
            false,
        );
        // 解析失败时原样返回
        assert_eq!(&result[..], body.as_bytes());
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
    fn test_invalid_request_public_message_special_cases_tool_mismatch() {
        let message = KiroProvider::invalid_request_public_message(
            r#"{"reason":"TOOL_USE_RESULT_MISMATCH","message":"Bedrock error message: The number of toolResult blocks at messages.4.content exceeds the number of toolUse blocks of previous turn."}"#,
        );
        assert_eq!(
            message,
            "Upstream rejected mismatched tool history. Ensure each tool_result immediately follows the assistant tool_use it answers."
        );
    }

    #[test]
    fn test_invalid_request_public_message_special_cases_context_length() {
        let message = KiroProvider::invalid_request_public_message(
            r#"{"reason":"CONTENT_LENGTH_EXCEEDS_THRESHOLD","message":"Input is too long and must be reduced immediately"}"#,
        );
        assert_eq!(message, CONTEXT_LENGTH_EXCEEDED_PUBLIC_MESSAGE);
    }

    #[test]
    fn test_invalid_request_public_message_special_cases_tool_schema_invalid() {
        let message = KiroProvider::invalid_request_public_message(
            r#"{"reason":"TOOL_SCHEMA_INVALID","message":"tools.0.custom.input_schema: JSON schema is invalid. It must match JSON Schema draft 2020-12"}"#,
        );
        assert_eq!(
            message,
            "Upstream rejected one of the tool definitions as invalid. Review the request tools and try again."
        );
    }

    #[test]
    fn test_invalid_request_public_message_special_cases_invalid_thinking_signature() {
        let message = KiroProvider::invalid_request_public_message(
            r#"{"error":{"message":"Invalid thinking signature at messages[315] thinking block 0"}}"#,
        );
        assert_eq!(
            message,
            "Upstream rejected a historical thinking signature. Retry with unmodified thinking blocks or start a new conversation."
        );
        assert!(KiroProvider::is_invalid_thinking_signature_error(
            "",
            "body_excerpt=\"Invalid thinking signature at messages[315] thinking block 0\""
        ));
    }

    fn unique_capture_test_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("kiro-rs-{name}-{}", Uuid::new_v4()))
    }

    fn test_capture_config(dir: PathBuf) -> Capture400BodiesConfig {
        Capture400BodiesConfig {
            enabled: true,
            dir,
            max_per_class: 2,
            max_body_bytes: 1024,
            ttl: Duration::from_secs(24 * 60 * 60),
            max_total_bytes: 1024 * 1024,
        }
    }

    fn capture_test_request<'a>(
        request_id: &'a str,
        error_body: &'a str,
        request_body: &'a str,
    ) -> Capture400Request<'a> {
        Capture400Request {
            request_id,
            api_type: "非流式",
            model: Some("claude-sonnet-4.6"),
            credential_id: 919,
            attempt: 1,
            max_retries: 9,
            region: "us-east-1",
            stream: false,
            status_code: 400,
            error_summary: "status=400 reason=REQUEST_BODY_INVALID message=\"Improperly formed request.\"",
            upstream_error_body: error_body,
            request_body,
        }
    }

    #[test]
    fn test_capture_400_error_classifies_common_failures() {
        assert_eq!(
            capture_400_error_class(
                r#"{"reason":"REQUEST_BODY_INVALID","message":"Improperly formed request."}"#,
                ""
            ),
            "request_body_invalid"
        );
        assert_eq!(
            capture_400_error_class(
                r#"{"reason":"CONTENT_LENGTH_EXCEEDS_THRESHOLD","message":"Input is too long."}"#,
                ""
            ),
            "context_length_exceeded"
        );
        assert_eq!(
            capture_400_error_class(
                r#"{"reason":"TOOL_SCHEMA_INVALID","message":"tools.0.custom.input_schema: JSON schema is invalid."}"#,
                ""
            ),
            "tool_schema_invalid"
        );
        assert_eq!(
            capture_400_error_class(
                r#"{"message":"The number of toolResult blocks exceeds the number of toolUse blocks of previous turn."}"#,
                ""
            ),
            "tool_use_result_mismatch"
        );
        assert_eq!(
            capture_400_error_class(
                r#"{"error":{"message":"Invalid thinking signature at messages[1] thinking block 0"}}"#,
                ""
            ),
            "invalid_thinking_signature"
        );
    }

    #[test]
    fn test_capture_400_request_body_prunes_per_class() {
        let dir = unique_capture_test_dir("capture-prune");
        let config = test_capture_config(dir.clone());

        for index in 0..3 {
            let body = format!(r#"{{"conversationState":{{"index":{index}}}}}"#);
            capture_400_request_body_for_diagnostics_inner(
                &config,
                capture_test_request(
                    &format!("request-{index}"),
                    r#"{"reason":"REQUEST_BODY_INVALID","message":"Improperly formed request."}"#,
                    &body,
                ),
            )
            .expect("capture should succeed");
        }

        let class_dir = dir.join("request_body_invalid");
        let pairs = capture_pairs_in_dir(&class_dir);
        assert_eq!(pairs.len(), 2);
        for pair in &pairs {
            assert!(pair.meta_path.exists());
            assert!(pair.body_path.exists());
        }

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn test_capture_400_request_body_disabled_does_not_write() {
        let dir = unique_capture_test_dir("capture-disabled");
        let mut config = test_capture_config(dir.clone());
        config.enabled = false;

        capture_400_request_body_for_diagnostics(
            &config,
            capture_test_request(
                "disabled-request",
                r#"{"reason":"REQUEST_BODY_INVALID","message":"Improperly formed request."}"#,
                r#"{"conversationState":{"index":0}}"#,
            ),
        );

        assert!(!dir.exists());
    }

    #[test]
    fn test_capture_400_request_body_omits_oversized_body() {
        let dir = unique_capture_test_dir("capture-oversized");
        let mut config = test_capture_config(dir.clone());
        config.max_body_bytes = 4;

        capture_400_request_body_for_diagnostics_inner(
            &config,
            capture_test_request(
                "oversized-request",
                r#"{"reason":"REQUEST_BODY_INVALID","message":"Improperly formed request."}"#,
                r#"{"large":"body"}"#,
            ),
        )
        .expect("capture should succeed");

        let class_dir = dir.join("request_body_invalid");
        let pairs = capture_pairs_in_dir(&class_dir);
        assert_eq!(pairs.len(), 1);
        let pair = &pairs[0];
        assert!(pair.meta_path.exists());
        assert!(!pair.body_path.exists());

        let metadata: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&pair.meta_path).unwrap()).unwrap();
        assert_eq!(metadata["body_saved"], false);
        assert_eq!(
            metadata["body_omitted_reason"],
            "request body exceeds KIRO_CAPTURE_400_MAX_BODY_BYTES"
        );

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn test_capture_400_request_body_prunes_total_bytes() {
        let dir = unique_capture_test_dir("capture-total");
        let mut config = test_capture_config(dir.clone());
        config.max_per_class = 10;
        config.max_total_bytes = u64::MAX;

        for index in 0..3 {
            let body = format!(r#"{{"conversationState":{{"index":{index}}}}}"#);
            capture_400_request_body_for_diagnostics_inner(
                &config,
                capture_test_request(
                    &format!("request-{index}"),
                    r#"{"reason":"REQUEST_BODY_INVALID","message":"Improperly formed request."}"#,
                    &body,
                ),
            )
            .expect("capture should succeed");
            std::thread::sleep(Duration::from_millis(10));
        }

        let mut pairs = capture_pairs_in_tree(&dir);
        assert_eq!(pairs.len(), 3);
        pairs.sort_by(|a, b| b.modified.cmp(&a.modified));
        let newest_meta_path = pairs[0].meta_path.clone();
        let newest_body_path = pairs[0].body_path.clone();

        prune_capture_total(&dir, pairs[0].bytes + 32);

        let remaining_pairs = capture_pairs_in_tree(&dir);
        assert_eq!(remaining_pairs.len(), 1);
        assert_eq!(remaining_pairs[0].meta_path, newest_meta_path);
        assert_eq!(remaining_pairs[0].body_path, newest_body_path);

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn test_summarize_kiro_request_body_for_log_is_bounded_shape_only() {
        let body = r#"{
            "profileArn":"arn:test",
            "conversationState":{
                "conversationId":"conv-1",
                "agentTaskType":"vibe",
                "chatTriggerType":"MANUAL",
                "currentMessage":{"userInputMessage":{
                    "modelId":"claude-opus-4.6",
                    "origin":"AI_EDITOR",
                    "content":"secret current text",
                    "images":[{"format":"png","source":{"bytes":"AA=="}}],
                    "documents":[{"name":"Current","format":"pdf","source":{"bytes":"AA=="}}],
                    "userInputMessageContext":{
                        "tools":[
                            {"toolSpecification":{"name":"Read","description":"read","inputSchema":{"json":{"type":"object"}}}},
                            {"toolSpecification":{"name":"BadScalar","description":"bad","inputSchema":{"json":{"type":"string"}}}},
                            {"toolSpecification":{"name":"Nullable","description":"nullable","inputSchema":{"json":{"type":["object","null"]}}}},
                            {"toolSpecification":{"name":"MissingType","description":"missing","inputSchema":{"json":{"properties":{}}}}}
                        ],
                        "toolResults":[
                            {"toolUseId":"toolu_1","content":[{"text":"hidden"}],"status":"success"},
                            {"toolUseId":"toolu_2","content":[{"text":"hidden"}],"status":"error","isError":true}
                        ]
                    }
                }},
                "history":[
                    {"userInputMessage":{"content":"old","modelId":"m","images":[{"format":"png","source":{"bytes":"AA=="}}],"documents":[{"name":"History","format":"pdf","source":{"bytes":"AA=="}}],"userInputMessageContext":{"toolResults":[{"toolUseId":"toolu_old","content":[{"text":"hidden"}]}]}}},
                    {"assistantResponseMessage":{"content":"old","toolUses":[{"toolUseId":"toolu_old","name":"Read","input":{"path":"secret"}}]}},
                    {"unknownMessage":{}}
                ]
            }
        }"#;

        let diagnostics =
            KiroProvider::summarize_kiro_request_body_for_log(body).expect("expected diagnostics");

        assert_eq!(diagnostics.body_bytes, body.len());
        assert!(diagnostics.profile_arn_present);
        assert!(diagnostics.conversation_id_present);
        assert!(diagnostics.agent_task_type_present);
        assert_eq!(diagnostics.chat_trigger_type.as_deref(), Some("MANUAL"));
        assert_eq!(
            diagnostics.current_model_id.as_deref(),
            Some("claude-opus-4.6")
        );
        assert_eq!(diagnostics.current_origin.as_deref(), Some("AI_EDITOR"));
        assert_eq!(
            diagnostics.current_content_bytes,
            "secret current text".len()
        );
        assert_eq!(diagnostics.current_image_count, 1);
        assert_eq!(diagnostics.current_document_count, 1);
        assert_eq!(diagnostics.current_tool_count, 4);
        assert_eq!(
            diagnostics.current_tool_schemas,
            vec![
                KiroToolSchemaDiagnostics {
                    index: 0,
                    name: "Read".to_string(),
                    root_type: "object".to_string(),
                },
                KiroToolSchemaDiagnostics {
                    index: 1,
                    name: "BadScalar".to_string(),
                    root_type: "string".to_string(),
                },
                KiroToolSchemaDiagnostics {
                    index: 2,
                    name: "Nullable".to_string(),
                    root_type: "[object,null]".to_string(),
                },
                KiroToolSchemaDiagnostics {
                    index: 3,
                    name: "MissingType".to_string(),
                    root_type: "<missing>".to_string(),
                },
            ]
        );
        assert_eq!(diagnostics.current_tool_result_count, 2);
        assert_eq!(diagnostics.current_tool_result_error_count, 1);
        assert_eq!(diagnostics.history_count, 3);
        assert_eq!(diagnostics.history_user_count, 1);
        assert_eq!(diagnostics.history_assistant_count, 1);
        assert_eq!(diagnostics.history_user_image_count, 1);
        assert_eq!(diagnostics.history_user_document_count, 1);
        assert_eq!(diagnostics.history_tool_result_count, 1);
        assert_eq!(diagnostics.history_tool_use_count, 1);
        assert_eq!(
            diagnostics.tail_history_roles,
            vec![
                "user".to_string(),
                "assistant".to_string(),
                "unknown".to_string()
            ]
        );
    }

    #[test]
    fn test_rejected_tool_index_from_error_body_detects_bedrock_paths() {
        assert_eq!(
            KiroProvider::rejected_tool_index_from_error_body(
                r#"{"reason":"TOOL_SCHEMA_INVALID","message":"Bedrock error message: The value at toolConfig.tools.20.toolSpec.inputSchema.json.type must be one of the following: object."}"#
            ),
            Some(20)
        );
        assert_eq!(
            KiroProvider::rejected_tool_index_from_error_body(
                r#"{"reason":"TOOL_SCHEMA_INVALID","message":"tools.0.custom.input_schema: JSON schema is invalid."}"#
            ),
            Some(0)
        );
        assert_eq!(
            KiroProvider::rejected_tool_index_from_error_body(
                r#"{"reason":"TOOL_SCHEMA_INVALID","message":"tool schema is invalid"}"#
            ),
            None
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
    fn test_public_client_error_for_413_context_length_has_claude_code_code() {
        let err = KiroProvider::public_client_error_for_status(
            reqwest::StatusCode::PAYLOAD_TOO_LARGE,
            "非流式",
            "reason=CONTENT_LENGTH_EXCEEDS_THRESHOLD",
            r#"{"reason":"CONTENT_LENGTH_EXCEEDS_THRESHOLD"}"#,
        )
        .unwrap();

        assert_eq!(err.status_code(), 413);
        assert_eq!(err.error_code(), Some(CONTEXT_LENGTH_EXCEEDED_CODE));
        assert_eq!(err.public_message(), CONTEXT_LENGTH_EXCEEDED_PUBLIC_MESSAGE);
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
        assert_eq!(KiroProvider::rate_limit_retry_cap(40, 40, None), 24);
        assert_eq!(KiroProvider::rate_limit_retry_cap(40, 12, None), 12);
        assert_eq!(KiroProvider::rate_limit_retry_cap(2, 2, None), 6);
        assert_eq!(
            KiroProvider::rate_limit_retry_cap(164, 164, Some("claude-opus-4.7")),
            24
        );
        assert_eq!(
            KiroProvider::rate_limit_retry_cap(164, 164, Some("claude-opus-4.8")),
            24
        );
    }

    #[test]
    fn test_initial_opus_retry_cap_is_bounded_for_large_pools() {
        assert_eq!(
            KiroProvider::initial_api_retry_cap(25, Some("claude-opus-4.7")),
            24
        );
        assert_eq!(
            KiroProvider::initial_api_retry_cap(25, Some("claude-opus-4.8")),
            24
        );
        assert_eq!(
            KiroProvider::initial_api_retry_cap(25, Some("claude-sonnet-4.5")),
            9
        );
        assert_eq!(
            KiroProvider::initial_api_retry_cap(164, Some("claude-opus-4.7")),
            24
        );
    }

    #[test]
    fn test_insufficient_model_capacity_detection_uses_body_or_summary() {
        assert!(KiroProvider::is_insufficient_model_capacity(
            r#"{"reason":"INSUFFICIENT_MODEL_CAPACITY"}"#,
            "status=429 body_len=44"
        ));
        assert!(KiroProvider::is_insufficient_model_capacity(
            "{}",
            "status=429 body_len=110 reason=INSUFFICIENT_MODEL_CAPACITY"
        ));
        assert!(KiroProvider::is_insufficient_model_capacity(
            r#"{"reason":"MODEL_TEMPORARILY_UNAVAILABLE"}"#,
            "status=500 body_len=136"
        ));
        assert!(KiroProvider::is_insufficient_model_capacity(
            "{}",
            "status=500 body_len=136 reason=MODEL_TEMPORARILY_UNAVAILABLE"
        ));
        assert!(!KiroProvider::is_insufficient_model_capacity(
            r#"{"reason":"RATE_LIMIT"}"#,
            "status=429 body_len=24 reason=RATE_LIMIT"
        ));
    }

    #[test]
    fn test_insufficient_model_capacity_public_error_is_503() {
        let public_error = KiroProvider::insufficient_model_capacity_error(
            "流式",
            "status=429 body_len=110 reason=INSUFFICIENT_MODEL_CAPACITY",
        );

        assert_eq!(public_error.status_code(), 503);
        assert_eq!(public_error.error_type(), "service_unavailable");
        assert_eq!(
            public_error.public_message(),
            KiroProvider::insufficient_model_capacity_public_message()
        );
        assert!(
            public_error
                .to_string()
                .contains("INSUFFICIENT_MODEL_CAPACITY")
        );
    }

    #[test]
    fn test_suspicious_activity_limit_detection_uses_body_or_summary() {
        assert!(KiroProvider::is_suspicious_activity_limited(
            r#"{"message":"Due to suspicious activity, we are imposing temporary limits"}"#,
            "status=429 body_len=80"
        ));
        assert!(KiroProvider::is_suspicious_activity_limited(
            "{}",
            "status=429 message=\"Due to suspicious activity, temporary limits\""
        ));
        assert!(!KiroProvider::is_suspicious_activity_limited(
            r#"{"reason":"RATE_LIMIT"}"#,
            "status=429 reason=RATE_LIMIT"
        ));
    }

    #[test]
    fn test_retryable_exclusions_reset_after_one_pass_when_budget_remains() {
        assert!(KiroProvider::should_reset_retryable_exclusions_for_next_pass(2, 2, 2, 6));
        assert!(!KiroProvider::should_reset_retryable_exclusions_for_next_pass(2, 2, 6, 6));
        assert!(!KiroProvider::should_reset_retryable_exclusions_for_next_pass(1, 2, 1, 6));
    }

    #[test]
    fn test_retryable_candidate_count_excludes_empty_body_credentials() {
        let mut model_unsupported = HashSet::new();
        model_unsupported.insert(1);
        model_unsupported.insert(2);

        let mut empty_body = HashSet::new();
        empty_body.insert(2);
        empty_body.insert(3);

        assert_eq!(
            KiroProvider::request_retryable_candidate_count(5, &model_unsupported, &empty_body),
            2
        );
    }

    #[test]
    fn test_combined_request_exclusions_includes_empty_body_credentials() {
        let mut model_unsupported = HashSet::new();
        model_unsupported.insert(1);

        let mut rate_limited = HashSet::new();
        rate_limited.insert(2);

        let mut slow_first_content = HashSet::new();
        slow_first_content.insert(3);

        let mut empty_body = HashSet::new();
        empty_body.insert(4);

        let mut transient_errors = HashSet::new();
        transient_errors.insert(5);

        let excluded = KiroProvider::combined_request_exclusions(
            &model_unsupported,
            &rate_limited,
            &slow_first_content,
            &empty_body,
            &transient_errors,
        );

        assert_eq!(excluded.len(), 5);
        for credential_id in 1..=5 {
            assert!(excluded.contains(&credential_id));
        }
    }

    #[test]
    fn test_retryable_exclusion_count_deduplicates_transient_and_rate_limited() {
        let mut rate_limited = HashSet::new();
        rate_limited.insert(1);
        rate_limited.insert(2);

        let mut transient_errors = HashSet::new();
        transient_errors.insert(2);
        transient_errors.insert(3);

        assert_eq!(
            KiroProvider::request_scoped_retryable_exclusion_count(
                &rate_limited,
                &transient_errors
            ),
            3
        );
    }

    #[test]
    fn test_slow_first_content_shared_cooldown_skips_large_request_body() {
        assert!(
            KiroProvider::should_apply_slow_first_content_shared_cooldown(
                SLOW_FIRST_CONTENT_SHARED_COOLDOWN_MAX_REQUEST_BODY_BYTES
            )
        );
        assert!(
            !KiroProvider::should_apply_slow_first_content_shared_cooldown(
                SLOW_FIRST_CONTENT_SHARED_COOLDOWN_MAX_REQUEST_BODY_BYTES + 1
            )
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
    fn test_stream_pre_sse_attempt_timeout_uses_small_request_fast_failover() {
        let config = StreamPreSseFailoverConfig::default();
        let timeout = KiroProvider::stream_pre_sse_attempt_timeout(
            &config,
            64 * 1024,
            Some("claude-sonnet-4.5"),
            0,
            true,
            1,
            Duration::from_millis(config.total_budget_ms),
        );

        assert_eq!(timeout.timeout, Duration::from_millis(30_000));
        assert!(timeout.fast_failover);
    }

    #[test]
    fn test_stream_pre_sse_attempt_timeout_respects_opus_minimum() {
        let config = StreamPreSseFailoverConfig::default();
        let timeout = KiroProvider::stream_pre_sse_attempt_timeout(
            &config,
            64 * 1024,
            Some("claude-opus-4-8"),
            0,
            true,
            1,
            Duration::from_millis(config.total_budget_ms),
        );

        assert_eq!(timeout.timeout, Duration::from_millis(60_000));
        assert!(timeout.fast_failover);
    }

    #[test]
    fn test_stream_pre_sse_attempt_timeout_uses_remaining_for_huge_request() {
        let config = StreamPreSseFailoverConfig::default();
        let remaining = Duration::from_millis(config.total_budget_ms);
        let timeout = KiroProvider::stream_pre_sse_attempt_timeout(
            &config,
            config.large_request_threshold_bytes + 1,
            Some("claude-opus-4-8"),
            0,
            true,
            1,
            remaining,
        );

        assert_eq!(timeout.timeout, remaining);
        assert!(!timeout.fast_failover);
    }

    #[test]
    fn test_stream_pre_sse_attempt_timeout_uses_remaining_after_fast_failover_cap() {
        let config = StreamPreSseFailoverConfig::default();
        let remaining = Duration::from_millis(config.total_budget_ms);
        let timeout = KiroProvider::stream_pre_sse_attempt_timeout(
            &config,
            64 * 1024,
            Some("claude-sonnet-4.5"),
            config.max_fast_failovers,
            true,
            1,
            remaining,
        );

        assert_eq!(timeout.timeout, remaining);
        assert!(!timeout.fast_failover);
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
    fn test_stream_content_start_probe_thinking_bounds_whitespace_buffer() {
        let mut probe = StreamContentStartProbe::new(true);
        assert!(!probe.observe_assistant_content(&" ".repeat(256)));
        assert!(probe.buffer.len() <= THINKING_START_TAG.len());
        assert!(!probe.observe_assistant_content("<think"));
        assert!(probe.observe_assistant_content("ing>step 1"));
    }

    #[test]
    fn test_stream_content_start_probe_thinking_accepts_plain_text_prefix() {
        let mut probe = StreamContentStartProbe::new(true);
        assert!(probe.observe_assistant_content("This is normal text before thinking."));
    }

    #[test]
    fn test_stream_content_start_probe_thinking_accepts_multibyte_plain_text_prefix() {
        let mut probe = StreamContentStartProbe::new(true);
        assert!(probe.observe_assistant_content("commit 成功了，"));
    }

    #[test]
    fn test_stream_content_start_probe_releases_active_thinking_stream() {
        let mut probe = StreamContentStartProbe::new(true);
        assert!(
            !probe.should_release_after_stream_activity(STREAM_CONTENT_START_ACTIVITY_READY_BYTES)
        );

        assert!(!probe.observe(&Event::Metering(())));
        assert!(
            !probe.should_release_after_stream_activity(
                STREAM_CONTENT_START_ACTIVITY_READY_BYTES - 1
            )
        );
        assert!(
            probe.should_release_after_stream_activity(STREAM_CONTENT_START_ACTIVITY_READY_BYTES)
        );
    }

    #[test]
    fn test_stream_content_start_probe_thinking_accepts_reasoning_content() {
        let event = Event::ReasoningContent(
            serde_json::from_str(r#"{"text":"thinking","signature":"sig"}"#).unwrap(),
        );

        let mut probe = StreamContentStartProbe::new(true);
        assert!(probe.observe(&event));
        let diagnostics = probe.diagnostics();
        assert_eq!(diagnostics.reasoning_events, 1);
        assert_eq!(diagnostics.reasoning_text_bytes, 8);

        let mut non_thinking_probe = StreamContentStartProbe::new(false);
        assert!(!non_thinking_probe.observe(&event));
        let diagnostics = non_thinking_probe.diagnostics();
        assert_eq!(diagnostics.reasoning_events, 1);
        assert_eq!(diagnostics.reasoning_text_bytes, 8);
    }

    #[test]
    fn test_stream_content_start_probe_does_not_release_errors_or_non_thinking() {
        let mut errored_probe = StreamContentStartProbe::new(true);
        assert!(!errored_probe.observe(&Event::Error {
            error_code: "Bad".to_string(),
            error_message: "bad".to_string(),
        }));
        assert!(
            !errored_probe
                .should_release_after_stream_activity(STREAM_CONTENT_START_ACTIVITY_READY_BYTES)
        );

        let mut non_thinking_probe = StreamContentStartProbe::new(false);
        assert!(!non_thinking_probe.observe(&Event::Metering(())));
        assert!(
            !non_thinking_probe
                .should_release_after_stream_activity(STREAM_CONTENT_START_ACTIVITY_READY_BYTES)
        );
    }

    #[test]
    fn test_non_stream_eventstream_diagnostics_counts_reasoning_as_output() {
        let mut decoder = EventStreamDecoder::new();
        let frame = event_frame(
            "reasoningContentEvent",
            br#"{"text":"thinking","signature":"sig"}"#,
        );
        decoder.feed(&frame).unwrap();

        let parsed = decoder.decode().unwrap().unwrap();
        let mut diagnostics = NonStreamEventStreamReadDiagnostics::default();
        diagnostics.observe_frame(parsed);

        assert_eq!(diagnostics.observed_frames, 1);
        assert_eq!(diagnostics.reasoning_events, 1);
        assert_eq!(diagnostics.reasoning_text_bytes, 8);
        assert_eq!(diagnostics.unknown_events, 0);
        assert!(diagnostics.has_usable_output());
        assert!(!diagnostics.safe_to_retry_stall());
    }

    #[test]
    fn test_non_stream_eventstream_diagnostics_does_not_retry_after_output() {
        let mut decoder = EventStreamDecoder::new();
        let frame = event_frame("assistantResponseEvent", br#"{"content":"hello"}"#);
        decoder.feed(&frame).unwrap();

        let parsed = decoder.decode().unwrap().unwrap();
        let mut diagnostics = NonStreamEventStreamReadDiagnostics::default();
        diagnostics.observe_frame(parsed);

        assert_eq!(diagnostics.assistant_events, 1);
        assert_eq!(diagnostics.assistant_content_bytes, 5);
        assert!(diagnostics.has_usable_output());
        assert!(!diagnostics.safe_to_retry_stall());
    }

    #[tokio::test]
    async fn test_non_stream_eventstream_reader_times_out_on_idle_with_diagnostics() {
        let event = event_frame("reasoningContentEvent", br#"{"text":"thinking"}"#);
        let app = Router::new().route(
            "/eventstream",
            get(move || {
                let event = event.clone();
                async move {
                    let body_stream =
                        stream::once(async move { Ok::<Bytes, Infallible>(Bytes::from(event)) })
                            .chain(stream::pending::<Result<Bytes, Infallible>>());

                    Response::builder()
                        .status(200)
                        .header("content-type", AMAZON_EVENTSTREAM_CONTENT_TYPE)
                        .body(Body::from_stream(body_stream))
                        .unwrap()
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let response = reqwest::Client::new()
            .get(format!("http://{addr}/eventstream"))
            .send()
            .await
            .unwrap();
        let trace = test_response_trace(Some(AMAZON_EVENTSTREAM_CONTENT_TYPE));

        let result = read_response_body_with_trace_timeout(response, &trace, 10_000, 20).await;

        match result {
            Err(ResponseBodyReadFailure::Timeout {
                timeout_ms,
                reason,
                eventstream_diagnostics: Some(diagnostics),
            }) => {
                assert_eq!(timeout_ms, 20);
                assert_eq!(reason, "eventstream_idle_timeout");
                assert_eq!(diagnostics.observed_frames, 1);
                assert_eq!(diagnostics.reasoning_events, 1);
                assert_eq!(diagnostics.reasoning_text_bytes, 8);
                assert_eq!(diagnostics.unknown_events, 0);
                assert!(!diagnostics.safe_to_retry_stall());
            }
            other => panic!(
                "unexpected read result: {:?}",
                other.map(|bytes| bytes.len())
            ),
        }

        server.abort();
    }
}
