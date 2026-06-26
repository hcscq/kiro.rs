//! Anthropic API Handler 函数

use std::{convert::Infallible, sync::Arc};

use crate::common::logging::summarize_text_for_log;
use crate::kiro::model::events::Event;
use crate::kiro::model::requests::kiro::KiroRequest;
use crate::kiro::parser::decoder::EventStreamDecoder;
use crate::kiro::parser::error::ParseError;
use crate::kiro::parser::frame::Frame;
use crate::kiro::token_manager::{
    CredentialScopeForbiddenError, RuntimeRefreshLeaderRequiredError, RuntimeRefreshLeaseBusyError,
};
use crate::model::config::ThinkingSignatureValidationMode;
use crate::model::model_catalog::built_in_model_catalog;
use crate::token;
use anyhow::Error;
use axum::{
    body::Body,
    extract::{Extension, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Json, Response},
};
use bytes::Bytes;
use futures::{Stream, StreamExt, stream};
use serde_json::json;
use std::time::Duration;
use tokio::time::{Instant, interval_at};
use uuid::Uuid;

use super::conversion_runtime::{ConversionRuntime, ConversionRuntimeError};
use super::converter::{ConversionError, ConversionResult};
use super::diagnostics;
use super::extractor::AnthropicJson;
use super::middleware::{ANTHROPIC_RUNTIME_LEADER_REQUIRED_HEADER, AppState};
use super::multimodal;
use super::probe::{UpstreamProbe, parse_upstream_probe};
use super::stream::{BufferedStreamContext, SseEvent, StreamContext};
use super::structured_outputs::{self, JsonSchemaOutput, StructuredOutputError};
use super::thinking_compat::{
    ThinkingSignatureInvalidDiagnostic, ThinkingSignatureValidationStats,
    extract_thinking_and_text, inspect_thinking_signatures, sign_thinking_block,
    strip_invalid_own_thinking_signatures, validate_thinking_signatures,
};
use super::types::{
    CountTokensRequest, CountTokensResponse, ErrorResponse, MessagesRequest, Model, ModelsResponse,
    OutputConfig, Thinking,
};
use super::webfetch;
use crate::common::auth::ApiKeyAuthContext;
use crate::kiro::provider::{PublicProviderError, RequestOptions, ResponseUsageRecorder};

const LARGE_ANTHROPIC_REQUEST_WARN_THRESHOLD_BYTES: usize = 16 * 1024 * 1024;
const DETAILED_ANTHROPIC_PRE_UPSTREAM_TRACE_THRESHOLD_BYTES: usize = 512 * 1024;
const DETAILED_ANTHROPIC_PRE_UPSTREAM_TRACE_MESSAGE_THRESHOLD: usize = 250;
const SLOW_ANTHROPIC_PRE_UPSTREAM_PHASE_MS: u128 = 1_000;
const SLOW_ANTHROPIC_PRE_UPSTREAM_STAGE_MS: u128 = 250;
const TOOL_SCHEMA_DIAGNOSTIC_MAX_TOOLS: usize = 64;
const TOOL_SCHEMA_DIAGNOSTIC_MAX_SCHEMA_BYTES: usize = 128 * 1024;
const TOOL_SCHEMA_DIAGNOSTIC_MAX_TOTAL_BYTES: usize = 512 * 1024;
const TOOL_SCHEMA_DIAGNOSTIC_MAX_INVALID_LOGS: usize = 4;

#[derive(Debug, Clone)]
pub(crate) struct NonStreamMessageResponse {
    pub body: serde_json::Value,
    pub content: Vec<serde_json::Value>,
    pub stop_reason: String,
    pub input_tokens: i32,
    pub output_tokens: i32,
    pub input_token_source: &'static str,
}

fn request_id_from_headers(headers: &HeaderMap) -> String {
    headers
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| format!("kirors-{}", Uuid::new_v4().simple()))
}

fn json_schema_output_or_response(
    payload: &MessagesRequest,
    request_id: &str,
) -> Result<Option<JsonSchemaOutput>, Response> {
    if payload
        .output_config
        .as_ref()
        .and_then(|config| config.format.as_ref())
        .is_some_and(|format| format.format_type == "json_schema" && format.schema.is_none())
    {
        let message = "output_config.format.schema is required when format.type is json_schema";
        tracing::warn!(request_id = %request_id, error = %message, "Structured output schema missing");
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::new("invalid_request_error", message)),
        )
            .into_response());
    }
    let Some(output) = structured_outputs::json_schema_output(payload) else {
        return Ok(None);
    };
    if let Err(message) = structured_outputs::validate_json_schema_output(&output) {
        tracing::warn!(request_id = %request_id, error = %message, "Structured output schema validation failed");
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::new("invalid_request_error", message)),
        )
            .into_response());
    }
    let stats = structured_outputs::schema_stats(&output.schema);
    tracing::info!(
        request_id = %request_id,
        schema_bytes = stats.bytes,
        schema_depth = stats.max_depth,
        schema_nodes = stats.nodes,
        schema_combinator_branches = stats.combinator_branches,
        schema_properties = stats.properties,
        "Structured output JSON Schema accepted"
    );
    Ok(Some(output))
}

async fn convert_request_on_runtime(
    conversion_runtime: Arc<ConversionRuntime>,
    payload: &MessagesRequest,
    probe: UpstreamProbe,
    request_id: Option<&str>,
    route: &str,
    body_bytes: Option<usize>,
) -> Result<ConversionResult, Response> {
    match conversion_runtime.convert(payload, probe, body_bytes).await {
        Ok(result) => Ok(result),
        Err(err) => Err(conversion_runtime_error_response(err, request_id, route)),
    }
}

fn log_converted_tool_schema_diagnostics(
    request_id: Option<&str>,
    route: &str,
    model: &str,
    conversion_result: &ConversionResult,
) {
    let tools = &conversion_result
        .conversation_state
        .current_message
        .user_input_message
        .user_input_message_context
        .tools;
    if tools.is_empty() {
        return;
    }

    let request_id = request_id.unwrap_or("");
    let mut total_schema_bytes = 0usize;
    let mut skipped_tools = 0usize;
    let mut invalid_logged = 0usize;

    for (tool_index, tool) in tools.iter().enumerate() {
        if tool_index >= TOOL_SCHEMA_DIAGNOSTIC_MAX_TOOLS {
            skipped_tools = skipped_tools.saturating_add(tools.len().saturating_sub(tool_index));
            break;
        }

        let schema = &tool.tool_specification.input_schema.json;
        let schema_bytes = serde_json::to_vec(schema)
            .map(|bytes| bytes.len())
            .unwrap_or(0);
        if schema_bytes > TOOL_SCHEMA_DIAGNOSTIC_MAX_SCHEMA_BYTES
            || total_schema_bytes.saturating_add(schema_bytes)
                > TOOL_SCHEMA_DIAGNOSTIC_MAX_TOTAL_BYTES
        {
            skipped_tools = skipped_tools.saturating_add(1);
            continue;
        }
        total_schema_bytes = total_schema_bytes.saturating_add(schema_bytes);

        if let Some(schema_uri) = non_2020_12_schema_uri(schema) {
            invalid_logged = invalid_logged.saturating_add(1);
            tracing::warn!(
                request_id = %request_id,
                route = %route,
                model = %model,
                mapped_model_id = %conversion_result.model_id,
                tool_index,
                tool_name = %summarize_text_for_log(&tool.tool_specification.name, 96),
                schema_bytes,
                schema_uri = %summarize_text_for_log(schema_uri, 160),
                "converted tool input_schema declares a non-2020-12 JSON Schema draft"
            );
            if invalid_logged >= TOOL_SCHEMA_DIAGNOSTIC_MAX_INVALID_LOGS {
                break;
            }
        }

        if let Err(err) = jsonschema::validator_for(schema) {
            invalid_logged = invalid_logged.saturating_add(1);
            tracing::warn!(
                request_id = %request_id,
                route = %route,
                model = %model,
                mapped_model_id = %conversion_result.model_id,
                tool_index,
                tool_name = %summarize_text_for_log(&tool.tool_specification.name, 96),
                schema_bytes,
                error = %summarize_text_for_log(&err.to_string(), 240),
                "converted tool input_schema failed local JSON Schema validation"
            );
            if invalid_logged >= TOOL_SCHEMA_DIAGNOSTIC_MAX_INVALID_LOGS {
                break;
            }
        }
    }

    if skipped_tools > 0 {
        tracing::debug!(
            request_id = %request_id,
            route = %route,
            model = %model,
            tool_count = tools.len(),
            skipped_tools,
            total_schema_bytes,
            max_tools = TOOL_SCHEMA_DIAGNOSTIC_MAX_TOOLS,
            max_schema_bytes = TOOL_SCHEMA_DIAGNOSTIC_MAX_SCHEMA_BYTES,
            max_total_schema_bytes = TOOL_SCHEMA_DIAGNOSTIC_MAX_TOTAL_BYTES,
            "skipped converted tool input_schema diagnostics due to budget"
        );
    }
}

fn non_2020_12_schema_uri(schema: &serde_json::Value) -> Option<&str> {
    let schema_uri = schema
        .as_object()
        .and_then(|obj| obj.get("$schema"))
        .and_then(|value| value.as_str())?;
    if schema_uri.contains("2020-12") {
        return None;
    }
    Some(schema_uri)
}

fn conversion_runtime_error_response(
    err: ConversionRuntimeError,
    request_id: Option<&str>,
    route: &str,
) -> Response {
    match err {
        ConversionRuntimeError::Conversion(e) => {
            let (error_type, error_code, message) = match &e {
                ConversionError::UnsupportedModel(model) => (
                    "invalid_request_error",
                    None,
                    format!("模型不支持: {}", model),
                ),
                ConversionError::EmptyMessages => {
                    ("invalid_request_error", None, "消息列表为空".to_string())
                }
                ConversionError::DocumentValidation(message) => {
                    ("invalid_request_error", None, message.clone())
                }
                ConversionError::KiroHistoryLimitExceeded { .. } => (
                    "invalid_request_error",
                    Some("context_length_exceeded"),
                    format!(
                        "prompt is too long: {}. Reduce or compact conversation history.",
                        e
                    ),
                ),
            };
            if let Some(request_id) = request_id {
                tracing::warn!(request_id = %request_id, route, error = %e, "请求转换失败");
            } else {
                tracing::warn!(route, error = %e, "请求转换失败");
            }
            let error_response = match error_code {
                Some(code) => ErrorResponse::with_code(error_type, code, message),
                None => ErrorResponse::new(error_type, message),
            };
            (StatusCode::BAD_REQUEST, Json(error_response)).into_response()
        }
        ConversionRuntimeError::QueueFull { stats, workload } => {
            if let Some(request_id) = request_id {
                tracing::warn!(
                    request_id = %request_id,
                    route,
                    max_concurrent = stats.max_concurrent,
                    available_permits = stats.available_permits,
                    used_permits = stats.used_permits,
                    waiting = stats.waiting,
                    waiting_weight = stats.waiting_weight,
                    max_queue = stats.max_queue,
                    max_queue_weight = stats.max_queue_weight,
                    queue_wait_ms = stats.queue_wait_ms,
                    request_weight = workload.weight,
                    body_bytes = workload.body_bytes,
                    message_count = workload.message_count,
                    image_count = workload.image_count,
                    document_count = workload.document_count,
                    source_data_bytes = workload.source_data_bytes,
                    "转换运行池队列已满，拒绝请求并要求客户端重试"
                );
            } else {
                tracing::warn!(
                    route,
                    max_concurrent = stats.max_concurrent,
                    available_permits = stats.available_permits,
                    used_permits = stats.used_permits,
                    waiting = stats.waiting,
                    waiting_weight = stats.waiting_weight,
                    max_queue = stats.max_queue,
                    max_queue_weight = stats.max_queue_weight,
                    queue_wait_ms = stats.queue_wait_ms,
                    request_weight = workload.weight,
                    body_bytes = workload.body_bytes,
                    message_count = workload.message_count,
                    image_count = workload.image_count,
                    document_count = workload.document_count,
                    source_data_bytes = workload.source_data_bytes,
                    "转换运行池队列已满，拒绝请求并要求客户端重试"
                );
            }
            conversion_overloaded_response("conversion_queue_full")
        }
        ConversionRuntimeError::WaitTimeout { stats, workload } => {
            if let Some(request_id) = request_id {
                tracing::warn!(
                    request_id = %request_id,
                    route,
                    max_concurrent = stats.max_concurrent,
                    available_permits = stats.available_permits,
                    used_permits = stats.used_permits,
                    waiting = stats.waiting,
                    waiting_weight = stats.waiting_weight,
                    max_queue = stats.max_queue,
                    max_queue_weight = stats.max_queue_weight,
                    queue_wait_ms = stats.queue_wait_ms,
                    request_weight = workload.weight,
                    body_bytes = workload.body_bytes,
                    message_count = workload.message_count,
                    image_count = workload.image_count,
                    document_count = workload.document_count,
                    source_data_bytes = workload.source_data_bytes,
                    "转换运行池等待超时，拒绝请求并要求客户端重试"
                );
            } else {
                tracing::warn!(
                    route,
                    max_concurrent = stats.max_concurrent,
                    available_permits = stats.available_permits,
                    used_permits = stats.used_permits,
                    waiting = stats.waiting,
                    waiting_weight = stats.waiting_weight,
                    max_queue = stats.max_queue,
                    max_queue_weight = stats.max_queue_weight,
                    queue_wait_ms = stats.queue_wait_ms,
                    request_weight = workload.weight,
                    body_bytes = workload.body_bytes,
                    message_count = workload.message_count,
                    image_count = workload.image_count,
                    document_count = workload.document_count,
                    source_data_bytes = workload.source_data_bytes,
                    "转换运行池等待超时，拒绝请求并要求客户端重试"
                );
            }
            conversion_overloaded_response("conversion_queue_timeout")
        }
        ConversionRuntimeError::WorkerJoin(message) => {
            tracing::error!(route, request_id = ?request_id, error = %message, "转换 blocking worker 失败");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new(
                    "internal_error",
                    "conversion worker failed",
                )),
            )
                .into_response()
        }
    }
}

fn conversion_overloaded_response(reason: &'static str) -> Response {
    let mut response = (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(ErrorResponse::new(
            "service_overloaded",
            format!("conversion runtime overloaded: {reason}"),
        )),
    )
        .into_response();
    response
        .headers_mut()
        .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
    response
}

fn built_in_models() -> Vec<Model> {
    built_in_model_catalog()
        .iter()
        .map(|item| Model {
            id: item.api_id.to_string(),
            object: "model".to_string(),
            created: item.created,
            owned_by: "anthropic".to_string(),
            display_name: item.display_name.to_string(),
            model_type: "chat".to_string(),
            max_tokens: item.max_tokens,
        })
        .collect()
}

/// 将 KiroProvider 错误映射为 HTTP 响应
pub(crate) fn map_provider_error(err: Error) -> Response {
    let err_str = err.to_string();
    let err_summary = summarize_text_for_log(&err_str, 240);

    if err
        .downcast_ref::<RuntimeRefreshLeaderRequiredError>()
        .is_some()
    {
        tracing::warn!(error = %err_summary, "共享凭据刷新需由 leader 处理，当前请求快速失败");
        let mut response = (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse::new(
                "service_unavailable",
                "Shared credential refresh must be handled by the runtime leader. Retry later or route this request to the leader.",
            )),
        )
            .into_response();
        response.headers_mut().insert(
            ANTHROPIC_RUNTIME_LEADER_REQUIRED_HEADER,
            HeaderValue::from_static("1"),
        );
        return response;
    }

    if err.downcast_ref::<RuntimeRefreshLeaseBusyError>().is_some() {
        tracing::info!(
            error = %err_summary,
            "共享凭据正在由其他实例刷新，当前请求快速失败"
        );
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse::new(
                "service_unavailable",
                "Shared credential refresh is already in progress on another runtime instance. Retry later.",
            )),
        )
            .into_response();
    }

    if err
        .downcast_ref::<CredentialScopeForbiddenError>()
        .is_some()
    {
        tracing::warn!(error = %err_summary, "API key 凭据分组范围内没有可用凭据");
        return (
            StatusCode::FORBIDDEN,
            Json(ErrorResponse::new(
                "permission_error",
                "No available credentials are assigned to this API key's credential groups.",
            )),
        )
            .into_response();
    }

    if let Some(public_err) = err.downcast_ref::<PublicProviderError>() {
        let status =
            StatusCode::from_u16(public_err.status_code()).unwrap_or(StatusCode::BAD_GATEWAY);
        if status.is_client_error() {
            tracing::warn!(error = %err_summary, "Kiro API 调用失败（客户端请求错误）");
        } else {
            tracing::error!(error = %err_summary, "Kiro API 调用失败（公开错误映射）");
        }
        let error_response = match public_err.error_code() {
            Some(code) => {
                ErrorResponse::with_code(public_err.error_type(), code, public_err.public_message())
            }
            None => ErrorResponse::new(public_err.error_type(), public_err.public_message()),
        };
        return (status, Json(error_response)).into_response();
    }

    // 上下文窗口满了（对话历史累积超出模型上下文窗口限制）
    if err_str.contains("CONTENT_LENGTH_EXCEEDS_THRESHOLD") {
        tracing::warn!(error = %err_summary, "上游拒绝请求：上下文窗口已满（不应重试）");
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::context_length_exceeded()),
        )
            .into_response();
    }

    // 单次输入太长（请求体本身超出上游限制）
    if err_str.contains("Input is too long") {
        tracing::warn!(error = %err_summary, "上游拒绝请求：输入过长（不应重试）");
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::new(
                "invalid_request_error",
                "Input is too long. Reduce the size of your messages.",
            )),
        )
            .into_response();
    }
    if err_str.contains("等待队列已满") {
        tracing::warn!(error = %err_summary, "本地等待队列已满");
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(ErrorResponse::new(
                "rate_limit_error",
                "Request queue is full. Retry later or raise queueMaxSize.",
            )),
        )
            .into_response();
    }
    if err_str.contains("等待可用凭据超时") {
        tracing::warn!(error = %err_summary, "请求等待可用凭据超时");
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse::new(
                "service_unavailable",
                "Timed out waiting for a dispatchable account. Accounts may be at maxConcurrency, cooling down after 429s, or waiting for local token-bucket refill. Retry later or tune queueMaxWaitMs/maxConcurrency/token-bucket settings.",
            )),
        )
            .into_response();
    }
    if err_str.contains("并发上限") {
        tracing::warn!(error = %err_summary, "所有可用凭据暂时不可用");
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse::new(
                "service_unavailable",
                "All available accounts are saturated, cooling down, or throttled by the local token bucket. Retry later or tune maxConcurrency/token-bucket settings.",
            )),
        )
            .into_response();
    }
    tracing::error!(error = %err_summary, "Kiro API 调用失败");
    (
        StatusCode::BAD_GATEWAY,
        Json(ErrorResponse::new(
            "api_error",
            "上游 API 调用失败，请稍后重试。",
        )),
    )
        .into_response()
}

async fn normalize_multimodal_payload(
    payload: &mut MessagesRequest,
    request_id: &str,
    route: &'static str,
    original_body_bytes: usize,
) -> Result<(), Response> {
    match multimodal::normalize_multimodal_urls(payload).await {
        Ok(stats) => {
            if stats.remote_images > 0
                || stats.data_url_images > 0
                || stats.remote_documents > 0
                || stats.data_url_documents > 0
                || stats.openai_image_url_blocks > 0
                || stats.image_url_document_blocks > 0
                || stats.anthropic_url_blocks > 0
                || stats.document_url_blocks > 0
                || stats.anthropic_document_url_blocks > 0
            {
                tracing::info!(
                    request_id = %request_id,
                    remote_images = stats.remote_images,
                    data_url_images = stats.data_url_images,
                    remote_documents = stats.remote_documents,
                    data_url_documents = stats.data_url_documents,
                    openai_image_url_blocks = stats.openai_image_url_blocks,
                    image_url_document_blocks = stats.image_url_document_blocks,
                    anthropic_url_blocks = stats.anthropic_url_blocks,
                    document_url_blocks = stats.document_url_blocks,
                    anthropic_document_url_blocks = stats.anthropic_document_url_blocks,
                    "normalized multimodal references"
                );
            }
            Ok(())
        }
        Err(err) => {
            tracing::warn!(request_id = %request_id, error = %err, "多模态归一化失败");
            diagnostics::capture_invalid_multimodal_request(
                request_id,
                route,
                payload,
                original_body_bytes,
                &err.to_string(),
            );
            Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::new(
                    "invalid_request_error",
                    format!("Invalid multimodal URL: {err}"),
                )),
            )
                .into_response())
        }
    }
}

fn log_thinking_signature_invalid_diagnostic(
    request_id: &str,
    diagnostic: &ThinkingSignatureInvalidDiagnostic,
    validation_mode: ThinkingSignatureValidationMode,
    validation_action: &'static str,
    error: Option<&dyn std::fmt::Display>,
) {
    if let Some(err) = error {
        tracing::warn!(
            request_id = %request_id,
            error = %err,
            validation_mode = validation_mode.as_str(),
            validation_action,
            message_index = diagnostic.message_index,
            thinking_ordinal = diagnostic.thinking_ordinal,
            invalid_reason = diagnostic.reason.as_str(),
            signed_ordinal = diagnostic.signed_ordinal,
            raw_signature_len = diagnostic.raw_signature_len,
            signature_sha256_prefix = %diagnostic.signature_sha256_prefix.as_str(),
            canonical_thinking_len = diagnostic.canonical_thinking_len,
            signed_thinking_hash_prefix = %diagnostic.signed_thinking_hash_prefix.as_str(),
            computed_thinking_hash_prefix = %diagnostic.computed_thinking_hash_prefix.as_str(),
            "thinking 签名校验失败"
        );
    } else {
        tracing::warn!(
            request_id = %request_id,
            validation_mode = validation_mode.as_str(),
            validation_action,
            message_index = diagnostic.message_index,
            thinking_ordinal = diagnostic.thinking_ordinal,
            invalid_reason = diagnostic.reason.as_str(),
            signed_ordinal = diagnostic.signed_ordinal,
            raw_signature_len = diagnostic.raw_signature_len,
            signature_sha256_prefix = %diagnostic.signature_sha256_prefix.as_str(),
            canonical_thinking_len = diagnostic.canonical_thinking_len,
            signed_thinking_hash_prefix = %diagnostic.signed_thinking_hash_prefix.as_str(),
            computed_thinking_hash_prefix = %diagnostic.computed_thinking_hash_prefix.as_str(),
            "thinking 签名校验失败"
        );
    }
}

fn validate_thinking_signature_payload(
    payload: &mut MessagesRequest,
    request_id: &str,
    validation_mode: ThinkingSignatureValidationMode,
) -> Result<ThinkingSignatureValidationStats, Response> {
    if validation_mode == ThinkingSignatureValidationMode::Disabled {
        tracing::warn!(
            request_id = %request_id,
            validation_mode = validation_mode.as_str(),
            "Anthropic thinking signature validation skipped by configuration"
        );
        return Ok(ThinkingSignatureValidationStats::default());
    }

    let mut stripped_invalid_signatures = 0usize;
    if validation_mode == ThinkingSignatureValidationMode::StripInvalid {
        let diagnostics = strip_invalid_own_thinking_signatures(payload);
        stripped_invalid_signatures = diagnostics.len();
        for diagnostic in &diagnostics {
            log_thinking_signature_invalid_diagnostic(
                request_id,
                diagnostic,
                validation_mode,
                "strip_invalid",
                None,
            );
        }
        if stripped_invalid_signatures > 0 {
            tracing::warn!(
                request_id = %request_id,
                validation_mode = validation_mode.as_str(),
                stripped_invalid_thinking_signatures = stripped_invalid_signatures,
                "stripped invalid thinking signatures before upstream dispatch"
            );
        }
    }

    match validate_thinking_signatures(payload) {
        Ok(mut stats) => {
            stats.invalid_own_signatures = stats
                .invalid_own_signatures
                .saturating_add(stripped_invalid_signatures);
            if stats.valid_own_signatures > 0
                || stats.invalid_own_signatures > 0
                || stats.foreign_signatures > 0
                || stats.missing_signatures > 0
            {
                tracing::info!(
                    request_id = %request_id,
                    validation_mode = validation_mode.as_str(),
                    valid_own_signatures = stats.valid_own_signatures,
                    invalid_own_signatures = stats.invalid_own_signatures,
                    foreign_signatures = stats.foreign_signatures,
                    missing_signatures = stats.missing_signatures,
                    "validated historical thinking signatures"
                );
            }
            Ok(stats)
        }
        Err(err) => {
            if let Some(diagnostic) = err.diagnostic() {
                let validation_action =
                    if validation_mode == ThinkingSignatureValidationMode::WarnOnly {
                        "warn_only_continue"
                    } else {
                        "reject"
                    };
                log_thinking_signature_invalid_diagnostic(
                    request_id,
                    diagnostic,
                    validation_mode,
                    validation_action,
                    Some(&err),
                );
            } else {
                tracing::warn!(
                    request_id = %request_id,
                    error = %err,
                    validation_mode = validation_mode.as_str(),
                    "thinking 签名校验失败"
                );
            }
            if validation_mode == ThinkingSignatureValidationMode::WarnOnly {
                let report = inspect_thinking_signatures(payload);
                tracing::warn!(
                    request_id = %request_id,
                    validation_mode = validation_mode.as_str(),
                    invalid_own_signatures = report.stats.invalid_own_signatures,
                    "continuing despite invalid thinking signature"
                );
                return Ok(report.stats);
            }
            Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::new(
                    "invalid_request_error",
                    err.public_message(),
                )),
            )
                .into_response())
        }
    }
}

fn should_trace_anthropic_pre_upstream(body_bytes: usize, message_count: usize) -> bool {
    body_bytes >= DETAILED_ANTHROPIC_PRE_UPSTREAM_TRACE_THRESHOLD_BYTES
        || message_count >= DETAILED_ANTHROPIC_PRE_UPSTREAM_TRACE_MESSAGE_THRESHOLD
}

struct PreUpstreamTrace {
    enabled: bool,
    request_id: String,
    route: &'static str,
    model: String,
    stream: bool,
    body_bytes: usize,
    content_length_header: Option<u64>,
    message_count: usize,
    started_at: Instant,
    last_stage: &'static str,
    completed: bool,
}

impl PreUpstreamTrace {
    fn new(
        enabled: bool,
        request_id: &str,
        route: &'static str,
        model: &str,
        stream: bool,
        body_bytes: usize,
        content_length_header: Option<u64>,
        message_count: usize,
    ) -> Self {
        Self {
            enabled,
            request_id: request_id.to_string(),
            route,
            model: model.to_string(),
            stream,
            body_bytes,
            content_length_header,
            message_count,
            started_at: Instant::now(),
            last_stage: "signature_validation_completed",
            completed: false,
        }
    }

    fn enabled(&self) -> bool {
        self.enabled
    }

    fn elapsed_ms(&self) -> u128 {
        self.started_at.elapsed().as_millis()
    }

    fn mark_signature_validation_completed(
        &mut self,
        signature_validation_ms: u128,
        stats: ThinkingSignatureValidationStats,
    ) {
        if !self.enabled && signature_validation_ms < SLOW_ANTHROPIC_PRE_UPSTREAM_STAGE_MS {
            return;
        }
        let elapsed_since_validation_ms = self.elapsed_ms();
        if signature_validation_ms >= SLOW_ANTHROPIC_PRE_UPSTREAM_STAGE_MS {
            tracing::warn!(
                request_id = %self.request_id,
                route = self.route,
                model = %self.model,
                stream = self.stream,
                body_bytes = self.body_bytes,
                content_length_header = ?self.content_length_header,
                message_count = self.message_count,
                signature_validation_ms,
                elapsed_since_validation_ms,
                valid_own_signatures = stats.valid_own_signatures,
                invalid_own_signatures = stats.invalid_own_signatures,
                foreign_signatures = stats.foreign_signatures,
                missing_signatures = stats.missing_signatures,
                "Anthropic thinking signature validation completed slowly"
            );
        } else {
            tracing::info!(
                request_id = %self.request_id,
                route = self.route,
                model = %self.model,
                stream = self.stream,
                body_bytes = self.body_bytes,
                content_length_header = ?self.content_length_header,
                message_count = self.message_count,
                signature_validation_ms,
                elapsed_since_validation_ms,
                valid_own_signatures = stats.valid_own_signatures,
                invalid_own_signatures = stats.invalid_own_signatures,
                foreign_signatures = stats.foreign_signatures,
                missing_signatures = stats.missing_signatures,
                "Anthropic thinking signature validation completed"
            );
        }
    }

    fn mark_stage_started(&mut self, stage: &'static str) {
        self.last_stage = stage;
        if !self.enabled {
            return;
        }
        tracing::info!(
            request_id = %self.request_id,
            route = self.route,
            model = %self.model,
            stream = self.stream,
            body_bytes = self.body_bytes,
            content_length_header = ?self.content_length_header,
            message_count = self.message_count,
            stage,
            elapsed_since_validation_ms = self.elapsed_ms(),
            "Anthropic pre-upstream phase started"
        );
    }

    fn mark_stage_completed(&mut self, stage: &'static str, stage_ms: u128) {
        if !self.enabled && stage_ms < SLOW_ANTHROPIC_PRE_UPSTREAM_STAGE_MS {
            return;
        }
        if stage_ms >= SLOW_ANTHROPIC_PRE_UPSTREAM_STAGE_MS {
            tracing::warn!(
                request_id = %self.request_id,
                route = self.route,
                model = %self.model,
                stream = self.stream,
                body_bytes = self.body_bytes,
                content_length_header = ?self.content_length_header,
                message_count = self.message_count,
                stage,
                stage_ms,
                elapsed_since_validation_ms = self.elapsed_ms(),
                "Anthropic pre-upstream phase completed slowly"
            );
        } else {
            tracing::info!(
                request_id = %self.request_id,
                route = self.route,
                model = %self.model,
                stream = self.stream,
                body_bytes = self.body_bytes,
                content_length_header = ?self.content_length_header,
                message_count = self.message_count,
                stage,
                stage_ms,
                elapsed_since_validation_ms = self.elapsed_ms(),
                "Anthropic pre-upstream phase completed"
            );
        }
    }

    fn mark_terminal(&mut self, outcome: &'static str) {
        self.completed = true;
        if !self.enabled {
            return;
        }
        tracing::info!(
            request_id = %self.request_id,
            route = self.route,
            model = %self.model,
            stream = self.stream,
            body_bytes = self.body_bytes,
            content_length_header = ?self.content_length_header,
            message_count = self.message_count,
            outcome,
            elapsed_since_validation_ms = self.elapsed_ms(),
            "Anthropic pre-upstream trace ended before provider dispatch"
        );
    }

    fn mark_dispatching(
        &mut self,
        request_body_bytes: usize,
        estimated_input_tokens: i32,
        billing_input_tokens: i32,
        request_weight: f64,
        thinking_enabled: bool,
    ) {
        self.completed = true;
        if !self.enabled {
            return;
        }
        tracing::info!(
            request_id = %self.request_id,
            route = self.route,
            model = %self.model,
            stream = self.stream,
            body_bytes = self.body_bytes,
            content_length_header = ?self.content_length_header,
            message_count = self.message_count,
            request_body_bytes,
            estimated_input_tokens,
            billing_input_tokens,
            request_weight,
            thinking_enabled,
            elapsed_since_validation_ms = self.elapsed_ms(),
            "Anthropic pre-upstream trace dispatching to provider"
        );
    }
}

impl Drop for PreUpstreamTrace {
    fn drop(&mut self) {
        if !self.enabled || self.completed {
            return;
        }
        tracing::warn!(
            request_id = %self.request_id,
            route = self.route,
            model = %self.model,
            stream = self.stream,
            body_bytes = self.body_bytes,
            content_length_header = ?self.content_length_header,
            message_count = self.message_count,
            last_stage = self.last_stage,
            elapsed_since_validation_ms = self.elapsed_ms(),
            "Anthropic pre-upstream trace dropped before provider dispatch"
        );
    }
}

/// GET /v1/models
///
/// 返回可用的模型列表
pub async fn get_models(
    State(state): State<AppState>,
    api_key_context: Option<Extension<ApiKeyAuthContext>>,
) -> impl IntoResponse {
    tracing::info!("Received GET /v1/models request");

    let mut models = built_in_models();
    if let Some(provider) = &state.kiro_provider {
        let scope = api_key_context
            .as_ref()
            .map(|Extension(context)| &context.credential_group_scope);
        models.retain(|model| provider.supports_model_for_scope(&model.id, scope));
    }

    Json(ModelsResponse {
        object: "list".to_string(),
        data: models,
    })
}

/// POST /v1/messages
///
/// 创建消息（对话）
pub async fn post_messages(
    State(state): State<AppState>,
    api_key_context: Option<Extension<ApiKeyAuthContext>>,
    headers: HeaderMap,
    payload: AnthropicJson<MessagesRequest>,
) -> Response {
    let handler_started_at = Instant::now();
    let body_bytes = payload.body_len();
    let content_length_header = payload.content_length_header();
    let body_buffer_ms = payload.body_buffer_ms();
    let json_parse_ms = payload.json_parse_ms();
    let request_id = request_id_from_headers(&headers);
    tracing::info!(
        request_id = %request_id,
        model = %payload.model,
        max_tokens = %payload.max_tokens,
        stream = %payload.stream,
        body_bytes,
        content_length_header = ?content_length_header,
        body_buffer_ms,
        json_parse_ms,
        message_count = %payload.messages.len(),
        "Received POST /v1/messages request"
    );
    if body_bytes >= LARGE_ANTHROPIC_REQUEST_WARN_THRESHOLD_BYTES {
        tracing::warn!(
            request_id = %request_id,
            model = %payload.model,
            body_bytes,
            content_length_header = ?content_length_header,
            body_buffer_ms,
            json_parse_ms,
            "Large Anthropic request body observed"
        );
    }
    let mut payload = payload.into_inner();
    // 检查 KiroProvider 是否可用
    let provider = match &state.kiro_provider {
        Some(p) => p.clone(),
        None => {
            tracing::error!("KiroProvider 未配置");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse::new(
                    "service_unavailable",
                    "Kiro API provider not configured",
                )),
            )
                .into_response();
        }
    };

    let api_key_context = api_key_context.map(|Extension(context)| context);
    let probe = parse_upstream_probe(&headers);
    if probe.is_enabled() {
        tracing::info!(request_id = %request_id, ?probe, "启用上游裸探针选项");
    }

    // 检测模型名是否包含 "thinking" 后缀，若包含则覆写 thinking 配置
    override_thinking_from_model_name(&mut payload);

    let thinking_signature_validation_mode = provider.thinking_signature_validation_mode();
    let signature_validation_started_at = Instant::now();
    let signature_stats = match validate_thinking_signature_payload(
        &mut payload,
        &request_id,
        thinking_signature_validation_mode,
    ) {
        Ok(stats) => stats,
        Err(response) => {
            tracing::warn!(
                request_id = %request_id,
                route = "messages",
                model = %payload.model,
                validation_mode = thinking_signature_validation_mode.as_str(),
                stream = payload.stream,
                body_bytes,
                content_length_header = ?content_length_header,
                message_count = payload.messages.len(),
                signature_validation_ms = signature_validation_started_at.elapsed().as_millis(),
                "Anthropic request rejected during thinking signature validation"
            );
            return response;
        }
    };
    let signature_validation_ms = signature_validation_started_at.elapsed().as_millis();
    let mut pre_upstream_trace = PreUpstreamTrace::new(
        should_trace_anthropic_pre_upstream(body_bytes, payload.messages.len()),
        &request_id,
        "messages",
        &payload.model,
        payload.stream,
        body_bytes,
        content_length_header,
        payload.messages.len(),
    );
    pre_upstream_trace
        .mark_signature_validation_completed(signature_validation_ms, signature_stats);

    pre_upstream_trace.mark_stage_started("normalize_multimodal");
    let normalize_started_at = Instant::now();
    if let Err(response) =
        normalize_multimodal_payload(&mut payload, &request_id, "messages", body_bytes).await
    {
        pre_upstream_trace.mark_terminal("normalize_multimodal_failed");
        return response;
    }
    let normalize_ms = normalize_started_at.elapsed().as_millis();
    pre_upstream_trace.mark_stage_completed("normalize_multimodal", normalize_ms);

    pre_upstream_trace.mark_stage_started("structured_output_detection");
    let structured_output_started_at = Instant::now();
    let structured_output = match json_schema_output_or_response(&payload, &request_id) {
        Ok(output) => output,
        Err(response) => {
            pre_upstream_trace.mark_terminal("structured_output_schema_rejected");
            return response;
        }
    };
    let structured_output_ms = structured_output_started_at.elapsed().as_millis();
    pre_upstream_trace.mark_stage_completed("structured_output_detection", structured_output_ms);
    if pre_upstream_trace.enabled() {
        tracing::info!(
            request_id = %request_id,
            route = "messages",
            structured_output = structured_output.is_some(),
            structured_output_ms,
            "Anthropic structured output detection completed"
        );
    }
    if webfetch::has_server_web_tool(&payload) {
        tracing::info!(request_id = %request_id, "检测到 server-side Web 工具，路由到统一 Web 工具处理");

        pre_upstream_trace.mark_stage_started("server_web_tool_token_count");
        let token_count_started_at = Instant::now();
        let input_tokens = if structured_output.is_some() {
            count_billing_input_tokens_with_structured_instruction(&payload).await
        } else {
            count_billing_input_tokens(&payload).await
        };
        let token_count_ms = token_count_started_at.elapsed().as_millis();
        pre_upstream_trace.mark_stage_completed("server_web_tool_token_count", token_count_ms);
        if pre_upstream_trace.enabled() {
            tracing::info!(
                request_id = %request_id,
                route = "server_web_tool",
                token_count_ms,
                input_tokens,
                structured_output = structured_output.is_some(),
                "Anthropic server web tool token count completed"
            );
        }
        let pre_upstream_ms = handler_started_at.elapsed().as_millis();
        if pre_upstream_trace.enabled()
            || body_bytes >= LARGE_ANTHROPIC_REQUEST_WARN_THRESHOLD_BYTES
            || pre_upstream_ms >= SLOW_ANTHROPIC_PRE_UPSTREAM_PHASE_MS
        {
            tracing::warn!(
                request_id = %request_id,
                route = "server_web_tool",
                model = %payload.model,
                body_bytes,
                content_length_header = ?content_length_header,
                body_buffer_ms,
                json_parse_ms,
                normalize_ms,
                token_count_ms,
                pre_upstream_ms,
                input_tokens,
                structured_output = structured_output.is_some(),
                "Anthropic request pre-upstream phases completed"
            );
        }

        pre_upstream_trace.mark_terminal("server_web_tool_handoff");
        return webfetch::handle_server_web_tool_request(
            provider,
            &payload,
            input_tokens,
            state.conversion_runtime.clone(),
            probe.clone(),
            &request_id,
            structured_output,
            api_key_context.clone(),
        )
        .await;
    }
    if let Some(output) = structured_output.clone() {
        if payload.stream {
            pre_upstream_trace.mark_terminal("structured_output_stream_handoff");
            return handle_structured_stream_request(
                provider,
                payload,
                state.conversion_runtime.clone(),
                probe.clone(),
                request_id.clone(),
                output,
                api_key_context.clone(),
            )
            .await;
        }
        pre_upstream_trace.mark_terminal("structured_output_non_stream_handoff");
        return handle_structured_non_stream_request(
            provider,
            &payload,
            state.conversion_runtime.clone(),
            probe.clone(),
            request_id,
            output,
            api_key_context.clone(),
        )
        .await;
    }

    // 转换请求
    pre_upstream_trace.mark_stage_started("convert_request");
    let convert_started_at = Instant::now();
    let conversion_result = match convert_request_on_runtime(
        state.conversion_runtime.clone(),
        &payload,
        probe.clone(),
        Some(&request_id),
        "messages",
        Some(body_bytes),
    )
    .await
    {
        Ok(result) => result,
        Err(response) => {
            pre_upstream_trace
                .mark_stage_completed("convert_request", convert_started_at.elapsed().as_millis());
            pre_upstream_trace.mark_terminal("convert_request_failed");
            return response;
        }
    };
    let convert_ms = convert_started_at.elapsed().as_millis();
    pre_upstream_trace.mark_stage_completed("convert_request", convert_ms);
    if pre_upstream_trace.enabled() {
        tracing::info!(
            request_id = %request_id,
            route = "messages",
            convert_ms,
            mapped_model_id = %conversion_result.model_id,
            session_affinity_present = conversion_result.session_id.is_some(),
            tool_name_map_len = conversion_result.tool_name_map.len(),
            "Anthropic request conversion completed"
        );
    }
    log_converted_tool_schema_diagnostics(
        Some(&request_id),
        "messages",
        &payload.model,
        &conversion_result,
    );

    // 构建 Kiro 请求（profile_arn 由 provider 层根据实际凭据注入）
    let kiro_request = KiroRequest {
        conversation_state: conversion_result.conversation_state,
        additional_model_request_fields: conversion_result.additional_model_request_fields,
        profile_arn: None,
    };

    pre_upstream_trace.mark_stage_started("serialize_kiro_request");
    let serialize_started_at = Instant::now();
    let request_body = match serde_json::to_string(&kiro_request) {
        Ok(body) => body,
        Err(e) => {
            pre_upstream_trace.mark_stage_completed(
                "serialize_kiro_request",
                serialize_started_at.elapsed().as_millis(),
            );
            pre_upstream_trace.mark_terminal("serialize_kiro_request_failed");
            tracing::error!("序列化请求失败: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new(
                    "internal_error",
                    format!("序列化请求失败: {}", e),
                )),
            )
                .into_response();
        }
    };
    let serialize_ms = serialize_started_at.elapsed().as_millis();
    pre_upstream_trace.mark_stage_completed("serialize_kiro_request", serialize_ms);

    tracing::debug!(
        request_id = %request_id,
        request_body_len = request_body.len(),
        "Kiro request body prepared"
    );

    // 检查是否启用了thinking
    let thinking_enabled = request_thinking_enabled(&payload);
    let synthesize_hidden_thinking_signature =
        should_synthesize_hidden_thinking_signature_for_request(
            &payload,
            provider.response_thinking_signature_compat_enabled(),
        );
    let request_weighting = provider.request_weighting_config();

    // 调度使用本地估算，返回给下游的 usage 使用远端计数优先。
    pre_upstream_trace.mark_stage_started("estimate_input_tokens");
    let token_count_started_at = Instant::now();
    let estimated_input_tokens = estimate_input_tokens(&payload);
    let token_count_ms = token_count_started_at.elapsed().as_millis();
    pre_upstream_trace.mark_stage_completed("estimate_input_tokens", token_count_ms);
    if pre_upstream_trace.enabled() {
        tracing::info!(
            request_id = %request_id,
            route = "messages",
            token_count_ms,
            estimated_input_tokens,
            "Anthropic local token estimate completed"
        );
    }
    pre_upstream_trace.mark_stage_started("billing_input_token_count");
    let billing_token_count_started_at = Instant::now();
    let billing_input_tokens = count_billing_input_tokens(&payload).await;
    let billing_token_count_ms = billing_token_count_started_at.elapsed().as_millis();
    pre_upstream_trace.mark_stage_completed("billing_input_token_count", billing_token_count_ms);
    let request_weight =
        payload.request_weight_with_config(&request_weighting, Some(estimated_input_tokens));
    let pre_upstream_ms = handler_started_at.elapsed().as_millis();
    if pre_upstream_trace.enabled() {
        tracing::info!(
            request_id = %request_id,
            route = "messages",
            billing_token_count_ms,
            billing_input_tokens,
            request_weight,
            "Anthropic billing token count and request weighting completed"
        );
    }

    tracing::debug!(
        request_id = %request_id,
        estimated_input_tokens,
        billing_input_tokens,
        request_weight,
        "已完成请求轻重分级"
    );
    if pre_upstream_trace.enabled()
        || body_bytes >= LARGE_ANTHROPIC_REQUEST_WARN_THRESHOLD_BYTES
        || pre_upstream_ms >= SLOW_ANTHROPIC_PRE_UPSTREAM_PHASE_MS
        || convert_ms >= SLOW_ANTHROPIC_PRE_UPSTREAM_PHASE_MS
        || serialize_ms >= SLOW_ANTHROPIC_PRE_UPSTREAM_PHASE_MS
        || token_count_ms >= SLOW_ANTHROPIC_PRE_UPSTREAM_PHASE_MS
        || billing_token_count_ms >= SLOW_ANTHROPIC_PRE_UPSTREAM_PHASE_MS
    {
        tracing::warn!(
            request_id = %request_id,
            route = "messages",
            model = %payload.model,
            stream = payload.stream,
            body_bytes,
            content_length_header = ?content_length_header,
            body_buffer_ms,
            json_parse_ms,
            normalize_ms,
            convert_ms,
            serialize_ms,
            token_count_ms,
            billing_token_count_ms,
            pre_upstream_ms,
            kiro_request_body_bytes = request_body.len(),
            estimated_input_tokens,
            billing_input_tokens,
            request_weight,
            "Anthropic request pre-upstream phases completed"
        );
    }

    let model_id = conversion_result.model_id.clone();
    let session_affinity_key = conversion_result.session_id.clone();
    let tool_name_map = conversion_result.tool_name_map;

    pre_upstream_trace.mark_stage_started("provider_dispatch");
    pre_upstream_trace.mark_dispatching(
        request_body.len(),
        estimated_input_tokens,
        billing_input_tokens,
        request_weight,
        thinking_enabled,
    );
    if payload.stream {
        // 流式响应
        handle_stream_request(
            provider,
            &request_body,
            &payload.model,
            billing_input_tokens,
            thinking_enabled,
            synthesize_hidden_thinking_signature,
            tool_name_map,
            RequestOptions {
                omit_agent_mode_header: probe.omit_agent_mode_header,
                request_id: Some(request_id.clone()),
                model_id: Some(model_id.clone()),
                session_affinity_key: session_affinity_key.clone(),
                credential_group_scope: api_key_context
                    .as_ref()
                    .map(|context| context.credential_group_scope.clone()),
                api_key_id: api_key_context.as_ref().map(|context| context.id.clone()),
                request_weight,
                wait_for_stream_content_start: true,
                stream_thinking_enabled: thinking_enabled,
            },
        )
        .await
    } else {
        // 非流式响应
        handle_non_stream_request(
            provider,
            &request_body,
            &payload.model,
            billing_input_tokens,
            tool_name_map,
            false,
            RequestOptions {
                omit_agent_mode_header: probe.omit_agent_mode_header,
                request_id: Some(request_id),
                model_id: Some(model_id),
                session_affinity_key,
                credential_group_scope: api_key_context
                    .as_ref()
                    .map(|context| context.credential_group_scope.clone()),
                api_key_id: api_key_context.as_ref().map(|context| context.id.clone()),
                request_weight,
                wait_for_stream_content_start: false,
                stream_thinking_enabled: false,
            },
        )
        .await
    }
}

/// 处理流式请求
async fn handle_stream_request(
    provider: std::sync::Arc<crate::kiro::provider::KiroProvider>,
    request_body: &str,
    model: &str,
    input_tokens: i32,
    thinking_enabled: bool,
    synthesize_hidden_thinking_signature: bool,
    tool_name_map: std::collections::HashMap<String, String>,
    request_options: RequestOptions,
) -> Response {
    let request_id = request_options.request_id.clone();
    // 调用 Kiro API（支持多凭据故障转移）
    let response = match provider
        .call_api_stream_with_options(request_body, request_options)
        .await
    {
        Ok(resp) => resp,
        Err(e) => return map_provider_error(e),
    };

    // 创建流处理上下文。普通 /v1 流式路径也延迟 message_start，
    // 以便 Kiro 的 contextUsageEvent 能修正首包 usage.input_tokens。
    let ctx =
        StreamContext::new_with_thinking(model, input_tokens, thinking_enabled, tool_name_map)
            .with_synthetic_hidden_thinking_signature(synthesize_hidden_thinking_signature);

    let usage_recorder = response.usage_recorder();

    // 创建 SSE 流
    let stream = create_sse_stream(response, ctx, request_id, usage_recorder);

    // 返回 SSE 响应
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        .header(header::CONNECTION, "keep-alive")
        .body(Body::from_stream(stream))
        .unwrap()
}

/// Ping 事件间隔（25秒）
const PING_INTERVAL_SECS: u64 = 25;

/// 创建 ping 事件的 SSE 字符串
fn create_ping_sse() -> Bytes {
    Bytes::from("event: ping\ndata: {\"type\": \"ping\"}\n\n")
}

fn create_stream_error_sse(message: impl AsRef<str>) -> Bytes {
    let event = SseEvent::new(
        "error",
        json!({
            "type": "error",
            "error": {
                "type": "api_error",
                "message": message.as_ref()
            }
        }),
    );
    Bytes::from(event.to_sse_string())
}

fn log_kiro_event_parse_error(
    context: &'static str,
    request_id: Option<&str>,
    frame: &Frame,
    error: &ParseError,
) {
    let payload_text = String::from_utf8_lossy(&frame.payload);
    tracing::warn!(
        context,
        request_id = request_id.unwrap_or("unknown"),
        message_type = frame.message_type().unwrap_or("unknown"),
        event_type = frame.event_type().unwrap_or("unknown"),
        payload_len = frame.payload.len(),
        payload_excerpt = %summarize_text_for_log(&payload_text, 240),
        error = %error,
        "Kiro Event Stream frame payload 解析失败"
    );
}

fn decode_error_sse_if_fatal(error: &ParseError, decoder: &EventStreamDecoder) -> Option<Bytes> {
    if !error.is_fatal_stream_error() {
        return None;
    }

    tracing::error!(
        error = %error,
        decoder_buffer_len = decoder.buffer_len(),
        decoder_error_count = decoder.error_count(),
        decoder_bytes_skipped = decoder.bytes_skipped(),
        "Kiro Event Stream 解码进入不可恢复状态"
    );
    Some(create_stream_error_sse(
        "Upstream stream could not be decoded reliably.",
    ))
}

/// 创建 SSE 事件流
fn create_sse_stream(
    response: crate::kiro::provider::ManagedResponse,
    ctx: StreamContext,
    request_id: Option<String>,
    usage_recorder: Option<ResponseUsageRecorder>,
) -> impl Stream<Item = Result<Bytes, Infallible>> {
    // 处理 Kiro 响应流，同时每25秒发送 ping 保活。
    // message_start 会延迟到 contextUsageEvent 或首个可见内容事件之后发送，
    // 这样 Claude Code 能在首包 usage 中看到真实上下文占用。
    let body_stream = response.into_bytes_stream();

    let processing_stream = stream::unfold(
        (
            body_stream,
            ctx,
            EventStreamDecoder::new(),
            false,
            false,
            interval_at(
                Instant::now() + Duration::from_secs(PING_INTERVAL_SECS),
                Duration::from_secs(PING_INTERVAL_SECS),
            ),
            false,
            request_id,
            usage_recorder,
        ),
        |(
            mut body_stream,
            mut ctx,
            mut decoder,
            finished,
            message_started,
            mut ping_interval,
            can_ping,
            request_id,
            usage_recorder,
        )| async move {
            if finished {
                return None;
            }

            // 使用 select! 同时等待数据和 ping 定时器
            tokio::select! {
                // 处理数据流
                chunk_result = body_stream.next() => {
                    match chunk_result {
                        Some(Ok(chunk)) => {
                            // 解码事件
                            if let Err(e) = decoder.feed(&chunk) {
                                tracing::warn!(
                                    error = %e,
                                    chunk_len = chunk.len(),
                                    decoder_buffer_len = decoder.buffer_len(),
                                    "Kiro Event Stream 缓冲失败"
                                );
                                if let Some(error_bytes) = decode_error_sse_if_fatal(&e, &decoder) {
                                    return Some((stream::iter(vec![Ok(error_bytes)]), (body_stream, ctx, decoder, true, message_started, ping_interval, can_ping, request_id, usage_recorder)));
                                }
                            }

                            let mut events = Vec::new();
                            let mut observed_non_error_event = false;
                            loop {
                                match decoder.decode() {
                                    Ok(Some(frame)) => {
                                        match Event::from_frame(frame.clone()) {
                                            Ok(event) => {
                                                if !matches!(
                                                    &event,
                                                    Event::Error { .. } | Event::Exception { .. }
                                                ) {
                                                    observed_non_error_event = true;
                                                }
                                                let sse_events = ctx.process_kiro_event(&event);
                                                events.extend(sse_events);
                                            }
                                            Err(e) => {
                                                log_kiro_event_parse_error(
                                                    "stream",
                                                    request_id.as_deref(),
                                                    &frame,
                                                    &e,
                                                );
                                            }
                                        }
                                    }
                                    Ok(None) => break,
                                    Err(e) => {
                                        tracing::warn!(
                                            error = %e,
                                            decoder_buffer_len = decoder.buffer_len(),
                                            decoder_error_count = decoder.error_count(),
                                            decoder_bytes_skipped = decoder.bytes_skipped(),
                                            "Kiro Event Stream frame 解码失败"
                                        );
                                        if let Some(error_bytes) = decode_error_sse_if_fatal(&e, &decoder) {
                                            return Some((stream::iter(vec![Ok(error_bytes)]), (body_stream, ctx, decoder, true, message_started, ping_interval, can_ping, request_id, usage_recorder)));
                                        }
                                    }
                                }
                            }

                            let mut message_started = message_started;
                            let events = prepare_stream_events_for_emit(
                                &mut ctx,
                                &mut message_started,
                                events,
                                observed_non_error_event,
                            );
                            let next_can_ping = can_ping
                                || events.iter().any(|event| {
                                    event.event == "message_start"
                                        || event.event == "content_block_start"
                                });

                            // 转换为 SSE 字节流
                            let bytes: Vec<Result<Bytes, Infallible>> = events
                                .into_iter()
                                .map(|e| Ok(Bytes::from(e.to_sse_string())))
                                .collect();

                            Some((stream::iter(bytes), (body_stream, ctx, decoder, false, message_started, ping_interval, next_can_ping, request_id, usage_recorder)))
                        }
                        Some(Err(e)) => {
                            tracing::error!(
                                request_id = %request_id.as_deref().unwrap_or(""),
                                error = %e,
                                "读取响应流失败"
                            );
                            // 流读取失败时不要伪装成正常 message_stop。
                            let bytes: Vec<Result<Bytes, Infallible>> = vec![Ok(create_stream_error_sse(
                                "Upstream stream ended with a transport error.",
                            ))];
                            Some((stream::iter(bytes), (body_stream, ctx, decoder, true, message_started, ping_interval, can_ping, request_id, usage_recorder)))
                        }
                        None => {
                            // 流结束，发送最终事件
                            let mut message_started = message_started;
                            let final_events = finalize_stream_events(&mut ctx, &mut message_started);
                            if let Some(recorder) = &usage_recorder {
                                let (input_tokens, output_tokens, token_source) = ctx.final_usage();
                                recorder.record_complete(input_tokens, output_tokens, token_source);
                            }
                            let bytes: Vec<Result<Bytes, Infallible>> = final_events
                                .into_iter()
                                .map(|e| Ok(Bytes::from(e.to_sse_string())))
                                .collect();
                            Some((stream::iter(bytes), (body_stream, ctx, decoder, true, message_started, ping_interval, can_ping, request_id, usage_recorder)))
                        }
                    }
                }
                // 发送 ping 保活
                _ = ping_interval.tick(), if can_ping => {
                    tracing::trace!("发送 ping 保活事件");
                    let bytes: Vec<Result<Bytes, Infallible>> = vec![Ok(create_ping_sse())];
                    Some((stream::iter(bytes), (body_stream, ctx, decoder, false, message_started, ping_interval, can_ping, request_id, usage_recorder)))
                }
            }
        },
    )
    .flatten();

    processing_stream
}

fn prepare_stream_events_for_emit(
    ctx: &mut StreamContext,
    message_started: &mut bool,
    mut events: Vec<SseEvent>,
    observed_non_error_event: bool,
) -> Vec<SseEvent> {
    let should_start_message = !*message_started
        && (ctx.context_input_tokens.is_some() || !events.is_empty() || observed_non_error_event);
    if !*message_started && !should_start_message {
        return Vec::new();
    }

    let mut output = Vec::new();
    if !*message_started {
        output.extend(ctx.generate_initial_events());
        *message_started = true;
    }
    output.append(&mut events);
    output
}

fn finalize_stream_events(ctx: &mut StreamContext, message_started: &mut bool) -> Vec<SseEvent> {
    let mut events = Vec::new();
    if !*message_started {
        events.extend(ctx.generate_initial_events());
        *message_started = true;
    }
    events.extend(ctx.generate_final_events());
    events
}

use super::converter::get_context_window_size;

pub(crate) async fn execute_non_stream_round(
    provider: Arc<crate::kiro::provider::KiroProvider>,
    payload: &MessagesRequest,
    conversion_runtime: Arc<ConversionRuntime>,
    probe: UpstreamProbe,
    request_id: Option<String>,
    api_key_context: Option<ApiKeyAuthContext>,
) -> Result<NonStreamMessageResponse, Response> {
    let conversion_result = convert_request_on_runtime(
        conversion_runtime,
        payload,
        probe.clone(),
        request_id.as_deref(),
        "non_stream_round",
        None,
    )
    .await?;
    log_converted_tool_schema_diagnostics(
        request_id.as_deref(),
        "non_stream_round",
        &payload.model,
        &conversion_result,
    );

    let kiro_request = KiroRequest {
        conversation_state: conversion_result.conversation_state,
        additional_model_request_fields: conversion_result.additional_model_request_fields,
        profile_arn: None,
    };

    let request_body = match serde_json::to_string(&kiro_request) {
        Ok(body) => body,
        Err(e) => {
            tracing::error!("序列化请求失败: {}", e);
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new(
                    "internal_error",
                    format!("序列化请求失败: {}", e),
                )),
            )
                .into_response());
        }
    };

    let estimated_input_tokens = estimate_input_tokens_with_structured_instruction(payload);
    let billing_input_tokens =
        count_billing_input_tokens_with_structured_instruction(payload).await;
    let request_weighting = provider.request_weighting_config();
    let request_weight =
        payload.request_weight_with_config(&request_weighting, Some(estimated_input_tokens));

    execute_non_stream_request_body(
        provider,
        &request_body,
        &payload.model,
        billing_input_tokens,
        conversion_result.tool_name_map,
        false,
        RequestOptions {
            omit_agent_mode_header: probe.omit_agent_mode_header,
            request_id,
            model_id: Some(conversion_result.model_id),
            session_affinity_key: conversion_result.session_id,
            credential_group_scope: api_key_context
                .as_ref()
                .map(|context| context.credential_group_scope.clone()),
            api_key_id: api_key_context.as_ref().map(|context| context.id.clone()),
            request_weight,
            wait_for_stream_content_start: false,
            stream_thinking_enabled: false,
        },
    )
    .await
}

fn tokens_to_i32(tokens: u64) -> i32 {
    tokens.min(i32::MAX as u64) as i32
}

fn estimate_input_tokens(payload: &MessagesRequest) -> i32 {
    tokens_to_i32(token::count_all_tokens_borrowed(
        &payload.model,
        payload.system.as_deref(),
        &payload.messages,
        payload.tools.as_deref(),
    ))
}

async fn count_billing_input_tokens(payload: &MessagesRequest) -> i32 {
    tokens_to_i32(
        token::count_all_tokens_remote_or_local(
            &payload.model,
            payload.system.as_deref(),
            &payload.messages,
            payload.tools.as_deref(),
        )
        .await,
    )
}

fn estimate_input_tokens_with_structured_instruction(payload: &MessagesRequest) -> i32 {
    let base = estimate_input_tokens(payload);
    let output = structured_outputs::json_schema_output(payload);
    base.saturating_add(structured_outputs::estimate_instruction_tokens(
        output.as_ref(),
    ))
}

async fn count_billing_input_tokens_with_structured_instruction(payload: &MessagesRequest) -> i32 {
    let base = count_billing_input_tokens(payload).await;
    let output = structured_outputs::json_schema_output(payload);
    base.saturating_add(structured_outputs::estimate_instruction_tokens(
        output.as_ref(),
    ))
}

async fn handle_structured_non_stream_request(
    provider: Arc<crate::kiro::provider::KiroProvider>,
    payload: &MessagesRequest,
    conversion_runtime: Arc<ConversionRuntime>,
    probe: UpstreamProbe,
    request_id: String,
    output: JsonSchemaOutput,
    api_key_context: Option<ApiKeyAuthContext>,
) -> Response {
    match execute_structured_non_stream(
        provider,
        payload,
        conversion_runtime,
        probe,
        request_id,
        output,
        api_key_context,
    )
    .await
    {
        Ok(result) => (StatusCode::OK, Json(result.body)).into_response(),
        Err(response) => response,
    }
}

async fn handle_structured_stream_request(
    provider: Arc<crate::kiro::provider::KiroProvider>,
    payload: MessagesRequest,
    conversion_runtime: Arc<ConversionRuntime>,
    probe: UpstreamProbe,
    request_id: String,
    output: JsonSchemaOutput,
    api_key_context: Option<ApiKeyAuthContext>,
) -> Response {
    match execute_structured_non_stream(
        provider,
        &payload,
        conversion_runtime,
        probe,
        request_id,
        output,
        api_key_context,
    )
    .await
    {
        Ok(result) => {
            let events = structured_non_stream_to_sse_events(result);
            let stream = stream::iter(
                events
                    .into_iter()
                    .map(|event| Ok::<Bytes, Infallible>(Bytes::from(event.to_sse_string()))),
            );
            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "text/event-stream")
                .header(header::CACHE_CONTROL, "no-cache")
                .header(header::CONNECTION, "keep-alive")
                .body(Body::from_stream(stream))
                .unwrap()
        }
        Err(response) => response,
    }
}

async fn execute_structured_non_stream(
    provider: Arc<crate::kiro::provider::KiroProvider>,
    payload: &MessagesRequest,
    conversion_runtime: Arc<ConversionRuntime>,
    probe: UpstreamProbe,
    request_id: String,
    output: JsonSchemaOutput,
    api_key_context: Option<ApiKeyAuthContext>,
) -> Result<NonStreamMessageResponse, Response> {
    let mut attempt_payload = payload.clone();
    let structured_started_at = Instant::now();
    let schema_stats = structured_outputs::schema_stats(&output.schema);
    tracing::info!(
        request_id = %request_id,
        stream = payload.stream,
        schema_bytes = schema_stats.bytes,
        schema_depth = schema_stats.max_depth,
        schema_nodes = schema_stats.nodes,
        schema_combinator_branches = schema_stats.combinator_branches,
        schema_properties = schema_stats.properties,
        max_retries = structured_outputs::MAX_JSON_SCHEMA_RETRIES,
        "Structured output request routed through JSON Schema compatibility path"
    );
    for attempt in 0..=structured_outputs::MAX_JSON_SCHEMA_RETRIES {
        let result = execute_non_stream_round(
            provider.clone(),
            &attempt_payload,
            conversion_runtime.clone(),
            probe.clone(),
            Some(request_id.clone()),
            api_key_context.clone(),
        )
        .await?;
        let previous_text = structured_outputs::collect_text_content(&result.content);
        let previous_text_chars = previous_text.chars().count();
        match coerce_structured_response(result, &output) {
            Ok(result) => {
                let json_text_chars = result
                    .content
                    .first()
                    .and_then(|block| block.get("text"))
                    .and_then(serde_json::Value::as_str)
                    .map(str::len)
                    .unwrap_or_default();
                tracing::info!(
                    request_id = %request_id,
                    stream = payload.stream,
                    attempts = attempt + 1,
                    retried = attempt > 0,
                    elapsed_ms = structured_started_at.elapsed().as_millis(),
                    raw_text_chars = previous_text_chars,
                    json_text_chars,
                    input_tokens = result.input_tokens,
                    output_tokens = result.output_tokens,
                    "Structured output request satisfied JSON Schema"
                );
                return Ok(result);
            }
            Err(err) if attempt < structured_outputs::MAX_JSON_SCHEMA_RETRIES => {
                tracing::warn!(
                    request_id = %request_id,
                    attempt,
                    error = %err,
                    previous_text_chars,
                    "Structured output response failed validation; retrying"
                );
                attempt_payload =
                    structured_outputs::build_retry_payload(payload, &previous_text, &err);
            }
            Err(err) => {
                tracing::warn!(
                    request_id = %request_id,
                    attempt,
                    error = %err,
                    previous_text_chars,
                    elapsed_ms = structured_started_at.elapsed().as_millis(),
                    "Structured output response failed validation after retries"
                );
                return Err((
                    StatusCode::BAD_GATEWAY,
                    Json(ErrorResponse::new(
                        "api_error",
                        format!(
                            "Upstream response did not satisfy output_config.format.json_schema: {err}"
                        ),
                    )),
                )
                    .into_response());
            }
        }
    }

    unreachable!("structured output retry loop always returns")
}

fn coerce_structured_response(
    mut result: NonStreamMessageResponse,
    output: &JsonSchemaOutput,
) -> Result<NonStreamMessageResponse, StructuredOutputError> {
    let text = structured_outputs::collect_text_content(&result.content);
    let value = structured_outputs::extract_json_value(&text)?;
    structured_outputs::validate_instance(output, &value)?;
    let text = serde_json::to_string(&value)
        .map_err(|err| StructuredOutputError::InvalidJson(err.to_string()))?;
    let content = vec![json!({
        "type": "text",
        "text": text,
    })];
    let output_tokens = token::estimate_output_tokens(&content);

    result.content = content.clone();
    result.stop_reason = "end_turn".to_string();
    result.output_tokens = output_tokens;
    result.body["content"] = serde_json::Value::Array(content);
    result.body["stop_reason"] = json!("end_turn");
    result.body["usage"]["output_tokens"] = json!(output_tokens);
    Ok(result)
}

fn structured_non_stream_to_sse_events(result: NonStreamMessageResponse) -> Vec<SseEvent> {
    let response_id = result
        .body
        .get("id")
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| format!("msg_{}", Uuid::new_v4().to_string().replace('-', "")));
    let model = result
        .body
        .get("model")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .to_string();
    let text = result
        .content
        .first()
        .and_then(|block| block.get("text"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .to_string();

    vec![
        SseEvent::new(
            "message_start",
            json!({
                "type": "message_start",
                "message": {
                    "id": response_id,
                    "type": "message",
                    "role": "assistant",
                    "content": [],
                    "model": model,
                    "stop_reason": null,
                    "stop_sequence": null,
                    "usage": {
                        "input_tokens": result.input_tokens,
                        "output_tokens": 1
                    }
                }
            }),
        ),
        SseEvent::new(
            "content_block_start",
            json!({
                "type": "content_block_start",
                "index": 0,
                "content_block": {
                    "type": "text",
                    "text": ""
                }
            }),
        ),
        SseEvent::new(
            "content_block_delta",
            json!({
                "type": "content_block_delta",
                "index": 0,
                "delta": {
                    "type": "text_delta",
                    "text": text
                }
            }),
        ),
        SseEvent::new(
            "content_block_stop",
            json!({
                "type": "content_block_stop",
                "index": 0
            }),
        ),
        SseEvent::new(
            "message_delta",
            json!({
                "type": "message_delta",
                "delta": {
                    "stop_reason": result.stop_reason,
                    "stop_sequence": null
                },
                "usage": {
                    "output_tokens": result.output_tokens
                }
            }),
        ),
        SseEvent::new("message_stop", json!({"type": "message_stop"})),
    ]
}

/// 处理非流式请求
async fn handle_non_stream_request(
    provider: std::sync::Arc<crate::kiro::provider::KiroProvider>,
    request_body: &str,
    model: &str,
    input_tokens: i32,
    tool_name_map: std::collections::HashMap<String, String>,
    prefer_context_input_tokens: bool,
    request_options: RequestOptions,
) -> Response {
    match execute_non_stream_request_body(
        provider,
        request_body,
        model,
        input_tokens,
        tool_name_map,
        prefer_context_input_tokens,
        request_options,
    )
    .await
    {
        Ok(result) => (StatusCode::OK, Json(result.body)).into_response(),
        Err(resp) => resp,
    }
}

async fn execute_non_stream_request_body(
    provider: std::sync::Arc<crate::kiro::provider::KiroProvider>,
    request_body: &str,
    model: &str,
    input_tokens: i32,
    tool_name_map: std::collections::HashMap<String, String>,
    prefer_context_input_tokens: bool,
    request_options: RequestOptions,
) -> Result<NonStreamMessageResponse, Response> {
    let request_id_for_log = request_options.request_id.clone();

    // 调用 Kiro API（支持多凭据故障转移）
    let response = match provider
        .call_api_with_options(request_body, request_options)
        .await
    {
        Ok(resp) => resp,
        Err(e) => return Err(map_provider_error(e)),
    };
    let usage_recorder = response.usage_recorder();

    // 读取响应体
    let body_bytes = match response.bytes().await {
        Ok(bytes) => bytes,
        Err(e) => {
            if let Some(request_id) = request_id_for_log.as_deref() {
                tracing::error!(request_id = %request_id, error = %e, "读取响应体失败");
            } else {
                tracing::error!(error = %e, "读取响应体失败");
            }
            return Err((
                StatusCode::BAD_GATEWAY,
                Json(ErrorResponse::new(
                    "api_error",
                    format!("读取响应失败: {}", e),
                )),
            )
                .into_response());
        }
    };

    let result = decode_non_stream_message(
        &body_bytes,
        model,
        input_tokens,
        tool_name_map,
        prefer_context_input_tokens,
    );
    if let Some(recorder) = usage_recorder {
        recorder.record_complete(
            result.input_tokens,
            result.output_tokens,
            result.input_token_source,
        );
    }

    Ok(result)
}

fn decode_non_stream_message(
    body_bytes: &[u8],
    model: &str,
    input_tokens: i32,
    tool_name_map: std::collections::HashMap<String, String>,
    prefer_context_input_tokens: bool,
) -> NonStreamMessageResponse {
    // 解析事件流
    let mut decoder = EventStreamDecoder::new();
    if let Err(e) = decoder.feed(&body_bytes) {
        tracing::warn!("缓冲区溢出: {}", e);
    }

    let mut text_content = String::new();
    let mut tool_uses: Vec<serde_json::Value> = Vec::new();
    let mut has_tool_use = false;
    let mut stop_reason = "end_turn".to_string();
    // 从 contextUsageEvent 计算的实际输入 tokens
    let mut context_input_tokens: Option<i32> = None;

    // 收集工具调用的增量 JSON
    let mut tool_json_buffers: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();

    for result in decoder.decode_iter() {
        match result {
            Ok(frame) => {
                match Event::from_frame(frame.clone()) {
                    Ok(event) => match event {
                        Event::AssistantResponse(resp) => {
                            text_content.push_str(&resp.content);
                        }
                        Event::ToolUse(tool_use) => {
                            has_tool_use = true;

                            // 累积工具的 JSON 输入
                            let buffer = tool_json_buffers
                                .entry(tool_use.tool_use_id.clone())
                                .or_insert_with(String::new);
                            buffer.push_str(&tool_use.input);

                            // 如果是完整的工具调用，添加到列表
                            if tool_use.stop {
                                let input: serde_json::Value = if buffer.is_empty() {
                                    serde_json::json!({})
                                } else {
                                    serde_json::from_str(buffer).unwrap_or_else(|e| {
                                        tracing::warn!(
                                            "工具输入 JSON 解析失败: {}, tool_use_id: {}",
                                            e,
                                            tool_use.tool_use_id
                                        );
                                        serde_json::json!({})
                                    })
                                };

                                let original_name = tool_name_map
                                    .get(&tool_use.name)
                                    .cloned()
                                    .unwrap_or_else(|| tool_use.name.clone());

                                tool_uses.push(json!({
                                    "type": "tool_use",
                                    "id": tool_use.tool_use_id,
                                    "name": original_name,
                                    "input": input
                                }));
                            }
                        }
                        Event::ContextUsage(context_usage) => {
                            // 从上下文使用百分比计算实际的 input_tokens
                            let window_size = get_context_window_size(model);
                            let actual_input_tokens =
                                (context_usage.context_usage_percentage * (window_size as f64)
                                    / 100.0) as i32;
                            context_input_tokens = Some(actual_input_tokens);
                            // 上下文使用量达到 100% 时，设置 stop_reason 为 model_context_window_exceeded
                            if context_usage.context_usage_percentage >= 100.0 {
                                stop_reason = "model_context_window_exceeded".to_string();
                            }
                            tracing::debug!(
                                "收到 contextUsageEvent: {}%, 计算 input_tokens: {}",
                                context_usage.context_usage_percentage,
                                actual_input_tokens
                            );
                        }
                        Event::Exception { exception_type, .. } => {
                            if exception_type == "ContentLengthExceededException" {
                                stop_reason = "max_tokens".to_string();
                            }
                        }
                        _ => {}
                    },
                    Err(e) => {
                        log_kiro_event_parse_error("non_stream", None, &frame, &e);
                    }
                }
            }
            Err(e) => {
                tracing::warn!("解码事件失败: {}", e);
            }
        }
    }

    // 确定 stop_reason
    if has_tool_use && stop_reason == "end_turn" {
        stop_reason = "tool_use".to_string();
    }

    // 构建响应内容
    let mut content: Vec<serde_json::Value> = Vec::new();
    let response_id = format!("msg_{}", Uuid::new_v4().to_string().replace('-', ""));

    if !text_content.is_empty() {
        if let Some((thinking, remaining_text)) = extract_thinking_and_text(&text_content) {
            let signature = sign_thinking_block(0, &thinking, model);
            content.push(json!({
                "type": "thinking",
                "thinking": thinking,
                "signature": signature
            }));
            if !remaining_text.is_empty() {
                content.push(json!({
                    "type": "text",
                    "text": remaining_text
                }));
            }
        } else {
            content.push(json!({
                "type": "text",
                "text": text_content
            }));
        }
    }

    content.extend(tool_uses);

    // 估算输出 tokens
    let output_tokens = token::estimate_output_tokens(&content);

    // 普通 Anthropic 路由保留远端 count_tokens 结果作为 usage；CC 兼容路由仍可选择
    // 用 contextUsageEvent 修正首包/非流式 usage。
    let final_input_tokens = if prefer_context_input_tokens {
        context_input_tokens.unwrap_or(input_tokens)
    } else {
        input_tokens
    };
    let input_token_source = if prefer_context_input_tokens && context_input_tokens.is_some() {
        "context_usage"
    } else {
        "billing_input_tokens"
    };

    // 构建 Anthropic 响应
    let response_body = json!({
        "id": response_id,
        "type": "message",
        "role": "assistant",
        "content": content.clone(),
        "model": model,
        "stop_reason": stop_reason.clone(),
        "stop_sequence": null,
        "usage": {
            "input_tokens": final_input_tokens,
            "output_tokens": output_tokens
        }
    });

    NonStreamMessageResponse {
        body: response_body,
        content,
        stop_reason,
        input_tokens: final_input_tokens,
        output_tokens,
        input_token_source,
    }
}

/// 检测模型名是否包含 "thinking" 后缀，若包含则覆写 thinking 配置
///
/// - Opus 4.7/4.8：覆写为 adaptive 类型，并默认显示 summarized thinking
/// - 其他模型：覆写为 enabled 类型
/// - budget_tokens 固定为 20000
fn override_thinking_from_model_name(payload: &mut MessagesRequest) {
    let model_lower = payload.model.to_lowercase();
    if !model_lower.contains("thinking") {
        return;
    }

    let is_adaptive_opus = is_adaptive_opus_model(&payload.model);

    let thinking_type = if is_adaptive_opus {
        "adaptive"
    } else {
        "enabled"
    };

    tracing::info!(
        model = %payload.model,
        thinking_type = thinking_type,
        "模型名包含 thinking 后缀，覆写 thinking 配置"
    );

    payload.thinking = Some(Thinking {
        thinking_type: thinking_type.to_string(),
        display: if is_adaptive_opus {
            Some("summarized".to_string())
        } else {
            None
        },
        budget_tokens: 20000,
    });

    if is_adaptive_opus {
        payload.output_config = Some(OutputConfig {
            effort: "high".to_string(),
            format: None,
            effort_explicit: false,
        });
    }
}

fn is_adaptive_opus_model(model: &str) -> bool {
    let model_lower = model.to_lowercase();
    model_lower.contains("opus")
        && (model_lower.contains("4-8")
            || model_lower.contains("4.8")
            || model_lower.contains("4-7")
            || model_lower.contains("4.7"))
}

fn system_contains_thinking_tags(system: Option<&Vec<super::types::SystemMessage>>) -> bool {
    system.is_some_and(|messages| {
        messages.iter().any(|message| {
            message.text.contains("<thinking_mode>")
                || message.text.contains("<max_thinking_length>")
                || message.text.contains("<thinking_effort>")
        })
    })
}

fn request_thinking_enabled(payload: &MessagesRequest) -> bool {
    payload
        .thinking
        .as_ref()
        .map(|t| t.is_enabled())
        .unwrap_or(false)
        || system_contains_thinking_tags(payload.system.as_ref())
}

fn should_synthesize_hidden_thinking_signature_for_request(
    payload: &MessagesRequest,
    response_thinking_signature_compat_enabled: bool,
) -> bool {
    response_thinking_signature_compat_enabled && request_thinking_enabled(payload)
}

/// POST /v1/messages/count_tokens
///
/// 计算消息的 token 数量
pub async fn count_tokens(payload: AnthropicJson<CountTokensRequest>) -> impl IntoResponse {
    let body_bytes = payload.body_len();
    let content_length_header = payload.content_length_header();
    tracing::info!(
        model = %payload.model,
        body_bytes,
        content_length_header = ?content_length_header,
        message_count = %payload.messages.len(),
        "Received POST /v1/messages/count_tokens request"
    );
    if body_bytes >= LARGE_ANTHROPIC_REQUEST_WARN_THRESHOLD_BYTES {
        tracing::warn!(
            model = %payload.model,
            body_bytes,
            content_length_header = ?content_length_header,
            "Large Anthropic count_tokens request body observed"
        );
    }
    let payload = payload.into_inner();

    let total_tokens = token::count_all_tokens_remote_or_local(
        &payload.model,
        payload.system.as_deref(),
        &payload.messages,
        payload.tools.as_deref(),
    )
    .await as i32;

    Json(CountTokensResponse {
        input_tokens: total_tokens.max(1) as i32,
    })
}

/// POST /cc/v1/messages
///
/// Claude Code 兼容端点，与 /v1/messages 的区别在于：
/// - 流式响应会等待 kiro 端返回 contextUsageEvent 后再发送 message_start
/// - message_start 中的 input_tokens 是从 contextUsageEvent 计算的准确值
pub async fn post_messages_cc(
    State(state): State<AppState>,
    api_key_context: Option<Extension<ApiKeyAuthContext>>,
    headers: HeaderMap,
    payload: AnthropicJson<MessagesRequest>,
) -> Response {
    let handler_started_at = Instant::now();
    let body_bytes = payload.body_len();
    let content_length_header = payload.content_length_header();
    let body_buffer_ms = payload.body_buffer_ms();
    let json_parse_ms = payload.json_parse_ms();
    let request_id = request_id_from_headers(&headers);
    tracing::info!(
        request_id = %request_id,
        model = %payload.model,
        max_tokens = %payload.max_tokens,
        stream = %payload.stream,
        body_bytes,
        content_length_header = ?content_length_header,
        body_buffer_ms,
        json_parse_ms,
        message_count = %payload.messages.len(),
        "Received POST /cc/v1/messages request"
    );
    if body_bytes >= LARGE_ANTHROPIC_REQUEST_WARN_THRESHOLD_BYTES {
        tracing::warn!(
            request_id = %request_id,
            model = %payload.model,
            body_bytes,
            content_length_header = ?content_length_header,
            body_buffer_ms,
            json_parse_ms,
            "Large Claude Code compatible request body observed"
        );
    }
    let mut payload = payload.into_inner();

    // 检查 KiroProvider 是否可用
    let provider = match &state.kiro_provider {
        Some(p) => p.clone(),
        None => {
            tracing::error!("KiroProvider 未配置");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse::new(
                    "service_unavailable",
                    "Kiro API provider not configured",
                )),
            )
                .into_response();
        }
    };

    let api_key_context = api_key_context.map(|Extension(context)| context);
    let probe = parse_upstream_probe(&headers);
    if probe.is_enabled() {
        tracing::info!(request_id = %request_id, ?probe, "启用上游裸探针选项");
    }

    // 检测模型名是否包含 "thinking" 后缀，若包含则覆写 thinking 配置
    override_thinking_from_model_name(&mut payload);

    let thinking_signature_validation_mode = provider.thinking_signature_validation_mode();
    let signature_validation_started_at = Instant::now();
    let signature_stats = match validate_thinking_signature_payload(
        &mut payload,
        &request_id,
        thinking_signature_validation_mode,
    ) {
        Ok(stats) => stats,
        Err(response) => {
            tracing::warn!(
                request_id = %request_id,
                route = "cc_messages",
                model = %payload.model,
                validation_mode = thinking_signature_validation_mode.as_str(),
                stream = payload.stream,
                body_bytes,
                content_length_header = ?content_length_header,
                message_count = payload.messages.len(),
                signature_validation_ms = signature_validation_started_at.elapsed().as_millis(),
                "Anthropic request rejected during thinking signature validation"
            );
            return response;
        }
    };
    let signature_validation_ms = signature_validation_started_at.elapsed().as_millis();
    let mut pre_upstream_trace = PreUpstreamTrace::new(
        should_trace_anthropic_pre_upstream(body_bytes, payload.messages.len()),
        &request_id,
        "cc_messages",
        &payload.model,
        payload.stream,
        body_bytes,
        content_length_header,
        payload.messages.len(),
    );
    pre_upstream_trace
        .mark_signature_validation_completed(signature_validation_ms, signature_stats);

    pre_upstream_trace.mark_stage_started("normalize_multimodal");
    let normalize_started_at = Instant::now();
    if let Err(response) =
        normalize_multimodal_payload(&mut payload, &request_id, "cc_messages", body_bytes).await
    {
        pre_upstream_trace.mark_terminal("normalize_multimodal_failed");
        return response;
    }
    let normalize_ms = normalize_started_at.elapsed().as_millis();
    pre_upstream_trace.mark_stage_completed("normalize_multimodal", normalize_ms);

    pre_upstream_trace.mark_stage_started("structured_output_detection");
    let structured_output_started_at = Instant::now();
    let structured_output = match json_schema_output_or_response(&payload, &request_id) {
        Ok(output) => output,
        Err(response) => {
            pre_upstream_trace.mark_terminal("structured_output_schema_rejected");
            return response;
        }
    };
    let structured_output_ms = structured_output_started_at.elapsed().as_millis();
    pre_upstream_trace.mark_stage_completed("structured_output_detection", structured_output_ms);
    if pre_upstream_trace.enabled() {
        tracing::info!(
            request_id = %request_id,
            route = "cc_messages",
            structured_output = structured_output.is_some(),
            structured_output_ms,
            "Anthropic structured output detection completed"
        );
    }
    if webfetch::has_server_web_tool(&payload) {
        tracing::info!(request_id = %request_id, "检测到 server-side Web 工具，路由到统一 Web 工具处理");

        pre_upstream_trace.mark_stage_started("server_web_tool_token_count");
        let token_count_started_at = Instant::now();
        let input_tokens = if structured_output.is_some() {
            count_billing_input_tokens_with_structured_instruction(&payload).await
        } else {
            count_billing_input_tokens(&payload).await
        };
        let token_count_ms = token_count_started_at.elapsed().as_millis();
        pre_upstream_trace.mark_stage_completed("server_web_tool_token_count", token_count_ms);
        if pre_upstream_trace.enabled() {
            tracing::info!(
                request_id = %request_id,
                route = "cc_server_web_tool",
                token_count_ms,
                input_tokens,
                structured_output = structured_output.is_some(),
                "Anthropic server web tool token count completed"
            );
        }

        pre_upstream_trace.mark_terminal("server_web_tool_handoff");
        return webfetch::handle_server_web_tool_request(
            provider,
            &payload,
            input_tokens,
            state.conversion_runtime.clone(),
            probe.clone(),
            &request_id,
            structured_output,
            api_key_context.clone(),
        )
        .await;
    }
    if let Some(output) = structured_output.clone() {
        if payload.stream {
            pre_upstream_trace.mark_terminal("structured_output_stream_handoff");
            return handle_structured_stream_request(
                provider,
                payload,
                state.conversion_runtime.clone(),
                probe.clone(),
                request_id.clone(),
                output,
                api_key_context.clone(),
            )
            .await;
        }
        pre_upstream_trace.mark_terminal("structured_output_non_stream_handoff");
        return handle_structured_non_stream_request(
            provider,
            &payload,
            state.conversion_runtime.clone(),
            probe.clone(),
            request_id,
            output,
            api_key_context.clone(),
        )
        .await;
    }

    // 转换请求
    pre_upstream_trace.mark_stage_started("convert_request");
    let convert_started_at = Instant::now();
    let conversion_result = match convert_request_on_runtime(
        state.conversion_runtime.clone(),
        &payload,
        probe.clone(),
        Some(&request_id),
        "cc_messages",
        Some(body_bytes),
    )
    .await
    {
        Ok(result) => result,
        Err(response) => {
            pre_upstream_trace
                .mark_stage_completed("convert_request", convert_started_at.elapsed().as_millis());
            pre_upstream_trace.mark_terminal("convert_request_failed");
            return response;
        }
    };
    let convert_ms = convert_started_at.elapsed().as_millis();
    pre_upstream_trace.mark_stage_completed("convert_request", convert_ms);
    if pre_upstream_trace.enabled() {
        tracing::info!(
            request_id = %request_id,
            route = "cc_messages",
            convert_ms,
            mapped_model_id = %conversion_result.model_id,
            session_affinity_present = conversion_result.session_id.is_some(),
            tool_name_map_len = conversion_result.tool_name_map.len(),
            "Anthropic request conversion completed"
        );
    }
    log_converted_tool_schema_diagnostics(
        Some(&request_id),
        "cc_messages",
        &payload.model,
        &conversion_result,
    );

    // 构建 Kiro 请求（profile_arn 由 provider 层根据实际凭据注入）
    let kiro_request = KiroRequest {
        conversation_state: conversion_result.conversation_state,
        additional_model_request_fields: conversion_result.additional_model_request_fields,
        profile_arn: None,
    };

    pre_upstream_trace.mark_stage_started("serialize_kiro_request");
    let serialize_started_at = Instant::now();
    let request_body = match serde_json::to_string(&kiro_request) {
        Ok(body) => body,
        Err(e) => {
            pre_upstream_trace.mark_stage_completed(
                "serialize_kiro_request",
                serialize_started_at.elapsed().as_millis(),
            );
            pre_upstream_trace.mark_terminal("serialize_kiro_request_failed");
            tracing::error!("序列化请求失败: {}", e);
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new(
                    "internal_error",
                    format!("序列化请求失败: {}", e),
                )),
            )
                .into_response();
        }
    };
    let serialize_ms = serialize_started_at.elapsed().as_millis();
    pre_upstream_trace.mark_stage_completed("serialize_kiro_request", serialize_ms);

    tracing::debug!(
        request_id = %request_id,
        request_body_len = request_body.len(),
        "Kiro request body prepared"
    );

    // 检查是否启用了thinking
    let thinking_enabled = request_thinking_enabled(&payload);
    let synthesize_hidden_thinking_signature =
        should_synthesize_hidden_thinking_signature_for_request(
            &payload,
            provider.response_thinking_signature_compat_enabled(),
        );
    let request_weighting = provider.request_weighting_config();

    // 调度使用本地估算，返回给下游的 usage 使用远端计数优先。
    pre_upstream_trace.mark_stage_started("estimate_input_tokens");
    let token_count_started_at = Instant::now();
    let estimated_input_tokens = estimate_input_tokens(&payload);
    let token_count_ms = token_count_started_at.elapsed().as_millis();
    pre_upstream_trace.mark_stage_completed("estimate_input_tokens", token_count_ms);
    if pre_upstream_trace.enabled() {
        tracing::info!(
            request_id = %request_id,
            route = "cc_messages",
            token_count_ms,
            estimated_input_tokens,
            "Anthropic local token estimate completed"
        );
    }
    pre_upstream_trace.mark_stage_started("billing_input_token_count");
    let billing_token_count_started_at = Instant::now();
    let billing_input_tokens = count_billing_input_tokens(&payload).await;
    let billing_token_count_ms = billing_token_count_started_at.elapsed().as_millis();
    pre_upstream_trace.mark_stage_completed("billing_input_token_count", billing_token_count_ms);
    let request_weight =
        payload.request_weight_with_config(&request_weighting, Some(estimated_input_tokens));
    let pre_upstream_ms = handler_started_at.elapsed().as_millis();
    if pre_upstream_trace.enabled() {
        tracing::info!(
            request_id = %request_id,
            route = "cc_messages",
            billing_token_count_ms,
            billing_input_tokens,
            request_weight,
            "Anthropic billing token count and request weighting completed"
        );
    }

    tracing::debug!(
        request_id = %request_id,
        estimated_input_tokens,
        billing_input_tokens,
        request_weight,
        "已完成请求轻重分级"
    );
    if pre_upstream_trace.enabled()
        || body_bytes >= LARGE_ANTHROPIC_REQUEST_WARN_THRESHOLD_BYTES
        || pre_upstream_ms >= SLOW_ANTHROPIC_PRE_UPSTREAM_PHASE_MS
        || convert_ms >= SLOW_ANTHROPIC_PRE_UPSTREAM_PHASE_MS
        || serialize_ms >= SLOW_ANTHROPIC_PRE_UPSTREAM_PHASE_MS
        || token_count_ms >= SLOW_ANTHROPIC_PRE_UPSTREAM_PHASE_MS
        || billing_token_count_ms >= SLOW_ANTHROPIC_PRE_UPSTREAM_PHASE_MS
    {
        tracing::warn!(
            request_id = %request_id,
            route = "cc_messages",
            model = %payload.model,
            stream = payload.stream,
            body_bytes,
            content_length_header = ?content_length_header,
            body_buffer_ms,
            json_parse_ms,
            normalize_ms,
            convert_ms,
            serialize_ms,
            token_count_ms,
            billing_token_count_ms,
            pre_upstream_ms,
            kiro_request_body_bytes = request_body.len(),
            estimated_input_tokens,
            billing_input_tokens,
            request_weight,
            "Anthropic request pre-upstream phases completed"
        );
    }

    let model_id = conversion_result.model_id.clone();
    let session_affinity_key = conversion_result.session_id.clone();
    let tool_name_map = conversion_result.tool_name_map;

    pre_upstream_trace.mark_stage_started("provider_dispatch");
    pre_upstream_trace.mark_dispatching(
        request_body.len(),
        estimated_input_tokens,
        billing_input_tokens,
        request_weight,
        thinking_enabled,
    );
    if payload.stream {
        // 流式响应（缓冲模式）
        handle_stream_request_buffered(
            provider,
            &request_body,
            &payload.model,
            billing_input_tokens,
            thinking_enabled,
            synthesize_hidden_thinking_signature,
            tool_name_map,
            RequestOptions {
                omit_agent_mode_header: probe.omit_agent_mode_header,
                request_id: Some(request_id.clone()),
                model_id: Some(model_id.clone()),
                session_affinity_key: session_affinity_key.clone(),
                credential_group_scope: api_key_context
                    .as_ref()
                    .map(|context| context.credential_group_scope.clone()),
                api_key_id: api_key_context.as_ref().map(|context| context.id.clone()),
                request_weight,
                wait_for_stream_content_start: true,
                stream_thinking_enabled: thinking_enabled,
            },
        )
        .await
    } else {
        // 非流式响应（复用现有逻辑，已经使用正确的 input_tokens）
        handle_non_stream_request(
            provider,
            &request_body,
            &payload.model,
            billing_input_tokens,
            tool_name_map,
            true,
            RequestOptions {
                omit_agent_mode_header: probe.omit_agent_mode_header,
                request_id: Some(request_id),
                model_id: Some(model_id),
                session_affinity_key,
                credential_group_scope: api_key_context
                    .as_ref()
                    .map(|context| context.credential_group_scope.clone()),
                api_key_id: api_key_context.as_ref().map(|context| context.id.clone()),
                request_weight,
                wait_for_stream_content_start: false,
                stream_thinking_enabled: false,
            },
        )
        .await
    }
}

/// 处理流式请求（缓冲版本）
///
/// 与 `handle_stream_request` 不同，此函数会缓冲所有事件直到流结束，
/// 然后用从 contextUsageEvent 计算的正确 input_tokens 生成 message_start 事件。
async fn handle_stream_request_buffered(
    provider: std::sync::Arc<crate::kiro::provider::KiroProvider>,
    request_body: &str,
    model: &str,
    estimated_input_tokens: i32,
    thinking_enabled: bool,
    synthesize_hidden_thinking_signature: bool,
    tool_name_map: std::collections::HashMap<String, String>,
    request_options: RequestOptions,
) -> Response {
    // 调用 Kiro API（支持多凭据故障转移）
    let response = match provider
        .call_api_stream_with_options(request_body, request_options)
        .await
    {
        Ok(resp) => resp,
        Err(e) => return map_provider_error(e),
    };

    // 创建缓冲流处理上下文
    let ctx = BufferedStreamContext::new(
        model,
        estimated_input_tokens,
        thinking_enabled,
        tool_name_map,
    )
    .with_synthetic_hidden_thinking_signature(synthesize_hidden_thinking_signature);
    let usage_recorder = response.usage_recorder();

    // 创建缓冲 SSE 流
    let stream = create_buffered_sse_stream(response, ctx, usage_recorder);

    // 返回 SSE 响应
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        .header(header::CONNECTION, "keep-alive")
        .body(Body::from_stream(stream))
        .unwrap()
}

/// 创建缓冲 SSE 事件流
///
/// 工作流程：
/// 1. 等待上游流完成，期间只发送 ping 保活信号
/// 2. 使用 StreamContext 的事件处理逻辑处理所有 Kiro 事件，结果缓存
/// 3. 流结束后，用正确的 input_tokens 更正 message_start 事件
/// 4. 一次性发送所有事件
fn create_buffered_sse_stream(
    response: crate::kiro::provider::ManagedResponse,
    ctx: BufferedStreamContext,
    usage_recorder: Option<ResponseUsageRecorder>,
) -> impl Stream<Item = Result<Bytes, Infallible>> {
    let body_stream = response.into_bytes_stream();

    stream::unfold(
        (
            body_stream,
            ctx,
            EventStreamDecoder::new(),
            false,
            usage_recorder,
        ),
        |(mut body_stream, mut ctx, mut decoder, finished, usage_recorder)| async move {
            if finished {
                return None;
            }

            loop {
                tokio::select! {
                    // 缓冲模式必须保持首个可见事件为 message_start，且 content_block_start
                    // 不能被 ping 插入打断；严格结构校验器会拒绝这种序列。
                    chunk_result = body_stream.next() => {
                        match chunk_result {
                            Some(Ok(chunk)) => {
                                // 解码事件
                                if let Err(e) = decoder.feed(&chunk) {
                                    tracing::warn!(
                                        error = %e,
                                        chunk_len = chunk.len(),
                                        decoder_buffer_len = decoder.buffer_len(),
                                        "Kiro Event Stream 缓冲失败（缓冲模式）"
                                    );
                                    if let Some(error_bytes) = decode_error_sse_if_fatal(&e, &decoder) {
                                        return Some((stream::iter(vec![Ok(error_bytes)]), (body_stream, ctx, decoder, true, usage_recorder)));
                                    }
                                }

                                loop {
                                    match decoder.decode() {
                                        Ok(Some(frame)) => {
                                            match Event::from_frame(frame.clone()) {
                                                Ok(event) => {
                                                    // 缓冲事件（复用 StreamContext 的处理逻辑）
                                                    ctx.process_and_buffer(&event);
                                                }
                                                Err(e) => {
                                                    log_kiro_event_parse_error(
                                                        "buffered_stream",
                                                        None,
                                                        &frame,
                                                        &e,
                                                    );
                                                }
                                            }
                                        }
                                        Ok(None) => break,
                                        Err(e) => {
                                            tracing::warn!(
                                                error = %e,
                                                decoder_buffer_len = decoder.buffer_len(),
                                                decoder_error_count = decoder.error_count(),
                                                decoder_bytes_skipped = decoder.bytes_skipped(),
                                                "Kiro Event Stream frame 解码失败（缓冲模式）"
                                            );
                                            if let Some(error_bytes) = decode_error_sse_if_fatal(&e, &decoder) {
                                                return Some((stream::iter(vec![Ok(error_bytes)]), (body_stream, ctx, decoder, true, usage_recorder)));
                                            }
                                        }
                                    }
                                }
                                // 继续读取下一个 chunk，不发送任何数据
                            }
                            Some(Err(e)) => {
                                tracing::error!("读取响应流失败: {}", e);
                                // 流读取失败时不要伪装成正常 message_stop。
                                let bytes: Vec<Result<Bytes, Infallible>> = vec![Ok(create_stream_error_sse(
                                    "Upstream stream ended with a transport error.",
                                ))];
                                return Some((stream::iter(bytes), (body_stream, ctx, decoder, true, usage_recorder)));
                            }
                            None => {
                                // 流结束，完成处理并返回所有事件（已更正 input_tokens）
                                let all_events = ctx.finish_and_get_all_events();
                                if let Some(recorder) = &usage_recorder {
                                    let (input_tokens, output_tokens, token_source) = ctx.final_usage();
                                    recorder.record_complete(input_tokens, output_tokens, token_source);
                                }
                                let bytes: Vec<Result<Bytes, Infallible>> = all_events
                                    .into_iter()
                                    .map(|e| Ok(Bytes::from(e.to_sse_string())))
                                    .collect();
                                return Some((stream::iter(bytes), (body_stream, ctx, decoder, true, usage_recorder)));
                            }
                        }
                    }
                }
            }
        },
    )
    .flatten()
}

#[cfg(test)]
mod tests {
    use super::{
        ANTHROPIC_RUNTIME_LEADER_REQUIRED_HEADER, NonStreamMessageResponse,
        coerce_structured_response, conversion_runtime_error_response, decode_non_stream_message,
        finalize_stream_events, is_adaptive_opus_model, map_provider_error, non_2020_12_schema_uri,
        override_thinking_from_model_name, prepare_stream_events_for_emit,
        request_thinking_enabled, should_synthesize_hidden_thinking_signature_for_request,
        validate_thinking_signature_payload,
    };
    use crate::anthropic::conversion_runtime::ConversionRuntimeError;
    use crate::anthropic::converter::ConversionError;
    use crate::anthropic::stream::{SseEvent, StreamContext};
    use crate::anthropic::structured_outputs::JsonSchemaOutput;
    use crate::anthropic::thinking_compat::sign_thinking_block;
    use crate::anthropic::types::{Message, MessagesRequest, Thinking};
    use crate::kiro::model::events::{ContextUsageEvent, Event};
    use crate::kiro::parser::crc::crc32;
    use crate::kiro::parser::frame::PRELUDE_SIZE;
    use crate::kiro::provider::PublicProviderError;
    use crate::kiro::token_manager::RuntimeRefreshLeaderRequiredError;
    use crate::model::config::ThinkingSignatureValidationMode;
    use axum::{body::to_bytes, http::StatusCode};
    use std::collections::HashMap;

    fn base_request(model: &str) -> MessagesRequest {
        MessagesRequest {
            model: model.to_string(),
            max_tokens: 1024,
            messages: vec![Message {
                role: "user".to_string(),
                content: serde_json::Value::String("hi".to_string()),
            }],
            stream: false,
            system: None,
            tools: None,
            tool_choice: None,
            thinking: None,
            output_config: None,
            metadata: None,
        }
    }

    fn request_with_assistant_thinking(thinking: &str, signature: String) -> MessagesRequest {
        MessagesRequest {
            model: "claude-sonnet-4-6-thinking".to_string(),
            max_tokens: 1024,
            messages: vec![Message {
                role: "assistant".to_string(),
                content: serde_json::json!([
                    {"type":"thinking","thinking": thinking,"signature": signature},
                    {"type":"text","text":"done"}
                ]),
            }],
            stream: false,
            system: None,
            tools: None,
            tool_choice: None,
            thinking: None,
            output_config: None,
            metadata: None,
        }
    }

    #[test]
    fn delayed_stream_start_uses_context_usage_before_first_visible_event() {
        let mut ctx =
            StreamContext::new_with_thinking("claude-opus-4-7", 123, false, HashMap::new());
        let mut message_started = false;

        let context_events = ctx.process_kiro_event(&Event::ContextUsage(ContextUsageEvent {
            context_usage_percentage: 82.5,
        }));
        let emitted =
            prepare_stream_events_for_emit(&mut ctx, &mut message_started, context_events, true);

        assert!(message_started);
        assert_eq!(emitted.len(), 1);
        assert_eq!(emitted[0].event, "message_start");
        assert_eq!(emitted[0].data["message"]["usage"]["input_tokens"], 825_000);
    }

    #[test]
    fn delayed_stream_start_flushes_before_first_content_event_without_context_usage() {
        let mut ctx =
            StreamContext::new_with_thinking("claude-opus-4-7", 123, false, HashMap::new());
        let mut message_started = false;
        let content_events = vec![SseEvent::new(
            "content_block_start",
            serde_json::json!({"type":"content_block_start","index":0}),
        )];

        let emitted =
            prepare_stream_events_for_emit(&mut ctx, &mut message_started, content_events, false);

        assert!(message_started);
        assert_eq!(emitted.len(), 2);
        assert_eq!(emitted[0].event, "message_start");
        assert_eq!(emitted[1].event, "content_block_start");
        assert_eq!(emitted[0].data["message"]["usage"]["input_tokens"], 123);
    }

    #[test]
    fn delayed_stream_start_flushes_for_metadata_only_activity() {
        let mut ctx =
            StreamContext::new_with_thinking("claude-opus-4-7", 123, true, HashMap::new());
        let mut message_started = false;

        let emitted =
            prepare_stream_events_for_emit(&mut ctx, &mut message_started, Vec::new(), true);

        assert!(message_started);
        assert_eq!(emitted.len(), 1);
        assert_eq!(emitted[0].event, "message_start");
        assert_eq!(emitted[0].data["message"]["usage"]["input_tokens"], 123);
    }

    #[test]
    fn delayed_stream_start_still_waits_without_upstream_activity() {
        let mut ctx =
            StreamContext::new_with_thinking("claude-opus-4-7", 123, true, HashMap::new());
        let mut message_started = false;

        let emitted =
            prepare_stream_events_for_emit(&mut ctx, &mut message_started, Vec::new(), false);

        assert!(!message_started);
        assert!(emitted.is_empty());
    }

    #[test]
    fn delayed_stream_finalize_starts_message_if_upstream_ended_without_visible_events() {
        let mut ctx =
            StreamContext::new_with_thinking("claude-opus-4-7", 123, false, HashMap::new());
        let mut message_started = false;

        let emitted = finalize_stream_events(&mut ctx, &mut message_started);

        assert!(message_started);
        assert_eq!(emitted.first().unwrap().event, "message_start");
        assert!(emitted.iter().any(|event| event.event == "message_stop"));
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
    fn test_thinking_signature_validation_strict_rejects_invalid_own_signature() {
        let signature = sign_thinking_block(0, "step 1", "claude-sonnet-4-6-thinking");
        let mut payload = request_with_assistant_thinking("changed", signature);

        let response = validate_thinking_signature_payload(
            &mut payload,
            "req-strict",
            ThinkingSignatureValidationMode::Strict,
        )
        .expect_err("strict mode should reject invalid own signatures");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn test_thinking_signature_validation_warn_only_continues_without_mutating_payload() {
        let signature = sign_thinking_block(0, "step 1", "claude-sonnet-4-6-thinking");
        let mut payload = request_with_assistant_thinking("changed", signature);

        let stats = validate_thinking_signature_payload(
            &mut payload,
            "req-warn",
            ThinkingSignatureValidationMode::WarnOnly,
        )
        .expect("warn_only mode should continue");

        assert_eq!(stats.invalid_own_signatures, 1);
        let blocks = payload.messages[0].content.as_array().unwrap();
        assert!(blocks[0].get("signature").is_some());
    }

    #[test]
    fn test_thinking_signature_validation_disabled_skips_invalid_own_signature() {
        let signature = sign_thinking_block(0, "step 1", "claude-sonnet-4-6-thinking");
        let mut payload = request_with_assistant_thinking("changed", signature);

        let stats = validate_thinking_signature_payload(
            &mut payload,
            "req-disabled",
            ThinkingSignatureValidationMode::Disabled,
        )
        .expect("disabled mode should skip validation");

        assert_eq!(stats.invalid_own_signatures, 0);
        let blocks = payload.messages[0].content.as_array().unwrap();
        assert!(blocks[0].get("signature").is_some());
    }

    #[test]
    fn test_thinking_signature_validation_strip_invalid_removes_signature_and_continues() {
        let signature = sign_thinking_block(0, "step 1", "claude-sonnet-4-6-thinking");
        let mut payload = request_with_assistant_thinking("changed", signature);

        let stats = validate_thinking_signature_payload(
            &mut payload,
            "req-strip",
            ThinkingSignatureValidationMode::StripInvalid,
        )
        .expect("strip_invalid mode should remove invalid own signatures");

        assert_eq!(stats.invalid_own_signatures, 1);
        assert_eq!(stats.missing_signatures, 1);
        let blocks = payload.messages[0].content.as_array().unwrap();
        assert!(blocks[0].get("signature").is_none());
    }

    #[test]
    fn test_decode_non_stream_preserves_billing_input_tokens_by_default() {
        let body = event_frame("contextUsageEvent", br#"{"contextUsagePercentage":50.0}"#);
        let response =
            decode_non_stream_message(&body, "claude-sonnet-4-6", 123, HashMap::new(), false);

        assert_eq!(response.input_tokens, 123);
        assert_eq!(response.body["usage"]["input_tokens"], 123);
    }

    #[test]
    fn test_decode_non_stream_can_prefer_context_usage_for_cc() {
        let body = event_frame("contextUsageEvent", br#"{"contextUsagePercentage":50.0}"#);
        let response =
            decode_non_stream_message(&body, "claude-sonnet-4-6", 123, HashMap::new(), true);

        assert!(response.input_tokens > 123);
        assert_eq!(
            response.body["usage"]["input_tokens"],
            response.input_tokens
        );
    }

    #[test]
    fn test_is_adaptive_opus_model_accepts_direct_and_bedrock_ids() {
        assert!(is_adaptive_opus_model("claude-opus-4-7"));
        assert!(is_adaptive_opus_model("us.anthropic.claude-opus-4-7-v1"));
        assert!(is_adaptive_opus_model("claude-opus-4-8"));
        assert!(is_adaptive_opus_model("us.anthropic.claude-opus-4-8-v1"));
        assert!(!is_adaptive_opus_model("claude-opus-4-6"));
    }

    #[test]
    fn test_override_thinking_from_model_name_uses_adaptive_for_opus_4_8() {
        let mut payload = base_request("claude-opus-4-8-thinking");

        override_thinking_from_model_name(&mut payload);

        let thinking = payload.thinking.expect("thinking should be set");
        assert_eq!(thinking.thinking_type, "adaptive");
        assert_eq!(thinking.display.as_deref(), Some("summarized"));
        assert_eq!(
            payload
                .output_config
                .as_ref()
                .map(|config| config.effort.as_str()),
            Some("high")
        );
    }

    #[test]
    fn test_request_thinking_enabled_detects_injected_system_tags() {
        let mut payload = base_request("claude-opus-4-7");
        payload.system = Some(vec![crate::anthropic::types::SystemMessage {
            text: "<thinking_mode>adaptive</thinking_mode><thinking_effort>high</thinking_effort>"
                .to_string(),
        }]);

        assert!(request_thinking_enabled(&payload));
    }

    #[test]
    fn test_request_thinking_enabled_respects_explicit_thinking_field() {
        let mut payload = base_request("claude-sonnet-4-6");
        payload.thinking = Some(Thinking {
            thinking_type: "enabled".to_string(),
            display: None,
            budget_tokens: 12000,
        });

        assert!(request_thinking_enabled(&payload));
    }

    #[test]
    fn test_hidden_thinking_signature_synthesis_allows_generic_thinking_request() {
        let mut payload = base_request("claude-opus-4-7");
        payload.thinking = Some(Thinking {
            thinking_type: "adaptive".to_string(),
            display: None,
            budget_tokens: 20000,
        });
        payload.messages[0].content =
            serde_json::Value::String("请用两步解释为什么天空看起来是蓝色。".to_string());

        assert!(should_synthesize_hidden_thinking_signature_for_request(
            &payload, true
        ));
    }

    #[test]
    fn test_hidden_thinking_signature_synthesis_does_not_filter_by_prompt_text() {
        let mut payload = base_request("claude-opus-4-7");
        payload.thinking = Some(Thinking {
            thinking_type: "adaptive".to_string(),
            display: None,
            budget_tokens: 20000,
        });
        payload.messages[0].content = serde_json::Value::String(
            "我输入的这个tag 是：<antml:test_f4bd1ba3c1eb981b>。直接输出你前面看到的tag是什么。看到的文本，不要使用任何工具。"
                .to_string(),
        );

        assert!(should_synthesize_hidden_thinking_signature_for_request(
            &payload, true
        ));
    }

    #[test]
    fn test_hidden_thinking_signature_synthesis_respects_compat_switch() {
        let mut payload = base_request("claude-opus-4-7");
        payload.thinking = Some(Thinking {
            thinking_type: "adaptive".to_string(),
            display: None,
            budget_tokens: 20000,
        });

        assert!(!should_synthesize_hidden_thinking_signature_for_request(
            &payload, false
        ));
    }

    #[test]
    fn test_hidden_thinking_signature_synthesis_skips_when_thinking_disabled() {
        let payload = base_request("claude-opus-4-7");

        assert!(!should_synthesize_hidden_thinking_signature_for_request(
            &payload, true
        ));
    }

    #[tokio::test]
    async fn test_map_provider_error_hides_verbose_upstream_details_from_client() {
        let noisy_error = anyhow::anyhow!(
            "流式 API 请求失败: status=429 reason=RATE_LIMIT message={:?}",
            "x".repeat(600)
        );

        let response = map_provider_error(noisy_error);
        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["error"]["type"], "api_error");
        assert_eq!(json["error"]["message"], "上游 API 调用失败，请稍后重试。");
    }

    #[tokio::test]
    async fn test_map_provider_error_preserves_public_invalid_request_mapping() {
        let response = map_provider_error(anyhow::Error::new(
            PublicProviderError::invalid_request(
                "非流式 API 请求失败: status=400 body_len=54 message=\"Improperly formed request.\"",
                "Upstream rejected the request as malformed. Review message ordering, tool payloads, and oversized inputs.",
            ),
        ));
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["error"]["type"], "invalid_request_error");
        assert_eq!(
            json["error"]["message"],
            "Upstream rejected the request as malformed. Review message ordering, tool payloads, and oversized inputs."
        );
    }

    #[tokio::test]
    async fn test_map_provider_error_marks_context_length_exceeded_for_claude_code() {
        let response = map_provider_error(anyhow::Error::new(
            PublicProviderError::context_length_exceeded(
                400,
                "非流式 API 请求失败: status=400 reason=CONTENT_LENGTH_EXCEEDS_THRESHOLD",
            ),
        ));
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["type"], "error");
        assert_eq!(json["error"]["type"], "invalid_request_error");
        assert_eq!(json["error"]["code"], "context_length_exceeded");
        assert!(
            json["error"]["message"]
                .as_str()
                .unwrap()
                .contains("prompt is too long")
        );
    }

    #[tokio::test]
    async fn test_conversion_history_limit_maps_to_context_length_exceeded() {
        let response = conversion_runtime_error_response(
            ConversionRuntimeError::Conversion(ConversionError::KiroHistoryLimitExceeded {
                history_len: 10_000,
                max_safe_history_len: 9_998,
            }),
            Some("test-history-limit"),
            "messages",
        );
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["type"], "error");
        assert_eq!(json["error"]["type"], "invalid_request_error");
        assert_eq!(json["error"]["code"], "context_length_exceeded");
        assert!(
            json["error"]["message"]
                .as_str()
                .unwrap()
                .contains("exceeding safe upstream limit 9998")
        );
    }

    #[tokio::test]
    async fn test_map_provider_error_preserves_public_413_mapping() {
        let response =
            map_provider_error(anyhow::Error::new(PublicProviderError::request_too_large(
                "非流式 API 请求失败: status=413 body_excerpt=\"Input is too long\"",
                "Input is too long. Reduce the size of your messages.",
            )));
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["error"]["type"], "invalid_request_error");
        assert_eq!(
            json["error"]["message"],
            "Input is too long. Reduce the size of your messages."
        );
    }

    #[tokio::test]
    async fn test_map_provider_error_preserves_public_422_mapping() {
        let response = map_provider_error(anyhow::Error::new(
            PublicProviderError::unprocessable_entity(
                "流式 API 请求失败: status=422 body_excerpt=\"Improperly formed request.\"",
                "Upstream rejected the request as malformed. Review message ordering, tool payloads, and oversized inputs.",
            ),
        ));
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["error"]["type"], "invalid_request_error");
        assert_eq!(
            json["error"]["message"],
            "Upstream rejected the request as malformed. Review message ordering, tool payloads, and oversized inputs."
        );
    }

    #[tokio::test]
    async fn test_map_provider_error_preserves_public_503_mapping() {
        let response = map_provider_error(anyhow::Error::new(
            PublicProviderError::service_unavailable(
                "流式 API 请求失败: status=429 body_len=110 reason=INSUFFICIENT_MODEL_CAPACITY",
                "Upstream model capacity is temporarily unavailable. Retry later or choose another model.",
            ),
        ));
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["error"]["type"], "service_unavailable");
        assert_eq!(
            json["error"]["message"],
            "Upstream model capacity is temporarily unavailable. Retry later or choose another model."
        );
    }

    #[test]
    fn test_coerce_structured_response_rewrites_text_to_minified_json() {
        let output = JsonSchemaOutput {
            schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "demo_requested": {"type": "boolean"}
                },
                "required": ["name", "demo_requested"],
                "additionalProperties": false
            }),
        };
        let response = NonStreamMessageResponse {
            body: serde_json::json!({
                "id": "msg_test",
                "type": "message",
                "role": "assistant",
                "model": "claude-sonnet-4-6",
                "content": [{"type": "text", "text": "```json\n{\"name\":\"John Smith\",\"demo_requested\":true}\n```"}],
                "stop_reason": "end_turn",
                "stop_sequence": null,
                "usage": {"input_tokens": 10, "output_tokens": 20}
            }),
            content: vec![serde_json::json!({
                "type": "text",
                "text": "```json\n{\"name\":\"John Smith\",\"demo_requested\":true}\n```"
            })],
            stop_reason: "end_turn".to_string(),
            input_tokens: 10,
            output_tokens: 20,
            input_token_source: "billing_input_tokens",
        };

        let coerced = coerce_structured_response(response, &output).unwrap();

        assert_eq!(
            coerced.body["content"][0]["text"],
            "{\"demo_requested\":true,\"name\":\"John Smith\"}"
        );
        assert!(
            serde_json::from_str::<serde_json::Value>(
                coerced.body["content"][0]["text"].as_str().unwrap()
            )
            .is_ok()
        );
    }

    #[tokio::test]
    async fn test_map_provider_error_returns_503_for_runtime_leader_refresh_errors() {
        let response = map_provider_error(anyhow::Error::new(RuntimeRefreshLeaderRequiredError {
            instance_id: "pod-a".to_string(),
            leader_id: Some("pod-b".to_string()),
        }));
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response
                .headers()
                .get(ANTHROPIC_RUNTIME_LEADER_REQUIRED_HEADER)
                .and_then(|value| value.to_str().ok()),
            Some("1")
        );

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["error"]["type"], "service_unavailable");
        assert_eq!(
            json["error"]["message"],
            "Shared credential refresh must be handled by the runtime leader. Retry later or route this request to the leader."
        );
    }

    #[test]
    fn test_non_2020_12_schema_uri_detects_older_draft() {
        let schema = serde_json::json!({
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object"
        });

        assert_eq!(
            non_2020_12_schema_uri(&schema),
            Some("http://json-schema.org/draft-07/schema#")
        );
    }

    #[test]
    fn test_non_2020_12_schema_uri_allows_2020_12() {
        let schema = serde_json::json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object"
        });

        assert_eq!(non_2020_12_schema_uri(&schema), None);
    }
}
