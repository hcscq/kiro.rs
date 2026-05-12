//! Anthropic API Handler 函数

use std::convert::Infallible;

use crate::common::logging::summarize_text_for_log;
use crate::kiro::model::events::Event;
use crate::kiro::model::requests::kiro::KiroRequest;
use crate::kiro::parser::decoder::EventStreamDecoder;
use crate::kiro::token_manager::{RuntimeRefreshLeaderRequiredError, RuntimeRefreshLeaseBusyError};
use crate::model::model_catalog::built_in_model_catalog;
use crate::token;
use anyhow::Error;
use axum::{
    body::Body,
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Json, Response},
};
use bytes::Bytes;
use futures::{Stream, StreamExt, stream};
use serde_json::json;
use std::time::Duration;
use tokio::time::{Instant, interval_at};
use uuid::Uuid;

use super::converter::{ConversionError, convert_request_with_probe};
use super::extractor::AnthropicJson;
use super::middleware::{ANTHROPIC_RUNTIME_LEADER_REQUIRED_HEADER, AppState};
use super::multimodal;
use super::probe::{UpstreamProbe, parse_upstream_probe};
use super::stream::{BufferedStreamContext, SseEvent, StreamContext};
use super::thinking_compat::{build_synthetic_thinking_signature, extract_thinking_and_text};
use super::types::{
    CountTokensRequest, CountTokensResponse, ErrorResponse, MessagesRequest, Model, ModelsResponse,
    OutputConfig, Thinking,
};
use super::webfetch;
use super::websearch;
use crate::kiro::provider::{PublicProviderError, RequestOptions};

const LARGE_ANTHROPIC_REQUEST_WARN_THRESHOLD_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone)]
pub(crate) struct NonStreamMessageResponse {
    pub body: serde_json::Value,
    pub content: Vec<serde_json::Value>,
    pub stop_reason: String,
    pub input_tokens: i32,
    pub output_tokens: i32,
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

    if let Some(public_err) = err.downcast_ref::<PublicProviderError>() {
        let status =
            StatusCode::from_u16(public_err.status_code()).unwrap_or(StatusCode::BAD_GATEWAY);
        if status.is_client_error() {
            tracing::warn!(error = %err_summary, "Kiro API 调用失败（客户端请求错误）");
        } else {
            tracing::error!(error = %err_summary, "Kiro API 调用失败（公开错误映射）");
        }
        return (
            status,
            Json(ErrorResponse::new(
                public_err.error_type(),
                public_err.public_message(),
            )),
        )
            .into_response();
    }

    // 上下文窗口满了（对话历史累积超出模型上下文窗口限制）
    if err_str.contains("CONTENT_LENGTH_EXCEEDS_THRESHOLD") {
        tracing::warn!(error = %err_summary, "上游拒绝请求：上下文窗口已满（不应重试）");
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::new(
                "invalid_request_error",
                "Context window is full. Reduce conversation history, system prompt, or tools.",
            )),
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
) -> Result<(), Response> {
    match multimodal::normalize_multimodal_urls(payload).await {
        Ok(stats) => {
            if stats.remote_images > 0
                || stats.data_url_images > 0
                || stats.openai_image_url_blocks > 0
                || stats.anthropic_url_blocks > 0
            {
                tracing::info!(
                    request_id = %request_id,
                    remote_images = stats.remote_images,
                    data_url_images = stats.data_url_images,
                    openai_image_url_blocks = stats.openai_image_url_blocks,
                    anthropic_url_blocks = stats.anthropic_url_blocks,
                    "normalized multimodal image references"
                );
            }
            Ok(())
        }
        Err(err) => {
            tracing::warn!(request_id = %request_id, error = %err, "多模态图片归一化失败");
            Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::new(
                    "invalid_request_error",
                    format!("Invalid image URL: {err}"),
                )),
            )
                .into_response())
        }
    }
}

/// GET /v1/models
///
/// 返回可用的模型列表
pub async fn get_models(State(state): State<AppState>) -> impl IntoResponse {
    tracing::info!("Received GET /v1/models request");

    let mut models = built_in_models();
    if let Some(provider) = &state.kiro_provider {
        models.retain(|model| provider.supports_model(&model.id));
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
    headers: HeaderMap,
    payload: AnthropicJson<MessagesRequest>,
) -> Response {
    let body_bytes = payload.body_len();
    let content_length_header = payload.content_length_header();
    let request_id = request_id_from_headers(&headers);
    tracing::info!(
        request_id = %request_id,
        model = %payload.model,
        max_tokens = %payload.max_tokens,
        stream = %payload.stream,
        body_bytes,
        content_length_header = ?content_length_header,
        message_count = %payload.messages.len(),
        "Received POST /v1/messages request"
    );
    if body_bytes >= LARGE_ANTHROPIC_REQUEST_WARN_THRESHOLD_BYTES {
        tracing::warn!(
            request_id = %request_id,
            model = %payload.model,
            body_bytes,
            content_length_header = ?content_length_header,
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

    let probe = parse_upstream_probe(&headers);
    if probe.is_enabled() {
        tracing::info!(request_id = %request_id, ?probe, "启用上游裸探针选项");
    }

    // 检测模型名是否包含 "thinking" 后缀，若包含则覆写 thinking 配置
    override_thinking_from_model_name(&mut payload);

    if let Err(response) = normalize_multimodal_payload(&mut payload, &request_id).await {
        return response;
    }

    // 检查是否为 WebFetch 请求
    if webfetch::has_web_fetch_tool(&payload) {
        tracing::info!(request_id = %request_id, "检测到 WebFetch 工具，路由到 WebFetch 处理");

        let input_tokens = token::count_all_tokens(
            payload.model.clone(),
            payload.system.clone(),
            payload.messages.clone(),
            payload.tools.clone(),
        ) as i32;

        return webfetch::handle_webfetch_request(
            provider,
            &payload,
            input_tokens,
            probe.clone(),
            &request_id,
        )
        .await;
    }

    // 检查是否为 WebSearch 请求
    if websearch::has_web_search_tool(&payload) {
        tracing::info!(request_id = %request_id, "检测到 WebSearch 工具，路由到 WebSearch 处理");

        // 估算输入 tokens
        let input_tokens = token::count_all_tokens(
            payload.model.clone(),
            payload.system.clone(),
            payload.messages.clone(),
            payload.tools.clone(),
        ) as i32;

        return websearch::handle_websearch_request(provider, &payload, input_tokens).await;
    }

    // 转换请求
    let conversion_result = match convert_request_with_probe(&payload, probe.clone()) {
        Ok(result) => result,
        Err(e) => {
            let (error_type, message) = match &e {
                ConversionError::UnsupportedModel(model) => {
                    ("invalid_request_error", format!("模型不支持: {}", model))
                }
                ConversionError::EmptyMessages => {
                    ("invalid_request_error", "消息列表为空".to_string())
                }
            };
            tracing::warn!(request_id = %request_id, error = %e, "请求转换失败");
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::new(error_type, message)),
            )
                .into_response();
        }
    };

    // 构建 Kiro 请求（profile_arn 由 provider 层根据实际凭据注入）
    let kiro_request = KiroRequest {
        conversation_state: conversion_result.conversation_state,
        profile_arn: None,
    };

    let request_body = match serde_json::to_string(&kiro_request) {
        Ok(body) => body,
        Err(e) => {
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

    tracing::debug!(
        request_id = %request_id,
        request_body_len = request_body.len(),
        "Kiro request body prepared"
    );

    // 检查是否启用了thinking
    let thinking_enabled = request_thinking_enabled(&payload);
    let request_weighting = provider.request_weighting_config();

    // 估算输入 tokens
    let input_tokens = token::count_all_tokens(
        payload.model.clone(),
        payload.system.clone(),
        payload.messages.clone(),
        payload.tools.clone(),
    ) as i32;
    let request_weight = payload.request_weight_with_config(&request_weighting, Some(input_tokens));

    tracing::debug!(
        request_id = %request_id,
        input_tokens,
        request_weight,
        "已完成请求轻重分级"
    );

    let tool_name_map = conversion_result.tool_name_map;

    if payload.stream {
        // 流式响应
        handle_stream_request(
            provider,
            &request_body,
            &payload.model,
            input_tokens,
            thinking_enabled,
            tool_name_map,
            RequestOptions {
                omit_agent_mode_header: probe.omit_agent_mode_header,
                request_id: Some(request_id.clone()),
                request_weight,
                wait_for_stream_content_start: thinking_enabled,
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
            input_tokens,
            tool_name_map,
            RequestOptions {
                omit_agent_mode_header: probe.omit_agent_mode_header,
                request_id: Some(request_id),
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

    // 创建流处理上下文
    let mut ctx =
        StreamContext::new_with_thinking(model, input_tokens, thinking_enabled, tool_name_map);

    // 生成初始事件
    let initial_events = ctx.generate_initial_events();

    // 创建 SSE 流
    let stream = create_sse_stream(response, ctx, initial_events);

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

/// 创建 SSE 事件流
fn create_sse_stream(
    response: crate::kiro::provider::ManagedResponse,
    ctx: StreamContext,
    initial_events: Vec<SseEvent>,
) -> impl Stream<Item = Result<Bytes, Infallible>> {
    // 先发送初始事件
    let initial_stream = stream::iter(
        initial_events
            .into_iter()
            .map(|e| Ok(Bytes::from(e.to_sse_string()))),
    );

    // 然后处理 Kiro 响应流，同时每25秒发送 ping 保活
    let body_stream = response.into_bytes_stream();

    let processing_stream = stream::unfold(
        (
            body_stream,
            ctx,
            EventStreamDecoder::new(),
            false,
            interval_at(
                Instant::now() + Duration::from_secs(PING_INTERVAL_SECS),
                Duration::from_secs(PING_INTERVAL_SECS),
            ),
            false,
        ),
        |(mut body_stream, mut ctx, mut decoder, finished, mut ping_interval, can_ping)| async move {
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
                                tracing::warn!("缓冲区溢出: {}", e);
                            }

                            let mut events = Vec::new();
                            for result in decoder.decode_iter() {
                                match result {
                                    Ok(frame) => {
                                        if let Ok(event) = Event::from_frame(frame) {
                                            let sse_events = ctx.process_kiro_event(&event);
                                            events.extend(sse_events);
                                        }
                                    }
                                    Err(e) => {
                                        tracing::warn!("解码事件失败: {}", e);
                                    }
                                }
                            }

                            let next_can_ping = can_ping
                                || events.iter().any(|event| event.event == "content_block_start");

                            // 转换为 SSE 字节流
                            let bytes: Vec<Result<Bytes, Infallible>> = events
                                .into_iter()
                                .map(|e| Ok(Bytes::from(e.to_sse_string())))
                                .collect();

                            Some((stream::iter(bytes), (body_stream, ctx, decoder, false, ping_interval, next_can_ping)))
                        }
                        Some(Err(e)) => {
                            tracing::error!("读取响应流失败: {}", e);
                            // 发送最终事件并结束
                            let final_events = ctx.generate_final_events();
                            let bytes: Vec<Result<Bytes, Infallible>> = final_events
                                .into_iter()
                                .map(|e| Ok(Bytes::from(e.to_sse_string())))
                                .collect();
                            Some((stream::iter(bytes), (body_stream, ctx, decoder, true, ping_interval, can_ping)))
                        }
                        None => {
                            // 流结束，发送最终事件
                            let final_events = ctx.generate_final_events();
                            let bytes: Vec<Result<Bytes, Infallible>> = final_events
                                .into_iter()
                                .map(|e| Ok(Bytes::from(e.to_sse_string())))
                                .collect();
                            Some((stream::iter(bytes), (body_stream, ctx, decoder, true, ping_interval, can_ping)))
                        }
                    }
                }
                // 发送 ping 保活
                _ = ping_interval.tick(), if can_ping => {
                    tracing::trace!("发送 ping 保活事件");
                    let bytes: Vec<Result<Bytes, Infallible>> = vec![Ok(create_ping_sse())];
                    Some((stream::iter(bytes), (body_stream, ctx, decoder, false, ping_interval, can_ping)))
                }
            }
        },
    )
    .flatten();

    initial_stream.chain(processing_stream)
}

use super::converter::get_context_window_size;

pub(crate) async fn execute_non_stream_round(
    provider: std::sync::Arc<crate::kiro::provider::KiroProvider>,
    payload: &MessagesRequest,
    probe: UpstreamProbe,
    request_id: Option<String>,
) -> Result<NonStreamMessageResponse, Response> {
    let conversion_result = match convert_request_with_probe(payload, probe.clone()) {
        Ok(result) => result,
        Err(e) => {
            let (error_type, message) = match &e {
                ConversionError::UnsupportedModel(model) => {
                    ("invalid_request_error", format!("模型不支持: {}", model))
                }
                ConversionError::EmptyMessages => {
                    ("invalid_request_error", "消息列表为空".to_string())
                }
            };
            if let Some(request_id) = request_id.as_deref() {
                tracing::warn!(request_id = %request_id, error = %e, "请求转换失败");
            } else {
                tracing::warn!(error = %e, "请求转换失败");
            }
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::new(error_type, message)),
            )
                .into_response());
        }
    };

    let kiro_request = KiroRequest {
        conversation_state: conversion_result.conversation_state,
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

    let input_tokens = token::count_all_tokens(
        payload.model.clone(),
        payload.system.clone(),
        payload.messages.clone(),
        payload.tools.clone(),
    ) as i32;
    let request_weighting = provider.request_weighting_config();
    let request_weight = payload.request_weight_with_config(&request_weighting, Some(input_tokens));

    execute_non_stream_request_body(
        provider,
        &request_body,
        &payload.model,
        input_tokens,
        conversion_result.tool_name_map,
        RequestOptions {
            omit_agent_mode_header: probe.omit_agent_mode_header,
            request_id,
            request_weight,
            wait_for_stream_content_start: false,
            stream_thinking_enabled: false,
        },
    )
    .await
}

/// 处理非流式请求
async fn handle_non_stream_request(
    provider: std::sync::Arc<crate::kiro::provider::KiroProvider>,
    request_body: &str,
    model: &str,
    input_tokens: i32,
    tool_name_map: std::collections::HashMap<String, String>,
    request_options: RequestOptions,
) -> Response {
    match execute_non_stream_request_body(
        provider,
        request_body,
        model,
        input_tokens,
        tool_name_map,
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
    request_options: RequestOptions,
) -> Result<NonStreamMessageResponse, Response> {
    // 调用 Kiro API（支持多凭据故障转移）
    let response = match provider
        .call_api_with_options(request_body, request_options)
        .await
    {
        Ok(resp) => resp,
        Err(e) => return Err(map_provider_error(e)),
    };

    // 读取响应体
    let body_bytes = match response.bytes().await {
        Ok(bytes) => bytes,
        Err(e) => {
            tracing::error!("读取响应体失败: {}", e);
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

    Ok(decode_non_stream_message(
        &body_bytes,
        model,
        input_tokens,
        tool_name_map,
    ))
}

fn decode_non_stream_message(
    body_bytes: &[u8],
    model: &str,
    input_tokens: i32,
    tool_name_map: std::collections::HashMap<String, String>,
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
                if let Ok(event) = Event::from_frame(frame) {
                    match event {
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
            let signature = build_synthetic_thinking_signature(&response_id, 0, &thinking);
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

    // 使用从 contextUsageEvent 计算的 input_tokens，如果没有则使用估算值
    let final_input_tokens = context_input_tokens.unwrap_or(input_tokens);

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
    }
}

/// 检测模型名是否包含 "thinking" 后缀，若包含则覆写 thinking 配置
///
/// - Opus 4.7：覆写为 adaptive 类型，并默认显示 summarized thinking
/// - 其他模型：覆写为 enabled 类型
/// - budget_tokens 固定为 20000
fn override_thinking_from_model_name(payload: &mut MessagesRequest) {
    let model_lower = payload.model.to_lowercase();
    if !model_lower.contains("thinking") {
        return;
    }

    let is_opus_4_7 = is_opus_4_7_model(&payload.model);

    let thinking_type = if is_opus_4_7 { "adaptive" } else { "enabled" };

    tracing::info!(
        model = %payload.model,
        thinking_type = thinking_type,
        "模型名包含 thinking 后缀，覆写 thinking 配置"
    );

    payload.thinking = Some(Thinking {
        thinking_type: thinking_type.to_string(),
        display: if is_opus_4_7 {
            Some("summarized".to_string())
        } else {
            None
        },
        budget_tokens: 20000,
    });

    if is_opus_4_7 {
        payload.output_config = Some(OutputConfig {
            effort: "high".to_string(),
        });
    }
}

fn is_opus_4_7_model(model: &str) -> bool {
    let model_lower = model.to_lowercase();
    model_lower.contains("opus") && (model_lower.contains("4-7") || model_lower.contains("4.7"))
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

    let total_tokens = token::count_all_tokens(
        payload.model,
        payload.system,
        payload.messages,
        payload.tools,
    ) as i32;

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
    headers: HeaderMap,
    payload: AnthropicJson<MessagesRequest>,
) -> Response {
    let body_bytes = payload.body_len();
    let content_length_header = payload.content_length_header();
    let request_id = request_id_from_headers(&headers);
    tracing::info!(
        request_id = %request_id,
        model = %payload.model,
        max_tokens = %payload.max_tokens,
        stream = %payload.stream,
        body_bytes,
        content_length_header = ?content_length_header,
        message_count = %payload.messages.len(),
        "Received POST /cc/v1/messages request"
    );
    if body_bytes >= LARGE_ANTHROPIC_REQUEST_WARN_THRESHOLD_BYTES {
        tracing::warn!(
            request_id = %request_id,
            model = %payload.model,
            body_bytes,
            content_length_header = ?content_length_header,
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

    let probe = parse_upstream_probe(&headers);
    if probe.is_enabled() {
        tracing::info!(request_id = %request_id, ?probe, "启用上游裸探针选项");
    }

    // 检测模型名是否包含 "thinking" 后缀，若包含则覆写 thinking 配置
    override_thinking_from_model_name(&mut payload);

    if let Err(response) = normalize_multimodal_payload(&mut payload, &request_id).await {
        return response;
    }

    // 检查是否为 WebFetch 请求
    if webfetch::has_web_fetch_tool(&payload) {
        tracing::info!(request_id = %request_id, "检测到 WebFetch 工具，路由到 WebFetch 处理");

        let input_tokens = token::count_all_tokens(
            payload.model.clone(),
            payload.system.clone(),
            payload.messages.clone(),
            payload.tools.clone(),
        ) as i32;

        return webfetch::handle_webfetch_request(
            provider,
            &payload,
            input_tokens,
            probe.clone(),
            &request_id,
        )
        .await;
    }

    // 检查是否为 WebSearch 请求
    if websearch::has_web_search_tool(&payload) {
        tracing::info!(request_id = %request_id, "检测到 WebSearch 工具，路由到 WebSearch 处理");

        // 估算输入 tokens
        let input_tokens = token::count_all_tokens(
            payload.model.clone(),
            payload.system.clone(),
            payload.messages.clone(),
            payload.tools.clone(),
        ) as i32;

        return websearch::handle_websearch_request(provider, &payload, input_tokens).await;
    }

    // 转换请求
    let conversion_result = match convert_request_with_probe(&payload, probe.clone()) {
        Ok(result) => result,
        Err(e) => {
            let (error_type, message) = match &e {
                ConversionError::UnsupportedModel(model) => {
                    ("invalid_request_error", format!("模型不支持: {}", model))
                }
                ConversionError::EmptyMessages => {
                    ("invalid_request_error", "消息列表为空".to_string())
                }
            };
            tracing::warn!(request_id = %request_id, error = %e, "请求转换失败");
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::new(error_type, message)),
            )
                .into_response();
        }
    };

    // 构建 Kiro 请求（profile_arn 由 provider 层根据实际凭据注入）
    let kiro_request = KiroRequest {
        conversation_state: conversion_result.conversation_state,
        profile_arn: None,
    };

    let request_body = match serde_json::to_string(&kiro_request) {
        Ok(body) => body,
        Err(e) => {
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

    tracing::debug!(
        request_id = %request_id,
        request_body_len = request_body.len(),
        "Kiro request body prepared"
    );

    // 检查是否启用了thinking
    let thinking_enabled = request_thinking_enabled(&payload);
    let request_weighting = provider.request_weighting_config();

    // 估算输入 tokens
    let input_tokens = token::count_all_tokens(
        payload.model.clone(),
        payload.system.clone(),
        payload.messages.clone(),
        payload.tools.clone(),
    ) as i32;
    let request_weight = payload.request_weight_with_config(&request_weighting, Some(input_tokens));

    tracing::debug!(
        request_id = %request_id,
        input_tokens,
        request_weight,
        "已完成请求轻重分级"
    );

    let tool_name_map = conversion_result.tool_name_map;

    if payload.stream {
        // 流式响应（缓冲模式）
        handle_stream_request_buffered(
            provider,
            &request_body,
            &payload.model,
            input_tokens,
            thinking_enabled,
            tool_name_map,
            RequestOptions {
                omit_agent_mode_header: probe.omit_agent_mode_header,
                request_id: Some(request_id.clone()),
                request_weight,
                wait_for_stream_content_start: false,
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
            input_tokens,
            tool_name_map,
            RequestOptions {
                omit_agent_mode_header: probe.omit_agent_mode_header,
                request_id: Some(request_id),
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
    );

    // 创建缓冲 SSE 流
    let stream = create_buffered_sse_stream(response, ctx);

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
) -> impl Stream<Item = Result<Bytes, Infallible>> {
    let body_stream = response.into_bytes_stream();

    stream::unfold(
        (
            body_stream,
            ctx,
            EventStreamDecoder::new(),
            false,
        ),
        |(mut body_stream, mut ctx, mut decoder, finished)| async move {
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
                                    tracing::warn!("缓冲区溢出: {}", e);
                                }

                                for result in decoder.decode_iter() {
                                    match result {
                                        Ok(frame) => {
                                            if let Ok(event) = Event::from_frame(frame) {
                                                // 缓冲事件（复用 StreamContext 的处理逻辑）
                                                ctx.process_and_buffer(&event);
                                            }
                                        }
                                        Err(e) => {
                                            tracing::warn!("解码事件失败: {}", e);
                                        }
                                    }
                                }
                                // 继续读取下一个 chunk，不发送任何数据
                            }
                            Some(Err(e)) => {
                                tracing::error!("读取响应流失败: {}", e);
                                // 发生错误，完成处理并返回所有事件
                                let all_events = ctx.finish_and_get_all_events();
                                let bytes: Vec<Result<Bytes, Infallible>> = all_events
                                    .into_iter()
                                    .map(|e| Ok(Bytes::from(e.to_sse_string())))
                                    .collect();
                                return Some((stream::iter(bytes), (body_stream, ctx, decoder, true)));
                            }
                            None => {
                                // 流结束，完成处理并返回所有事件（已更正 input_tokens）
                                let all_events = ctx.finish_and_get_all_events();
                                let bytes: Vec<Result<Bytes, Infallible>> = all_events
                                    .into_iter()
                                    .map(|e| Ok(Bytes::from(e.to_sse_string())))
                                    .collect();
                                return Some((stream::iter(bytes), (body_stream, ctx, decoder, true)));
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
        ANTHROPIC_RUNTIME_LEADER_REQUIRED_HEADER, is_opus_4_7_model, map_provider_error,
        override_thinking_from_model_name, request_thinking_enabled,
    };
    use crate::anthropic::types::{Message, MessagesRequest, Thinking};
    use crate::kiro::provider::PublicProviderError;
    use crate::kiro::token_manager::RuntimeRefreshLeaderRequiredError;
    use axum::{body::to_bytes, http::StatusCode};

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

    #[test]
    fn test_is_opus_4_7_model_accepts_direct_and_bedrock_ids() {
        assert!(is_opus_4_7_model("claude-opus-4-7"));
        assert!(is_opus_4_7_model("us.anthropic.claude-opus-4-7-v1"));
        assert!(!is_opus_4_7_model("claude-opus-4-6"));
    }

    #[test]
    fn test_override_thinking_from_model_name_uses_adaptive_for_opus_4_7() {
        let mut payload = base_request("claude-opus-4-7-thinking");

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
}
