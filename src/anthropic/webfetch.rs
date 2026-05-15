//! WebFetch 工具处理模块
//!
//! 将 Anthropic WebFetch server tool 请求转换为：
//! 1. 对内：Kiro 普通客户端工具回环
//! 2. 对外：Anthropic 官方 `server_tool_use` / `web_fetch_tool_result` 语义

use std::collections::HashSet;
use std::convert::Infallible;
use std::io::Cursor;
use std::sync::OnceLock;

use axum::{
    body::Body,
    http::{StatusCode, header},
    response::{IntoResponse, Json, Response},
};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use bytes::Bytes;
use futures::{Stream, stream};
use regex::Regex;
use reqwest::redirect::Policy;
use serde_json::json;
use url::Url;
use uuid::Uuid;

use crate::token;

use super::handlers::execute_non_stream_round;
use super::probe::UpstreamProbe;
use super::stream::SseEvent;
use super::types::{ErrorResponse, Message, MessagesRequest, Tool};
use super::websearch;

const WEB_FETCH_TOOL_TYPE_20250910: &str = "web_fetch_20250910";
const WEB_FETCH_TOOL_TYPE_20260209: &str = "web_fetch_20260209";
const SUPPORTED_WEB_FETCH_TOOL_TYPES: &[&str] =
    &[WEB_FETCH_TOOL_TYPE_20250910, WEB_FETCH_TOOL_TYPE_20260209];
const INTERNAL_WEB_FETCH_TOOL_NAME: &str = "__anthropic_server_web_fetch";
const INTERNAL_WEB_SEARCH_TOOL_NAME: &str = "__anthropic_server_web_search";
const EXTERNAL_WEB_FETCH_TOOL_NAME: &str = "web_fetch";
const EXTERNAL_WEB_SEARCH_TOOL_NAME: &str = "web_search";
const WEB_FETCH_REQUEST_TIMEOUT_SECS: u64 = 20;
const WEB_FETCH_MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const WEB_FETCH_MAX_URL_LEN: usize = 250;
const WEB_FETCH_HARD_MAX_USES: usize = 16;
const WEB_FETCH_DEFAULT_MAX_CONTENT_TOKENS: i32 = 100_000;
const TEXT_CHUNK_SIZE: usize = 120;

#[derive(Debug, Clone)]
struct WebFetchConfig {
    max_uses: Option<usize>,
    max_content_tokens: Option<i32>,
    citations_enabled: bool,
    allowed_domains: Vec<DomainPattern>,
    blocked_domains: Vec<DomainPattern>,
}

#[derive(Debug, Clone)]
struct DomainPattern {
    host: String,
    include_subdomains: bool,
    path: Option<PathPattern>,
}

#[derive(Debug, Clone)]
enum PathPattern {
    Prefix(String),
    Wildcard { prefix: String, suffix: String },
}

#[derive(Debug)]
struct FetchSuccess {
    retrieved_url: String,
    model_text: String,
    external_document: serde_json::Value,
}

#[derive(Debug)]
struct ServerToolExecution {
    server_tool_use_block: serde_json::Value,
    result_block: serde_json::Value,
    internal_tool_result: serde_json::Value,
    web_fetch_requests: i32,
    web_search_requests: i32,
}

pub fn has_web_fetch_tool(req: &MessagesRequest) -> bool {
    req.tools
        .as_ref()
        .is_some_and(|tools| tools.iter().any(is_any_web_fetch_tool))
}

pub async fn handle_webfetch_request(
    provider: std::sync::Arc<crate::kiro::provider::KiroProvider>,
    payload: &MessagesRequest,
    input_tokens: i32,
    probe: UpstreamProbe,
    request_id: &str,
) -> Response {
    let Some(tools) = payload.tools.as_ref() else {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::new(
                "invalid_request_error",
                "web_fetch tool declaration is missing",
            )),
        )
            .into_response();
    };

    let web_fetch_tools: Vec<&Tool> = tools
        .iter()
        .filter(|tool| is_any_web_fetch_tool(tool))
        .collect();
    if web_fetch_tools.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::new(
                "invalid_request_error",
                "web_fetch tool declaration is missing",
            )),
        )
            .into_response();
    }
    if web_fetch_tools.len() > 1 {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::new(
                "invalid_request_error",
                "only one web_fetch tool declaration is supported per request",
            )),
        )
            .into_response();
    }

    let tool = web_fetch_tools[0];
    if !is_supported_web_fetch_tool(tool) {
        let version = tool.tool_type.as_deref().unwrap_or("unknown");
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::new(
                "invalid_request_error",
                format!(
                    "kiro.rs currently supports Anthropic web_fetch server tool versions {} only; received {}",
                    SUPPORTED_WEB_FETCH_TOOL_TYPES.join(", "), version
                ),
            )),
        )
            .into_response();
    }

    let config = match build_web_fetch_config(tool) {
        Ok(config) => config,
        Err(message) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::new("invalid_request_error", message)),
            )
                .into_response();
        }
    };

    let mut internal_payload = build_internal_webfetch_payload(payload, tool);
    let mut outward_content: Vec<serde_json::Value> = Vec::new();
    let mut total_input_tokens = 0;
    let mut total_output_tokens = 0;
    let mut web_fetch_requests = 0;
    let mut web_search_requests = 0;

    for round_idx in 0..WEB_FETCH_HARD_MAX_USES {
        let round_request_id = format!("{request_id}-webfetch-{round_idx}");
        let round = match execute_non_stream_round(
            provider.clone(),
            &internal_payload,
            probe.clone(),
            Some(round_request_id),
        )
        .await
        {
            Ok(round) => round,
            Err(resp) => return resp,
        };

        total_input_tokens += round.input_tokens.max(0);
        total_output_tokens += round.output_tokens.max(0);

        let mut tool_result_blocks = Vec::new();
        let mut saw_client_tool_use = false;

        for block in &round.content {
            if is_internal_web_fetch_tool_use(block) {
                let execution = execute_web_fetch_tool(
                    &internal_payload,
                    &config,
                    block,
                    web_fetch_requests as usize,
                )
                .await;
                web_fetch_requests += execution.web_fetch_requests;
                web_search_requests += execution.web_search_requests;
                outward_content.push(execution.server_tool_use_block);
                outward_content.push(execution.result_block);
                tool_result_blocks.push(execution.internal_tool_result);
                continue;
            }

            if is_internal_web_search_tool_use(block) {
                let execution = execute_web_search_tool(provider.as_ref(), block).await;
                web_fetch_requests += execution.web_fetch_requests;
                web_search_requests += execution.web_search_requests;
                outward_content.push(execution.server_tool_use_block);
                outward_content.push(execution.result_block);
                tool_result_blocks.push(execution.internal_tool_result);
                continue;
            }

            if is_client_tool_use(block) {
                saw_client_tool_use = true;
            }
            outward_content.push(block.clone());
        }

        if tool_result_blocks.is_empty() || saw_client_tool_use {
            return finalize_web_fetch_response(
                payload,
                outward_content,
                if saw_client_tool_use {
                    "tool_use".to_string()
                } else {
                    round.stop_reason
                },
                input_tokens,
                total_input_tokens,
                total_output_tokens,
                web_fetch_requests,
                web_search_requests,
            );
        }

        internal_payload.messages.push(Message {
            role: "assistant".to_string(),
            content: serde_json::Value::Array(round.content.clone()),
        });
        internal_payload.messages.push(Message {
            role: "user".to_string(),
            content: serde_json::Value::Array(tool_result_blocks),
        });
    }

    (
        StatusCode::BAD_GATEWAY,
        Json(ErrorResponse::new(
            "api_error",
            "web_fetch tool loop exceeded the internal safety limit",
        )),
    )
        .into_response()
}

fn is_any_web_fetch_tool(tool: &Tool) -> bool {
    tool.tool_type
        .as_deref()
        .is_some_and(|tool_type| tool_type.trim().starts_with("web_fetch_"))
}

fn is_supported_web_fetch_tool(tool: &Tool) -> bool {
    tool.tool_type
        .as_deref()
        .is_some_and(|tool_type| SUPPORTED_WEB_FETCH_TOOL_TYPES.contains(&tool_type.trim()))
}

fn build_web_fetch_config(tool: &Tool) -> Result<WebFetchConfig, String> {
    if tool.allowed_domains.as_ref().is_some_and(|v| !v.is_empty())
        && tool.blocked_domains.as_ref().is_some_and(|v| !v.is_empty())
    {
        return Err(
            "web_fetch may declare either allowed_domains or blocked_domains, but not both"
                .to_string(),
        );
    }

    let allowed_domains = tool
        .allowed_domains
        .clone()
        .unwrap_or_default()
        .into_iter()
        .map(|entry| DomainPattern::parse(&entry))
        .collect::<Result<Vec<_>, _>>()?;
    let blocked_domains = tool
        .blocked_domains
        .clone()
        .unwrap_or_default()
        .into_iter()
        .map(|entry| DomainPattern::parse(&entry))
        .collect::<Result<Vec<_>, _>>()?;

    if let Some(max_content_tokens) = tool.max_content_tokens {
        if max_content_tokens <= 0 {
            return Err("web_fetch max_content_tokens must be positive".to_string());
        }
    }

    Ok(WebFetchConfig {
        max_uses: tool
            .max_uses
            .and_then(|value| usize::try_from(value.max(0)).ok()),
        max_content_tokens: tool.max_content_tokens,
        citations_enabled: tool.citations.as_ref().is_some_and(|c| c.enabled),
        allowed_domains,
        blocked_domains,
    })
}

fn build_internal_webfetch_payload(payload: &MessagesRequest, tool: &Tool) -> MessagesRequest {
    let mut cloned = payload.clone();
    cloned.stream = false;
    cloned.tools = payload.tools.as_ref().map(|tools| {
        tools
            .iter()
            .map(|declared_tool| {
                if std::ptr::eq(declared_tool, tool) || is_supported_web_fetch_tool(declared_tool) {
                    build_internal_webfetch_tool(tool)
                } else if websearch::is_web_search_tool(declared_tool) {
                    build_internal_websearch_tool(declared_tool)
                } else {
                    declared_tool.clone()
                }
            })
            .collect()
    });
    cloned
}

fn build_internal_webfetch_tool(tool: &Tool) -> Tool {
    Tool {
        tool_type: None,
        name: INTERNAL_WEB_FETCH_TOOL_NAME.to_string(),
        description: build_internal_webfetch_description(tool),
        input_schema: serde_json::from_value(json!({
            "type": "object",
            "properties": {
                "url": {
                    "type": "string",
                    "description": "Absolute HTTP(S) URL that already appeared in the conversation."
                }
            },
            "required": ["url"],
            "additionalProperties": false
        }))
        .unwrap_or_default(),
        max_uses: None,
        allowed_domains: None,
        blocked_domains: None,
        citations: None,
        max_content_tokens: None,
    }
}

fn build_internal_websearch_tool(tool: &Tool) -> Tool {
    let mut description = "Search the web for a specific query. Input must be a JSON object with a single string field named `query`. Use this tool when you need fresh or external information.".to_string();
    if let Some(max_uses) = tool.max_uses {
        description.push_str(&format!(
            "\nThe tool may be used at most {} time(s) in this response.",
            max_uses.max(0)
        ));
    }

    Tool {
        tool_type: None,
        name: INTERNAL_WEB_SEARCH_TOOL_NAME.to_string(),
        description,
        input_schema: serde_json::from_value(json!({
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Search query to run on the web."
                }
            },
            "required": ["query"],
            "additionalProperties": false
        }))
        .unwrap_or_default(),
        max_uses: None,
        allowed_domains: None,
        blocked_domains: None,
        citations: None,
        max_content_tokens: None,
    }
}

fn build_internal_webfetch_description(tool: &Tool) -> String {
    let mut description = "Fetch the contents of a specific URL that already appeared in the conversation. Use this tool only when you need the contents of an exact page or PDF. Input must be a JSON object with a single string field named `url`. Do not invent or rewrite URLs.".to_string();

    if let Some(allowed) = &tool.allowed_domains {
        if !allowed.is_empty() {
            description.push_str("\nAllowed domains or paths: ");
            description.push_str(&allowed.join(", "));
            description.push('.');
        }
    }
    if let Some(blocked) = &tool.blocked_domains {
        if !blocked.is_empty() {
            description.push_str("\nBlocked domains or paths: ");
            description.push_str(&blocked.join(", "));
            description.push('.');
        }
    }
    if let Some(max_uses) = tool.max_uses {
        description.push_str(&format!(
            "\nThe tool may be used at most {} time(s) in this response.",
            max_uses.max(0)
        ));
    }
    if let Some(max_content_tokens) = tool.max_content_tokens {
        description.push_str(&format!(
            "\nFetched content should stay concise and within roughly {} tokens.",
            max_content_tokens.max(0)
        ));
    }

    description
}

fn is_internal_web_fetch_tool_use(block: &serde_json::Value) -> bool {
    block.get("type").and_then(|v| v.as_str()) == Some("tool_use")
        && block.get("name").and_then(|v| v.as_str()) == Some(INTERNAL_WEB_FETCH_TOOL_NAME)
}

fn is_internal_web_search_tool_use(block: &serde_json::Value) -> bool {
    block.get("type").and_then(|v| v.as_str()) == Some("tool_use")
        && block.get("name").and_then(|v| v.as_str()) == Some(INTERNAL_WEB_SEARCH_TOOL_NAME)
}

fn is_client_tool_use(block: &serde_json::Value) -> bool {
    block.get("type").and_then(|v| v.as_str()) == Some("tool_use")
}

async fn execute_web_fetch_tool(
    payload: &MessagesRequest,
    config: &WebFetchConfig,
    tool_use: &serde_json::Value,
    completed_uses: usize,
) -> ServerToolExecution {
    let internal_tool_use_id = tool_use
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or("tooluse_web_fetch_invalid");
    let external_tool_use_id = generate_server_tool_use_id();

    let url = tool_use
        .get("input")
        .and_then(|value| value.get("url"))
        .and_then(|value| value.as_str())
        .map(str::trim)
        .unwrap_or("");

    let outcome = if completed_uses >= config.max_uses.unwrap_or(usize::MAX) {
        Err((
            "max_uses_exceeded",
            "web_fetch max_uses exceeded".to_string(),
            false,
        ))
    } else {
        execute_web_fetch(payload, config, url).await
    };

    match outcome {
        Ok(success) => ServerToolExecution {
            server_tool_use_block: json!({
                "type": "server_tool_use",
                "id": external_tool_use_id,
                "name": EXTERNAL_WEB_FETCH_TOOL_NAME,
                "input": {
                    "url": url
                }
            }),
            result_block: json!({
                "type": "web_fetch_tool_result",
                "tool_use_id": external_tool_use_id,
                "content": {
                    "type": "web_fetch_result",
                    "url": success.retrieved_url,
                    "content": success.external_document,
                    "retrieved_at": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
                }
            }),
            internal_tool_result: json!({
                "type": "tool_result",
                "tool_use_id": internal_tool_use_id,
                "content": success.model_text
            }),
            web_fetch_requests: 1,
            web_search_requests: 0,
        },
        Err((error_code, message, performed_request)) => ServerToolExecution {
            server_tool_use_block: json!({
                "type": "server_tool_use",
                "id": external_tool_use_id,
                "name": EXTERNAL_WEB_FETCH_TOOL_NAME,
                "input": {
                    "url": url
                }
            }),
            result_block: json!({
                "type": "web_fetch_tool_result",
                "tool_use_id": external_tool_use_id,
                "content": {
                    "type": "web_fetch_tool_error",
                    "error_code": error_code
                }
            }),
            internal_tool_result: json!({
                "type": "tool_result",
                "tool_use_id": internal_tool_use_id,
                "content": format!("web_fetch error ({}): {}", error_code, message),
                "is_error": true
            }),
            web_fetch_requests: if performed_request { 1 } else { 0 },
            web_search_requests: 0,
        },
    }
}

async fn execute_web_search_tool(
    provider: &crate::kiro::provider::KiroProvider,
    tool_use: &serde_json::Value,
) -> ServerToolExecution {
    let internal_tool_use_id = tool_use
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or("tooluse_web_search_invalid");
    let external_tool_use_id = generate_server_tool_use_id();
    let query = tool_use
        .get("input")
        .and_then(|value| value.get("query"))
        .and_then(|value| value.as_str())
        .map(str::trim)
        .unwrap_or("");

    let performed_request = !query.is_empty();
    let outcome = if performed_request {
        websearch::perform_web_search(provider, query).await
    } else {
        websearch::WebSearchOutcome::Unavailable("Missing `query` in web_search input".to_string())
    };
    let summary = websearch::generate_search_summary(query, &outcome);

    ServerToolExecution {
        server_tool_use_block: json!({
            "type": "server_tool_use",
            "id": external_tool_use_id,
            "name": EXTERNAL_WEB_SEARCH_TOOL_NAME,
            "input": {
                "query": query
            }
        }),
        result_block: json!({
            "type": "web_search_tool_result",
            "content": websearch::build_search_content(&outcome)
        }),
        internal_tool_result: json!({
            "type": "tool_result",
            "tool_use_id": internal_tool_use_id,
            "content": summary,
            "is_error": query.is_empty()
        }),
        web_fetch_requests: 0,
        web_search_requests: if performed_request { 1 } else { 0 },
    }
}

async fn execute_web_fetch(
    payload: &MessagesRequest,
    config: &WebFetchConfig,
    url: &str,
) -> Result<FetchSuccess, (&'static str, String, bool)> {
    if url.is_empty() {
        return Err((
            "invalid_input",
            "Missing `url` in web_fetch input".to_string(),
            false,
        ));
    }
    if url.len() > WEB_FETCH_MAX_URL_LEN {
        return Err((
            "url_too_long",
            "URL exceeds the 250 character limit".to_string(),
            false,
        ));
    }

    let parsed_url = parse_fetch_url(url).ok_or_else(|| {
        (
            "invalid_input",
            "URL must be an absolute HTTP(S) URL".to_string(),
            false,
        )
    })?;

    if !url_appears_in_context(payload, parsed_url.as_str()) {
        return Err((
            "url_not_allowed",
            "web_fetch may only fetch URLs that already appeared in the conversation".to_string(),
            false,
        ));
    }
    if !url_allowed_by_patterns(
        &parsed_url,
        &config.allowed_domains,
        &config.blocked_domains,
    ) {
        return Err((
            "url_not_allowed",
            "URL is excluded by the web_fetch domain policy".to_string(),
            false,
        ));
    }

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(
            WEB_FETCH_REQUEST_TIMEOUT_SECS,
        ))
        .redirect(Policy::limited(5))
        .build()
        .map_err(|err| ("unavailable", err.to_string(), false))?;

    let response = client
        .get(parsed_url.clone())
        .header("User-Agent", "kiro-rs-web-fetch/1.0")
        .send()
        .await
        .map_err(|err| ("url_not_accessible", err.to_string(), true))?;

    let status = response.status();
    if status == StatusCode::TOO_MANY_REQUESTS {
        return Err((
            "too_many_requests",
            "Origin returned HTTP 429 while fetching the URL".to_string(),
            true,
        ));
    }
    if !status.is_success() {
        return Err((
            "url_not_accessible",
            format!("Origin returned HTTP {}", status.as_u16()),
            true,
        ));
    }

    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_string();
    let body = response
        .bytes()
        .await
        .map_err(|err| ("url_not_accessible", err.to_string(), true))?;

    if body.len() > WEB_FETCH_MAX_RESPONSE_BYTES {
        return Err((
            "url_not_accessible",
            "Fetched document exceeded the local size budget".to_string(),
            true,
        ));
    }

    if is_pdf_content(&content_type, &body) {
        return build_pdf_fetch_success(&parsed_url, &body, config);
    }

    if !is_text_content_type(&content_type, &body) {
        return Err((
            "unsupported_content_type",
            format!("Unsupported content type: {}", content_type),
            true,
        ));
    }

    build_text_fetch_success(&parsed_url, &content_type, &body, config)
}

fn build_text_fetch_success(
    url: &Url,
    content_type: &str,
    body: &[u8],
    config: &WebFetchConfig,
) -> Result<FetchSuccess, (&'static str, String, bool)> {
    let raw_text = String::from_utf8_lossy(body).into_owned();
    let (title, text) = if content_type.to_ascii_lowercase().contains("html") {
        (
            extract_html_title(&raw_text),
            html2text::from_read(Cursor::new(body), 80).unwrap_or_else(|_| raw_text.clone()),
        )
    } else {
        (None, raw_text)
    };
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err((
            "unsupported_content_type",
            "Fetched text document was empty after extraction".to_string(),
            true,
        ));
    }

    let truncated = truncate_text(
        trimmed,
        config
            .max_content_tokens
            .unwrap_or(WEB_FETCH_DEFAULT_MAX_CONTENT_TOKENS),
    );
    Ok(FetchSuccess {
        retrieved_url: url.as_str().to_string(),
        model_text: build_internal_tool_result_text(url.as_str(), title.as_deref(), &truncated),
        external_document: build_text_document(
            &truncated,
            title.as_deref(),
            config.citations_enabled,
        ),
    })
}

fn build_pdf_fetch_success(
    url: &Url,
    body: &[u8],
    config: &WebFetchConfig,
) -> Result<FetchSuccess, (&'static str, String, bool)> {
    let extracted_text = pdf_extract::extract_text_from_mem(body).map_err(|err| {
        (
            "unsupported_content_type",
            format!("PDF text extraction failed: {}", err),
            true,
        )
    })?;
    let truncated = truncate_text(
        extracted_text.trim(),
        config
            .max_content_tokens
            .unwrap_or(WEB_FETCH_DEFAULT_MAX_CONTENT_TOKENS),
    );
    if truncated.is_empty() {
        return Err((
            "unsupported_content_type",
            "Fetched PDF did not contain extractable text".to_string(),
            true,
        ));
    }

    Ok(FetchSuccess {
        retrieved_url: url.as_str().to_string(),
        model_text: build_internal_tool_result_text(url.as_str(), None, &truncated),
        external_document: json!({
            "type": "document",
            "source": {
                "type": "base64",
                "media_type": "application/pdf",
                "data": BASE64_STANDARD.encode(body)
            },
            "citations": {
                "enabled": config.citations_enabled
            }
        }),
    })
}

fn build_text_document(
    text: &str,
    title: Option<&str>,
    citations_enabled: bool,
) -> serde_json::Value {
    let mut document = json!({
        "type": "document",
        "source": {
            "type": "text",
            "media_type": "text/plain",
            "data": text
        },
        "citations": {
            "enabled": citations_enabled
        }
    });
    if let Some(title) = title.filter(|title| !title.trim().is_empty()) {
        document["title"] = json!(title.trim());
    }
    document
}

fn build_internal_tool_result_text(url: &str, title: Option<&str>, text: &str) -> String {
    let mut result = format!("Fetched URL: {}\n", url);
    if let Some(title) = title.filter(|title| !title.trim().is_empty()) {
        result.push_str(&format!("Title: {}\n", title.trim()));
    }
    result.push_str("Content:\n");
    result.push_str(text);
    result
}

fn finalize_web_fetch_response(
    payload: &MessagesRequest,
    content: Vec<serde_json::Value>,
    stop_reason: String,
    fallback_input_tokens: i32,
    total_input_tokens: i32,
    total_output_tokens: i32,
    web_fetch_requests: i32,
    web_search_requests: i32,
) -> Response {
    let final_input_tokens = total_input_tokens.max(fallback_input_tokens).max(1);
    let final_output_tokens = total_output_tokens
        .max(estimate_webfetch_output_tokens(&content))
        .max(1);
    let response_body = create_webfetch_json_response(
        &payload.model,
        content.clone(),
        &normalize_stop_reason(&stop_reason),
        final_input_tokens,
        final_output_tokens,
        web_fetch_requests,
        web_search_requests,
    );

    if !payload.stream {
        return (StatusCode::OK, Json(response_body)).into_response();
    }

    let stream = create_webfetch_sse_stream(
        payload.model.clone(),
        content,
        normalize_stop_reason(&stop_reason),
        final_input_tokens,
        final_output_tokens,
        web_fetch_requests,
        web_search_requests,
    );

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        .header(header::CONNECTION, "keep-alive")
        .body(Body::from_stream(stream))
        .unwrap()
}

fn create_webfetch_json_response(
    model: &str,
    content: Vec<serde_json::Value>,
    stop_reason: &str,
    input_tokens: i32,
    output_tokens: i32,
    web_fetch_requests: i32,
    web_search_requests: i32,
) -> serde_json::Value {
    let message_id = format!(
        "msg_{}",
        Uuid::new_v4().to_string().replace('-', "")[..24].to_string()
    );
    let server_tool_use = build_server_tool_usage(web_fetch_requests, web_search_requests);
    json!({
        "id": message_id,
        "type": "message",
        "role": "assistant",
        "content": content,
        "model": model,
        "stop_reason": stop_reason,
        "stop_sequence": null,
        "usage": {
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "server_tool_use": server_tool_use
        }
    })
}

fn create_webfetch_sse_stream(
    model: String,
    content: Vec<serde_json::Value>,
    stop_reason: String,
    input_tokens: i32,
    output_tokens: i32,
    web_fetch_requests: i32,
    web_search_requests: i32,
) -> impl Stream<Item = Result<Bytes, Infallible>> {
    let events = generate_webfetch_events(
        &model,
        &content,
        &stop_reason,
        input_tokens,
        output_tokens,
        web_fetch_requests,
        web_search_requests,
    );
    stream::iter(
        events
            .into_iter()
            .map(|event| Ok(Bytes::from(event.to_sse_string()))),
    )
}

fn generate_webfetch_events(
    model: &str,
    content: &[serde_json::Value],
    stop_reason: &str,
    input_tokens: i32,
    output_tokens: i32,
    web_fetch_requests: i32,
    web_search_requests: i32,
) -> Vec<SseEvent> {
    let message_id = format!(
        "msg_{}",
        Uuid::new_v4().to_string().replace('-', "")[..24].to_string()
    );
    let mut events = vec![SseEvent::new(
        "message_start",
        json!({
            "type": "message_start",
            "message": {
                "id": message_id,
                "type": "message",
                "role": "assistant",
                "model": model,
                "content": [],
                "stop_reason": null,
                "usage": {
                    "input_tokens": input_tokens,
                    "output_tokens": 0,
                    "cache_creation_input_tokens": 0,
                    "cache_read_input_tokens": 0
                }
            }
        }),
    )];

    for (index, block) in content.iter().enumerate() {
        let idx = index as i32;
        match block.get("type").and_then(|value| value.as_str()) {
            Some("text") => {
                let text = block
                    .get("text")
                    .and_then(|value| value.as_str())
                    .unwrap_or("");
                events.push(SseEvent::new(
                    "content_block_start",
                    json!({
                        "type": "content_block_start",
                        "index": idx,
                        "content_block": {
                            "type": "text",
                            "text": ""
                        }
                    }),
                ));
                for chunk in chunk_text(text, TEXT_CHUNK_SIZE) {
                    events.push(SseEvent::new(
                        "content_block_delta",
                        json!({
                            "type": "content_block_delta",
                            "index": idx,
                            "delta": {
                                "type": "text_delta",
                                "text": chunk
                            }
                        }),
                    ));
                }
                events.push(SseEvent::new(
                    "content_block_stop",
                    json!({
                        "type": "content_block_stop",
                        "index": idx
                    }),
                ));
            }
            Some("thinking") => {
                let thinking = block
                    .get("thinking")
                    .and_then(|value| value.as_str())
                    .unwrap_or("");
                events.push(SseEvent::new(
                    "content_block_start",
                    json!({
                        "type": "content_block_start",
                        "index": idx,
                        "content_block": {
                            "type": "thinking",
                            "thinking": ""
                        }
                    }),
                ));
                for chunk in chunk_text(thinking, TEXT_CHUNK_SIZE) {
                    events.push(SseEvent::new(
                        "content_block_delta",
                        json!({
                            "type": "content_block_delta",
                            "index": idx,
                            "delta": {
                                "type": "thinking_delta",
                                "thinking": chunk
                            }
                        }),
                    ));
                }
                events.push(SseEvent::new(
                    "content_block_delta",
                    json!({
                        "type": "content_block_delta",
                        "index": idx,
                        "delta": {
                            "type": "thinking_delta",
                            "thinking": ""
                        }
                    }),
                ));
                if let Some(signature) = block.get("signature").and_then(|value| value.as_str()) {
                    events.push(SseEvent::new(
                        "content_block_delta",
                        json!({
                            "type": "content_block_delta",
                            "index": idx,
                            "delta": {
                                "type": "signature_delta",
                                "signature": signature
                            }
                        }),
                    ));
                }
                events.push(SseEvent::new(
                    "content_block_stop",
                    json!({
                        "type": "content_block_stop",
                        "index": idx
                    }),
                ));
            }
            Some("server_tool_use") => {
                let id = block
                    .get("id")
                    .and_then(|value| value.as_str())
                    .unwrap_or("");
                let name = block
                    .get("name")
                    .and_then(|value| value.as_str())
                    .unwrap_or("");
                let input = block.get("input").cloned().unwrap_or_else(|| json!({}));
                events.push(SseEvent::new(
                    "content_block_start",
                    json!({
                        "type": "content_block_start",
                        "index": idx,
                        "content_block": {
                            "type": "server_tool_use",
                            "id": id,
                            "name": name
                        }
                    }),
                ));
                events.push(SseEvent::new(
                    "content_block_delta",
                    json!({
                        "type": "content_block_delta",
                        "index": idx,
                        "delta": {
                            "type": "input_json_delta",
                            "partial_json": serde_json::to_string(&input).unwrap_or_else(|_| "{}".to_string())
                        }
                    }),
                ));
                events.push(SseEvent::new(
                    "content_block_stop",
                    json!({
                        "type": "content_block_stop",
                        "index": idx
                    }),
                ));
            }
            Some("web_fetch_tool_result") => {
                events.push(SseEvent::new(
                    "content_block_start",
                    json!({
                        "type": "content_block_start",
                        "index": idx,
                        "content_block": block
                    }),
                ));
                events.push(SseEvent::new(
                    "content_block_stop",
                    json!({
                        "type": "content_block_stop",
                        "index": idx
                    }),
                ));
            }
            _ => {
                events.push(SseEvent::new(
                    "content_block_start",
                    json!({
                        "type": "content_block_start",
                        "index": idx,
                        "content_block": block
                    }),
                ));
                events.push(SseEvent::new(
                    "content_block_stop",
                    json!({
                        "type": "content_block_stop",
                        "index": idx
                    }),
                ));
            }
        }
    }

    events.push(SseEvent::new(
        "message_delta",
        json!({
            "type": "message_delta",
            "delta": {
                "stop_reason": stop_reason
            },
            "usage": {
                "output_tokens": output_tokens,
                "server_tool_use": build_server_tool_usage(web_fetch_requests, web_search_requests)
            }
        }),
    ));
    events.push(SseEvent::new(
        "message_stop",
        json!({ "type": "message_stop" }),
    ));
    events
}

fn normalize_stop_reason(stop_reason: &str) -> String {
    match stop_reason {
        "end_turn" | "max_tokens" | "model_context_window_exceeded" | "tool_use" | "pause_turn" => {
            stop_reason.to_string()
        }
        _ => "end_turn".to_string(),
    }
}

fn parse_fetch_url(raw: &str) -> Option<Url> {
    let mut url = Url::parse(raw.trim()).ok()?;
    if !matches!(url.scheme(), "http" | "https") {
        return None;
    }
    url.set_fragment(None);
    Some(url)
}

fn url_appears_in_context(payload: &MessagesRequest, target_url: &str) -> bool {
    let Some(target) = parse_fetch_url(target_url).map(|url| url.to_string()) else {
        return false;
    };
    collect_context_urls(payload).contains(&target)
}

fn collect_context_urls(payload: &MessagesRequest) -> HashSet<String> {
    let mut urls = HashSet::new();
    if let Some(system) = &payload.system {
        for message in system {
            collect_urls_from_text(&message.text, &mut urls);
        }
    }
    for message in &payload.messages {
        collect_urls_from_value(&message.content, &mut urls);
    }
    urls
}

fn build_server_tool_usage(web_fetch_requests: i32, web_search_requests: i32) -> serde_json::Value {
    let mut usage = serde_json::Map::new();
    if web_fetch_requests > 0 {
        usage.insert("web_fetch_requests".to_string(), json!(web_fetch_requests));
    }
    if web_search_requests > 0 {
        usage.insert(
            "web_search_requests".to_string(),
            json!(web_search_requests),
        );
    }
    serde_json::Value::Object(usage)
}

fn estimate_webfetch_output_tokens(content: &[serde_json::Value]) -> i32 {
    let total = content
        .iter()
        .map(|block| {
            if let Some(text) = block.get("text").and_then(|value| value.as_str()) {
                token::count_tokens(text) as i32
            } else if let Some(thinking) = block.get("thinking").and_then(|value| value.as_str()) {
                token::count_tokens(thinking) as i32
            } else {
                token::count_tokens(&serde_json::to_string(block).unwrap_or_default()) as i32
            }
        })
        .sum::<i32>();
    total.max(1)
}

fn collect_urls_from_value(value: &serde_json::Value, urls: &mut HashSet<String>) {
    match value {
        serde_json::Value::String(text) => collect_urls_from_text(text, urls),
        serde_json::Value::Array(items) => {
            for item in items {
                collect_urls_from_value(item, urls);
            }
        }
        serde_json::Value::Object(map) => {
            if let Some(text) = map.get("text").and_then(|value| value.as_str()) {
                collect_urls_from_text(text, urls);
            }
            if let Some(url) = map.get("url").and_then(|value| value.as_str()) {
                if let Some(normalized) = parse_fetch_url(url).map(|url| url.to_string()) {
                    urls.insert(normalized);
                }
            }
            if let Some(content) = map.get("content") {
                collect_urls_from_value(content, urls);
            }
            if let Some(input) = map.get("input") {
                collect_urls_from_value(input, urls);
            }
            for value in map.values() {
                if !value.is_string() && !value.is_array() && !value.is_object() {
                    continue;
                }
                collect_urls_from_value(value, urls);
            }
        }
        _ => {}
    }
}

fn collect_urls_from_text(text: &str, urls: &mut HashSet<String>) {
    for matched in url_regex().find_iter(text) {
        let candidate = sanitize_url_candidate(matched.as_str());
        if let Some(normalized) = parse_fetch_url(&candidate).map(|url| url.to_string()) {
            urls.insert(normalized);
        }
    }
}

fn sanitize_url_candidate(raw: &str) -> String {
    raw.trim_matches(|c: char| {
        matches!(
            c,
            '.' | ',' | ';' | ':' | '!' | '?' | ')' | ']' | '}' | '"' | '\''
        )
    })
    .to_string()
}

fn url_regex() -> &'static Regex {
    static URL_RE: OnceLock<Regex> = OnceLock::new();
    URL_RE.get_or_init(|| Regex::new(r#"https?://[^\s<>"]+"#).expect("valid URL regex"))
}

fn title_regex() -> &'static Regex {
    static TITLE_RE: OnceLock<Regex> = OnceLock::new();
    TITLE_RE
        .get_or_init(|| Regex::new(r"(?is)<title[^>]*>(.*?)</title>").expect("valid title regex"))
}

fn extract_html_title(html: &str) -> Option<String> {
    title_regex()
        .captures(html)
        .and_then(|captures| captures.get(1))
        .and_then(|value| html2text::from_read(Cursor::new(value.as_str().as_bytes()), 80).ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn url_allowed_by_patterns(
    url: &Url,
    allowed: &[DomainPattern],
    blocked: &[DomainPattern],
) -> bool {
    if !allowed.is_empty() && !allowed.iter().any(|pattern| pattern.matches(url)) {
        return false;
    }
    if blocked.iter().any(|pattern| pattern.matches(url)) {
        return false;
    }
    true
}

impl DomainPattern {
    fn parse(raw: &str) -> Result<Self, String> {
        let value = raw.trim().to_ascii_lowercase();
        if value.is_empty() {
            return Err("web_fetch domain filters cannot be empty".to_string());
        }
        if value.contains("://") {
            return Err(format!(
                "web_fetch domain filter must not include a scheme: {}",
                raw
            ));
        }

        let (host_part, path_part) = match value.split_once('/') {
            Some((host, path)) => (host.trim_end_matches('.'), Some(format!("/{}", path))),
            None => (value.trim_end_matches('.'), None),
        };
        if host_part.is_empty() || host_part.contains('*') {
            return Err(format!("invalid web_fetch domain filter: {}", raw));
        }

        let label_count = host_part.split('.').filter(|part| !part.is_empty()).count();
        if label_count < 2 {
            return Err(format!("invalid web_fetch domain filter: {}", raw));
        }

        let path = match path_part {
            Some(path) if path.matches('*').count() > 1 => {
                return Err(format!("web_fetch path wildcard is invalid: {}", raw));
            }
            Some(path) if path.contains('*') => {
                let (prefix, suffix) = path.split_once('*').unwrap_or((&path, ""));
                Some(PathPattern::Wildcard {
                    prefix: prefix.to_string(),
                    suffix: suffix.to_string(),
                })
            }
            Some(path) => Some(PathPattern::Prefix(path)),
            None => None,
        };

        Ok(Self {
            host: host_part.to_string(),
            include_subdomains: label_count <= 2,
            path,
        })
    }

    fn matches(&self, url: &Url) -> bool {
        let Some(host) = url.host_str() else {
            return false;
        };
        let host = host.to_ascii_lowercase();
        let host_matches = if self.include_subdomains {
            host == self.host || host.ends_with(&format!(".{}", self.host))
        } else {
            host == self.host
        };
        if !host_matches {
            return false;
        }

        match &self.path {
            None => true,
            Some(PathPattern::Prefix(prefix)) => url.path().starts_with(prefix),
            Some(PathPattern::Wildcard { prefix, suffix }) => {
                let path = url.path();
                if !path.starts_with(prefix) {
                    return false;
                }
                path[prefix.len()..].contains(suffix)
            }
        }
    }
}

fn is_pdf_content(content_type: &str, body: &[u8]) -> bool {
    content_type
        .to_ascii_lowercase()
        .starts_with("application/pdf")
        || body.starts_with(b"%PDF-")
}

fn is_text_content_type(content_type: &str, body: &[u8]) -> bool {
    let lower = content_type.to_ascii_lowercase();
    lower.starts_with("text/")
        || lower.contains("html")
        || lower.contains("json")
        || lower.contains("xml")
        || lower.contains("javascript")
        || body.starts_with(b"<!DOCTYPE html")
        || body.starts_with(b"<html")
}

fn truncate_text(text: &str, max_tokens: i32) -> String {
    if text.is_empty() {
        return String::new();
    }
    let char_cap = (max_tokens.max(1) as usize).saturating_mul(4);
    match text.char_indices().nth(char_cap) {
        Some((idx, _)) => format!("{}…", &text[..idx]),
        None => text.to_string(),
    }
}

fn chunk_text(text: &str, size: usize) -> Vec<String> {
    if text.is_empty() {
        return Vec::new();
    }
    text.chars()
        .collect::<Vec<_>>()
        .chunks(size)
        .map(|chunk| chunk.iter().collect())
        .collect()
}

fn generate_server_tool_use_id() -> String {
    format!(
        "srvtoolu_{}",
        Uuid::new_v4().to_string().replace('-', "")[..32].to_string()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::anthropic::types::{Message, MessagesRequest, Tool};
    use axum::{Router, routing::get};
    use serde_json::json;
    use std::collections::HashMap;
    use tokio::net::TcpListener;

    fn sample_request(tool: Tool) -> MessagesRequest {
        MessagesRequest {
            model: "claude-sonnet-4-6".to_string(),
            max_tokens: 512,
            messages: vec![Message {
                role: "user".to_string(),
                content: json!("Fetch https://example.com"),
            }],
            stream: false,
            system: None,
            tools: Some(vec![tool]),
            tool_choice: Some(json!({"type":"auto"})),
            thinking: None,
            output_config: None,
            metadata: None,
        }
    }

    fn sample_request_with_tools(tools: Vec<Tool>) -> MessagesRequest {
        let mut req = sample_request(sample_tool());
        req.tools = Some(tools);
        req
    }

    fn sample_tool() -> Tool {
        Tool {
            tool_type: Some(WEB_FETCH_TOOL_TYPE_20250910.to_string()),
            name: EXTERNAL_WEB_FETCH_TOOL_NAME.to_string(),
            description: String::new(),
            input_schema: HashMap::new(),
            max_uses: Some(2),
            allowed_domains: None,
            blocked_domains: None,
            citations: None,
            max_content_tokens: Some(1024),
        }
    }

    #[test]
    fn test_has_web_fetch_tool_only_single_tool() {
        let req = sample_request(sample_tool());
        assert!(has_web_fetch_tool(&req));
    }

    #[test]
    fn test_has_web_fetch_tool_detects_multi_tool_request() {
        let req = sample_request_with_tools(vec![
            Tool {
                tool_type: Some("web_search_20250305".to_string()),
                name: EXTERNAL_WEB_SEARCH_TOOL_NAME.to_string(),
                description: String::new(),
                input_schema: HashMap::new(),
                max_uses: Some(1),
                allowed_domains: None,
                blocked_domains: None,
                citations: None,
                max_content_tokens: None,
            },
            sample_tool(),
        ]);

        assert!(has_web_fetch_tool(&req));
    }

    #[test]
    fn test_supported_web_fetch_tool_versions() {
        let mut old_tool = sample_tool();
        old_tool.tool_type = Some(WEB_FETCH_TOOL_TYPE_20250910.to_string());
        assert!(is_supported_web_fetch_tool(&old_tool));

        let mut new_tool = sample_tool();
        new_tool.tool_type = Some(WEB_FETCH_TOOL_TYPE_20260209.to_string());
        assert!(is_supported_web_fetch_tool(&new_tool));

        let mut unsupported_tool = sample_tool();
        unsupported_tool.tool_type = Some("web_fetch_20990101".to_string());
        assert!(!is_supported_web_fetch_tool(&unsupported_tool));
    }

    #[test]
    fn test_domain_pattern_matches_root_and_subdomain() {
        let pattern = DomainPattern::parse("example.com/docs").unwrap();
        assert!(pattern.matches(&Url::parse("https://example.com/docs/page").unwrap()));
        assert!(pattern.matches(&Url::parse("https://www.example.com/docs/page").unwrap()));
        assert!(!pattern.matches(&Url::parse("https://www.example.com/blog/page").unwrap()));
    }

    #[test]
    fn test_domain_pattern_specific_subdomain_is_exact() {
        let pattern = DomainPattern::parse("docs.example.com").unwrap();
        assert!(pattern.matches(&Url::parse("https://docs.example.com/page").unwrap()));
        assert!(!pattern.matches(&Url::parse("https://api.example.com/page").unwrap()));
        assert!(!pattern.matches(&Url::parse("https://foo.docs.example.com/page").unwrap()));
    }

    #[test]
    fn test_collect_context_urls_reads_tool_results() {
        let mut req = sample_request(sample_tool());
        req.system = Some(vec![crate::anthropic::types::SystemMessage {
            text: "System URL https://system.example.com/policy".to_string(),
        }]);
        req.messages.push(Message {
            role: "assistant".to_string(),
            content: json!([{
                "type": "web_search_tool_result",
                "content": [{
                    "type": "web_search_result",
                    "url": "https://docs.example.com/rust"
                }]
            }]),
        });

        let urls = collect_context_urls(&req);
        assert!(urls.contains("https://example.com/"));
        assert!(urls.contains("https://system.example.com/policy"));
        assert!(urls.contains("https://docs.example.com/rust"));
    }

    #[test]
    fn test_build_internal_payload_rewrites_server_tools_and_preserves_client_tools() {
        let req = sample_request_with_tools(vec![
            sample_tool(),
            Tool {
                tool_type: Some("web_search_20250305".to_string()),
                name: EXTERNAL_WEB_SEARCH_TOOL_NAME.to_string(),
                description: String::new(),
                input_schema: HashMap::new(),
                max_uses: Some(1),
                allowed_domains: None,
                blocked_domains: None,
                citations: None,
                max_content_tokens: None,
            },
            Tool {
                tool_type: None,
                name: "custom_tool".to_string(),
                description: "custom".to_string(),
                input_schema: HashMap::new(),
                max_uses: None,
                allowed_domains: None,
                blocked_domains: None,
                citations: None,
                max_content_tokens: None,
            },
        ]);

        let rewritten =
            build_internal_webfetch_payload(&req, req.tools.as_ref().unwrap().first().unwrap());
        let tools = rewritten.tools.expect("rewritten tools should exist");

        assert_eq!(tools[0].name, INTERNAL_WEB_FETCH_TOOL_NAME);
        assert_eq!(tools[1].name, INTERNAL_WEB_SEARCH_TOOL_NAME);
        assert_eq!(tools[2].name, "custom_tool");
    }

    #[test]
    fn test_build_text_document_nests_title() {
        let document = build_text_document("Example Domain", Some("Example Title"), true);
        assert_eq!(document["type"], "document");
        assert_eq!(document["title"], "Example Title");
        assert_eq!(document["source"]["data"], "Example Domain");
        assert_eq!(document["citations"]["enabled"], true);
    }

    #[test]
    fn test_create_webfetch_json_response_includes_server_tool_usage() {
        let response = create_webfetch_json_response(
            "claude-sonnet-4-6",
            vec![json!({"type":"text","text":"done"})],
            "end_turn",
            10,
            5,
            1,
            0,
        );

        assert_eq!(
            response["usage"]["server_tool_use"]["web_fetch_requests"],
            1
        );
        assert_eq!(response["content"][0]["type"], "text");
    }

    #[test]
    fn test_generate_webfetch_events_contains_server_tool_blocks() {
        let content = vec![
            json!({"type":"text","text":"I'll fetch that page."}),
            json!({
                "type":"server_tool_use",
                "id":"srvtoolu_test_01",
                "name":"web_fetch",
                "input":{"url":"https://example.com"}
            }),
            json!({
                "type":"web_fetch_tool_result",
                "tool_use_id":"srvtoolu_test_01",
                "content":{
                    "type":"web_fetch_result",
                    "url":"https://example.com",
                    "content": build_text_document("Example Domain", Some("Example Title"), false),
                    "retrieved_at":"2026-04-20T00:00:00Z"
                }
            }),
            json!({"type":"text","text":"Done."}),
        ];

        let events =
            generate_webfetch_events("claude-sonnet-4-6", &content, "end_turn", 12, 34, 1, 0);

        assert!(events.iter().any(|event| {
            event.event == "content_block_start"
                && event.data["content_block"]["type"] == "server_tool_use"
        }));
        assert!(events.iter().any(|event| {
            event.event == "content_block_delta"
                && event.data["delta"]["type"] == "input_json_delta"
        }));
        assert!(events.iter().any(|event| {
            event.event == "content_block_start"
                && event.data["content_block"]["type"] == "web_fetch_tool_result"
        }));
        assert_eq!(
            events
                .iter()
                .find(|event| event.event == "message_delta")
                .expect("message_delta should exist")
                .data["usage"]["server_tool_use"]["web_fetch_requests"],
            1
        );
    }

    #[tokio::test]
    async fn test_execute_web_fetch_html_success() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let app = Router::new().route(
                "/article",
                get(|| async {
                    axum::response::Html(
                        "<html><head><title>Example Article</title></head><body><h1>Hello</h1><p>Fetched content.</p></body></html>",
                    )
                }),
            );
            axum::serve(listener, app).await.unwrap();
        });

        let url = format!("http://{addr}/article");
        let req = MessagesRequest {
            model: "claude-sonnet-4-6".to_string(),
            max_tokens: 512,
            messages: vec![Message {
                role: "user".to_string(),
                content: json!(format!("Read {}", url)),
            }],
            stream: false,
            system: None,
            tools: Some(vec![sample_tool()]),
            tool_choice: Some(json!({"type":"auto"})),
            thinking: None,
            output_config: None,
            metadata: None,
        };
        let config = WebFetchConfig {
            max_uses: None,
            max_content_tokens: Some(1024),
            citations_enabled: true,
            allowed_domains: Vec::new(),
            blocked_domains: Vec::new(),
        };

        let result = execute_web_fetch(&req, &config, &url).await.unwrap();
        assert_eq!(result.retrieved_url, url);
        assert!(result.model_text.contains("Example Article"));
        assert_eq!(result.external_document["type"], "document");
        assert_eq!(result.external_document["title"], "Example Article");
        assert_eq!(result.external_document["citations"]["enabled"], true);

        server.abort();
    }
}
