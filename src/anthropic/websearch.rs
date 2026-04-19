//! WebSearch 工具处理模块
//!
//! 实现 Anthropic WebSearch 请求到 Kiro MCP 的转换和响应生成

use std::convert::Infallible;

use axum::{
    body::Body,
    http::{StatusCode, header},
    response::{IntoResponse, Json, Response},
};
use bytes::Bytes;
use futures::{Stream, stream};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use crate::common::logging::summarize_text_for_log;

use super::stream::SseEvent;
use super::types::{ErrorResponse, MessagesRequest, Tool};

/// MCP 请求
#[derive(Debug, Serialize)]
pub struct McpRequest {
    pub id: String,
    pub jsonrpc: String,
    pub method: String,
    pub params: McpParams,
}

/// MCP 请求参数
#[derive(Debug, Serialize)]
pub struct McpParams {
    pub name: String,
    pub arguments: McpArguments,
}

/// MCP 参数
#[derive(Debug, Serialize)]
pub struct McpArguments {
    pub query: String,
}

/// MCP 响应
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct McpResponse {
    pub error: Option<McpError>,
    pub id: String,
    pub jsonrpc: String,
    pub result: Option<McpResult>,
}

/// MCP 错误
#[derive(Debug, Deserialize)]
pub struct McpError {
    pub code: Option<i32>,
    pub message: Option<String>,
}

/// MCP 结果
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct McpResult {
    pub content: Vec<McpContent>,
    #[serde(rename = "isError")]
    pub is_error: bool,
}

/// MCP 内容
#[derive(Debug, Deserialize)]
pub struct McpContent {
    #[serde(rename = "type")]
    pub content_type: String,
    pub text: String,
}

/// WebSearch 搜索结果
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct WebSearchResults {
    #[serde(default)]
    pub results: Vec<WebSearchResult>,
    #[serde(rename = "totalResults")]
    pub total_results: Option<i32>,
    pub query: Option<String>,
    pub error: Option<String>,
}

/// 单个搜索结果
#[derive(Debug, Deserialize, Clone)]
#[allow(dead_code)]
pub struct WebSearchResult {
    pub title: String,
    pub url: String,
    pub snippet: Option<String>,
    #[serde(rename = "publishedDate")]
    pub published_date: Option<i64>,
    pub id: Option<String>,
    pub domain: Option<String>,
    #[serde(rename = "maxVerbatimWordLimit")]
    pub max_verbatim_word_limit: Option<i32>,
    #[serde(rename = "publicDomain")]
    pub public_domain: Option<bool>,
}

#[derive(Debug)]
enum WebSearchOutcome {
    Results(WebSearchResults),
    Unavailable(String),
    ParseError(String),
}

/// 检查请求是否为纯 WebSearch 请求
///
/// 条件：
/// 1. tools 有且只有一个，且该工具是 Anthropic/Kiro WebSearch 声明
/// 2. 当前消息里能直接提取出文本查询
///
/// 如果最后一条 user turn 只有 tool_result / image 等非文本内容，说明这是一次
/// 工具调用后的 continuation，而不是“直接发起一次 web search”，应回落到普通
/// Anthropic -> Kiro 转换链路处理。
pub fn has_web_search_tool(req: &MessagesRequest) -> bool {
    req.tools
        .as_ref()
        .is_some_and(|tools| {
            tools.len() == 1
                && tools.first().is_some_and(is_web_search_tool)
                && extract_search_query(req).is_some()
        })
}

fn is_web_search_tool(tool: &Tool) -> bool {
    let name = tool.name.trim();
    let tool_type = tool.tool_type.as_deref().unwrap_or("").trim();

    name == "web_search" || tool_type == "web_search" || tool_type.starts_with("web_search_")
}

/// 从消息中提取搜索查询
///
/// 优先读取最后一条 user 消息，并兼容常见 WebSearch 前缀。
pub fn extract_search_query(req: &MessagesRequest) -> Option<String> {
    let msg = req
        .messages
        .iter()
        .rev()
        .find(|msg| msg.role == "user")
        .or_else(|| req.messages.last())?;

    let text = extract_text_content(&msg.content)?;
    normalize_search_query(&text)
}

fn extract_text_content(content: &serde_json::Value) -> Option<String> {
    match content {
        serde_json::Value::String(s) => {
            let text = s.trim();
            if text.is_empty() {
                None
            } else {
                Some(text.to_string())
            }
        }
        serde_json::Value::Array(arr) => {
            let parts: Vec<&str> = arr
                .iter()
                .filter_map(|block| {
                    if block.get("type").and_then(|v| v.as_str()) == Some("text") {
                        block.get("text").and_then(|v| v.as_str())
                    } else {
                        None
                    }
                })
                .filter(|text| !text.trim().is_empty())
                .collect();

            if parts.is_empty() {
                None
            } else {
                Some(parts.join("\n"))
            }
        }
        _ => None,
    }
}

fn normalize_search_query(text: &str) -> Option<String> {
    let text = text.trim();
    if text.is_empty() {
        return None;
    }

    const PREFIXES: &[&str] = &[
        "perform a web search for the query",
        "web search for the query",
        "search for",
        "搜索",
    ];

    let lower = text.to_ascii_lowercase();
    for prefix in PREFIXES {
        if let Some(pos) = lower.find(prefix) {
            let start = pos + prefix.len();
            let query = text[start..]
                .trim_start_matches(|c: char| c == ':' || c == '：' || c.is_whitespace())
                .trim();
            if !query.is_empty() {
                return Some(query.to_string());
            }
        }
    }

    Some(text.to_string())
}

/// 生成22位大小写字母和数字的随机字符串
fn generate_random_id_22() -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    (0..22)
        .map(|_| {
            let idx = fastrand::usize(..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}

/// 生成8位小写字母和数字的随机字符串
fn generate_random_id_8() -> String {
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    (0..8)
        .map(|_| {
            let idx = fastrand::usize(..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}

/// 创建 MCP 请求
///
/// ID 格式: web_search_tooluse_{22位随机}_{毫秒时间戳}_{8位随机}
pub fn create_mcp_request(query: &str) -> (String, McpRequest) {
    let random_22 = generate_random_id_22();
    let timestamp = chrono::Utc::now().timestamp_millis();
    let random_8 = generate_random_id_8();

    let request_id = format!(
        "web_search_tooluse_{}_{}_{}",
        random_22, timestamp, random_8
    );

    // tool_use_id 使用相同格式
    let tool_use_id = format!(
        "srvtoolu_{}",
        Uuid::new_v4().to_string().replace('-', "")[..32].to_string()
    );

    let request = McpRequest {
        id: request_id,
        jsonrpc: "2.0".to_string(),
        method: "tools/call".to_string(),
        params: McpParams {
            name: "web_search".to_string(),
            arguments: McpArguments {
                query: query.to_string(),
            },
        },
    };

    (tool_use_id, request)
}

/// 解析 MCP 响应中的搜索结果
pub fn parse_search_results(mcp_response: &McpResponse) -> Option<WebSearchResults> {
    parse_search_results_checked(mcp_response).ok()
}

fn parse_search_results_checked(mcp_response: &McpResponse) -> Result<WebSearchResults, String> {
    let result = mcp_response
        .result
        .as_ref()
        .ok_or_else(|| "missing MCP result".to_string())?;
    if result.is_error {
        return Err("MCP returned isError=true".to_string());
    }

    let content = result
        .content
        .first()
        .ok_or_else(|| "missing MCP text content".to_string())?;

    if content.content_type != "text" {
        return Err(format!(
            "unsupported MCP content type: {}",
            content.content_type
        ));
    }

    serde_json::from_str(&content.text)
        .map_err(|err| format!("invalid MCP search result JSON: {}", err))
}

/// 生成 WebSearch SSE 响应流
fn create_websearch_sse_stream(
    model: String,
    query: String,
    tool_use_id: String,
    outcome: WebSearchOutcome,
    input_tokens: i32,
) -> impl Stream<Item = Result<Bytes, Infallible>> {
    let events = generate_websearch_events(&model, &query, &tool_use_id, &outcome, input_tokens);

    stream::iter(
        events
            .into_iter()
            .map(|e| Ok(Bytes::from(e.to_sse_string()))),
    )
}

/// 生成 WebSearch SSE 事件序列
fn generate_websearch_events(
    model: &str,
    query: &str,
    tool_use_id: &str,
    outcome: &WebSearchOutcome,
    input_tokens: i32,
) -> Vec<SseEvent> {
    let mut events = Vec::new();
    let message_id = format!(
        "msg_{}",
        Uuid::new_v4().to_string().replace('-', "")[..24].to_string()
    );

    // 1. message_start
    events.push(SseEvent::new(
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
    ));

    // 2. content_block_start (text - 搜索决策说明, index 0)
    let decision_text = format!("I'll search for \"{}\".", query);
    events.push(SseEvent::new(
        "content_block_start",
        json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {
                "type": "text",
                "text": ""
            }
        }),
    ));

    events.push(SseEvent::new(
        "content_block_delta",
        json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {
                "type": "text_delta",
                "text": decision_text
            }
        }),
    ));

    events.push(SseEvent::new(
        "content_block_stop",
        json!({
            "type": "content_block_stop",
            "index": 0
        }),
    ));

    // 3. content_block_start (server_tool_use, index 1)
    // server_tool_use 是服务端工具，input 在 content_block_start 中一次性完整发送，
    // 不像客户端 tool_use 需要通过 input_json_delta 增量传输。
    events.push(SseEvent::new(
        "content_block_start",
        json!({
            "type": "content_block_start",
            "index": 1,
            "content_block": {
                "id": tool_use_id,
                "type": "server_tool_use",
                "name": "web_search",
                "input": {"query": query}
            }
        }),
    ));

    // 4. content_block_stop (server_tool_use)
    events.push(SseEvent::new(
        "content_block_stop",
        json!({
            "type": "content_block_stop",
            "index": 1
        }),
    ));

    // 5. content_block_start (web_search_tool_result, index 2)
    // 官方 API 的 web_search_tool_result 没有 tool_use_id 字段
    let search_content = build_search_content(outcome);

    events.push(SseEvent::new(
        "content_block_start",
        json!({
            "type": "content_block_start",
            "index": 2,
            "content_block": {
                "type": "web_search_tool_result",
                "content": search_content
            }
        }),
    ));

    // 6. content_block_stop (web_search_tool_result)
    events.push(SseEvent::new(
        "content_block_stop",
        json!({
            "type": "content_block_stop",
            "index": 2
        }),
    ));

    // 7. content_block_start (text, index 3)
    events.push(SseEvent::new(
        "content_block_start",
        json!({
            "type": "content_block_start",
            "index": 3,
            "content_block": {
                "type": "text",
                "text": ""
            }
        }),
    ));

    // 8. content_block_delta (text_delta) - 生成搜索结果摘要
    let summary = generate_search_summary(query, outcome);

    // 分块发送文本
    let chunk_size = 100;
    for chunk in summary.chars().collect::<Vec<_>>().chunks(chunk_size) {
        let text: String = chunk.iter().collect();
        events.push(SseEvent::new(
            "content_block_delta",
            json!({
                "type": "content_block_delta",
                "index": 3,
                "delta": {
                    "type": "text_delta",
                    "text": text
                }
            }),
        ));
    }

    // 9. content_block_stop (text)
    events.push(SseEvent::new(
        "content_block_stop",
        json!({
            "type": "content_block_stop",
            "index": 3
        }),
    ));

    // 10. message_delta
    // 官方 API 的 message_delta.delta 中没有 stop_sequence 字段
    let output_tokens = estimate_websearch_output_tokens(&summary);
    events.push(SseEvent::new(
        "message_delta",
        json!({
            "type": "message_delta",
            "delta": {
                "stop_reason": "end_turn"
            },
            "usage": {
                "output_tokens": output_tokens,
                "server_tool_use": {
                    "web_search_requests": 1
                }
            }
        }),
    ));

    // 11. message_stop
    events.push(SseEvent::new(
        "message_stop",
        json!({
            "type": "message_stop"
        }),
    ));

    events
}

/// 生成 WebSearch 非流式 Anthropic Messages 响应
fn create_websearch_json_response(
    model: &str,
    query: &str,
    tool_use_id: &str,
    outcome: &WebSearchOutcome,
    input_tokens: i32,
) -> serde_json::Value {
    let decision_text = format!("I'll search for \"{}\".", query);
    let search_content = build_search_content(outcome);
    let summary = generate_search_summary(query, outcome);
    let content = vec![
        json!({
            "type": "text",
            "text": decision_text
        }),
        json!({
            "id": tool_use_id,
            "type": "server_tool_use",
            "name": "web_search",
            "input": {"query": query}
        }),
        json!({
            "type": "web_search_tool_result",
            "content": search_content
        }),
        json!({
            "type": "text",
            "text": summary
        }),
    ];
    let output_tokens = estimate_websearch_output_tokens(&summary);
    let message_id = format!(
        "msg_{}",
        Uuid::new_v4().to_string().replace('-', "")[..24].to_string()
    );

    json!({
        "id": message_id,
        "type": "message",
        "role": "assistant",
        "content": content,
        "model": model,
        "stop_reason": "end_turn",
        "stop_sequence": null,
        "usage": {
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "server_tool_use": {
                "web_search_requests": 1
            }
        }
    })
}

fn build_search_content(outcome: &WebSearchOutcome) -> Vec<serde_json::Value> {
    match outcome {
        WebSearchOutcome::Results(results) => results
            .results
            .iter()
            .map(|r| {
                let page_age = r.published_date.and_then(|ms| {
                    chrono::DateTime::from_timestamp_millis(ms)
                        .map(|dt| dt.format("%B %-d, %Y").to_string())
                });
                json!({
                    "type": "web_search_result",
                    "title": r.title,
                    "url": r.url,
                    "encrypted_content": r.snippet.clone().unwrap_or_default(),
                    "page_age": page_age
                })
            })
            .collect(),
        WebSearchOutcome::Unavailable(_) | WebSearchOutcome::ParseError(_) => vec![],
    }
}

fn estimate_websearch_output_tokens(summary: &str) -> i32 {
    ((summary.len() as i32 + 3) / 4).max(1)
}

/// 生成搜索结果摘要
fn generate_search_summary(query: &str, outcome: &WebSearchOutcome) -> String {
    let mut summary = format!("Here are the search results for \"{}\":\n\n", query);

    match outcome {
        WebSearchOutcome::Results(results) => {
            if results.results.is_empty() {
                summary.push_str("No results found.\n");
            }

            for (i, result) in results.results.iter().enumerate() {
                summary.push_str(&format!("{}. **{}**\n", i + 1, result.title));
                if let Some(ref snippet) = result.snippet {
                    // 截断过长的摘要（安全处理 UTF-8 多字节字符）
                    let truncated = match snippet.char_indices().nth(200) {
                        Some((idx, _)) => format!("{}...", &snippet[..idx]),
                        None => snippet.clone(),
                    };
                    summary.push_str(&format!("   {}\n", truncated));
                }
                summary.push_str(&format!("   Source: {}\n\n", result.url));
            }
        }
        WebSearchOutcome::Unavailable(error) => {
            summary.push_str(&format!(
                "The web_search tool is currently unavailable: {}\n",
                error
            ));
        }
        WebSearchOutcome::ParseError(error) => {
            summary.push_str(&format!(
                "The web_search tool returned a response that could not be parsed: {}\n",
                error
            ));
        }
    }

    summary.push_str("\nPlease note that these are web search results and may not be fully accurate or up-to-date.");

    summary
}

/// 处理 WebSearch 请求
pub async fn handle_websearch_request(
    provider: std::sync::Arc<crate::kiro::provider::KiroProvider>,
    payload: &MessagesRequest,
    input_tokens: i32,
) -> Response {
    // 1. 提取搜索查询
    let query = match extract_search_query(payload) {
        Some(q) => q,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::new(
                    "invalid_request_error",
                    "无法从消息中提取搜索查询",
                )),
            )
                .into_response();
        }
    };

    tracing::info!(query = %query, "处理 WebSearch 请求");

    // 2. 创建 MCP 请求
    let (tool_use_id, mcp_request) = create_mcp_request(&query);

    // 3. 调用 Kiro MCP API
    let outcome = match call_mcp_api(&provider, &mcp_request).await {
        Ok(response) => match parse_search_results_checked(&response) {
            Ok(results) => {
                if let Some(error) = results.error.as_deref().filter(|e| !e.trim().is_empty()) {
                    tracing::warn!(error = %error, "MCP WebSearch 返回错误结果");
                    WebSearchOutcome::Unavailable(error.to_string())
                } else {
                    WebSearchOutcome::Results(results)
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "MCP WebSearch 结果解析失败");
                WebSearchOutcome::ParseError(e)
            }
        },
        Err(e) => {
            tracing::warn!("MCP API 调用失败: {}", e);
            WebSearchOutcome::Unavailable(e.to_string())
        }
    };

    // 4. 根据 stream 参数生成响应
    let model = payload.model.clone();
    if !payload.stream {
        let response_body =
            create_websearch_json_response(&model, &query, &tool_use_id, &outcome, input_tokens);
        return (StatusCode::OK, Json(response_body)).into_response();
    }

    let stream = create_websearch_sse_stream(model, query, tool_use_id, outcome, input_tokens);

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        .header(header::CONNECTION, "keep-alive")
        .body(Body::from_stream(stream))
        .unwrap()
}

/// 调用 Kiro MCP API
async fn call_mcp_api(
    provider: &crate::kiro::provider::KiroProvider,
    request: &McpRequest,
) -> anyhow::Result<McpResponse> {
    let request_body = serde_json::to_string(request)?;

    tracing::debug!(
        request_id = %request.id,
        method = %request.method,
        request_body_len = request_body.len(),
        "MCP request prepared"
    );

    let response = provider.call_mcp(&request_body).await?;

    let body = response.text().await?;
    tracing::debug!(
        request_id = %request.id,
        response_body_len = body.len(),
        response_summary = %summarize_text_for_log(&body, 200),
        "MCP response body received"
    );

    let mcp_response: McpResponse = serde_json::from_str(&body)?;

    if let Some(ref error) = mcp_response.error {
        anyhow::bail!(
            "MCP error: {} - {}",
            error.code.unwrap_or(-1),
            error.message.as_deref().unwrap_or("Unknown error")
        );
    }

    Ok(mcp_response)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_has_web_search_tool_only_one() {
        use crate::anthropic::types::{Message, Tool};

        let req = MessagesRequest {
            model: "claude-sonnet-4".to_string(),
            max_tokens: 1024,
            messages: vec![Message {
                role: "user".to_string(),
                content: serde_json::json!("test"),
            }],
            stream: true,
            system: None,
            tools: Some(vec![Tool {
                tool_type: Some("web_search_20250305".to_string()),
                name: "web_search".to_string(),
                description: String::new(),
                input_schema: Default::default(),
                max_uses: Some(8),
            }]),
            tool_choice: None,
            thinking: None,
            output_config: None,
            metadata: None,
        };

        assert!(has_web_search_tool(&req));
    }

    #[test]
    fn test_has_web_search_tool_by_type_without_name() {
        use crate::anthropic::types::{Message, Tool};

        let req = MessagesRequest {
            model: "claude-sonnet-4".to_string(),
            max_tokens: 1024,
            messages: vec![Message {
                role: "user".to_string(),
                content: serde_json::json!("test"),
            }],
            stream: true,
            system: None,
            tools: Some(vec![Tool {
                tool_type: Some("web_search".to_string()),
                name: String::new(),
                description: String::new(),
                input_schema: Default::default(),
                max_uses: None,
            }]),
            tool_choice: None,
            thinking: None,
            output_config: None,
            metadata: None,
        };

        assert!(has_web_search_tool(&req));
    }

    #[test]
    fn test_has_web_search_tool_by_name_only() {
        use crate::anthropic::types::{Message, Tool};

        let req = MessagesRequest {
            model: "claude-sonnet-4".to_string(),
            max_tokens: 1024,
            messages: vec![Message {
                role: "user".to_string(),
                content: serde_json::json!("test"),
            }],
            stream: true,
            system: None,
            tools: Some(vec![Tool {
                tool_type: None,
                name: "web_search".to_string(),
                description: String::new(),
                input_schema: Default::default(),
                max_uses: Some(8),
            }]),
            tool_choice: None,
            thinking: None,
            output_config: None,
            metadata: None,
        };

        assert!(has_web_search_tool(&req));
    }

    #[test]
    fn test_has_web_search_tool_multiple_tools() {
        use crate::anthropic::types::{Message, Tool};

        let req = MessagesRequest {
            model: "claude-sonnet-4".to_string(),
            max_tokens: 1024,
            messages: vec![Message {
                role: "user".to_string(),
                content: serde_json::json!("test"),
            }],
            stream: true,
            system: None,
            tools: Some(vec![
                Tool {
                    tool_type: Some("web_search_20250305".to_string()),
                    name: "web_search".to_string(),
                    description: String::new(),
                    input_schema: Default::default(),
                    max_uses: Some(8),
                },
                Tool {
                    tool_type: None,
                    name: "other_tool".to_string(),
                    description: "Other tool".to_string(),
                    input_schema: Default::default(),
                    max_uses: None,
                },
            ]),
            tool_choice: None,
            thinking: None,
            output_config: None,
            metadata: None,
        };

        // 多个工具时不应该被识别为纯 websearch 请求
        assert!(!has_web_search_tool(&req));
    }

    #[test]
    fn test_has_web_search_tool_rejects_tool_result_only_current_turn() {
        use crate::anthropic::types::{Message, Tool};

        let req = MessagesRequest {
            model: "claude-sonnet-4".to_string(),
            max_tokens: 1024,
            messages: vec![
                Message {
                    role: "user".to_string(),
                    content: serde_json::json!("search for rust latest version"),
                },
                Message {
                    role: "assistant".to_string(),
                    content: serde_json::json!([{
                        "type": "tool_use",
                        "id": "toolu_web_01",
                        "name": "web_search",
                        "input": {"query": "rust latest version"}
                    }]),
                },
                Message {
                    role: "user".to_string(),
                    content: serde_json::json!([{
                        "type": "tool_result",
                        "tool_use_id": "toolu_web_01",
                        "content": "Rust 1.90"
                    }]),
                },
            ],
            stream: true,
            system: None,
            tools: Some(vec![Tool {
                tool_type: Some("web_search_20250305".to_string()),
                name: "web_search".to_string(),
                description: String::new(),
                input_schema: Default::default(),
                max_uses: Some(8),
            }]),
            tool_choice: None,
            thinking: None,
            output_config: None,
            metadata: None,
        };

        assert!(!has_web_search_tool(&req));
    }

    #[test]
    fn test_extract_search_query_with_prefix() {
        use crate::anthropic::types::Message;

        let req = MessagesRequest {
            model: "claude-sonnet-4".to_string(),
            max_tokens: 1024,
            messages: vec![Message {
                role: "user".to_string(),
                content: serde_json::json!([{
                    "type": "text",
                    "text": "Perform a web search for the query: rust latest version 2026"
                }]),
            }],
            stream: true,
            system: None,
            tools: None,
            tool_choice: None,
            thinking: None,
            output_config: None,
            metadata: None,
        };

        let query = extract_search_query(&req);
        // 前缀应该被去除
        assert_eq!(query, Some("rust latest version 2026".to_string()));
    }

    #[test]
    fn test_extract_search_query_uses_last_user_message() {
        use crate::anthropic::types::Message;

        let req = MessagesRequest {
            model: "claude-sonnet-4".to_string(),
            max_tokens: 1024,
            messages: vec![
                Message {
                    role: "user".to_string(),
                    content: serde_json::json!("old query"),
                },
                Message {
                    role: "assistant".to_string(),
                    content: serde_json::json!("ok"),
                },
                Message {
                    role: "user".to_string(),
                    content: serde_json::json!("search for latest rust release"),
                },
            ],
            stream: true,
            system: None,
            tools: None,
            tool_choice: None,
            thinking: None,
            output_config: None,
            metadata: None,
        };

        let query = extract_search_query(&req);
        assert_eq!(query, Some("latest rust release".to_string()));
    }

    #[test]
    fn test_extract_search_query_joins_multiple_text_blocks() {
        use crate::anthropic::types::Message;

        let req = MessagesRequest {
            model: "claude-sonnet-4".to_string(),
            max_tokens: 1024,
            messages: vec![Message {
                role: "user".to_string(),
                content: serde_json::json!([
                    {"type": "text", "text": "search for"},
                    {"type": "text", "text": "Kiro MCP web_search"}
                ]),
            }],
            stream: true,
            system: None,
            tools: None,
            tool_choice: None,
            thinking: None,
            output_config: None,
            metadata: None,
        };

        let query = extract_search_query(&req);
        assert_eq!(query, Some("Kiro MCP web_search".to_string()));
    }

    #[test]
    fn test_extract_search_query_chinese_prefix() {
        use crate::anthropic::types::Message;

        let req = MessagesRequest {
            model: "claude-sonnet-4".to_string(),
            max_tokens: 1024,
            messages: vec![Message {
                role: "user".to_string(),
                content: serde_json::json!("请帮我搜索 Rust 2026 最新版本"),
            }],
            stream: true,
            system: None,
            tools: None,
            tool_choice: None,
            thinking: None,
            output_config: None,
            metadata: None,
        };

        let query = extract_search_query(&req);
        assert_eq!(query, Some("Rust 2026 最新版本".to_string()));
    }

    #[test]
    fn test_extract_search_query_plain_text() {
        use crate::anthropic::types::Message;

        let req = MessagesRequest {
            model: "claude-sonnet-4".to_string(),
            max_tokens: 1024,
            messages: vec![Message {
                role: "user".to_string(),
                content: serde_json::json!("What is the weather today?"),
            }],
            stream: true,
            system: None,
            tools: None,
            tool_choice: None,
            thinking: None,
            output_config: None,
            metadata: None,
        };

        let query = extract_search_query(&req);
        assert_eq!(query, Some("What is the weather today?".to_string()));
    }

    #[test]
    fn test_create_mcp_request() {
        let (tool_use_id, request) = create_mcp_request("test query");

        assert!(tool_use_id.starts_with("srvtoolu_"));
        assert_eq!(request.jsonrpc, "2.0");
        assert_eq!(request.method, "tools/call");
        assert_eq!(request.params.name, "web_search");
        assert_eq!(request.params.arguments.query, "test query");

        // 验证 ID 格式: web_search_tooluse_{22位}_{时间戳}_{8位}
        assert!(request.id.starts_with("web_search_tooluse_"));
    }

    #[test]
    fn test_mcp_request_id_format() {
        let (_, request) = create_mcp_request("test");

        // 格式: web_search_tooluse_{22位}_{毫秒时间戳}_{8位}
        let id = &request.id;
        assert!(id.starts_with("web_search_tooluse_"));

        let suffix = &id["web_search_tooluse_".len()..];
        let parts: Vec<&str> = suffix.split('_').collect();
        assert_eq!(parts.len(), 3, "应该有3个部分: 22位随机_时间戳_8位随机");

        // 第一部分: 22位大小写字母和数字
        assert_eq!(parts[0].len(), 22);
        assert!(parts[0].chars().all(|c| c.is_ascii_alphanumeric()));

        // 第二部分: 毫秒时间戳
        assert!(parts[1].parse::<i64>().is_ok());

        // 第三部分: 8位小写字母和数字
        assert_eq!(parts[2].len(), 8);
        assert!(
            parts[2]
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        );
    }

    #[test]
    fn test_parse_search_results() {
        let response = McpResponse {
            error: None,
            id: "test_id".to_string(),
            jsonrpc: "2.0".to_string(),
            result: Some(McpResult {
                content: vec![McpContent {
                    content_type: "text".to_string(),
                    text: r#"{"results":[{"title":"Test","url":"https://example.com","snippet":"Test snippet"}],"totalResults":1}"#.to_string(),
                }],
                is_error: false,
            }),
        };

        let results = parse_search_results(&response);
        assert!(results.is_some());
        let results = results.unwrap();
        assert_eq!(results.results.len(), 1);
        assert_eq!(results.results[0].title, "Test");
    }

    #[test]
    fn test_parse_search_results_returns_none_for_is_error() {
        let response = McpResponse {
            error: None,
            id: "test_id".to_string(),
            jsonrpc: "2.0".to_string(),
            result: Some(McpResult {
                content: vec![McpContent {
                    content_type: "text".to_string(),
                    text: r#"{"results":[{"title":"Ignored","url":"https://example.com"}]}"#
                        .to_string(),
                }],
                is_error: true,
            }),
        };

        assert!(parse_search_results(&response).is_none());
    }

    #[test]
    fn test_generate_search_summary() {
        let results = WebSearchResults {
            results: vec![WebSearchResult {
                title: "Test Result".to_string(),
                url: "https://example.com".to_string(),
                snippet: Some("This is a test snippet".to_string()),
                published_date: None,
                id: None,
                domain: None,
                max_verbatim_word_limit: None,
                public_domain: None,
            }],
            total_results: Some(1),
            query: Some("test".to_string()),
            error: None,
        };

        let summary = generate_search_summary("test", &WebSearchOutcome::Results(results));

        assert!(summary.contains("Test Result"));
        assert!(summary.contains("https://example.com"));
        assert!(summary.contains("This is a test snippet"));
    }

    #[test]
    fn test_generate_search_summary_reports_mcp_unavailable() {
        let summary = generate_search_summary(
            "test",
            &WebSearchOutcome::Unavailable("network error".to_string()),
        );

        assert!(summary.contains("web_search tool is currently unavailable"));
        assert!(summary.contains("network error"));
    }

    #[test]
    fn test_generate_websearch_events_stream_shape() {
        let results = WebSearchResults {
            results: vec![WebSearchResult {
                title: "Rust 2026".to_string(),
                url: "https://example.com/rust".to_string(),
                snippet: Some("Rust release notes summary".to_string()),
                published_date: Some(1_767_225_600_000),
                id: None,
                domain: None,
                max_verbatim_word_limit: None,
                public_domain: None,
            }],
            total_results: Some(1),
            query: Some("rust".to_string()),
            error: None,
        };

        let events = generate_websearch_events(
            "claude-sonnet-4",
            "rust latest release",
            "srvtoolu_test",
            &WebSearchOutcome::Results(results),
            321,
        );

        assert_eq!(events.first().unwrap().event, "message_start");
        assert_eq!(events.last().unwrap().event, "message_stop");

        let tool_use_start = events
            .iter()
            .find(|event| {
                event.event == "content_block_start"
                    && event.data["index"] == 1
                    && event.data["content_block"]["type"] == "server_tool_use"
            })
            .expect("server_tool_use block should exist");
        assert_eq!(tool_use_start.data["content_block"]["name"], "web_search");
        assert_eq!(
            tool_use_start.data["content_block"]["input"]["query"],
            "rust latest release"
        );

        let tool_result_start = events
            .iter()
            .find(|event| {
                event.event == "content_block_start"
                    && event.data["index"] == 2
                    && event.data["content_block"]["type"] == "web_search_tool_result"
            })
            .expect("web_search_tool_result block should exist");
        assert_eq!(
            tool_result_start.data["content_block"]["content"][0]["title"],
            "Rust 2026"
        );
        assert_eq!(
            tool_result_start.data["content_block"]["content"][0]["page_age"],
            "January 1, 2026"
        );

        let message_delta = events
            .iter()
            .find(|event| event.event == "message_delta")
            .expect("message_delta should exist");
        assert_eq!(message_delta.data["delta"]["stop_reason"], "end_turn");
        assert_eq!(
            message_delta.data["usage"]["server_tool_use"]["web_search_requests"],
            1
        );
        assert!(
            message_delta.data["usage"]["output_tokens"]
                .as_i64()
                .is_some_and(|tokens| tokens > 0)
        );
    }

    #[test]
    fn test_create_websearch_json_response_non_stream() {
        let results = WebSearchResults {
            results: vec![WebSearchResult {
                title: "Test Result".to_string(),
                url: "https://example.com".to_string(),
                snippet: Some("This is a test snippet".to_string()),
                published_date: None,
                id: None,
                domain: None,
                max_verbatim_word_limit: None,
                public_domain: None,
            }],
            total_results: Some(1),
            query: Some("test".to_string()),
            error: None,
        };

        let response = create_websearch_json_response(
            "claude-sonnet-4",
            "test",
            "srvtoolu_test",
            &WebSearchOutcome::Results(results),
            123,
        );

        assert_eq!(response["type"], "message");
        assert_eq!(response["role"], "assistant");
        assert_eq!(response["model"], "claude-sonnet-4");
        assert_eq!(response["stop_reason"], "end_turn");
        assert_eq!(response["usage"]["input_tokens"], 123);
        assert_eq!(
            response["usage"]["server_tool_use"]["web_search_requests"],
            1
        );

        let content = response["content"].as_array().expect("content array");
        assert_eq!(content[1]["type"], "server_tool_use");
        assert_eq!(content[1]["name"], "web_search");
        assert_eq!(content[1]["input"]["query"], "test");
        assert_eq!(content[2]["type"], "web_search_tool_result");
        assert_eq!(content[2]["content"][0]["title"], "Test Result");
    }

    #[test]
    fn test_create_websearch_json_response_unavailable_has_empty_result_block() {
        let response = create_websearch_json_response(
            "claude-sonnet-4",
            "test",
            "srvtoolu_test",
            &WebSearchOutcome::Unavailable("network error".to_string()),
            12,
        );

        let content = response["content"].as_array().expect("content array");
        assert_eq!(content[2]["type"], "web_search_tool_result");
        assert_eq!(content[2]["content"], serde_json::json!([]));
        assert!(
            content[3]["text"]
                .as_str()
                .is_some_and(|text| text.contains("network error"))
        );
    }
}
