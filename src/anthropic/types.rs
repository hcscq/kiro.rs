//! Anthropic API 类型定义

use crate::model::config::RequestWeightingConfig;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// === 错误响应 ===

/// API 错误响应
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    #[serde(rename = "type")]
    pub response_type: &'static str,
    pub error: ErrorDetail,
}

/// 错误详情
#[derive(Debug, Serialize)]
pub struct ErrorDetail {
    #[serde(rename = "type")]
    pub error_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    pub message: String,
}

impl ErrorResponse {
    /// 创建新的错误响应
    pub fn new(error_type: impl Into<String>, message: impl Into<String>) -> Self {
        Self::with_optional_code(error_type, None, message)
    }

    /// 创建带错误码的错误响应
    pub fn with_code(
        error_type: impl Into<String>,
        code: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self::with_optional_code(error_type, Some(code.into()), message)
    }

    fn with_optional_code(
        error_type: impl Into<String>,
        code: Option<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            response_type: "error",
            error: ErrorDetail {
                error_type: error_type.into(),
                code,
                message: message.into(),
            },
        }
    }

    /// 创建上下文窗口超限错误响应
    pub fn context_length_exceeded() -> Self {
        Self::with_code(
            "invalid_request_error",
            "context_length_exceeded",
            "prompt is too long: context window is full. Reduce conversation history, system prompt, or tools.",
        )
    }

    /// 创建认证错误响应
    pub fn authentication_error() -> Self {
        Self::new("authentication_error", "Invalid API key")
    }
}

#[cfg(test)]
mod error_response_tests {
    use super::ErrorResponse;

    #[test]
    fn context_length_exceeded_serializes_claude_code_compatible_shape() {
        let json = serde_json::to_value(ErrorResponse::context_length_exceeded()).unwrap();

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

    #[test]
    fn ordinary_errors_omit_code() {
        let json =
            serde_json::to_value(ErrorResponse::new("api_error", "upstream failed")).unwrap();

        assert_eq!(json["type"], "error");
        assert!(json["error"].get("code").is_none());
    }
}

// === Models 端点类型 ===

/// 模型信息
#[derive(Debug, Serialize)]
pub struct Model {
    pub id: String,
    pub object: String,
    pub created: i64,
    pub owned_by: String,
    pub display_name: String,
    #[serde(rename = "type")]
    pub model_type: String,
    pub max_tokens: i32,
}

/// 模型列表响应
#[derive(Debug, Serialize)]
pub struct ModelsResponse {
    pub object: String,
    pub data: Vec<Model>,
}

// === Messages 端点类型 ===

/// 最大思考预算 tokens
const MAX_BUDGET_TOKENS: i32 = 24576;

/// Thinking 配置
#[derive(Debug, Deserialize, Clone)]
pub struct Thinking {
    #[serde(rename = "type")]
    pub thinking_type: String,
    #[serde(default)]
    pub display: Option<String>,
    #[serde(
        default = "default_budget_tokens",
        deserialize_with = "deserialize_budget_tokens"
    )]
    pub budget_tokens: i32,
}

impl Thinking {
    /// 是否启用了 thinking（enabled 或 adaptive）
    pub fn is_enabled(&self) -> bool {
        self.thinking_type == "enabled" || self.thinking_type == "adaptive"
    }
}

fn default_budget_tokens() -> i32 {
    20000
}
fn deserialize_budget_tokens<'de, D>(deserializer: D) -> Result<i32, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = i32::deserialize(deserializer)?;
    Ok(value.min(MAX_BUDGET_TOKENS))
}

/// OutputConfig 配置
#[derive(Debug, Deserialize, Clone)]
pub struct OutputConfig {
    #[serde(default = "default_effort")]
    pub effort: String,
    #[serde(default)]
    pub format: Option<OutputFormat>,
    #[serde(default, skip)]
    pub effort_explicit: bool,
}

/// Claude Structured Outputs 的 format 配置
#[derive(Debug, Deserialize, Clone)]
pub struct OutputFormat {
    #[serde(rename = "type")]
    pub format_type: String,
    #[serde(default)]
    pub schema: Option<serde_json::Value>,
}

fn default_effort() -> String {
    "high".to_string()
}

/// Claude Code 请求中的 metadata
#[derive(Debug, Clone, Deserialize)]
pub struct Metadata {
    /// 用户 ID，格式如: user_xxx_account__session_0b4445e1-f5be-49e1-87ce-62bbc28ad705
    pub user_id: Option<String>,
}

/// Messages 请求体
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct MessagesRequest {
    pub model: String,
    pub max_tokens: i32,
    pub messages: Vec<Message>,
    pub stream: bool,
    pub system: Option<Vec<SystemMessage>>,
    pub tools: Option<Vec<Tool>>,
    pub tool_choice: Option<serde_json::Value>,
    pub thinking: Option<Thinking>,
    pub output_config: Option<OutputConfig>,
    /// Claude Code 请求中的 metadata，包含 session 信息
    pub metadata: Option<Metadata>,
}

impl<'de> Deserialize<'de> for MessagesRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawOutputConfig {
            #[serde(default)]
            effort: Option<String>,
            #[serde(default)]
            format: Option<OutputFormat>,
        }

        #[derive(Deserialize)]
        struct RawReasoningConfig {
            #[serde(default)]
            effort: Option<String>,
        }

        #[derive(Deserialize)]
        struct RawMessagesRequest {
            model: String,
            max_tokens: i32,
            messages: Vec<Message>,
            #[serde(default)]
            stream: bool,
            #[serde(default, deserialize_with = "deserialize_system")]
            system: Option<Vec<SystemMessage>>,
            tools: Option<Vec<Tool>>,
            tool_choice: Option<serde_json::Value>,
            thinking: Option<Thinking>,
            output_config: Option<RawOutputConfig>,
            reasoning: Option<RawReasoningConfig>,
            metadata: Option<Metadata>,
        }

        let raw = RawMessagesRequest::deserialize(deserializer)?;
        let reasoning_effort = raw.reasoning.and_then(|reasoning| reasoning.effort);
        let output_config = match raw.output_config {
            Some(config) => {
                let effort_explicit = config.effort.is_some() || reasoning_effort.is_some();
                Some(OutputConfig {
                    effort: config
                        .effort
                        .or(reasoning_effort)
                        .unwrap_or_else(default_effort),
                    format: config.format,
                    effort_explicit,
                })
            }
            None => reasoning_effort.map(|effort| OutputConfig {
                effort,
                format: None,
                effort_explicit: true,
            }),
        };

        Ok(Self {
            model: raw.model,
            max_tokens: raw.max_tokens,
            messages: raw.messages,
            stream: raw.stream,
            system: raw.system,
            tools: raw.tools,
            tool_choice: raw.tool_choice,
            thinking: raw.thinking,
            output_config,
            metadata: raw.metadata,
        })
    }
}

impl MessagesRequest {
    pub fn request_weight(&self, estimated_input_tokens: Option<i32>) -> f64 {
        self.request_weight_with_config(&RequestWeightingConfig::default(), estimated_input_tokens)
    }

    pub fn request_weight_with_config(
        &self,
        config: &RequestWeightingConfig,
        estimated_input_tokens: Option<i32>,
    ) -> f64 {
        if !config.enabled {
            return 1.0;
        }

        let mut weight = config.base_weight;

        if self.tools.as_ref().is_some_and(|tools| !tools.is_empty()) || self.tool_choice.is_some()
        {
            weight += config.tools_bonus;
        }

        if self.max_tokens >= config.large_max_tokens_threshold {
            weight += config.large_max_tokens_bonus;
        }

        if let Some(input_tokens) = estimated_input_tokens {
            if input_tokens >= config.very_large_input_tokens_threshold {
                weight += config.very_large_input_tokens_bonus;
            } else if input_tokens >= config.large_input_tokens_threshold {
                weight += config.large_input_tokens_bonus;
            }
        }

        if let Some(thinking) = &self.thinking {
            if thinking.is_enabled() {
                weight += config.thinking_bonus;
                if thinking.budget_tokens >= config.heavy_thinking_budget_threshold {
                    weight += config.heavy_thinking_budget_bonus;
                }
            }
        }

        weight.clamp(
            config.base_weight,
            config.max_weight.max(config.base_weight),
        )
    }
}

/// 反序列化 system 字段，支持字符串或数组格式
fn deserialize_system<'de, D>(deserializer: D) -> Result<Option<Vec<SystemMessage>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    // 创建一个 visitor 来处理 string 或 array
    struct SystemVisitor;

    impl<'de> serde::de::Visitor<'de> for SystemVisitor {
        type Value = Option<Vec<SystemMessage>>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a string or an array of system messages")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(Some(vec![SystemMessage {
                text: value.to_string(),
            }]))
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let mut messages = Vec::new();
            while let Some(msg) = seq.next_element()? {
                messages.push(msg);
            }
            Ok(if messages.is_empty() {
                None
            } else {
                Some(messages)
            })
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Ok(None)
        }

        fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            serde::de::Deserialize::deserialize(deserializer)
        }
    }

    deserializer.deserialize_any(SystemVisitor)
}

/// 消息
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Message {
    pub role: String,
    /// 可以是 string 或 ContentBlock 数组
    pub content: serde_json::Value,
}

/// 系统消息
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SystemMessage {
    pub text: String,
}

/// 工具定义
///
/// 支持三种格式：
/// 1. 普通工具：{ name, description, input_schema }
/// 2. WebSearch 工具：{ type: "web_search_20250305", name: "web_search", max_uses: 8 }
/// 3. WebFetch 工具：{ type: "web_fetch_20250910"、"web_fetch_20260209" 或 "web_fetch_20260309", name: "web_fetch", max_uses, ... }
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct ToolCitations {
    #[serde(default)]
    pub enabled: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct Tool {
    /// 工具类型，如 "web_search_20250305"（可选，仅 WebSearch 工具）
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub tool_type: Option<String>,
    /// 工具名称
    #[serde(default)]
    pub name: String,
    /// 工具描述（普通工具必需，WebSearch 工具可选）
    #[serde(default)]
    pub description: String,
    /// 输入参数 schema（普通工具必需，WebSearch 工具无此字段）
    #[serde(default, deserialize_with = "deserialize_input_schema")]
    pub input_schema: HashMap<String, serde_json::Value>,
    /// 最大使用次数（仅 WebSearch 工具）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_uses: Option<i32>,
    /// WebFetch 允许抓取的域名/路径模式
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allowed_domains: Option<Vec<String>>,
    /// WebFetch 禁止抓取的域名/路径模式
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocked_domains: Option<Vec<String>>,
    /// WebFetch 是否启用 citations
    #[serde(skip_serializing_if = "Option::is_none")]
    pub citations: Option<ToolCitations>,
    /// WebFetch 抓取内容的最大 token 数
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_content_tokens: Option<i32>,
    /// WebFetch 是否使用缓存内容；20260309 版本支持 false 以绕过缓存
    #[serde(skip_serializing_if = "Option::is_none")]
    pub use_cache: Option<bool>,
}

fn deserialize_input_schema<'de, D>(
    deserializer: D,
) -> Result<HashMap<String, serde_json::Value>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(
        Option::<HashMap<String, serde_json::Value>>::deserialize(deserializer)?
            .unwrap_or_default(),
    )
}

/// 内容块
#[derive(Debug, Deserialize, Serialize)]
pub struct ContentBlock {
    #[serde(rename = "type")]
    pub block_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thinking: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub redacted_thinking: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_use_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_error: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<ImageSource>,
    #[serde(
        default,
        alias = "documentUrl",
        skip_serializing_if = "Option::is_none"
    )]
    pub document_url: Option<DocumentUrlSource>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource: Option<ResourceSource>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
}

/// 图片或文档数据源
#[derive(Debug, Deserialize, Serialize)]
pub struct ImageSource {
    #[serde(rename = "type")]
    #[serde(default)]
    pub source_type: String,
    #[serde(default)]
    pub media_type: String,
    #[serde(default)]
    pub data: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

/// document_url 数据源
#[derive(Debug, Deserialize, Serialize)]
pub struct DocumentUrlSource {
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub data: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default, alias = "mimeType")]
    pub mime_type: Option<String>,
}

/// ACP/resource 数据源
#[derive(Debug, Deserialize, Serialize)]
pub struct ResourceSource {
    #[serde(default)]
    pub blob: Option<String>,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default, alias = "mimeType")]
    pub mime_type: Option<String>,
    #[serde(default)]
    pub uri: Option<String>,
}

// === Count Tokens 端点类型 ===

/// Token 计数请求
#[derive(Debug, Serialize, Deserialize)]
pub struct CountTokensRequest {
    pub model: String,
    pub messages: Vec<Message>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_system"
    )]
    pub system: Option<Vec<SystemMessage>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<Tool>>,
}

/// Token 计数响应
#[derive(Debug, Serialize, Deserialize)]
pub struct CountTokensResponse {
    pub input_tokens: i32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::config::RequestWeightingConfig;
    use serde_json::json;

    fn sample_request() -> MessagesRequest {
        MessagesRequest {
            model: "claude-sonnet-4.5".to_string(),
            max_tokens: 1024,
            messages: vec![Message {
                role: "user".to_string(),
                content: json!("hello"),
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
    fn test_tool_input_schema_null_deserializes_as_empty_map() {
        let request: MessagesRequest = serde_json::from_value(json!({
            "model": "claude-sonnet-4.5",
            "max_tokens": 1024,
            "messages": [
                {"role": "user", "content": "hello"}
            ],
            "tools": [
                {
                    "name": "example_tool",
                    "description": "example",
                    "input_schema": null
                }
            ]
        }))
        .expect("null input_schema should be accepted for compatibility");

        let tools = request.tools.expect("tools should deserialize");
        assert!(tools[0].input_schema.is_empty());
    }

    #[test]
    fn test_reasoning_effort_deserializes_as_output_config_effort() {
        let request: MessagesRequest = serde_json::from_value(json!({
            "model": "claude-opus-4.8",
            "max_tokens": 1024,
            "messages": [{"role": "user", "content": "hello"}],
            "thinking": {"type": "adaptive"},
            "reasoning": {"effort": "medium"}
        }))
        .expect("reasoning.effort should deserialize");

        assert_eq!(
            request
                .output_config
                .as_ref()
                .map(|config| config.effort.as_str()),
            Some("medium")
        );
        assert!(
            request
                .output_config
                .as_ref()
                .is_some_and(|config| config.effort_explicit)
        );
    }

    #[test]
    fn test_output_config_effort_deserializes_as_output_config_effort() {
        let request: MessagesRequest = serde_json::from_value(json!({
            "model": "claude-opus-4.8",
            "max_tokens": 1024,
            "messages": [{"role": "user", "content": "hello"}],
            "thinking": {"type": "adaptive"},
            "output_config": {"effort": "low"}
        }))
        .expect("output_config.effort should deserialize");

        assert_eq!(
            request
                .output_config
                .as_ref()
                .map(|config| config.effort.as_str()),
            Some("low")
        );
        assert!(
            request
                .output_config
                .as_ref()
                .is_some_and(|config| config.effort_explicit)
        );
    }

    #[test]
    fn test_output_config_effort_takes_precedence_over_reasoning_effort() {
        let request: MessagesRequest = serde_json::from_value(json!({
            "model": "claude-opus-4.8",
            "max_tokens": 1024,
            "messages": [{"role": "user", "content": "hello"}],
            "thinking": {"type": "adaptive"},
            "reasoning": {"effort": "low"},
            "output_config": {"effort": "high"}
        }))
        .expect("nested output_config effort should deserialize");

        assert_eq!(
            request
                .output_config
                .as_ref()
                .map(|config| config.effort.as_str()),
            Some("high")
        );
        assert!(
            request
                .output_config
                .as_ref()
                .is_some_and(|config| config.effort_explicit)
        );
    }

    #[test]
    fn test_output_config_without_effort_uses_default_without_explicit_effort() {
        let request: MessagesRequest = serde_json::from_value(json!({
            "model": "claude-opus-4.8",
            "max_tokens": 1024,
            "messages": [{"role": "user", "content": "hello"}],
            "thinking": {"type": "adaptive"},
            "output_config": {"format": {"type": "text"}}
        }))
        .expect("output_config without effort should deserialize");

        let output_config = request
            .output_config
            .as_ref()
            .expect("output_config should be preserved");
        assert_eq!(output_config.effort, "high");
        assert!(!output_config.effort_explicit);
    }

    #[test]
    fn test_messages_request_weight_defaults_to_light_request() {
        let request = sample_request();
        assert!((request.request_weight(Some(512)) - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_messages_request_weight_defaults_to_light_request_when_disabled() {
        let mut request = sample_request();
        request.max_tokens = 8_192;
        request.tools = Some(vec![Tool {
            tool_type: None,
            name: "edit".to_string(),
            description: "edit files".to_string(),
            input_schema: HashMap::new(),
            max_uses: None,
            ..Tool::default()
        }]);
        request.thinking = Some(Thinking {
            thinking_type: "enabled".to_string(),
            display: None,
            budget_tokens: 20_000,
        });

        let config = RequestWeightingConfig {
            enabled: false,
            ..RequestWeightingConfig::default()
        };

        assert!(
            (request.request_weight_with_config(&config, Some(24_000)) - 1.0).abs() < f64::EPSILON
        );
    }

    #[test]
    fn test_messages_request_weight_clamps_heavy_request_to_max_when_enabled() {
        let mut request = sample_request();
        request.max_tokens = 8_192;
        request.tools = Some(vec![Tool {
            tool_type: None,
            name: "edit".to_string(),
            description: "edit files".to_string(),
            input_schema: HashMap::new(),
            max_uses: None,
            ..Tool::default()
        }]);
        request.thinking = Some(Thinking {
            thinking_type: "enabled".to_string(),
            display: None,
            budget_tokens: 32_000,
        });

        let config = RequestWeightingConfig {
            enabled: true,
            ..RequestWeightingConfig::default()
        };

        assert!(
            (request.request_weight_with_config(&config, Some(32_000)) - 2.5).abs() < f64::EPSILON
        );
    }

    #[test]
    fn test_messages_request_weight_can_be_disabled_by_config() {
        let mut request = sample_request();
        request.max_tokens = 8_192;
        request.tool_choice = Some(json!({"type": "auto"}));
        request.thinking = Some(Thinking {
            thinking_type: "enabled".to_string(),
            display: None,
            budget_tokens: 20_000,
        });

        let config = RequestWeightingConfig {
            enabled: false,
            ..RequestWeightingConfig::default()
        };

        assert!(
            (request.request_weight_with_config(&config, Some(24_000)) - 1.0).abs() < f64::EPSILON
        );
    }
}
