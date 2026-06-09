//! 推理内容事件
//!
//! 处理 reasoningContentEvent 类型的事件。

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::kiro::parser::error::ParseResult;
use crate::kiro::parser::frame::Frame;

use super::base::EventPayload;

/// 推理内容事件。
///
/// Opus 4.7/4.8 thinking 上游会通过该事件流式返回 reasoning 文本，
/// 并在末尾返回对应 signature。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReasoningContentEvent {
    /// 推理文本片段
    #[serde(default)]
    pub text: String,

    /// 上游返回的推理签名
    #[serde(default)]
    pub signature: Option<String>,

    /// 捕获其他未使用字段，确保反序列化兼容性
    #[serde(flatten)]
    #[serde(skip_serializing)]
    #[allow(dead_code)]
    extra: HashMap<String, serde_json::Value>,
}

impl EventPayload for ReasoningContentEvent {
    fn from_frame(frame: &Frame) -> ParseResult<Self> {
        frame.payload_as_json()
    }
}

impl Default for ReasoningContentEvent {
    fn default() -> Self {
        Self {
            text: String::new(),
            signature: None,
            extra: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_text() {
        let event: ReasoningContentEvent = serde_json::from_str(r#"{"text":"step one"}"#).unwrap();
        assert_eq!(event.text, "step one");
        assert!(event.signature.is_none());
    }

    #[test]
    fn test_deserialize_signature() {
        let event: ReasoningContentEvent = serde_json::from_str(r#"{"signature":"sig"}"#).unwrap();
        assert_eq!(event.text, "");
        assert_eq!(event.signature.as_deref(), Some("sig"));
    }
}
