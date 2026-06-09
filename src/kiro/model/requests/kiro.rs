//! Kiro 请求类型定义
//!
//! 定义 Kiro API 的主请求结构

use serde::{Deserialize, Serialize};

use super::conversation::ConversationState;

/// Kiro additional model request fields.
///
/// These fields are validated directly against the model's additional_fields
/// schema. For Opus adaptive effort, the wire shape is
/// `additionalModelRequestFields.output_config.effort`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct AdditionalModelRequestFields {
    #[serde(flatten)]
    pub fields: serde_json::Map<String, serde_json::Value>,
}

impl AdditionalModelRequestFields {
    pub fn with_field(key: impl Into<String>, value: impl Into<serde_json::Value>) -> Self {
        let mut fields = serde_json::Map::new();
        fields.insert(key.into(), value.into());
        Self { fields }
    }
}

/// Kiro API 请求
///
/// 用于构建发送给 Kiro API 的请求
///
/// # 示例
///
/// ```rust
/// use kiro_rs::kiro::model::requests::{
///     KiroRequest, ConversationState, CurrentMessage, UserInputMessage, Tool
/// };
///
/// // 创建简单请求
/// let state = ConversationState::new("conv-123")
///     .with_agent_task_type("vibe")
///     .with_current_message(CurrentMessage::new(
///         UserInputMessage::new("Hello", "claude-3-5-sonnet")
///     ));
///
/// let request = KiroRequest::new(state);
/// let json = request.to_json().unwrap();
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KiroRequest {
    /// 对话状态
    pub conversation_state: ConversationState,
    /// Additional model request fields, such as `output_config.effort`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub additional_model_request_fields: Option<AdditionalModelRequestFields>,
    /// Profile ARN（可选）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_arn: Option<String>,
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_kiro_request_deserialize() {
        let json = r#"{
            "conversationState": {
                "conversationId": "conv-456",
                "currentMessage": {
                    "userInputMessage": {
                        "content": "Test message",
                        "modelId": "claude-3-5-sonnet",
                        "userInputMessageContext": {}
                    }
                }
            }
        }"#;

        let request: KiroRequest = serde_json::from_str(json).unwrap();
        assert_eq!(request.conversation_state.conversation_id, "conv-456");
        assert_eq!(
            request
                .conversation_state
                .current_message
                .user_input_message
                .content,
            "Test message"
        );
    }
}
