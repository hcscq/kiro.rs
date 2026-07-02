//! Metadata events for generateAssistantResponse streams.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::kiro::parser::error::ParseResult;
use crate::kiro::parser::frame::Frame;

use super::base::EventPayload;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct MetadataEvent {
    #[serde(default)]
    pub stop_reason: Option<String>,

    #[serde(flatten)]
    #[serde(skip_serializing)]
    #[allow(dead_code)]
    extra: HashMap<String, serde_json::Value>,
}

impl EventPayload for MetadataEvent {
    fn from_frame(frame: &Frame) -> ParseResult<Self> {
        frame.payload_as_json()
    }
}
