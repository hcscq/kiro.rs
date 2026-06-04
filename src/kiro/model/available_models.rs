//! Kiro ListAvailableModels API response model.

use serde::Deserialize;

/// Prompt caching capability metadata returned with an available model.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct AvailableModelPromptCaching {
    #[serde(default)]
    pub maximum_cache_checkpoints_per_request: Option<i64>,
    #[serde(default)]
    pub minimum_tokens_per_cache_checkpoint: Option<i64>,
    #[serde(default)]
    pub supports_prompt_caching: Option<bool>,
}

/// Token limits returned with an available model.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct AvailableModelTokenLimits {
    #[serde(default)]
    pub max_input_tokens: Option<i64>,
    #[serde(default)]
    pub max_output_tokens: Option<i64>,
}

/// Single model entry returned by ListAvailableModels.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct AvailableModel {
    pub model_id: String,
    #[serde(default)]
    pub model_name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub provider: Option<String>,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub context_window: Option<i64>,
    #[serde(default)]
    pub is_default: Option<bool>,
    #[serde(default)]
    pub rate_multiplier: Option<f64>,
    #[serde(default)]
    pub rate_unit: Option<String>,
    #[serde(default)]
    pub prompt_caching: Option<AvailableModelPromptCaching>,
    #[serde(default)]
    pub supported_input_types: Vec<String>,
    #[serde(default)]
    pub token_limits: Option<AvailableModelTokenLimits>,
}

/// ListAvailableModels response.
///
/// Kiro has returned both `availableModels` and `models`; accept both shapes.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListAvailableModelsResponse {
    #[serde(default, alias = "models")]
    pub available_models: Vec<AvailableModel>,
    #[serde(default)]
    pub next_token: Option<String>,
    #[serde(default)]
    pub default_model: Option<AvailableModel>,
}

impl ListAvailableModelsResponse {
    pub fn model_ids(&self) -> Vec<String> {
        let mut ids: Vec<String> = self
            .available_models
            .iter()
            .map(|model| model.model_id.trim())
            .filter(|model_id| !model_id.is_empty())
            .map(str::to_string)
            .collect();

        if let Some(default_model) = &self.default_model {
            let model_id = default_model.model_id.trim();
            if !model_id.is_empty() {
                ids.push(model_id.to_string());
            }
        }

        ids.sort();
        ids.dedup();
        ids
    }
}

#[cfg(test)]
mod tests {
    use super::ListAvailableModelsResponse;

    #[test]
    fn parses_available_models_shape() {
        let response: ListAvailableModelsResponse = serde_json::from_str(
            r#"{
                "availableModels": [
                    {"modelId": "claude-sonnet-4.5"}
                ],
                "defaultModel": {"modelId": "claude-haiku-4.5"}
            }"#,
        )
        .unwrap();

        assert_eq!(
            response.model_ids(),
            vec![
                "claude-haiku-4.5".to_string(),
                "claude-sonnet-4.5".to_string()
            ]
        );
    }

    #[test]
    fn parses_models_alias_shape() {
        let response: ListAvailableModelsResponse = serde_json::from_str(
            r#"{
                "models": [
                    {"modelId": "claude-opus-4.6"},
                    {"modelId": "claude-opus-4.6"}
                ]
            }"#,
        )
        .unwrap();

        assert_eq!(response.model_ids(), vec!["claude-opus-4.6".to_string()]);
    }
}
