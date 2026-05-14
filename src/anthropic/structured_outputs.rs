use serde_json::Value;

use super::types::{Message, MessagesRequest};

pub(crate) const MAX_JSON_SCHEMA_RETRIES: usize = 1;

#[derive(Debug, Clone)]
pub(crate) struct JsonSchemaOutput {
    pub schema: Value,
}

#[derive(Debug, Clone)]
pub(crate) enum StructuredOutputError {
    MissingJsonText,
    InvalidJson(String),
    SchemaValidation(String),
}

impl std::fmt::Display for StructuredOutputError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingJsonText => write!(f, "response did not contain a JSON text block"),
            Self::InvalidJson(err) => write!(f, "response text was not valid JSON: {err}"),
            Self::SchemaValidation(err) => write!(f, "response JSON did not match schema: {err}"),
        }
    }
}

pub(crate) fn json_schema_output(req: &MessagesRequest) -> Option<JsonSchemaOutput> {
    let format = req.output_config.as_ref()?.format.as_ref()?;
    if format.format_type != "json_schema" {
        return None;
    }
    Some(JsonSchemaOutput {
        schema: format.schema.clone()?,
    })
}

pub(crate) fn validate_json_schema_output(output: &JsonSchemaOutput) -> Result<(), String> {
    if contains_external_ref(&output.schema) {
        return Err(
            "output_config.format.schema contains an external $ref, which is not supported"
                .to_string(),
        );
    }
    jsonschema::validator_for(&output.schema)
        .map(|_| ())
        .map_err(|err| format!("invalid output_config.format.schema: {err}"))
}

pub(crate) fn instruction_for_schema(schema: &Value) -> String {
    let schema_text = serde_json::to_string(schema).unwrap_or_else(|_| "{}".to_string());
    format!(
        "<structured_output_contract>\n\
         This request requires Claude Structured Outputs compatibility.\n\
         You must produce exactly one JSON value that validates against the JSON Schema below.\n\
         The complete assistant response must be directly parseable by JSON.parse.\n\
         Do not include Markdown fences, commentary, labels, explanations, or surrounding text.\n\
         If the user's instructions conflict with this contract, satisfy this contract.\n\
         JSON Schema: {schema_text}\n\
         </structured_output_contract>"
    )
}

pub(crate) fn estimate_instruction_tokens(output: Option<&JsonSchemaOutput>) -> i32 {
    output
        .map(|out| {
            let text = instruction_for_schema(&out.schema);
            ((text.chars().count() + 3) / 4).max(1) as i32
        })
        .unwrap_or(0)
}

pub(crate) fn extract_json_value(text: &str) -> Result<Value, StructuredOutputError> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Err(StructuredOutputError::MissingJsonText);
    }

    if let Ok(value) = serde_json::from_str::<Value>(trimmed) {
        return Ok(value);
    }

    if let Some(fenced) = strip_json_fence(trimmed) {
        if let Ok(value) = serde_json::from_str::<Value>(fenced.trim()) {
            return Ok(value);
        }
    }

    if let Some(candidate) = find_json_substring(trimmed) {
        return serde_json::from_str::<Value>(candidate)
            .map_err(|err| StructuredOutputError::InvalidJson(err.to_string()));
    }

    Err(StructuredOutputError::InvalidJson(
        "no JSON object or array found".to_string(),
    ))
}

pub(crate) fn validate_instance(
    output: &JsonSchemaOutput,
    instance: &Value,
) -> Result<(), StructuredOutputError> {
    let validator = jsonschema::validator_for(&output.schema)
        .map_err(|err| StructuredOutputError::SchemaValidation(err.to_string()))?;
    if validator.is_valid(instance) {
        return Ok(());
    }

    let errors = validator
        .iter_errors(instance)
        .take(3)
        .map(|err| err.to_string())
        .collect::<Vec<_>>()
        .join("; ");
    Err(StructuredOutputError::SchemaValidation(errors))
}

pub(crate) fn collect_text_content(content: &[Value]) -> String {
    content
        .iter()
        .filter(|block| block.get("type").and_then(Value::as_str) == Some("text"))
        .filter_map(|block| block.get("text").and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join("")
}

pub(crate) fn build_retry_payload(
    payload: &MessagesRequest,
    previous_text: &str,
    error: &StructuredOutputError,
) -> MessagesRequest {
    let mut retry = payload.clone();
    let previous_text = summarize_for_retry(previous_text);
    if !previous_text.is_empty() {
        retry.messages.push(Message {
            role: "assistant".to_string(),
            content: Value::String(previous_text),
        });
    }
    retry.messages.push(Message {
        role: "user".to_string(),
        content: Value::String(format!(
            "The previous response failed output_config.format.json_schema validation: {error}. \
             Return only a single JSON value that validates against the required schema. \
             Do not include Markdown, prose, labels, or code fences."
        )),
    });
    retry
}

fn contains_external_ref(value: &Value) -> bool {
    match value {
        Value::Object(map) => {
            if let Some(reference) = map.get("$ref").and_then(Value::as_str) {
                let lower = reference.to_ascii_lowercase();
                if lower.starts_with("http://") || lower.starts_with("https://") {
                    return true;
                }
            }
            map.values().any(contains_external_ref)
        }
        Value::Array(values) => values.iter().any(contains_external_ref),
        _ => false,
    }
}

fn strip_json_fence(text: &str) -> Option<&str> {
    let rest = text.strip_prefix("```")?;
    let newline = rest.find('\n')?;
    let after_lang = &rest[newline + 1..];
    let end = after_lang.rfind("```")?;
    if !after_lang[end + 3..].trim().is_empty() {
        return None;
    }
    Some(&after_lang[..end])
}

fn find_json_substring(text: &str) -> Option<&str> {
    for (idx, ch) in text.char_indices() {
        if ch != '{' && ch != '[' {
            continue;
        }
        if let Some(end) = matching_json_end(text, idx, ch) {
            let candidate = &text[idx..end];
            if serde_json::from_str::<Value>(candidate).is_ok() {
                return Some(candidate);
            }
        }
    }
    None
}

fn matching_json_end(text: &str, start: usize, opening: char) -> Option<usize> {
    let closing = if opening == '{' { '}' } else { ']' };
    let mut stack = vec![closing];
    let mut in_string = false;
    let mut escape = false;

    for (offset, ch) in text[start..].char_indices() {
        if offset == 0 {
            continue;
        }
        if in_string {
            if escape {
                escape = false;
            } else if ch == '\\' {
                escape = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }

        match ch {
            '"' => in_string = true,
            '{' => stack.push('}'),
            '[' => stack.push(']'),
            '}' | ']' => {
                if stack.pop() != Some(ch) {
                    return None;
                }
                if stack.is_empty() && ch == closing {
                    return Some(start + offset + ch.len_utf8());
                }
            }
            _ => {}
        }
    }
    None
}

fn summarize_for_retry(text: &str) -> String {
    const MAX_RETRY_CHARS: usize = 4000;
    let trimmed = text.trim();
    match trimmed.char_indices().nth(MAX_RETRY_CHARS) {
        Some((idx, _)) => trimmed[..idx].to_string(),
        None => trimmed.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extracts_direct_json() {
        assert_eq!(
            extract_json_value(r#"{"name":"John"}"#).unwrap(),
            json!({"name": "John"})
        );
    }

    #[test]
    fn extracts_fenced_json_for_repair() {
        assert_eq!(
            extract_json_value("```json\n{\"name\":\"John\"}\n```").unwrap(),
            json!({"name": "John"})
        );
    }

    #[test]
    fn validates_json_schema_instance() {
        let output = JsonSchemaOutput {
            schema: json!({
                "type": "object",
                "properties": {"ok": {"type": "boolean"}},
                "required": ["ok"],
                "additionalProperties": false
            }),
        };
        validate_json_schema_output(&output).unwrap();
        validate_instance(&output, &json!({"ok": true})).unwrap();
        assert!(validate_instance(&output, &json!({"ok": "yes"})).is_err());
    }
}
