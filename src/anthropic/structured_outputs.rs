use std::collections::VecDeque;
use std::sync::{Mutex, OnceLock};

use serde_json::Value;

use super::types::{Message, MessagesRequest};

pub(crate) const MAX_JSON_SCHEMA_RETRIES: usize = 1;
pub(crate) const MAX_JSON_SCHEMA_BYTES: usize = 256 * 1024;
pub(crate) const MAX_JSON_SCHEMA_DEPTH: usize = 96;
pub(crate) const MAX_JSON_SCHEMA_NODES: usize = 20_000;
pub(crate) const MAX_JSON_SCHEMA_COMBINATOR_BRANCHES: usize = 512;
pub(crate) const MAX_JSON_SCHEMA_PROPERTIES: usize = 4_096;

const MAX_VALIDATOR_CACHE_ENTRIES: usize = 64;

#[derive(Debug, Clone)]
pub(crate) struct JsonSchemaOutput {
    pub schema: Value,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct SchemaStats {
    pub bytes: usize,
    pub max_depth: usize,
    pub nodes: usize,
    pub combinator_branches: usize,
    pub properties: usize,
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
    let stats = schema_stats(&output.schema);
    if stats.bytes > MAX_JSON_SCHEMA_BYTES {
        return Err(format!(
            "output_config.format.schema is too large: {} bytes exceeds the {} byte limit",
            stats.bytes, MAX_JSON_SCHEMA_BYTES
        ));
    }
    if stats.max_depth > MAX_JSON_SCHEMA_DEPTH {
        return Err(format!(
            "output_config.format.schema is too deeply nested: depth {} exceeds the {} level limit",
            stats.max_depth, MAX_JSON_SCHEMA_DEPTH
        ));
    }
    if stats.nodes > MAX_JSON_SCHEMA_NODES {
        return Err(format!(
            "output_config.format.schema is too complex: {} JSON nodes exceeds the {} node limit",
            stats.nodes, MAX_JSON_SCHEMA_NODES
        ));
    }
    if stats.combinator_branches > MAX_JSON_SCHEMA_COMBINATOR_BRANCHES {
        return Err(format!(
            "output_config.format.schema is too complex: {} oneOf/anyOf/allOf branches exceeds the {} branch limit",
            stats.combinator_branches, MAX_JSON_SCHEMA_COMBINATOR_BRANCHES
        ));
    }
    if stats.properties > MAX_JSON_SCHEMA_PROPERTIES {
        return Err(format!(
            "output_config.format.schema has too many properties: {} exceeds the {} property limit",
            stats.properties, MAX_JSON_SCHEMA_PROPERTIES
        ));
    }
    if contains_external_ref(&output.schema) {
        return Err(
            "output_config.format.schema contains an external $ref, which is not supported"
                .to_string(),
        );
    }
    cached_validator_for(&output.schema)
        .map(|_| ())
        .map_err(|err| format!("invalid output_config.format.schema: {err}"))
}

pub(crate) fn schema_stats(schema: &Value) -> SchemaStats {
    let mut stats = SchemaStats {
        bytes: serde_json::to_string(schema)
            .map(|text| text.len())
            .unwrap_or_default(),
        ..SchemaStats::default()
    };
    collect_schema_stats(schema, 1, &mut stats);
    stats
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
    let validator = cached_validator_for(&output.schema)
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
    let schema_hint = json_schema_output(payload)
        .map(|output| retry_schema_hint(&output.schema))
        .filter(|hint| !hint.is_empty())
        .unwrap_or_default();
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
             Repair the response using the user's original facts and return only the corrected JSON value. \
             {schema_hint}\
             Do not include Markdown, prose, labels, XML tags, or code fences. \
             The first non-whitespace character must be '{{' or '[' and the last non-whitespace character must close that same JSON value."
        )),
    });
    retry
}

fn cached_validator_for(schema: &Value) -> Result<jsonschema::Validator, String> {
    let key = serde_json::to_string(schema).map_err(|err| err.to_string())?;
    let cache = validator_cache();

    {
        let mut guard = lock_validator_cache(cache);
        if let Some(pos) = guard.iter().position(|entry| entry.schema == key) {
            let entry = guard.remove(pos).expect("cache position should exist");
            let validator = entry.validator.clone();
            guard.push_front(entry);
            return Ok(validator);
        }
    }

    let validator = jsonschema::validator_for(schema).map_err(|err| err.to_string())?;

    let mut guard = lock_validator_cache(cache);
    if let Some(pos) = guard.iter().position(|entry| entry.schema == key) {
        let entry = guard.remove(pos).expect("cache position should exist");
        let validator = entry.validator.clone();
        guard.push_front(entry);
        return Ok(validator);
    }
    guard.push_front(CachedValidator {
        schema: key,
        validator: validator.clone(),
    });
    while guard.len() > MAX_VALIDATOR_CACHE_ENTRIES {
        guard.pop_back();
    }

    Ok(validator)
}

fn validator_cache() -> &'static Mutex<VecDeque<CachedValidator>> {
    static VALIDATOR_CACHE: OnceLock<Mutex<VecDeque<CachedValidator>>> = OnceLock::new();
    VALIDATOR_CACHE.get_or_init(|| Mutex::new(VecDeque::new()))
}

fn lock_validator_cache(
    cache: &'static Mutex<VecDeque<CachedValidator>>,
) -> std::sync::MutexGuard<'static, VecDeque<CachedValidator>> {
    cache
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[derive(Clone)]
struct CachedValidator {
    schema: String,
    validator: jsonschema::Validator,
}

fn collect_schema_stats(value: &Value, depth: usize, stats: &mut SchemaStats) {
    stats.nodes += 1;
    stats.max_depth = stats.max_depth.max(depth);
    if depth > MAX_JSON_SCHEMA_DEPTH
        || stats.nodes > MAX_JSON_SCHEMA_NODES
        || stats.properties > MAX_JSON_SCHEMA_PROPERTIES
        || stats.combinator_branches > MAX_JSON_SCHEMA_COMBINATOR_BRANCHES
    {
        return;
    }

    match value {
        Value::Object(map) => {
            if let Some(Value::Object(properties)) = map.get("properties") {
                stats.properties += properties.len();
            }
            for key in ["oneOf", "anyOf", "allOf"] {
                if let Some(Value::Array(branches)) = map.get(key) {
                    stats.combinator_branches += branches.len();
                }
            }
            for child in map.values() {
                collect_schema_stats(child, depth + 1, stats);
            }
        }
        Value::Array(values) => {
            for child in values {
                collect_schema_stats(child, depth + 1, stats);
            }
        }
        _ => {}
    }
}

fn retry_schema_hint(schema: &Value) -> String {
    let Some(map) = schema.as_object() else {
        return String::new();
    };

    let mut parts = Vec::new();
    if let Some(schema_type) = map.get("type").and_then(Value::as_str) {
        parts.push(format!("Expected root type: {schema_type}."));
    }
    if let Some(required) = map.get("required").and_then(Value::as_array) {
        let keys = required
            .iter()
            .filter_map(Value::as_str)
            .take(24)
            .collect::<Vec<_>>();
        if !keys.is_empty() {
            let suffix = if required.len() > keys.len() {
                ", ..."
            } else {
                ""
            };
            parts.push(format!(
                "Required top-level keys: {}{suffix}.",
                keys.join(", ")
            ));
        }
    }
    if let Some(properties) = map.get("properties").and_then(Value::as_object) {
        let keys = properties.keys().take(32).cloned().collect::<Vec<_>>();
        if !keys.is_empty() {
            let suffix = if properties.len() > keys.len() {
                ", ..."
            } else {
                ""
            };
            parts.push(format!(
                "Allowed top-level keys include: {}{suffix}.",
                keys.join(", ")
            ));
        }
    }
    if map.get("additionalProperties") == Some(&Value::Bool(false)) {
        parts.push("Do not add extra top-level keys.".to_string());
    }

    if parts.is_empty() {
        String::new()
    } else {
        format!("{} ", parts.join(" "))
    }
}

fn contains_external_ref(value: &Value) -> bool {
    match value {
        Value::Object(map) => {
            if let Some(reference) = map.get("$ref").and_then(Value::as_str) {
                let trimmed = reference.trim();
                if !trimmed.is_empty() && !trimmed.starts_with('#') {
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

    #[test]
    fn rejects_oversized_json_schema() {
        let mut properties = serde_json::Map::new();
        for idx in 0..(MAX_JSON_SCHEMA_PROPERTIES + 1) {
            properties.insert(format!("field_{idx}"), json!({"type": "string"}));
        }
        let output = JsonSchemaOutput {
            schema: json!({
                "type": "object",
                "properties": properties
            }),
        };

        let err = validate_json_schema_output(&output).unwrap_err();

        assert!(err.contains("too many properties"));
    }

    #[test]
    fn rejects_deeply_nested_json_schema_without_full_traversal() {
        let mut schema = json!({"type": "string"});
        for _ in 0..(MAX_JSON_SCHEMA_DEPTH + 8) {
            schema = json!({"type": "array", "items": schema});
        }
        let output = JsonSchemaOutput { schema };

        let err = validate_json_schema_output(&output).unwrap_err();

        assert!(err.contains("too deeply nested"));
    }

    #[test]
    fn allows_internal_ref_and_rejects_external_ref() {
        let internal = JsonSchemaOutput {
            schema: json!({
                "$defs": {"name": {"type": "string"}},
                "type": "object",
                "properties": {"name": {"$ref": "#/$defs/name"}},
                "required": ["name"]
            }),
        };
        validate_json_schema_output(&internal).unwrap();

        let external = JsonSchemaOutput {
            schema: json!({
                "type": "object",
                "properties": {"name": {"$ref": "common.json#/$defs/name"}}
            }),
        };
        let err = validate_json_schema_output(&external).unwrap_err();

        assert!(err.contains("external $ref"));
    }

    #[test]
    fn retry_payload_includes_schema_repair_hint() {
        let mut payload = MessagesRequest {
            model: "claude-sonnet-4-6".to_string(),
            max_tokens: 256,
            messages: vec![Message {
                role: "user".to_string(),
                content: Value::String("Extract the lead".to_string()),
            }],
            stream: false,
            system: None,
            tools: None,
            tool_choice: None,
            thinking: None,
            output_config: None,
            metadata: None,
        };
        payload.output_config = Some(super::super::types::OutputConfig {
            effort: "high".to_string(),
            format: Some(super::super::types::OutputFormat {
                format_type: "json_schema".to_string(),
                schema: Some(json!({
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "email": {"type": "string"}
                    },
                    "required": ["name", "email"],
                    "additionalProperties": false
                })),
            }),
        });

        let retry = build_retry_payload(
            &payload,
            "Name: Ada",
            &StructuredOutputError::MissingJsonText,
        );
        let repair_message = retry.messages.last().unwrap().content.as_str().unwrap();

        assert!(repair_message.contains("Required top-level keys: name, email."));
        assert!(repair_message.contains("Do not add extra top-level keys."));
        assert!(repair_message.contains("The first non-whitespace character"));
    }
}
