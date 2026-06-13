use std::fs::{self, OpenOptions};
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::types::MessagesRequest;

const DEFAULT_CAPTURE_400_DIR: &str = "/app/diagnostics/400-bodies";
const DEFAULT_CAPTURE_400_MAX_PER_CLASS: usize = 3;
const DEFAULT_CAPTURE_400_TTL_HOURS: u64 = 24;
const DEFAULT_CAPTURE_400_MAX_TOTAL_BYTES: u64 = 256 * 1024 * 1024;
const INVALID_MULTIMODAL_CLASS: &str = "invalid_multimodal_url";

#[derive(Debug, Clone, PartialEq, Eq)]
struct Local400CaptureConfig {
    enabled: bool,
    dir: PathBuf,
    max_per_class: usize,
    ttl: Duration,
    max_total_bytes: u64,
}

impl Local400CaptureConfig {
    fn disabled() -> Self {
        Self {
            enabled: false,
            dir: PathBuf::from(DEFAULT_CAPTURE_400_DIR),
            max_per_class: DEFAULT_CAPTURE_400_MAX_PER_CLASS,
            ttl: Duration::from_secs(DEFAULT_CAPTURE_400_TTL_HOURS * 60 * 60),
            max_total_bytes: DEFAULT_CAPTURE_400_MAX_TOTAL_BYTES,
        }
    }

    fn from_env() -> Self {
        let enabled = std::env::var("KIRO_CAPTURE_400_BODIES")
            .ok()
            .is_some_and(|value| env_bool_enabled(&value));
        let mut config = Self::disabled();
        config.enabled = enabled;
        config.dir = std::env::var("KIRO_CAPTURE_400_DIR")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_CAPTURE_400_DIR));
        config.max_per_class = env_usize_or(
            "KIRO_CAPTURE_400_MAX_PER_CLASS",
            DEFAULT_CAPTURE_400_MAX_PER_CLASS,
        )
        .min(50);
        let ttl_hours = env_u64_or("KIRO_CAPTURE_400_TTL_HOURS", DEFAULT_CAPTURE_400_TTL_HOURS);
        config.ttl = Duration::from_secs(ttl_hours.saturating_mul(60 * 60));
        config.max_total_bytes = env_u64_or(
            "KIRO_CAPTURE_400_MAX_TOTAL_BYTES",
            DEFAULT_CAPTURE_400_MAX_TOTAL_BYTES,
        );
        config
    }
}

#[derive(Debug)]
struct CaptureFilePair {
    meta_path: PathBuf,
    body_path: PathBuf,
    modified: SystemTime,
    bytes: u64,
}

pub(crate) fn capture_invalid_multimodal_request(
    request_id: &str,
    route: &'static str,
    payload: &MessagesRequest,
    original_body_bytes: usize,
    error: &str,
) {
    let config = Local400CaptureConfig::from_env();
    if !config.enabled || config.max_per_class == 0 {
        return;
    }

    if let Err(err) = capture_invalid_multimodal_request_inner(
        &config,
        request_id,
        route,
        payload,
        original_body_bytes,
        error,
    ) {
        tracing::warn!(
            request_id = %request_id,
            error = %err,
            "保存本地 400 请求体诊断失败"
        );
    }
}

fn capture_invalid_multimodal_request_inner(
    config: &Local400CaptureConfig,
    request_id: &str,
    route: &'static str,
    payload: &MessagesRequest,
    original_body_bytes: usize,
    error: &str,
) -> anyhow::Result<()> {
    let base_dir = &config.dir;
    let class_dir = base_dir.join(INVALID_MULTIMODAL_CLASS);
    fs::create_dir_all(&class_dir)?;
    set_private_dir_permissions(base_dir);
    set_private_dir_permissions(&class_dir);

    let request_body =
        serde_json::to_string(payload).context("serialize invalid multimodal request")?;
    let request_hash = sha256_hex_bytes(request_body.as_bytes());
    let name = format!(
        "{}-{}-{}",
        now_millis(),
        sanitize_capture_component(request_id, 96),
        &request_hash[..16]
    );
    let body_path = class_dir.join(format!("{name}.body.json"));
    let meta_path = class_dir.join(format!("{name}.meta.json"));

    let body_saved = should_save_body(request_body.len(), config.max_total_bytes);
    if body_saved {
        write_private_file(&body_path, request_body.as_bytes())?;
    }

    let metadata = serde_json::json!({
        "captured_at": chrono::Utc::now().to_rfc3339(),
        "request_id": request_id,
        "route": route,
        "model": payload.model,
        "stream": payload.stream,
        "status_code": 400,
        "error_class": INVALID_MULTIMODAL_CLASS,
        "error_summary": error,
        "original_request_body_bytes": original_body_bytes,
        "serialized_request_body_bytes": request_body.len(),
        "request_sha256": request_hash,
        "body_saved": body_saved,
        "body_file": if body_saved { Some(body_path.file_name().and_then(|name| name.to_str()).unwrap_or_default()) } else { None },
        "body_omitted_reason": if body_saved { None } else { Some("request body exceeds KIRO_CAPTURE_400_MAX_TOTAL_BYTES") },
        "max_per_class": config.max_per_class,
        "ttl_seconds": config.ttl.as_secs(),
        "max_total_bytes": config.max_total_bytes,
        "message_count": payload.messages.len(),
        "multimodal_reference_summary": summarize_multimodal_references(payload),
    });
    let metadata_bytes = serde_json::to_vec_pretty(&metadata)?;
    if let Err(err) = write_private_file(&meta_path, &metadata_bytes) {
        if body_saved {
            let _ = fs::remove_file(&body_path);
        }
        return Err(err.into());
    }

    prune_capture_class(&class_dir, config.max_per_class, config.ttl);
    prune_capture_expired_in_tree(base_dir, config.ttl);
    prune_capture_total(base_dir, config.max_total_bytes);

    tracing::warn!(
        request_id = %request_id,
        error_class = INVALID_MULTIMODAL_CLASS,
        body_saved,
        original_body_bytes,
        serialized_request_body_bytes = request_body.len(),
        capture_dir = %class_dir.display(),
        "已保存本地 400 请求体诊断样本"
    );

    Ok(())
}

fn should_save_body(body_len: usize, max_total_bytes: u64) -> bool {
    max_total_bytes == 0 || body_len as u64 <= max_total_bytes
}

fn summarize_multimodal_references(payload: &MessagesRequest) -> Value {
    let mut openai_image_url_blocks = 0usize;
    let mut anthropic_image_url_blocks = 0usize;
    let mut document_url_blocks = 0usize;
    let mut anthropic_document_url_blocks = 0usize;
    let mut tail_reference_summaries = Vec::new();

    for (message_index, message) in payload.messages.iter().enumerate() {
        let Value::Array(blocks) = &message.content else {
            continue;
        };
        for (block_index, block) in blocks.iter().enumerate() {
            let Value::Object(obj) = block else {
                continue;
            };
            match obj.get("type").and_then(Value::as_str) {
                Some("image_url") => {
                    openai_image_url_blocks += 1;
                    if let Some(reference) = openai_image_reference(obj) {
                        push_reference_summary(
                            &mut tail_reference_summaries,
                            message_index,
                            block_index,
                            "image_url",
                            reference,
                        );
                    }
                }
                Some("image") => {
                    let reference = obj
                        .get("source")
                        .and_then(Value::as_object)
                        .filter(|source| source.get("type").and_then(Value::as_str) == Some("url"))
                        .and_then(|source| {
                            source
                                .get("url")
                                .and_then(Value::as_str)
                                .or_else(|| source.get("data").and_then(Value::as_str))
                        });
                    if let Some(reference) = reference {
                        anthropic_image_url_blocks += 1;
                        push_reference_summary(
                            &mut tail_reference_summaries,
                            message_index,
                            block_index,
                            "image.source.url",
                            reference,
                        );
                    }
                }
                Some("document_url") | Some("documentUrl") => {
                    let document_url_key = if obj.contains_key("document_url") {
                        "document_url"
                    } else {
                        "documentUrl"
                    };
                    let reference = obj
                        .get(document_url_key)
                        .and_then(Value::as_object)
                        .and_then(|document_url| {
                            document_url
                                .get("data")
                                .and_then(Value::as_str)
                                .or_else(|| document_url.get("url").and_then(Value::as_str))
                        });
                    if let Some(reference) = reference {
                        document_url_blocks += 1;
                        push_reference_summary(
                            &mut tail_reference_summaries,
                            message_index,
                            block_index,
                            document_url_key,
                            reference,
                        );
                    }
                }
                Some("document") => {
                    let reference = obj
                        .get("source")
                        .and_then(Value::as_object)
                        .filter(|source| source.get("type").and_then(Value::as_str) == Some("url"))
                        .and_then(|source| {
                            source
                                .get("url")
                                .and_then(Value::as_str)
                                .or_else(|| source.get("data").and_then(Value::as_str))
                        });
                    if let Some(reference) = reference {
                        anthropic_document_url_blocks += 1;
                        push_reference_summary(
                            &mut tail_reference_summaries,
                            message_index,
                            block_index,
                            "document.source.url",
                            reference,
                        );
                    }
                }
                _ => {}
            }
        }
    }

    serde_json::json!({
        "openai_image_url_blocks": openai_image_url_blocks,
        "anthropic_image_url_blocks": anthropic_image_url_blocks,
        "document_url_blocks": document_url_blocks,
        "anthropic_document_url_blocks": anthropic_document_url_blocks,
        "tail_references": tail_reference_summaries,
    })
}

fn openai_image_reference(obj: &serde_json::Map<String, Value>) -> Option<&str> {
    match obj.get("image_url") {
        Some(Value::String(url)) => Some(url),
        Some(Value::Object(image_url)) => image_url.get("url").and_then(Value::as_str),
        _ => None,
    }
}

fn push_reference_summary(
    values: &mut Vec<Value>,
    message_index: usize,
    block_index: usize,
    kind: &'static str,
    reference: &str,
) {
    const MAX_REFERENCES: usize = 12;
    values.push(serde_json::json!({
        "message_index": message_index,
        "block_index": block_index,
        "kind": kind,
        "scheme": reference_scheme(reference),
        "length": reference.len(),
        "sha256_prefix": sha256_hex_bytes(reference.as_bytes()).chars().take(16).collect::<String>(),
        "preview": safe_reference_preview(reference),
    }));
    if values.len() > MAX_REFERENCES {
        values.remove(0);
    }
}

fn reference_scheme(reference: &str) -> &str {
    reference
        .split_once(':')
        .map(|(scheme, _)| scheme)
        .unwrap_or("")
}

fn safe_reference_preview(reference: &str) -> String {
    let trimmed = reference.trim();
    if trimmed
        .get(..5)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("data:"))
    {
        return trimmed
            .split_once(',')
            .map(|(metadata, _)| format!("{metadata},<omitted>"))
            .unwrap_or_else(|| "data:<invalid>".to_string());
    }

    let mut preview = String::new();
    for ch in trimmed.chars().take(240) {
        preview.push(ch);
    }
    if trimmed.len() > preview.len() {
        preview.push_str("...");
    }
    preview
}

fn env_bool_enabled(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

fn env_usize_or(name: &str, default_value: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(default_value)
}

fn env_u64_or(name: &str, default_value: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(default_value)
}

fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn sha256_hex_bytes(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn sanitize_capture_component(value: &str, limit: usize) -> String {
    let mut sanitized = String::with_capacity(value.len().min(limit));
    for ch in value.chars().take(limit) {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
            sanitized.push(ch);
        } else {
            sanitized.push('_');
        }
    }
    if sanitized.is_empty() {
        "unknown".to_string()
    } else {
        sanitized
    }
}

fn set_private_dir_permissions(path: &Path) {
    #[cfg(unix)]
    {
        let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o700));
    }
}

fn set_private_file_permissions(path: &Path) {
    #[cfg(unix)]
    {
        let _ = fs::set_permissions(path, fs::Permissions::from_mode(0o600));
    }
}

fn write_private_file(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);

    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    set_private_file_permissions(path);
    Ok(())
}

fn capture_body_path_for_meta(meta_path: &Path) -> PathBuf {
    let Some(file_name) = meta_path.file_name().and_then(|name| name.to_str()) else {
        return meta_path.with_extension("body.json");
    };
    let Some(prefix) = file_name.strip_suffix(".meta.json") else {
        return meta_path.with_extension("body.json");
    };
    meta_path.with_file_name(format!("{prefix}.body.json"))
}

fn capture_file_pair_from_meta(meta_path: PathBuf) -> Option<CaptureFilePair> {
    let body_path = capture_body_path_for_meta(&meta_path);
    let meta_metadata = fs::metadata(&meta_path).ok()?;
    let body_bytes = fs::metadata(&body_path)
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    Some(CaptureFilePair {
        meta_path,
        body_path,
        modified: meta_metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
        bytes: meta_metadata.len().saturating_add(body_bytes),
    })
}

fn capture_pairs_in_dir(dir: &Path) -> Vec<CaptureFilePair> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Vec::new();
    };
    entries
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(".meta.json"))
        })
        .filter_map(capture_file_pair_from_meta)
        .collect()
}

fn remove_capture_pair(pair: &CaptureFilePair) {
    let _ = fs::remove_file(&pair.meta_path);
    let _ = fs::remove_file(&pair.body_path);
}

fn prune_capture_class(class_dir: &Path, max_per_class: usize, ttl: Duration) {
    let mut pairs = capture_pairs_in_dir(class_dir);
    let cutoff = SystemTime::now()
        .checked_sub(ttl)
        .unwrap_or(SystemTime::UNIX_EPOCH);
    for pair in pairs.iter().filter(|pair| pair.modified < cutoff) {
        remove_capture_pair(pair);
    }

    pairs = capture_pairs_in_dir(class_dir);
    pairs.sort_by(|a, b| b.modified.cmp(&a.modified));
    for pair in pairs.iter().skip(max_per_class) {
        remove_capture_pair(pair);
    }
}

fn capture_pairs_in_tree(base_dir: &Path) -> Vec<CaptureFilePair> {
    let Ok(entries) = fs::read_dir(base_dir) else {
        return Vec::new();
    };
    let mut pairs = Vec::new();
    for class_dir in entries
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
    {
        pairs.extend(capture_pairs_in_dir(&class_dir));
    }
    pairs
}

fn prune_capture_expired_in_tree(base_dir: &Path, ttl: Duration) {
    let cutoff = SystemTime::now()
        .checked_sub(ttl)
        .unwrap_or(SystemTime::UNIX_EPOCH);
    for pair in capture_pairs_in_tree(base_dir) {
        if pair.modified < cutoff {
            remove_capture_pair(&pair);
        }
    }
}

fn prune_capture_total(base_dir: &Path, max_total_bytes: u64) {
    if max_total_bytes == 0 {
        return;
    }

    let mut pairs = capture_pairs_in_tree(base_dir);
    pairs.sort_by(|a, b| b.modified.cmp(&a.modified));
    let mut total: u64 = pairs.iter().map(|pair| pair.bytes).sum();
    for pair in pairs.iter().rev() {
        if total <= max_total_bytes {
            break;
        }
        total = total.saturating_sub(pair.bytes);
        remove_capture_pair(pair);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::anthropic::types::Message;
    use uuid::Uuid;

    fn test_config(dir: PathBuf) -> Local400CaptureConfig {
        Local400CaptureConfig {
            enabled: true,
            dir,
            max_per_class: 2,
            ttl: Duration::from_secs(24 * 60 * 60),
            max_total_bytes: 1024 * 1024,
        }
    }

    fn test_request(reference: &str) -> MessagesRequest {
        MessagesRequest {
            model: "claude-sonnet-4-6".to_string(),
            max_tokens: 64,
            messages: vec![Message {
                role: "user".to_string(),
                content: serde_json::json!([
                    {"type":"text","text":"describe"},
                    {"type":"image_url","image_url":{"url":reference}}
                ]),
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

    fn unique_test_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("kiro-rs-local-400-{name}-{}", Uuid::new_v4()))
    }

    #[test]
    fn captures_invalid_multimodal_request_with_reference_summary() {
        let dir = unique_test_dir("capture");
        let config = test_config(dir.clone());
        let payload = test_request("https://example.com/not-image");

        capture_invalid_multimodal_request_inner(
            &config,
            "request-1",
            "messages",
            &payload,
            512,
            "image URL did not contain a supported image",
        )
        .expect("capture should succeed");

        let class_dir = dir.join(INVALID_MULTIMODAL_CLASS);
        let pairs = capture_pairs_in_dir(&class_dir);
        assert_eq!(pairs.len(), 1);
        assert!(pairs[0].body_path.exists());

        let metadata: Value =
            serde_json::from_slice(&fs::read(&pairs[0].meta_path).unwrap()).unwrap();
        assert_eq!(metadata["error_class"], INVALID_MULTIMODAL_CLASS);
        assert_eq!(metadata["body_saved"], true);
        assert_eq!(
            metadata["multimodal_reference_summary"]["openai_image_url_blocks"],
            1
        );
        assert_eq!(
            metadata["multimodal_reference_summary"]["tail_references"][0]["preview"],
            "https://example.com/not-image"
        );

        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn omits_body_when_it_cannot_fit_total_budget() {
        let dir = unique_test_dir("omit");
        let mut config = test_config(dir.clone());
        config.max_total_bytes = 4096;
        let payload = test_request(&format!("https://example.com/{}", "x".repeat(12_000)));

        capture_invalid_multimodal_request_inner(
            &config,
            "request-2",
            "messages",
            &payload,
            512,
            "image URL did not contain a supported image",
        )
        .expect("capture should succeed");

        let class_dir = dir.join(INVALID_MULTIMODAL_CLASS);
        let pairs = capture_pairs_in_dir(&class_dir);
        assert_eq!(pairs.len(), 1);
        assert!(!pairs[0].body_path.exists());

        let metadata: Value =
            serde_json::from_slice(&fs::read(&pairs[0].meta_path).unwrap()).unwrap();
        assert_eq!(metadata["body_saved"], false);
        assert_eq!(
            metadata["body_omitted_reason"],
            "request body exceeds KIRO_CAPTURE_400_MAX_TOTAL_BYTES"
        );

        let _ = fs::remove_dir_all(dir);
    }
}
