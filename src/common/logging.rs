//! 公共日志摘要工具

use serde_json::Value;

/// 将长文本压缩成适合日志的一行摘要。
///
/// - 合并连续空白字符
/// - 按字符数截断，避免切断 UTF-8 多字节字符
/// - 空内容返回 `<empty>`
pub fn summarize_text_for_log(text: &str, max_chars: usize) -> String {
    let max_chars = max_chars.max(1);
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return "<empty>".to_string();
    }

    let mut result = String::new();
    let mut emitted_chars = 0usize;
    let mut previous_was_space = false;
    let mut truncated = false;

    for ch in trimmed.chars() {
        let normalized = if ch.is_whitespace() { ' ' } else { ch };

        if normalized == ' ' {
            if previous_was_space {
                continue;
            }
            previous_was_space = true;
        } else {
            previous_was_space = false;
        }

        if emitted_chars >= max_chars {
            truncated = true;
            break;
        }

        result.push(normalized);
        emitted_chars += 1;
    }

    while result.ends_with(' ') {
        result.pop();
    }

    if truncated {
        result.push('…');
    }

    if result.is_empty() {
        "<empty>".to_string()
    } else {
        result
    }
}

fn extract_json_string_field(body: &str, pointers: &[&str]) -> Option<String> {
    let value: Value = serde_json::from_str(body).ok()?;

    pointers.iter().find_map(|pointer| {
        value
            .pointer(pointer)
            .and_then(|field| field.as_str())
            .map(str::trim)
            .filter(|field| !field.is_empty())
            .map(ToOwned::to_owned)
    })
}

/// 将上游错误体收敛为稳定、简短的摘要字符串。
///
/// 优先提取结构化 `reason` / `message`，否则回退到 body 摘要。
pub fn summarize_upstream_error(status_code: u16, body: &str, excerpt_chars: usize) -> String {
    let mut parts = vec![
        format!("status={status_code}"),
        format!("body_len={}", body.len()),
    ];

    if let Some(reason) = extract_json_string_field(body, &["/reason", "/error/reason"]) {
        parts.push(format!("reason={reason}"));
    }

    if let Some(message) = extract_json_string_field(
        body,
        &[
            "/message",
            "/error/message",
            "/Message",
            "/error/Message",
            "/error_description",
            "/error/error_description",
        ],
    ) {
        parts.push(format!(
            "message={:?}",
            summarize_text_for_log(&message, excerpt_chars.min(160))
        ));
    } else {
        let excerpt = summarize_text_for_log(body, excerpt_chars);
        if excerpt != "<empty>" {
            parts.push(format!("body_excerpt={excerpt:?}"));
        }
    }

    parts.join(" ")
}

#[cfg(test)]
mod tests {
    use super::{summarize_text_for_log, summarize_upstream_error};

    #[test]
    fn test_summarize_text_for_log_collapses_whitespace_and_truncates() {
        let summary = summarize_text_for_log("  hello\n\n   world\tfrom   kiro  ", 12);
        assert_eq!(summary, "hello world…");
    }

    #[test]
    fn test_summarize_upstream_error_extracts_reason_and_message() {
        let body = r#"{"reason":"CONTENT_LENGTH_EXCEEDS_THRESHOLD","message":"Input is too long and must be reduced immediately"}"#;
        let summary = summarize_upstream_error(400, body, 40);

        assert!(summary.contains("status=400"));
        assert!(summary.contains("reason=CONTENT_LENGTH_EXCEEDS_THRESHOLD"));
        assert!(summary.contains("message=\"Input is too long and must be reduced"));
    }

    #[test]
    fn test_summarize_upstream_error_falls_back_to_excerpt() {
        let body = "gateway timeout\n\nupstream overloaded";
        let summary = summarize_upstream_error(504, body, 20);

        assert!(summary.contains("status=504"));
        assert!(summary.contains("body_excerpt=\"gateway timeout ups"));
    }
}
