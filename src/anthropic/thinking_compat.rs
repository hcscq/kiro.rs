//! Thinking 兼容辅助函数
//!
//! 为不具备 Anthropic 原生签名能力的上游生成“协议外形兼容”的 thinking 输出。

use sha2::{Digest, Sha256};

/// 生成稳定的 synthetic thinking signature。
///
/// 这不是 Anthropic 的真实签名，只用于兼容那些只检查 `signature` /
/// `signature_delta` 字段存在与基本形态的客户端。
pub fn build_synthetic_thinking_signature(
    message_id: &str,
    block_index: i32,
    thinking: &str,
) -> String {
    let salts = [
        "compat-thinking:v1:0",
        "compat-thinking:v1:1",
        "compat-thinking:v1:2",
        "compat-thinking:v1:3",
    ];

    let mut signature = String::new();
    for salt in salts {
        let mut hasher = Sha256::new();
        hasher.update(salt.as_bytes());
        hasher.update(b"\n");
        hasher.update(message_id.as_bytes());
        hasher.update(b"\n");
        hasher.update(block_index.to_string().as_bytes());
        hasher.update(b"\n");
        hasher.update(thinking.as_bytes());
        signature.push_str(&hex::encode(hasher.finalize()));
    }
    signature
}

/// 从文本中提取 `<thinking>...</thinking>` 并返回 (thinking, text)。
///
/// - 只处理第一个真实 thinking 块
/// - 去除包裹 thinking 内容的首尾换行
/// - 去除结束标签后的前导空白，保留剩余文本
pub fn extract_thinking_and_text(content: &str) -> Option<(String, String)> {
    let start = content.find("<thinking>")?;
    let thinking_start = start + "<thinking>".len();
    let end_rel = content[thinking_start..].find("</thinking>")?;
    let end = thinking_start + end_rel;

    let raw_thinking = &content[thinking_start..end];
    let thinking = raw_thinking
        .trim_start_matches('\n')
        .trim_end_matches('\n')
        .to_string();

    let suffix_start = end + "</thinking>".len();
    let remaining = content[suffix_start..].trim_start().to_string();

    Some((thinking, remaining))
}

#[cfg(test)]
mod tests {
    use super::{build_synthetic_thinking_signature, extract_thinking_and_text};

    #[test]
    fn test_build_synthetic_thinking_signature_is_stable_and_non_empty() {
        let a = build_synthetic_thinking_signature("msg_1", 0, "hello");
        let b = build_synthetic_thinking_signature("msg_1", 0, "hello");
        let c = build_synthetic_thinking_signature("msg_1", 0, "world");

        assert_eq!(a, b);
        assert_ne!(a, c);
        assert!(a.len() >= 128);
    }

    #[test]
    fn test_extract_thinking_and_text() {
        let input = "<thinking>\nstep 1\n</thinking>\n\nfinal answer";
        let (thinking, text) = extract_thinking_and_text(input).expect("should parse");
        assert_eq!(thinking, "step 1");
        assert_eq!(text, "final answer");
    }

    #[test]
    fn test_extract_thinking_and_text_without_text_suffix() {
        let input = "<thinking>\nstep 1\n</thinking>";
        let (thinking, text) = extract_thinking_and_text(input).expect("should parse");
        assert_eq!(thinking, "step 1");
        assert!(text.is_empty());
    }
}
