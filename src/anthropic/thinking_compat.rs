//! Thinking 兼容辅助函数
//!
//! 为不具备 Anthropic 原生签名能力的上游生成不透明 thinking signature，并在
//! 后续往返请求中校验本服务签发过的 thinking block。字段名保持 Anthropic 协议
//! 的 `signature` / `signature_delta.signature`，签名值本身不暴露本服务命名。

use std::sync::OnceLock;

use base64::Engine;
use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use super::types::MessagesRequest;

const SIGNATURE_ENV: &str = "KIRO_ANTHROPIC_THINKING_SIGNATURE_SECRET";
const SIGNATURE_VERSION: u8 = 0xa7;
const ISSUER_TAG_LEN: usize = 16;
const HMAC_LEN: usize = 32;
const HASH_LEN: usize = 32;
const SIGNATURE_RAW_LEN: usize = 1 + ISSUER_TAG_LEN + 4 + HASH_LEN + HMAC_LEN;

static SIGNING_KEY: OnceLock<[u8; 32]> = OnceLock::new();

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct ThinkingSignatureValidationStats {
    pub valid_own_signatures: usize,
    pub foreign_signatures: usize,
    pub missing_signatures: usize,
}

#[derive(Debug)]
pub(crate) struct ThinkingSignatureValidationError {
    message: String,
}

impl ThinkingSignatureValidationError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for ThinkingSignatureValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ThinkingSignatureValidationError {}

#[derive(Debug, PartialEq, Eq)]
enum SignatureClass {
    ValidOwn,
    InvalidOwn,
    Foreign,
}

/// 初始化 thinking signature 的服务端签名密钥。
///
/// 优先使用环境变量 `KIRO_ANTHROPIC_THINKING_SIGNATURE_SECRET`，否则从服务端
/// Anthropic API key 派生。这样同一部署内多副本会共享签名能力，同时不会把
/// 明文 key 放入 signature。
pub(crate) fn init_thinking_signature_key(api_key: &str) {
    let explicit = std::env::var(SIGNATURE_ENV)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let material = explicit.as_deref().unwrap_or(api_key);
    let key = derive_key(material.as_bytes());
    let _ = SIGNING_KEY.set(key);
}

/// 签发 Anthropic 风格的不透明 thinking signature。
///
/// `thinking_ordinal` 是同一 assistant message 中第几个 thinking block，而不是
/// SSE content block index；这样 agentgear 只要不改 thinking block 本身，就不会
/// 因为其他兼容性注入而破坏校验。
pub(crate) fn sign_thinking_block(thinking_ordinal: u32, thinking: &str) -> String {
    let key = signing_key();
    let issuer = issuer_tag(&key);
    let thinking_hash = sha256_bytes(thinking.as_bytes());

    let mut body = Vec::with_capacity(1 + ISSUER_TAG_LEN + 4 + HASH_LEN);
    body.push(SIGNATURE_VERSION);
    body.extend_from_slice(&issuer);
    body.extend_from_slice(&thinking_ordinal.to_be_bytes());
    body.extend_from_slice(&thinking_hash);

    let mac = signature_mac(&key, &body);
    let mut raw = body;
    raw.extend_from_slice(&mac);

    STANDARD_NO_PAD.encode(raw)
}

pub(crate) fn validate_thinking_signatures(
    req: &MessagesRequest,
) -> Result<ThinkingSignatureValidationStats, ThinkingSignatureValidationError> {
    let mut stats = ThinkingSignatureValidationStats::default();

    for (message_index, message) in req.messages.iter().enumerate() {
        if message.role != "assistant" {
            continue;
        }
        let serde_json::Value::Array(blocks) = &message.content else {
            continue;
        };

        let mut thinking_ordinal = 0u32;
        for block in blocks {
            let serde_json::Value::Object(obj) = block else {
                continue;
            };
            if obj.get("type").and_then(serde_json::Value::as_str) != Some("thinking") {
                continue;
            }

            let thinking = obj
                .get("thinking")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("");
            let signature = obj
                .get("signature")
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty());

            match signature {
                Some(signature) => {
                    match classify_signature(signature, thinking_ordinal, thinking) {
                        SignatureClass::ValidOwn => stats.valid_own_signatures += 1,
                        SignatureClass::Foreign => stats.foreign_signatures += 1,
                        SignatureClass::InvalidOwn => {
                            return Err(ThinkingSignatureValidationError::new(format!(
                                "Invalid thinking signature at messages[{message_index}] thinking block {thinking_ordinal}"
                            )));
                        }
                    }
                }
                None => stats.missing_signatures += 1,
            }

            thinking_ordinal = thinking_ordinal.saturating_add(1);
        }
    }

    Ok(stats)
}

fn classify_signature(signature: &str, thinking_ordinal: u32, thinking: &str) -> SignatureClass {
    let Some(raw) = decode_signature(signature) else {
        return SignatureClass::Foreign;
    };
    if raw.len() != SIGNATURE_RAW_LEN || raw[0] != SIGNATURE_VERSION {
        return SignatureClass::Foreign;
    }

    let key = signing_key();
    let issuer = issuer_tag(&key);
    let issuer_start = 1;
    let issuer_end = issuer_start + ISSUER_TAG_LEN;
    if raw[issuer_start..issuer_end].ct_eq(&issuer).unwrap_u8() != 1 {
        return SignatureClass::InvalidOwn;
    }

    let ordinal_start = issuer_end;
    let ordinal_end = ordinal_start + 4;
    let signed_ordinal = u32::from_be_bytes([
        raw[ordinal_start],
        raw[ordinal_start + 1],
        raw[ordinal_start + 2],
        raw[ordinal_start + 3],
    ]);
    if signed_ordinal != thinking_ordinal {
        return SignatureClass::InvalidOwn;
    }

    let hash_start = ordinal_end;
    let hash_end = hash_start + HASH_LEN;
    let expected_hash = sha256_bytes(thinking.as_bytes());
    if raw[hash_start..hash_end].ct_eq(&expected_hash).unwrap_u8() != 1 {
        return SignatureClass::InvalidOwn;
    }

    let body_end = hash_end;
    let mac_start = body_end;
    let expected_mac = signature_mac(&key, &raw[..body_end]);
    if raw[mac_start..].ct_eq(&expected_mac).unwrap_u8() == 1 {
        SignatureClass::ValidOwn
    } else {
        SignatureClass::InvalidOwn
    }
}

fn decode_signature(signature: &str) -> Option<Vec<u8>> {
    STANDARD_NO_PAD
        .decode(signature)
        .or_else(|_| STANDARD.decode(signature))
        .ok()
}

fn signing_key() -> [u8; 32] {
    *SIGNING_KEY.get_or_init(|| {
        let material = std::env::var(SIGNATURE_ENV)
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "kiro-rs-anthropic-thinking-signature-fallback".to_string());
        derive_key(material.as_bytes())
    })
}

fn derive_key(material: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"anthropic-thinking-signature-key\n");
    hasher.update(material);
    hasher.finalize().into()
}

fn issuer_tag(key: &[u8; 32]) -> [u8; ISSUER_TAG_LEN] {
    let mac = hmac_sha256(key, b"anthropic-thinking-signature-issuer");
    let mut tag = [0u8; ISSUER_TAG_LEN];
    tag.copy_from_slice(&mac[..ISSUER_TAG_LEN]);
    tag
}

fn signature_mac(key: &[u8; 32], body: &[u8]) -> [u8; HMAC_LEN] {
    let mut payload = Vec::with_capacity(b"anthropic-thinking-signature-body\n".len() + body.len());
    payload.extend_from_slice(b"anthropic-thinking-signature-body\n");
    payload.extend_from_slice(body);
    hmac_sha256(key, &payload)
}

fn sha256_bytes(input: &[u8]) -> [u8; HASH_LEN] {
    let mut hasher = Sha256::new();
    hasher.update(input);
    hasher.finalize().into()
}

fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; HMAC_LEN] {
    const BLOCK_LEN: usize = 64;

    let mut normalized_key = [0u8; BLOCK_LEN];
    if key.len() > BLOCK_LEN {
        let hashed = sha256_bytes(key);
        normalized_key[..HASH_LEN].copy_from_slice(&hashed);
    } else {
        normalized_key[..key.len()].copy_from_slice(key);
    }

    let mut ipad = [0x36u8; BLOCK_LEN];
    let mut opad = [0x5cu8; BLOCK_LEN];
    for i in 0..BLOCK_LEN {
        ipad[i] ^= normalized_key[i];
        opad[i] ^= normalized_key[i];
    }

    let mut inner = Sha256::new();
    inner.update(ipad);
    inner.update(message);
    let inner_hash = inner.finalize();

    let mut outer = Sha256::new();
    outer.update(opad);
    outer.update(inner_hash);
    outer.finalize().into()
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
    use super::{
        STANDARD_NO_PAD, SignatureClass, classify_signature, decode_signature,
        extract_thinking_and_text, sign_thinking_block, validate_thinking_signatures,
    };
    use crate::anthropic::types::{Message, MessagesRequest};
    use base64::Engine;

    fn request_with_assistant_content(content: serde_json::Value) -> MessagesRequest {
        MessagesRequest {
            model: "claude-sonnet-4-6-thinking".to_string(),
            max_tokens: 64,
            messages: vec![Message {
                role: "assistant".to_string(),
                content,
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
    fn test_sign_thinking_block_is_opaque_and_verifiable() {
        let signature = sign_thinking_block(0, "hello");

        assert!(!signature.contains('.'));
        assert!(!signature.to_lowercase().contains("kiro"));
        assert!(signature.len() >= 100);
        assert_eq!(
            classify_signature(&signature, 0, "hello"),
            SignatureClass::ValidOwn
        );
        assert_eq!(
            classify_signature(&signature, 0, "world"),
            SignatureClass::InvalidOwn
        );
        assert_eq!(
            classify_signature(&signature, 1, "hello"),
            SignatureClass::InvalidOwn
        );
    }

    #[test]
    fn test_validate_thinking_signatures_accepts_valid_own_signature() {
        let signature = sign_thinking_block(0, "step 1");
        let req = request_with_assistant_content(serde_json::json!([
            {"type":"thinking","thinking":"step 1","signature": signature},
            {"type":"text","text":"done"}
        ]));

        let stats = validate_thinking_signatures(&req).expect("signature should validate");

        assert_eq!(stats.valid_own_signatures, 1);
        assert_eq!(stats.foreign_signatures, 0);
        assert_eq!(stats.missing_signatures, 0);
    }

    #[test]
    fn test_validate_thinking_signatures_rejects_tampered_own_signature() {
        let signature = sign_thinking_block(0, "step 1");
        let req = request_with_assistant_content(serde_json::json!([
            {"type":"thinking","thinking":"changed","signature": signature}
        ]));

        let err = validate_thinking_signatures(&req).expect_err("tampered thinking should fail");

        assert!(err.to_string().contains("Invalid thinking signature"));
    }

    #[test]
    fn test_validate_thinking_signatures_accepts_foreign_and_missing_signatures() {
        let req = request_with_assistant_content(serde_json::json!([
            {"type":"thinking","thinking":"external","signature":"sig_1"},
            {"type":"thinking","thinking":"legacy"}
        ]));

        let stats =
            validate_thinking_signatures(&req).expect("foreign and missing signatures should pass");

        assert_eq!(stats.valid_own_signatures, 0);
        assert_eq!(stats.foreign_signatures, 1);
        assert_eq!(stats.missing_signatures, 1);
    }

    #[test]
    fn test_validate_thinking_signatures_rejects_tampered_signature_bytes() {
        let signature = sign_thinking_block(0, "step 1");
        let mut raw = decode_signature(&signature).expect("signature should decode");
        let last = raw.last_mut().expect("raw signature should not be empty");
        *last ^= 0x01;
        let signature = STANDARD_NO_PAD.encode(raw);
        let req = request_with_assistant_content(serde_json::json!([
            {"type":"thinking","thinking":"step 1","signature": signature}
        ]));

        validate_thinking_signatures(&req).expect_err("tampered signature should fail");
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
