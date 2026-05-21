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
static VALIDATION_KEYS: OnceLock<Vec<[u8; 32]>> = OnceLock::new();

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct ThinkingSignatureValidationStats {
    pub valid_own_signatures: usize,
    pub invalid_own_signatures: usize,
    pub foreign_signatures: usize,
    pub missing_signatures: usize,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct ThinkingSignatureValidationReport {
    pub stats: ThinkingSignatureValidationStats,
    pub invalid_diagnostics: Vec<ThinkingSignatureInvalidDiagnostic>,
}

#[derive(Debug)]
pub(crate) struct ThinkingSignatureValidationError {
    message: String,
    diagnostic: Option<ThinkingSignatureInvalidDiagnostic>,
}

impl ThinkingSignatureValidationError {
    fn invalid_diagnostic(diagnostic: ThinkingSignatureInvalidDiagnostic) -> Self {
        Self {
            message: format!(
                "Invalid thinking signature at messages[{}] thinking block {}",
                diagnostic.message_index, diagnostic.thinking_ordinal
            ),
            diagnostic: Some(diagnostic),
        }
    }

    pub(crate) fn diagnostic(&self) -> Option<&ThinkingSignatureInvalidDiagnostic> {
        self.diagnostic.as_ref()
    }

    pub(crate) fn public_message(&self) -> String {
        "Invalid thinking signature. Retry with unmodified thinking blocks or start a new conversation."
            .to_string()
    }
}

impl std::fmt::Display for ThinkingSignatureValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ThinkingSignatureValidationError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ThinkingSignatureInvalidReason {
    OrdinalMismatch,
    MacMismatch,
    ThinkingHashMismatch,
}

impl ThinkingSignatureInvalidReason {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::OrdinalMismatch => "ordinal_mismatch",
            Self::MacMismatch => "mac_mismatch",
            Self::ThinkingHashMismatch => "thinking_hash_mismatch",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ThinkingSignatureInvalidDiagnostic {
    pub(crate) message_index: usize,
    pub(crate) thinking_ordinal: u32,
    pub(crate) reason: ThinkingSignatureInvalidReason,
    pub(crate) signed_ordinal: u32,
    pub(crate) raw_signature_len: usize,
    pub(crate) signature_sha256_prefix: String,
    pub(crate) canonical_thinking_len: usize,
    pub(crate) signed_thinking_hash_prefix: String,
    pub(crate) computed_thinking_hash_prefix: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct InvalidOwnSignatureDetail {
    reason: ThinkingSignatureInvalidReason,
    signed_ordinal: u32,
    raw_signature_len: usize,
    signature_sha256_prefix: String,
    canonical_thinking_len: usize,
    signed_thinking_hash_prefix: String,
    computed_thinking_hash_prefix: String,
}

impl InvalidOwnSignatureDetail {
    fn into_diagnostic(
        self,
        message_index: usize,
        thinking_ordinal: u32,
    ) -> ThinkingSignatureInvalidDiagnostic {
        ThinkingSignatureInvalidDiagnostic {
            message_index,
            thinking_ordinal,
            reason: self.reason,
            signed_ordinal: self.signed_ordinal,
            raw_signature_len: self.raw_signature_len,
            signature_sha256_prefix: self.signature_sha256_prefix,
            canonical_thinking_len: self.canonical_thinking_len,
            signed_thinking_hash_prefix: self.signed_thinking_hash_prefix,
            computed_thinking_hash_prefix: self.computed_thinking_hash_prefix,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum SignatureClass {
    ValidOwn,
    InvalidOwn(InvalidOwnSignatureDetail),
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
    let validation_keys =
        validation_keys_for_material(material.as_bytes(), explicit.as_ref().map(|_| api_key));
    let key = validation_keys[0];
    let _ = SIGNING_KEY.set(key);
    let _ = VALIDATION_KEYS.set(validation_keys);
}

/// 签发 Anthropic 风格的不透明 thinking signature。
///
/// `thinking_ordinal` 是同一 assistant message 中第几个 thinking block，而不是
/// SSE content block index；这样 agentgear 只要不改 thinking block 本身，就不会
/// 因为其他兼容性注入而破坏校验。
pub(crate) fn sign_thinking_block(thinking_ordinal: u32, thinking: &str) -> String {
    let key = signing_key();
    let issuer = issuer_tag(&key);
    let thinking = canonicalize_thinking_for_signature(thinking);
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
    let report = inspect_thinking_signatures(req);
    if let Some(diagnostic) = report.invalid_diagnostics.into_iter().next() {
        return Err(ThinkingSignatureValidationError::invalid_diagnostic(
            diagnostic,
        ));
    }

    Ok(report.stats)
}

pub(crate) fn inspect_thinking_signatures(
    req: &MessagesRequest,
) -> ThinkingSignatureValidationReport {
    let mut report = ThinkingSignatureValidationReport::default();

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
                        SignatureClass::ValidOwn => report.stats.valid_own_signatures += 1,
                        SignatureClass::Foreign => report.stats.foreign_signatures += 1,
                        SignatureClass::InvalidOwn(detail) => {
                            report.stats.invalid_own_signatures += 1;
                            report
                                .invalid_diagnostics
                                .push(detail.into_diagnostic(message_index, thinking_ordinal));
                        }
                    }
                }
                None => report.stats.missing_signatures += 1,
            }

            thinking_ordinal = thinking_ordinal.saturating_add(1);
        }
    }

    report
}

pub(crate) fn strip_invalid_own_thinking_signatures(
    req: &mut MessagesRequest,
) -> Vec<ThinkingSignatureInvalidDiagnostic> {
    let mut diagnostics = Vec::new();

    for (message_index, message) in req.messages.iter_mut().enumerate() {
        if message.role != "assistant" {
            continue;
        }
        let serde_json::Value::Array(blocks) = &mut message.content else {
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
                .unwrap_or("")
                .to_string();
            let signature = obj
                .get("signature")
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string);

            if let Some(signature) = signature {
                if let SignatureClass::InvalidOwn(detail) =
                    classify_signature(&signature, thinking_ordinal, &thinking)
                {
                    diagnostics.push(detail.into_diagnostic(message_index, thinking_ordinal));
                    obj.remove("signature");
                }
            }

            thinking_ordinal = thinking_ordinal.saturating_add(1);
        }
    }

    diagnostics
}

fn classify_signature(signature: &str, thinking_ordinal: u32, thinking: &str) -> SignatureClass {
    let Some(raw) = decode_signature(signature) else {
        return SignatureClass::Foreign;
    };
    if raw.len() != SIGNATURE_RAW_LEN || raw[0] != SIGNATURE_VERSION {
        return SignatureClass::Foreign;
    }

    let issuer_start = 1;
    let issuer_end = issuer_start + ISSUER_TAG_LEN;
    let Some(key) = validation_keys().iter().find(|key| {
        raw[issuer_start..issuer_end]
            .ct_eq(&issuer_tag(key))
            .unwrap_u8()
            == 1
    }) else {
        return SignatureClass::Foreign;
    };

    let ordinal_start = issuer_end;
    let ordinal_end = ordinal_start + 4;
    let signed_ordinal = u32::from_be_bytes([
        raw[ordinal_start],
        raw[ordinal_start + 1],
        raw[ordinal_start + 2],
        raw[ordinal_start + 3],
    ]);
    if signed_ordinal != thinking_ordinal {
        return SignatureClass::InvalidOwn(invalid_own_signature_detail(
            signature,
            &raw,
            signed_ordinal,
            ThinkingSignatureInvalidReason::OrdinalMismatch,
            thinking,
        ));
    }

    let hash_start = ordinal_end;
    let hash_end = hash_start + HASH_LEN;
    let body_end = hash_end;
    let mac_start = body_end;
    let expected_mac = signature_mac(&key, &raw[..body_end]);
    if raw[mac_start..].ct_eq(&expected_mac).unwrap_u8() != 1 {
        return SignatureClass::InvalidOwn(invalid_own_signature_detail(
            signature,
            &raw,
            signed_ordinal,
            ThinkingSignatureInvalidReason::MacMismatch,
            thinking,
        ));
    }

    if thinking_hash_matches(&raw[hash_start..hash_end], thinking) {
        SignatureClass::ValidOwn
    } else {
        SignatureClass::InvalidOwn(invalid_own_signature_detail(
            signature,
            &raw,
            signed_ordinal,
            ThinkingSignatureInvalidReason::ThinkingHashMismatch,
            thinking,
        ))
    }
}

fn invalid_own_signature_detail(
    signature: &str,
    raw: &[u8],
    signed_ordinal: u32,
    reason: ThinkingSignatureInvalidReason,
    thinking: &str,
) -> InvalidOwnSignatureDetail {
    let hash_start = 1 + ISSUER_TAG_LEN + 4;
    let hash_end = hash_start + HASH_LEN;
    let canonical = canonicalize_thinking_for_signature(thinking);
    let computed_thinking_hash = sha256_bytes(canonical.as_bytes());
    let signature_hash = sha256_bytes(signature.as_bytes());

    InvalidOwnSignatureDetail {
        reason,
        signed_ordinal,
        raw_signature_len: raw.len(),
        signature_sha256_prefix: hex_prefix(&signature_hash),
        canonical_thinking_len: canonical.len(),
        signed_thinking_hash_prefix: raw
            .get(hash_start..hash_end)
            .map(hex_prefix)
            .unwrap_or_default(),
        computed_thinking_hash_prefix: hex_prefix(&computed_thinking_hash),
    }
}

fn hex_prefix(bytes: &[u8]) -> String {
    const PREFIX_BYTES: usize = 8;
    hex::encode(&bytes[..bytes.len().min(PREFIX_BYTES)])
}

fn thinking_hash_matches(signed_hash: &[u8], thinking: &str) -> bool {
    let canonical = canonicalize_thinking_for_signature(thinking);
    let canonical_hash = sha256_bytes(canonical.as_bytes());
    if signed_hash.ct_eq(&canonical_hash).unwrap_u8() == 1 {
        return true;
    }

    // Compatibility for stream signatures emitted before canonicalization:
    // those signatures could include the wrapper newline immediately before
    // `</thinking>`, while clients commonly trim it when storing history.
    let mut legacy = String::with_capacity(canonical.len() + 2);
    legacy.push_str(canonical);
    for _ in 0..2 {
        legacy.push('\n');
        let legacy_hash = sha256_bytes(legacy.as_bytes());
        if signed_hash.ct_eq(&legacy_hash).unwrap_u8() == 1 {
            return true;
        }
    }

    if canonical.len() != thinking.len() {
        let raw_hash = sha256_bytes(thinking.as_bytes());
        return signed_hash.ct_eq(&raw_hash).unwrap_u8() == 1;
    }

    false
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

fn validation_keys() -> &'static [[u8; 32]] {
    VALIDATION_KEYS
        .get_or_init(|| vec![signing_key()])
        .as_slice()
}

fn derive_key(material: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"anthropic-thinking-signature-key\n");
    hasher.update(material);
    hasher.finalize().into()
}

fn validation_keys_for_material(
    primary_material: &[u8],
    legacy_material: Option<&str>,
) -> Vec<[u8; 32]> {
    let key = derive_key(primary_material);
    let mut keys = vec![key];
    if let Some(legacy_material) = legacy_material {
        let legacy_key = derive_key(legacy_material.as_bytes());
        if legacy_key != key {
            keys.push(legacy_key);
        }
    }
    keys
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
    let thinking = canonicalize_thinking_for_signature(raw_thinking).to_string();

    let suffix_start = end + "</thinking>".len();
    let remaining = content[suffix_start..].trim_start().to_string();

    Some((thinking, remaining))
}

pub(crate) fn canonicalize_thinking_for_signature(thinking: &str) -> &str {
    thinking.trim_start_matches('\n').trim_end_matches('\n')
}

#[cfg(test)]
mod tests {
    use super::{
        HASH_LEN, HMAC_LEN, ISSUER_TAG_LEN, SIGNATURE_RAW_LEN, SIGNATURE_VERSION, STANDARD_NO_PAD,
        SignatureClass, ThinkingSignatureInvalidReason, classify_signature, decode_signature,
        extract_thinking_and_text, hmac_sha256, inspect_thinking_signatures, issuer_tag,
        sha256_bytes, sign_thinking_block, signature_mac, signing_key,
        strip_invalid_own_thinking_signatures, validate_thinking_signatures,
        validation_keys_for_material,
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

    fn sign_legacy_raw_thinking_block(thinking_ordinal: u32, thinking: &str) -> String {
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

    fn foreign_anthropic_shaped_signature(thinking_ordinal: u32, thinking: &str) -> String {
        let foreign_key = hmac_sha256(&[0x42; 32], b"foreign");
        let mut issuer = [0u8; ISSUER_TAG_LEN];
        issuer.copy_from_slice(&foreign_key[..ISSUER_TAG_LEN]);
        let thinking_hash = sha256_bytes(thinking.as_bytes());

        let mut raw = Vec::with_capacity(1 + ISSUER_TAG_LEN + 4 + HASH_LEN + HMAC_LEN);
        raw.push(SIGNATURE_VERSION);
        raw.extend_from_slice(&issuer);
        raw.extend_from_slice(&thinking_ordinal.to_be_bytes());
        raw.extend_from_slice(&thinking_hash);
        raw.extend_from_slice(&[0x11; HMAC_LEN]);

        STANDARD_NO_PAD.encode(raw)
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
        assert!(matches!(
            classify_signature(&signature, 0, "world"),
            SignatureClass::InvalidOwn(detail)
                if detail.reason == ThinkingSignatureInvalidReason::ThinkingHashMismatch
        ));
        assert!(matches!(
            classify_signature(&signature, 1, "hello"),
            SignatureClass::InvalidOwn(detail)
                if detail.reason == ThinkingSignatureInvalidReason::OrdinalMismatch
                    && detail.signed_ordinal == 0
        ));
    }

    #[test]
    fn test_sign_thinking_block_canonicalizes_boundary_newlines() {
        let signature = sign_thinking_block(0, "\nstep 1\n");

        assert_eq!(
            classify_signature(&signature, 0, "step 1"),
            SignatureClass::ValidOwn
        );
        assert_eq!(
            classify_signature(&signature, 0, "\nstep 1\n"),
            SignatureClass::ValidOwn
        );
    }

    #[test]
    fn test_validation_keys_include_legacy_api_key_when_explicit_secret_is_set() {
        let keys = validation_keys_for_material(b"stable-secret", Some("current-api-key"));

        assert_eq!(keys.len(), 2);
        assert_ne!(keys[0], keys[1]);
        assert_eq!(
            validation_keys_for_material(b"same-material", Some("same-material")).len(),
            1
        );
    }

    #[test]
    fn test_validate_thinking_signatures_accepts_legacy_stream_trailing_newline() {
        let signature = sign_legacy_raw_thinking_block(0, "step 1\n");
        let req = request_with_assistant_content(serde_json::json!([
            {"type":"thinking","thinking":"step 1","signature": signature}
        ]));

        let stats = validate_thinking_signatures(&req)
            .expect("legacy stream wrapper newline should validate");

        assert_eq!(stats.valid_own_signatures, 1);
    }

    #[test]
    fn test_validate_thinking_signatures_accepts_foreign_anthropic_shaped_signature() {
        let signature = foreign_anthropic_shaped_signature(0, "external");
        let req = request_with_assistant_content(serde_json::json!([
            {"type":"thinking","thinking":"external","signature": signature}
        ]));

        let stats =
            validate_thinking_signatures(&req).expect("foreign shaped signatures should pass");

        assert_eq!(stats.valid_own_signatures, 0);
        assert_eq!(stats.foreign_signatures, 1);
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
        let diagnostic = err.diagnostic().expect("diagnostic should be attached");
        assert_eq!(
            diagnostic.reason,
            ThinkingSignatureInvalidReason::ThinkingHashMismatch
        );
        assert_eq!(diagnostic.message_index, 0);
        assert_eq!(diagnostic.thinking_ordinal, 0);
        assert_eq!(diagnostic.signed_ordinal, 0);
        assert_eq!(diagnostic.canonical_thinking_len, "changed".len());
        assert_eq!(diagnostic.raw_signature_len, SIGNATURE_RAW_LEN);
        assert!(!diagnostic.signature_sha256_prefix.is_empty());
        assert!(!diagnostic.signed_thinking_hash_prefix.is_empty());
        assert!(!diagnostic.computed_thinking_hash_prefix.is_empty());
        let public_message = err.public_message();
        assert_eq!(
            public_message,
            "Invalid thinking signature. Retry with unmodified thinking blocks or start a new conversation."
        );
        assert!(!public_message.contains("thinking_hash_mismatch"));
        assert!(!public_message.contains("signature_sha256_prefix="));
        assert!(!public_message.contains("signed_thinking_hash_prefix="));
        assert!(!public_message.contains("computed_thinking_hash_prefix="));
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

        let err = validate_thinking_signatures(&req).expect_err("tampered signature should fail");
        let diagnostic = err.diagnostic().expect("diagnostic should be attached");
        assert_eq!(
            diagnostic.reason,
            ThinkingSignatureInvalidReason::MacMismatch
        );
    }

    #[test]
    fn test_inspect_thinking_signatures_counts_invalid_without_short_circuit() {
        let signature = sign_thinking_block(0, "step 1");
        let req = request_with_assistant_content(serde_json::json!([
            {"type":"thinking","thinking":"changed","signature": signature},
            {"type":"thinking","thinking":"external","signature":"sig_1"},
            {"type":"thinking","thinking":"legacy"}
        ]));

        let report = inspect_thinking_signatures(&req);

        assert_eq!(report.stats.valid_own_signatures, 0);
        assert_eq!(report.stats.invalid_own_signatures, 1);
        assert_eq!(report.stats.foreign_signatures, 1);
        assert_eq!(report.stats.missing_signatures, 1);
        assert_eq!(report.invalid_diagnostics.len(), 1);
    }

    #[test]
    fn test_strip_invalid_own_thinking_signatures_removes_only_invalid_own_signature() {
        let invalid_signature = sign_thinking_block(0, "step 1");
        let valid_signature = sign_thinking_block(1, "ok");
        let foreign_signature = foreign_anthropic_shaped_signature(2, "external");
        let mut req = request_with_assistant_content(serde_json::json!([
            {"type":"thinking","thinking":"changed","signature": invalid_signature},
            {"type":"thinking","thinking":"ok","signature": valid_signature},
            {"type":"thinking","thinking":"external","signature": foreign_signature}
        ]));

        let diagnostics = strip_invalid_own_thinking_signatures(&mut req);

        assert_eq!(diagnostics.len(), 1);
        let blocks = req.messages[0].content.as_array().unwrap();
        assert!(blocks[0].get("signature").is_none());
        assert!(blocks[1].get("signature").is_some());
        assert!(blocks[2].get("signature").is_some());
        let stats = validate_thinking_signatures(&req).expect("stripped request should validate");
        assert_eq!(stats.valid_own_signatures, 1);
        assert_eq!(stats.foreign_signatures, 1);
        assert_eq!(stats.missing_signatures, 1);
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
