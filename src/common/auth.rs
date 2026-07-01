//! 公共认证工具函数

use axum::{
    body::Body,
    http::{Request, header},
};
use parking_lot::RwLock;
use std::sync::Arc;
use subtle::ConstantTimeEq;

pub const DEFAULT_CREDENTIAL_GROUP: &str = "default";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CredentialGroupScope {
    All,
    Groups(Vec<String>),
}

impl CredentialGroupScope {
    pub fn all() -> Self {
        Self::All
    }

    pub fn allows_credential_groups(&self, credential_groups: &[String]) -> bool {
        match self {
            Self::All => true,
            Self::Groups(allowed_groups) => {
                if allowed_groups.is_empty() {
                    return false;
                }

                let credential_groups = effective_credential_groups(credential_groups);
                credential_groups
                    .iter()
                    .any(|group| allowed_groups.binary_search(group).is_ok())
            }
        }
    }

    pub fn cache_key_component(&self) -> String {
        match self {
            Self::All => "all".to_string(),
            Self::Groups(groups) => format!("groups:{}", groups.join(",")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiKeyAuthContext {
    pub id: String,
    pub credential_group_scope: CredentialGroupScope,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiKeyAuthEntry {
    pub id: String,
    pub key: String,
    pub credential_group_scope: CredentialGroupScope,
}

#[derive(Debug, Clone, Default)]
pub struct ApiKeyRegistry {
    entries: Arc<RwLock<Vec<ApiKeyAuthEntry>>>,
}

impl ApiKeyRegistry {
    pub fn new(entries: Vec<ApiKeyAuthEntry>) -> Self {
        Self {
            entries: Arc::new(RwLock::new(entries)),
        }
    }

    pub fn replace(&self, entries: Vec<ApiKeyAuthEntry>) {
        *self.entries.write() = entries;
    }

    pub fn find(&self, key: &str) -> Option<ApiKeyAuthEntry> {
        let entries = self.entries.read();
        let mut matched_entry = None;
        for entry in entries.iter() {
            if constant_time_eq(key, &entry.key) {
                matched_entry = Some(entry.clone());
            }
        }
        matched_entry
    }
}

pub fn normalize_credential_group(value: &str) -> Option<String> {
    let normalized = value.trim().to_ascii_lowercase();
    (!normalized.is_empty()).then_some(normalized)
}

pub fn normalize_credential_groups(groups: &[String]) -> Vec<String> {
    let mut groups = groups
        .iter()
        .filter_map(|group| normalize_credential_group(group))
        .collect::<Vec<_>>();
    groups.sort();
    groups.dedup();
    groups
}

pub fn effective_credential_groups(groups: &[String]) -> Vec<String> {
    let groups = normalize_credential_groups(groups);
    if groups.is_empty() {
        vec![DEFAULT_CREDENTIAL_GROUP.to_string()]
    } else {
        groups
    }
}

/// 从请求中提取 API Key
///
/// 支持两种认证方式：
/// - `x-api-key` header
/// - `Authorization: Bearer <token>` header
pub fn extract_api_key(request: &Request<Body>) -> Option<String> {
    // 优先检查 x-api-key
    if let Some(key) = request
        .headers()
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
    {
        return Some(key.to_string());
    }

    // 其次检查 Authorization: Bearer
    request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(|s| s.to_string())
}

/// 常量时间字符串比较，防止时序攻击
///
/// 无论字符串内容如何，比较所需的时间都是恒定的，
/// 这可以防止攻击者通过测量响应时间来猜测 API Key。
///
/// 使用经过安全审计的 `subtle` crate 实现
pub fn constant_time_eq(a: &str, b: &str) -> bool {
    a.as_bytes().ct_eq(b.as_bytes()).into()
}
