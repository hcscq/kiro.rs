//! Kiro OAuth 凭证数据模型
//!
//! 支持从 Kiro IDE 的凭证文件加载，使用 Social 认证方式
//! 支持单凭据和多凭据配置格式

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::fs;
use std::path::Path;

use crate::http_client::ProxyConfig;
use crate::model::account_type_preset::{
    infer_standard_account_type_id, infer_standard_account_type_id_from_subscription,
};
use crate::model::config::Config;
use crate::model::model_policy::{
    AccountTypeDispatchPolicy, ModelSupportPolicy, RuntimeModelRestriction, normalize_account_type,
    normalize_model_entries, normalize_model_selector,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResolvedAccountTypeSource {
    Explicit,
    SubscriptionTitle,
    SubscriptionType,
}

impl ResolvedAccountTypeSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Explicit => "credential",
            Self::SubscriptionTitle => "subscription-title",
            Self::SubscriptionType => "subscription-type",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchSettingSource {
    Credential,
    AccountType,
    GlobalDefault,
}

impl DispatchSettingSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Credential => "credential",
            Self::AccountType => "account-type",
            Self::GlobalDefault => "global-default",
        }
    }
}

/// Kiro OAuth 凭证
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct KiroCredentials {
    /// 凭据唯一标识符（自增 ID）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<u64>,

    /// 访问令牌
    #[serde(skip_serializing_if = "Option::is_none")]
    pub access_token: Option<String>,

    /// 刷新令牌
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refresh_token: Option<String>,

    /// Profile ARN
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_arn: Option<String>,

    /// 过期时间 (RFC3339 格式)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,

    /// 认证方式 (social / idc)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_method: Option<String>,

    /// OIDC Client ID (IdC 认证需要)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,

    /// OIDC Client Secret (IdC 认证需要)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_secret: Option<String>,

    /// AWS IAM Identity Center Start URL（企业 IdC 账号用于识别）
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub start_url: Option<String>,

    /// 凭据优先级（数字越小优先级越高，默认为 0）
    #[serde(default)]
    #[serde(skip_serializing_if = "is_zero")]
    pub priority: u32,

    /// 单账号并发上限（可选）
    /// 未配置或 <= 0 时表示不限制
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_concurrency: Option<u32>,

    /// 凭据级 token bucket 容量覆盖（可选）
    /// 未配置时回退到 config.json / Admin API 的全局值
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limit_bucket_capacity: Option<f64>,

    /// 凭据级 token bucket 回填速率覆盖（token/s，可选）
    /// 未配置时回退到 config.json / Admin API 的全局值
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limit_refill_per_second: Option<f64>,

    /// 凭据级 Region 配置（用于 OIDC token 刷新）
    /// 未配置时回退到 config.json 的全局 region
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,

    /// 凭据级 Auth Region（用于 Token 刷新）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_region: Option<String>,

    /// 凭据级 API Region（用于 API 请求）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_region: Option<String>,

    /// 凭据级 Machine ID 配置（可选）
    /// 未配置时回退到 config.json 的 machineId；都未配置时由 refreshToken 派生
    #[serde(skip_serializing_if = "Option::is_none")]
    pub machine_id: Option<String>,

    /// 用户邮箱（从 Anthropic API 获取）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,

    /// 订阅等级（KIRO PRO+ / KIRO FREE 等）
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub subscription_title: Option<String>,

    /// 订阅内部类型（如 Q_DEVELOPER_STANDALONE_PRO）
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub subscription_type: Option<String>,

    /// 账号类型（用于命中全局账号类型策略）
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub account_type: Option<String>,

    /// 账号级额外允许的模型列表
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_models: Vec<String>,

    /// 账号级额外禁用的模型列表
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blocked_models: Vec<String>,

    /// 运行时探测到的临时模型限制
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub runtime_model_restrictions: Vec<RuntimeModelRestriction>,

    /// 导入时间（RFC3339 格式）
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub imported_at: Option<String>,

    /// 凭据级代理 URL（可选）
    /// 支持 http/https/socks5 协议
    /// 特殊值 "direct" 表示显式不使用代理（即使全局配置了代理）
    /// 未配置时回退到全局代理配置
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proxy_url: Option<String>,

    /// 凭据级代理认证用户名（可选）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proxy_username: Option<String>,

    /// 凭据级代理认证密码（可选）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proxy_password: Option<String>,

    /// 凭据是否被禁用（默认为 false）
    #[serde(default)]
    pub disabled: bool,

    /// 凭据禁用原因（运行时自动标记或管理端手动标记）
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub disabled_reason: Option<String>,

    /// 凭据被禁用的时间（RFC3339 格式）
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub disabled_at: Option<String>,

    /// 最近一次导致异常标记的上游 HTTP 状态码
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub last_error_status: Option<u16>,

    /// 最近一次导致异常标记的上游错误摘要
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub last_error_summary: Option<String>,
}

/// 判断是否为零（用于跳过序列化）
fn is_zero(value: &u32) -> bool {
    *value == 0
}

fn non_negative_finite(value: f64) -> Option<f64> {
    value
        .is_finite()
        .then_some(value)
        .filter(|value| *value >= 0.0)
}

fn canonicalize_auth_method_value(value: &str) -> &str {
    if value.eq_ignore_ascii_case("builder-id") || value.eq_ignore_ascii_case("iam") {
        "idc"
    } else {
        value
    }
}

fn has_client_credentials(credentials: &KiroCredentials) -> bool {
    credentials
        .client_id
        .as_ref()
        .is_some_and(|value| !value.trim().is_empty())
        && credentials
            .client_secret
            .as_ref()
            .is_some_and(|value| !value.trim().is_empty())
}

fn extract_start_url_from_client_secret(client_secret: &str) -> Option<String> {
    let parts: Vec<&str> = client_secret.split('.').collect();
    if parts.len() < 2 {
        return None;
    }

    let decoded = URL_SAFE_NO_PAD.decode(parts[1]).ok()?;
    let payload_str = String::from_utf8(decoded).ok()?;
    let payload_json: serde_json::Value = serde_json::from_str(&payload_str).ok()?;
    let serialized_str = payload_json.get("serialized")?.as_str()?;
    let serialized: serde_json::Value = serde_json::from_str(serialized_str).ok()?;

    serialized
        .get("initiateLoginUri")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn is_builder_id_start_url(start_url: &str) -> bool {
    let trimmed = start_url.trim().trim_end_matches('/');
    if trimmed.eq_ignore_ascii_case("https://view.awsapps.com/start") {
        return true;
    }

    url::Url::parse(trimmed)
        .ok()
        .and_then(|url| {
            let host = url.host_str()?.to_ascii_lowercase();
            let path = url.path().trim_end_matches('/').to_ascii_lowercase();
            Some(host == "view.awsapps.com" && path == "/start")
        })
        .unwrap_or(false)
}

/// 凭据配置（支持单对象或数组格式）
///
/// 自动识别配置文件格式：
/// - 单对象格式（旧格式，向后兼容）
/// - 数组格式（新格式，支持多凭据）
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CredentialsConfig {
    /// 单个凭据（旧格式）
    Single(KiroCredentials),
    /// 多凭据数组（新格式）
    Multiple(Vec<KiroCredentials>),
}

impl CredentialsConfig {
    /// 从文件加载凭据配置
    ///
    /// - 如果文件不存在，返回空数组
    /// - 如果文件内容为空，返回空数组
    /// - 支持单对象或数组格式
    pub fn load<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let path = path.as_ref();

        // 文件不存在时返回空数组
        if !path.exists() {
            return Ok(CredentialsConfig::Multiple(vec![]));
        }

        let content = fs::read_to_string(path)?;

        // 文件为空时返回空数组
        if content.trim().is_empty() {
            return Ok(CredentialsConfig::Multiple(vec![]));
        }

        let config = serde_json::from_str(&content)?;
        Ok(config)
    }

    /// 转换为按优先级排序的凭据列表
    pub fn into_sorted_credentials(self) -> Vec<KiroCredentials> {
        match self {
            CredentialsConfig::Single(mut cred) => {
                cred.canonicalize_auth_method();
                cred.normalize_model_capabilities();
                vec![cred]
            }
            CredentialsConfig::Multiple(mut creds) => {
                // 按优先级排序（数字越小优先级越高）
                creds.sort_by_key(|c| c.priority);
                for cred in &mut creds {
                    cred.canonicalize_auth_method();
                    cred.normalize_model_capabilities();
                }
                creds
            }
        }
    }

    /// 判断是否为多凭据格式（数组格式）
    pub fn is_multiple(&self) -> bool {
        matches!(self, CredentialsConfig::Multiple(_))
    }
}

impl KiroCredentials {
    /// 特殊值：显式不使用代理
    pub const PROXY_DIRECT: &'static str = "direct";

    /// 获取默认凭证文件路径
    pub fn default_credentials_path() -> &'static str {
        "credentials.json"
    }

    /// 获取有效的 Auth Region（用于 Token 刷新）
    /// 优先级：凭据.auth_region > 凭据.region > config.auth_region > config.region
    pub fn effective_auth_region<'a>(&'a self, config: &'a Config) -> &'a str {
        self.auth_region
            .as_deref()
            .or(self.region.as_deref())
            .unwrap_or(config.effective_auth_region())
    }

    /// 获取有效的 API Region（用于 API 请求）
    /// 优先级：凭据.api_region > config.api_region > config.region
    pub fn effective_api_region<'a>(&'a self, config: &'a Config) -> &'a str {
        self.api_region
            .as_deref()
            .unwrap_or(config.effective_api_region())
    }

    /// 获取有效的代理配置
    /// 优先级：凭据代理 > 全局代理 > 无代理
    /// 特殊值 "direct" 表示显式不使用代理（即使全局配置了代理）
    pub fn effective_proxy(&self, global_proxy: Option<&ProxyConfig>) -> Option<ProxyConfig> {
        match self.proxy_url.as_deref() {
            Some(url) if url.eq_ignore_ascii_case(Self::PROXY_DIRECT) => None,
            Some(url) => {
                let mut proxy = ProxyConfig::new(url);
                if let (Some(username), Some(password)) =
                    (&self.proxy_username, &self.proxy_password)
                {
                    proxy = proxy.with_auth(username, password);
                }
                Some(proxy)
            }
            None => global_proxy.cloned(),
        }
    }

    pub fn effective_auth_method(&self) -> &'static str {
        match self.auth_method.as_deref() {
            Some(value)
                if value.eq_ignore_ascii_case("idc")
                    || value.eq_ignore_ascii_case("builder-id")
                    || value.eq_ignore_ascii_case("iam") =>
            {
                "idc"
            }
            Some(value) if value.eq_ignore_ascii_case("social") => "social",
            Some(_) => "social",
            None if has_client_credentials(self) => "idc",
            None => "social",
        }
    }

    pub fn detected_auth_account_type(&self) -> Option<String> {
        if self.effective_auth_method() == "social" {
            return Some("social".to_string());
        }

        let start_url = self.start_url.as_deref().and_then(|value| {
            let trimmed = value.trim();
            (!trimmed.is_empty()).then_some(trimmed)
        });
        let extracted_start_url = self
            .client_secret
            .as_deref()
            .and_then(extract_start_url_from_client_secret);
        let detected_start_url = start_url.or(extracted_start_url.as_deref());

        if let Some(start_url) = detected_start_url {
            if is_builder_id_start_url(start_url) {
                return Some("builder-id".to_string());
            }
            return Some("enterprise".to_string());
        }

        Some("idc".to_string())
    }

    pub fn canonicalize_auth_method(&mut self) {
        let auth_method = match &self.auth_method {
            Some(m) => m,
            None => return,
        };

        let canonical = canonicalize_auth_method_value(auth_method);
        if canonical != auth_method {
            self.auth_method = Some(canonical.to_string());
        }
    }

    pub fn normalize_model_capabilities(&mut self) {
        self.account_type = self
            .account_type
            .as_deref()
            .and_then(normalize_account_type);
        self.allowed_models = normalize_model_entries(&self.allowed_models);
        self.blocked_models = normalize_model_entries(&self.blocked_models);
        self.runtime_model_restrictions.retain_mut(|restriction| {
            restriction.normalize() && restriction.is_active_at(Utc::now())
        });
        self.runtime_model_restrictions
            .sort_by(|left, right| left.model.cmp(&right.model));
        self.runtime_model_restrictions
            .dedup_by(|left, right| left.model == right.model);
    }

    fn standard_subscription_account_type(&self) -> Option<&'static str> {
        infer_standard_account_type_id_from_subscription(
            self.subscription_title.as_deref(),
            self.subscription_type.as_deref(),
        )
    }

    pub fn max_concurrency_override(&self) -> Option<u32> {
        self.max_concurrency.filter(|limit| *limit > 0)
    }

    pub fn resolved_account_type_source(&self) -> Option<ResolvedAccountTypeSource> {
        if self.account_type.is_some() {
            return Some(ResolvedAccountTypeSource::Explicit);
        }

        if self
            .subscription_title
            .as_deref()
            .and_then(infer_standard_account_type_id)
            .is_some()
        {
            return Some(ResolvedAccountTypeSource::SubscriptionTitle);
        }

        self.subscription_type
            .as_deref()
            .and_then(infer_standard_account_type_id)
            .map(|_| ResolvedAccountTypeSource::SubscriptionType)
    }

    pub fn resolved_account_type(&self) -> Option<String> {
        self.resolved_account_type_key()
            .map(|value| value.into_owned())
    }

    fn resolved_account_type_key(&self) -> Option<Cow<'_, str>> {
        match self.resolved_account_type_source()? {
            ResolvedAccountTypeSource::Explicit => self.account_type.as_deref().map(Cow::Borrowed),
            ResolvedAccountTypeSource::SubscriptionTitle => self
                .subscription_title
                .as_deref()
                .and_then(infer_standard_account_type_id)
                .map(Cow::Borrowed),
            ResolvedAccountTypeSource::SubscriptionType => self
                .subscription_type
                .as_deref()
                .and_then(infer_standard_account_type_id)
                .map(Cow::Borrowed),
        }
    }

    /// 获取有效的并发上限
    ///
    /// 返回 `None` 表示不限制并发。
    pub fn effective_max_concurrency(&self) -> Option<usize> {
        self.effective_max_concurrency_with_default(None)
    }

    /// 获取有效的并发上限，并在凭据未配置时回退到默认值。
    ///
    /// 返回 `None` 表示不限制并发。
    pub fn effective_max_concurrency_with_default(
        &self,
        default_limit: Option<u32>,
    ) -> Option<usize> {
        self.effective_max_concurrency_with_policy(default_limit, None)
    }

    /// 获取有效的并发上限，并考虑账号类型调度策略。
    ///
    /// 优先级：凭据级 maxConcurrency > 账号类型调度策略 > 全局默认值。
    pub fn effective_max_concurrency_with_policy(
        &self,
        default_limit: Option<u32>,
        account_type_dispatch_policy: Option<&AccountTypeDispatchPolicy>,
    ) -> Option<usize> {
        self.max_concurrency_override()
            .or_else(|| {
                account_type_dispatch_policy
                    .and_then(AccountTypeDispatchPolicy::effective_max_concurrency)
            })
            .or(default_limit)
            .and_then(|limit| usize::try_from(limit).ok())
            .filter(|limit| *limit > 0)
    }

    pub fn effective_max_concurrency_source(
        &self,
        default_limit: Option<u32>,
        account_type_dispatch_policy: Option<&AccountTypeDispatchPolicy>,
    ) -> Option<DispatchSettingSource> {
        if self.max_concurrency_override().is_some() {
            return Some(DispatchSettingSource::Credential);
        }
        if account_type_dispatch_policy
            .and_then(AccountTypeDispatchPolicy::effective_max_concurrency)
            .is_some()
        {
            return Some(DispatchSettingSource::AccountType);
        }
        default_limit
            .filter(|limit| *limit > 0)
            .map(|_| DispatchSettingSource::GlobalDefault)
    }

    /// 获取凭据级 token bucket 容量覆盖
    pub fn rate_limit_bucket_capacity_override(&self) -> Option<f64> {
        self.rate_limit_bucket_capacity
            .and_then(non_negative_finite)
    }

    /// 获取凭据级 token bucket 回填速率覆盖
    pub fn rate_limit_refill_per_second_override(&self) -> Option<f64> {
        self.rate_limit_refill_per_second
            .and_then(non_negative_finite)
    }

    pub fn effective_rate_limit_bucket_capacity_source(
        &self,
        default_capacity: f64,
        account_type_dispatch_policy: Option<&AccountTypeDispatchPolicy>,
    ) -> Option<DispatchSettingSource> {
        if self.rate_limit_bucket_capacity_override().is_some() {
            return Some(DispatchSettingSource::Credential);
        }
        if account_type_dispatch_policy
            .and_then(AccountTypeDispatchPolicy::rate_limit_bucket_capacity_override)
            .is_some()
        {
            return Some(DispatchSettingSource::AccountType);
        }
        default_capacity
            .is_finite()
            .then_some(DispatchSettingSource::GlobalDefault)
    }

    pub fn effective_rate_limit_refill_per_second_source(
        &self,
        default_refill_per_second: f64,
        account_type_dispatch_policy: Option<&AccountTypeDispatchPolicy>,
    ) -> Option<DispatchSettingSource> {
        if self.rate_limit_refill_per_second_override().is_some() {
            return Some(DispatchSettingSource::Credential);
        }
        if account_type_dispatch_policy
            .and_then(AccountTypeDispatchPolicy::rate_limit_refill_per_second_override)
            .is_some()
        {
            return Some(DispatchSettingSource::AccountType);
        }
        default_refill_per_second
            .is_finite()
            .then_some(DispatchSettingSource::GlobalDefault)
    }

    /// 检查凭据是否支持 Opus 模型
    ///
    /// Free 账号不支持 Opus 模型，需要 PRO 或更高等级订阅
    pub fn supports_opus(&self) -> bool {
        self.standard_subscription_account_type()
            .map(|account_type| account_type != "free")
            // 如果还没有获取订阅信息，暂时允许（首次使用时会获取）
            .unwrap_or(true)
    }

    /// 检查凭据是否适合作为真实 Opus 4.7 的候选账号
    ///
    /// 在上游正式全量开放前，所有非 FREE 档位都保留为候选，
    /// 交由运行时根据 `INVALID_MODEL_ID` 动态探测。
    pub fn supports_real_opus_4_7(&self) -> bool {
        self.standard_subscription_account_type()
            .map(|account_type| account_type != "free")
            .unwrap_or(true)
    }

    /// 返回真实 Opus 4.7 的调度偏好，数值越小越优先。
    pub fn opus_4_7_preference_rank(&self) -> u8 {
        match self.standard_subscription_account_type() {
            Some("max" | "ultra" | "pro-plus") => 0,
            Some("free") => 2,
            Some(_) | None => 1,
        }
    }

    pub fn account_type_policy<'a>(
        &'a self,
        policies: &'a std::collections::BTreeMap<String, ModelSupportPolicy>,
    ) -> Option<&'a ModelSupportPolicy> {
        let resolved_account_type = self.resolved_account_type_key()?;
        policies.get(resolved_account_type.as_ref())
    }

    pub fn account_type_dispatch_policy<'a>(
        &'a self,
        policies: &'a std::collections::BTreeMap<String, AccountTypeDispatchPolicy>,
    ) -> Option<&'a AccountTypeDispatchPolicy> {
        let resolved_account_type = self.resolved_account_type_key()?;
        policies.get(resolved_account_type.as_ref())
    }

    pub fn policy_allows_model(
        &self,
        account_type_policy: Option<&ModelSupportPolicy>,
        model: &str,
        model_cooldown_enabled: bool,
    ) -> bool {
        let Some(selector) = normalize_model_selector(model) else {
            return true;
        };

        if model_cooldown_enabled {
            if self
                .runtime_model_restrictions
                .iter()
                .filter(|restriction| restriction.is_active_at(Utc::now()))
                .any(|restriction| {
                    normalize_model_selector(&restriction.model).is_some_and(|entry| {
                        entry.family == selector.family || entry.exact == selector.exact
                    })
                })
            {
                return false;
            }
        }

        if account_type_policy.is_some_and(|policy| policy.matches_blocked(&selector)) {
            return false;
        }
        if self
            .blocked_models
            .iter()
            .any(|entry| crate::model::model_policy::matches_model_entry(entry, &selector))
        {
            return false;
        }

        let has_any_allowlist = account_type_policy
            .is_some_and(|policy| !policy.allowed_models.is_empty())
            || !self.allowed_models.is_empty();
        if !has_any_allowlist {
            return true;
        }

        account_type_policy.is_some_and(|policy| policy.matches_allowed(&selector))
            || self
                .allowed_models
                .iter()
                .any(|entry| crate::model::model_policy::matches_model_entry(entry, &selector))
    }

    pub fn upsert_runtime_model_restriction(
        &mut self,
        model: &str,
        expires_at: DateTime<Utc>,
    ) -> bool {
        let Some(selector) = normalize_model_selector(model) else {
            return false;
        };
        let family = selector.family;
        let expires_at_rfc3339 = expires_at.to_rfc3339();

        let mut changed = false;
        let mut found = false;
        self.runtime_model_restrictions.retain_mut(|restriction| {
            let keep = restriction.normalize() && restriction.is_active_at(Utc::now());
            if !keep {
                changed = true;
            }
            keep
        });

        for restriction in &mut self.runtime_model_restrictions {
            if restriction.model == family {
                found = true;
                if restriction.expires_at != expires_at_rfc3339 {
                    restriction.expires_at = expires_at_rfc3339.clone();
                    changed = true;
                }
            }
        }

        if !found {
            self.runtime_model_restrictions
                .push(RuntimeModelRestriction::new(family, expires_at));
            changed = true;
        }

        self.runtime_model_restrictions
            .sort_by(|left, right| left.model.cmp(&right.model));
        changed
    }

    pub fn clear_runtime_model_restrictions(&mut self) -> bool {
        if self.runtime_model_restrictions.is_empty() {
            return false;
        }
        self.runtime_model_restrictions.clear();
        true
    }

    pub fn active_runtime_model_restrictions(&self) -> Vec<RuntimeModelRestriction> {
        let now = Utc::now();
        self.runtime_model_restrictions
            .iter()
            .filter(|restriction| restriction.is_active_at(now))
            .cloned()
            .collect()
    }
}

#[cfg(test)]
impl KiroCredentials {
    fn from_json(json_string: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json_string)
    }

    fn to_pretty_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::config::Config;

    #[test]
    fn test_from_json() {
        let json = r#"{
            "accessToken": "test_token",
            "refreshToken": "test_refresh",
            "profileArn": "arn:aws:test",
            "expiresAt": "2024-01-01T00:00:00Z",
            "authMethod": "social"
        }"#;

        let creds = KiroCredentials::from_json(json).unwrap();
        assert_eq!(creds.access_token, Some("test_token".to_string()));
        assert_eq!(creds.refresh_token, Some("test_refresh".to_string()));
        assert_eq!(creds.profile_arn, Some("arn:aws:test".to_string()));
        assert_eq!(creds.expires_at, Some("2024-01-01T00:00:00Z".to_string()));
        assert_eq!(creds.auth_method, Some("social".to_string()));
    }

    #[test]
    fn test_from_json_with_unknown_keys() {
        let json = r#"{
            "accessToken": "test_token",
            "unknownField": "should be ignored"
        }"#;

        let creds = KiroCredentials::from_json(json).unwrap();
        assert_eq!(creds.access_token, Some("test_token".to_string()));
    }

    #[test]
    fn test_to_json() {
        let creds = KiroCredentials {
            id: None,
            access_token: Some("token".to_string()),
            refresh_token: None,
            profile_arn: None,
            expires_at: None,
            auth_method: Some("social".to_string()),
            client_id: None,
            client_secret: None,
            start_url: None,
            priority: 0,
            max_concurrency: None,
            rate_limit_bucket_capacity: None,
            rate_limit_refill_per_second: None,
            region: None,
            auth_region: None,
            api_region: None,
            machine_id: None,
            email: None,
            subscription_title: None,
            subscription_type: None,
            account_type: None,
            allowed_models: vec![],
            blocked_models: vec![],
            runtime_model_restrictions: vec![],
            imported_at: None,
            proxy_url: None,
            proxy_username: None,
            proxy_password: None,
            disabled: false,
            disabled_reason: None,
            disabled_at: None,
            last_error_status: None,
            last_error_summary: None,
        };

        let json = creds.to_pretty_json().unwrap();
        assert!(json.contains("accessToken"));
        assert!(json.contains("authMethod"));
        assert!(!json.contains("refreshToken"));
        // priority 为 0 时不序列化
        assert!(!json.contains("priority"));
    }

    #[test]
    fn test_default_credentials_path() {
        assert_eq!(
            KiroCredentials::default_credentials_path(),
            "credentials.json"
        );
    }

    #[test]
    fn test_priority_default() {
        let json = r#"{"refreshToken": "test"}"#;
        let creds = KiroCredentials::from_json(json).unwrap();
        assert_eq!(creds.priority, 0);
    }

    #[test]
    fn test_priority_explicit() {
        let json = r#"{"refreshToken": "test", "priority": 5}"#;
        let creds = KiroCredentials::from_json(json).unwrap();
        assert_eq!(creds.priority, 5);
    }

    #[test]
    fn test_effective_max_concurrency_none_when_missing_or_zero() {
        let creds = KiroCredentials::default();
        assert_eq!(creds.effective_max_concurrency(), None);

        let json = r#"{"refreshToken":"test","maxConcurrency":0}"#;
        let creds = KiroCredentials::from_json(json).unwrap();
        assert_eq!(creds.effective_max_concurrency(), None);
    }

    #[test]
    fn test_effective_max_concurrency_returns_positive_limit() {
        let json = r#"{"refreshToken":"test","maxConcurrency":3}"#;
        let creds = KiroCredentials::from_json(json).unwrap();
        assert_eq!(creds.effective_max_concurrency(), Some(3));
    }

    #[test]
    fn test_effective_max_concurrency_falls_back_to_account_type_dispatch_policy() {
        let mut creds = KiroCredentials::default();
        creds.subscription_title = Some("KIRO POWER".to_string());

        let policy = AccountTypeDispatchPolicy {
            max_concurrency: Some(32),
            rate_limit_bucket_capacity: Some(0.0),
            rate_limit_refill_per_second: Some(0.0),
        };

        assert_eq!(
            creds.effective_max_concurrency_with_policy(Some(3), Some(&policy)),
            Some(32)
        );
        assert_eq!(
            creds.effective_max_concurrency_source(Some(3), Some(&policy)),
            Some(DispatchSettingSource::AccountType)
        );
    }

    #[test]
    fn test_resolved_account_type_prefers_explicit_value() {
        let mut creds = KiroCredentials::default();
        creds.account_type = Some("power-custom".to_string());
        creds.subscription_title = Some("KIRO POWER".to_string());

        assert_eq!(
            creds.resolved_account_type(),
            Some("power-custom".to_string())
        );
        assert_eq!(
            creds.resolved_account_type_source(),
            Some(ResolvedAccountTypeSource::Explicit)
        );
    }

    #[test]
    fn test_resolved_account_type_falls_back_to_subscription_title() {
        let mut creds = KiroCredentials::default();
        creds.subscription_title = Some("KIRO POWER".to_string());

        assert_eq!(creds.resolved_account_type(), Some("power".to_string()));
        assert_eq!(
            creds.resolved_account_type_source(),
            Some(ResolvedAccountTypeSource::SubscriptionTitle)
        );
    }

    #[test]
    fn test_resolved_account_type_falls_back_to_subscription_type() {
        let mut creds = KiroCredentials::default();
        creds.subscription_type = Some("Q_DEVELOPER_STANDALONE_PRO_PLUS".to_string());

        assert_eq!(creds.resolved_account_type(), Some("pro-plus".to_string()));
        assert_eq!(
            creds.resolved_account_type_source(),
            Some(ResolvedAccountTypeSource::SubscriptionType)
        );
    }

    #[test]
    fn test_detected_auth_account_type_uses_start_url_for_idc_accounts() {
        let mut builder = KiroCredentials {
            auth_method: Some("idc".to_string()),
            client_id: Some("client".to_string()),
            client_secret: Some("secret".to_string()),
            start_url: Some("https://view.awsapps.com/start/".to_string()),
            ..Default::default()
        };
        assert_eq!(
            builder.detected_auth_account_type().as_deref(),
            Some("builder-id")
        );

        builder.start_url = Some("https://example.awsapps.com/start".to_string());
        assert_eq!(
            builder.detected_auth_account_type().as_deref(),
            Some("enterprise")
        );
    }

    #[test]
    fn test_effective_rate_limit_sources_follow_override_priority() {
        let mut creds = KiroCredentials::default();
        creds.subscription_title = Some("KIRO POWER".to_string());

        let policy = AccountTypeDispatchPolicy {
            max_concurrency: Some(32),
            rate_limit_bucket_capacity: Some(0.0),
            rate_limit_refill_per_second: Some(0.0),
        };

        assert_eq!(
            creds.effective_rate_limit_bucket_capacity_source(6.0, Some(&policy)),
            Some(DispatchSettingSource::AccountType)
        );
        assert_eq!(
            creds.effective_rate_limit_refill_per_second_source(2.0, Some(&policy)),
            Some(DispatchSettingSource::AccountType)
        );

        creds.rate_limit_bucket_capacity = Some(3.0);
        creds.rate_limit_refill_per_second = Some(1.5);

        assert_eq!(
            creds.effective_rate_limit_bucket_capacity_source(6.0, Some(&policy)),
            Some(DispatchSettingSource::Credential)
        );
        assert_eq!(
            creds.effective_rate_limit_refill_per_second_source(2.0, Some(&policy)),
            Some(DispatchSettingSource::Credential)
        );
    }

    #[test]
    fn test_account_type_policy_falls_back_to_inferred_standard_account_type() {
        let mut creds = KiroCredentials::default();
        creds.subscription_title = Some("KIRO POWER".to_string());

        let mut policies = std::collections::BTreeMap::new();
        policies.insert(
            "power".to_string(),
            ModelSupportPolicy {
                allowed_models: vec!["claude-sonnet-4.6".to_string()],
                blocked_models: vec![],
            },
        );

        assert!(creds.account_type_policy(&policies).is_some());
    }

    #[test]
    fn test_account_type_dispatch_policy_falls_back_to_inferred_standard_account_type() {
        let mut creds = KiroCredentials::default();
        creds.subscription_title = Some("KIRO POWER".to_string());

        let mut policies = std::collections::BTreeMap::new();
        policies.insert(
            "power".to_string(),
            AccountTypeDispatchPolicy {
                max_concurrency: Some(32),
                rate_limit_bucket_capacity: Some(0.0),
                rate_limit_refill_per_second: Some(0.0),
            },
        );

        assert_eq!(
            creds
                .account_type_dispatch_policy(&policies)
                .and_then(AccountTypeDispatchPolicy::effective_max_concurrency),
            Some(32)
        );
    }

    #[test]
    fn test_credentials_config_single() {
        let json = r#"{"refreshToken": "test", "expiresAt": "2025-12-31T00:00:00Z"}"#;
        let config: CredentialsConfig = serde_json::from_str(json).unwrap();
        assert!(matches!(config, CredentialsConfig::Single(_)));
    }

    #[test]
    fn test_credentials_config_multiple() {
        let json = r#"[
            {"refreshToken": "test1", "priority": 1},
            {"refreshToken": "test2", "priority": 0}
        ]"#;
        let config: CredentialsConfig = serde_json::from_str(json).unwrap();
        assert!(matches!(config, CredentialsConfig::Multiple(_)));
        assert_eq!(config.into_sorted_credentials().len(), 2);
    }

    #[test]
    fn test_credentials_config_priority_sorting() {
        let json = r#"[
            {"refreshToken": "t1", "priority": 2},
            {"refreshToken": "t2", "priority": 0},
            {"refreshToken": "t3", "priority": 1}
        ]"#;
        let config: CredentialsConfig = serde_json::from_str(json).unwrap();
        let list = config.into_sorted_credentials();

        // 验证按优先级排序
        assert_eq!(list[0].refresh_token, Some("t2".to_string())); // priority 0
        assert_eq!(list[1].refresh_token, Some("t3".to_string())); // priority 1
        assert_eq!(list[2].refresh_token, Some("t1".to_string())); // priority 2
    }

    // ============ Region 字段测试 ============

    #[test]
    fn test_region_field_parsing() {
        // 测试解析包含 region 字段的 JSON
        let json = r#"{
            "refreshToken": "test_refresh",
            "region": "us-east-1"
        }"#;

        let creds = KiroCredentials::from_json(json).unwrap();
        assert_eq!(creds.refresh_token, Some("test_refresh".to_string()));
        assert_eq!(creds.region, Some("us-east-1".to_string()));
    }

    #[test]
    fn test_region_field_missing_backward_compat() {
        // 测试向后兼容：不包含 region 字段的旧格式 JSON
        let json = r#"{
            "refreshToken": "test_refresh",
            "authMethod": "social"
        }"#;

        let creds = KiroCredentials::from_json(json).unwrap();
        assert_eq!(creds.refresh_token, Some("test_refresh".to_string()));
        assert_eq!(creds.region, None);
    }

    #[test]
    fn test_region_field_serialization() {
        // 测试序列化时正确输出 region 字段
        let creds = KiroCredentials {
            id: None,
            access_token: None,
            refresh_token: Some("test".to_string()),
            profile_arn: None,
            expires_at: None,
            auth_method: None,
            client_id: None,
            client_secret: None,
            start_url: None,
            priority: 0,
            max_concurrency: None,
            rate_limit_bucket_capacity: None,
            rate_limit_refill_per_second: None,
            region: Some("eu-west-1".to_string()),
            auth_region: None,
            api_region: None,
            machine_id: None,
            email: None,
            subscription_title: None,
            subscription_type: None,
            account_type: None,
            allowed_models: vec![],
            blocked_models: vec![],
            runtime_model_restrictions: vec![],
            imported_at: None,
            proxy_url: None,
            proxy_username: None,
            proxy_password: None,
            disabled: false,
            disabled_reason: None,
            disabled_at: None,
            last_error_status: None,
            last_error_summary: None,
        };

        let json = creds.to_pretty_json().unwrap();
        assert!(json.contains("region"));
        assert!(json.contains("eu-west-1"));
    }

    #[test]
    fn test_region_field_none_not_serialized() {
        // 测试 region 为 None 时不序列化
        let creds = KiroCredentials {
            id: None,
            access_token: None,
            refresh_token: Some("test".to_string()),
            profile_arn: None,
            expires_at: None,
            auth_method: None,
            client_id: None,
            client_secret: None,
            start_url: None,
            priority: 0,
            max_concurrency: None,
            rate_limit_bucket_capacity: None,
            rate_limit_refill_per_second: None,
            region: None,
            auth_region: None,
            api_region: None,
            machine_id: None,
            email: None,
            subscription_title: None,
            subscription_type: None,
            account_type: None,
            allowed_models: vec![],
            blocked_models: vec![],
            runtime_model_restrictions: vec![],
            imported_at: None,
            proxy_url: None,
            proxy_username: None,
            proxy_password: None,
            disabled: false,
            disabled_reason: None,
            disabled_at: None,
            last_error_status: None,
            last_error_summary: None,
        };

        let json = creds.to_pretty_json().unwrap();
        assert!(!json.contains("region"));
    }

    // ============ MachineId 字段测试 ============

    #[test]
    fn test_machine_id_field_parsing() {
        let machine_id = "a".repeat(64);
        let json = format!(
            r#"{{
                "refreshToken": "test_refresh",
                "machineId": "{machine_id}"
            }}"#
        );

        let creds = KiroCredentials::from_json(&json).unwrap();
        assert_eq!(creds.refresh_token, Some("test_refresh".to_string()));
        assert_eq!(creds.machine_id, Some(machine_id));
    }

    #[test]
    fn test_machine_id_field_serialization() {
        let mut creds = KiroCredentials::default();
        creds.refresh_token = Some("test".to_string());
        creds.machine_id = Some("b".repeat(64));

        let json = creds.to_pretty_json().unwrap();
        assert!(json.contains("machineId"));
    }

    #[test]
    fn test_machine_id_field_none_not_serialized() {
        let mut creds = KiroCredentials::default();
        creds.refresh_token = Some("test".to_string());
        creds.machine_id = None;

        let json = creds.to_pretty_json().unwrap();
        assert!(!json.contains("machineId"));
    }

    #[test]
    fn test_multiple_credentials_with_different_regions() {
        // 测试多凭据场景下不同凭据使用各自的 region
        let json = r#"[
            {"refreshToken": "t1", "region": "us-east-1"},
            {"refreshToken": "t2", "region": "eu-west-1"},
            {"refreshToken": "t3"}
        ]"#;

        let config: CredentialsConfig = serde_json::from_str(json).unwrap();
        let list = config.into_sorted_credentials();

        assert_eq!(list[0].region, Some("us-east-1".to_string()));
        assert_eq!(list[1].region, Some("eu-west-1".to_string()));
        assert_eq!(list[2].region, None);
    }

    #[test]
    fn test_region_field_with_all_fields() {
        // 测试包含所有字段的完整 JSON
        let json = r#"{
            "id": 1,
            "accessToken": "access",
            "refreshToken": "refresh",
            "profileArn": "arn:aws:test",
            "expiresAt": "2025-12-31T00:00:00Z",
            "authMethod": "idc",
            "clientId": "client123",
            "clientSecret": "secret456",
            "priority": 5,
            "region": "ap-northeast-1"
        }"#;

        let creds = KiroCredentials::from_json(json).unwrap();
        assert_eq!(creds.id, Some(1));
        assert_eq!(creds.access_token, Some("access".to_string()));
        assert_eq!(creds.refresh_token, Some("refresh".to_string()));
        assert_eq!(creds.profile_arn, Some("arn:aws:test".to_string()));
        assert_eq!(creds.expires_at, Some("2025-12-31T00:00:00Z".to_string()));
        assert_eq!(creds.auth_method, Some("idc".to_string()));
        assert_eq!(creds.client_id, Some("client123".to_string()));
        assert_eq!(creds.client_secret, Some("secret456".to_string()));
        assert_eq!(creds.priority, 5);
        assert_eq!(creds.region, Some("ap-northeast-1".to_string()));
    }

    #[test]
    fn test_region_roundtrip() {
        // 测试序列化和反序列化的往返一致性
        let original = KiroCredentials {
            id: Some(42),
            access_token: Some("token".to_string()),
            refresh_token: Some("refresh".to_string()),
            profile_arn: None,
            expires_at: None,
            auth_method: Some("social".to_string()),
            client_id: None,
            client_secret: None,
            start_url: None,
            priority: 3,
            max_concurrency: None,
            rate_limit_bucket_capacity: None,
            rate_limit_refill_per_second: None,
            region: Some("us-west-2".to_string()),
            auth_region: None,
            api_region: None,
            machine_id: Some("c".repeat(64)),
            email: None,
            subscription_title: None,
            subscription_type: None,
            account_type: None,
            allowed_models: vec![],
            blocked_models: vec![],
            runtime_model_restrictions: vec![],
            imported_at: Some("2025-01-01T00:00:00Z".to_string()),
            proxy_url: None,
            proxy_username: None,
            proxy_password: None,
            disabled: false,
            disabled_reason: None,
            disabled_at: None,
            last_error_status: None,
            last_error_summary: None,
        };

        let json = original.to_pretty_json().unwrap();
        let parsed = KiroCredentials::from_json(&json).unwrap();

        assert_eq!(parsed.id, original.id);
        assert_eq!(parsed.access_token, original.access_token);
        assert_eq!(parsed.refresh_token, original.refresh_token);
        assert_eq!(parsed.priority, original.priority);
        assert_eq!(parsed.region, original.region);
        assert_eq!(parsed.machine_id, original.machine_id);
        assert_eq!(parsed.imported_at, original.imported_at);
    }

    // ============ auth_region / api_region 字段测试 ============

    #[test]
    fn test_auth_region_field_parsing() {
        let json = r#"{
            "refreshToken": "test_refresh",
            "authRegion": "eu-central-1"
        }"#;
        let creds = KiroCredentials::from_json(json).unwrap();
        assert_eq!(creds.auth_region, Some("eu-central-1".to_string()));
        assert_eq!(creds.api_region, None);
    }

    #[test]
    fn test_api_region_field_parsing() {
        let json = r#"{
            "refreshToken": "test_refresh",
            "apiRegion": "ap-southeast-1"
        }"#;
        let creds = KiroCredentials::from_json(json).unwrap();
        assert_eq!(creds.api_region, Some("ap-southeast-1".to_string()));
        assert_eq!(creds.auth_region, None);
    }

    #[test]
    fn test_auth_api_region_serialization() {
        let mut creds = KiroCredentials::default();
        creds.refresh_token = Some("test".to_string());
        creds.auth_region = Some("eu-west-1".to_string());
        creds.api_region = Some("us-west-2".to_string());

        let json = creds.to_pretty_json().unwrap();
        assert!(json.contains("authRegion"));
        assert!(json.contains("eu-west-1"));
        assert!(json.contains("apiRegion"));
        assert!(json.contains("us-west-2"));
    }

    #[test]
    fn test_auth_api_region_none_not_serialized() {
        let mut creds = KiroCredentials::default();
        creds.refresh_token = Some("test".to_string());
        creds.auth_region = None;
        creds.api_region = None;

        let json = creds.to_pretty_json().unwrap();
        assert!(!json.contains("authRegion"));
        assert!(!json.contains("apiRegion"));
    }

    #[test]
    fn test_auth_api_region_roundtrip() {
        let mut original = KiroCredentials::default();
        original.refresh_token = Some("refresh".to_string());
        original.region = Some("us-east-1".to_string());
        original.auth_region = Some("eu-west-1".to_string());
        original.api_region = Some("ap-northeast-1".to_string());

        let json = original.to_pretty_json().unwrap();
        let parsed = KiroCredentials::from_json(&json).unwrap();

        assert_eq!(parsed.region, original.region);
        assert_eq!(parsed.auth_region, original.auth_region);
        assert_eq!(parsed.api_region, original.api_region);
    }

    #[test]
    fn test_backward_compat_no_auth_api_region() {
        // 旧格式 JSON 不包含 authRegion/apiRegion，应正常解析
        let json = r#"{
            "refreshToken": "test_refresh",
            "region": "us-east-1"
        }"#;
        let creds = KiroCredentials::from_json(json).unwrap();
        assert_eq!(creds.region, Some("us-east-1".to_string()));
        assert_eq!(creds.auth_region, None);
        assert_eq!(creds.api_region, None);
    }

    // ============ effective_auth_region / effective_api_region 优先级测试 ============

    #[test]
    fn test_effective_auth_region_credential_auth_region_highest() {
        // 凭据.auth_region > 凭据.region > config.auth_region > config.region
        let mut config = Config::default();
        config.region = "config-region".to_string();
        config.auth_region = Some("config-auth-region".to_string());

        let mut creds = KiroCredentials::default();
        creds.region = Some("cred-region".to_string());
        creds.auth_region = Some("cred-auth-region".to_string());

        assert_eq!(creds.effective_auth_region(&config), "cred-auth-region");
    }

    #[test]
    fn test_effective_auth_region_fallback_to_credential_region() {
        let mut config = Config::default();
        config.region = "config-region".to_string();
        config.auth_region = Some("config-auth-region".to_string());

        let mut creds = KiroCredentials::default();
        creds.region = Some("cred-region".to_string());
        // auth_region 未设置

        assert_eq!(creds.effective_auth_region(&config), "cred-region");
    }

    #[test]
    fn test_effective_auth_region_fallback_to_config_auth_region() {
        let mut config = Config::default();
        config.region = "config-region".to_string();
        config.auth_region = Some("config-auth-region".to_string());

        let creds = KiroCredentials::default();
        // auth_region 和 region 均未设置

        assert_eq!(creds.effective_auth_region(&config), "config-auth-region");
    }

    #[test]
    fn test_effective_auth_region_fallback_to_config_region() {
        let mut config = Config::default();
        config.region = "config-region".to_string();
        // config.auth_region 未设置

        let creds = KiroCredentials::default();

        assert_eq!(creds.effective_auth_region(&config), "config-region");
    }

    #[test]
    fn test_effective_api_region_credential_api_region_highest() {
        // 凭据.api_region > config.api_region > config.region
        let mut config = Config::default();
        config.region = "config-region".to_string();
        config.api_region = Some("config-api-region".to_string());

        let mut creds = KiroCredentials::default();
        creds.api_region = Some("cred-api-region".to_string());

        assert_eq!(creds.effective_api_region(&config), "cred-api-region");
    }

    #[test]
    fn test_effective_api_region_fallback_to_config_api_region() {
        let mut config = Config::default();
        config.region = "config-region".to_string();
        config.api_region = Some("config-api-region".to_string());

        let creds = KiroCredentials::default();

        assert_eq!(creds.effective_api_region(&config), "config-api-region");
    }

    #[test]
    fn test_effective_api_region_fallback_to_config_region() {
        let mut config = Config::default();
        config.region = "config-region".to_string();

        let creds = KiroCredentials::default();

        assert_eq!(creds.effective_api_region(&config), "config-region");
    }

    #[test]
    fn test_effective_api_region_ignores_credential_region() {
        // 凭据.region 不参与 api_region 的回退链
        let mut config = Config::default();
        config.region = "config-region".to_string();

        let mut creds = KiroCredentials::default();
        creds.region = Some("cred-region".to_string());

        assert_eq!(creds.effective_api_region(&config), "config-region");
    }

    #[test]
    fn test_auth_and_api_region_independent() {
        // auth_region 和 api_region 互不影响
        let mut config = Config::default();
        config.region = "default".to_string();

        let mut creds = KiroCredentials::default();
        creds.auth_region = Some("auth-only".to_string());
        creds.api_region = Some("api-only".to_string());

        assert_eq!(creds.effective_auth_region(&config), "auth-only");
        assert_eq!(creds.effective_api_region(&config), "api-only");
    }

    // ============ 凭据级代理优先级测试 ============

    #[test]
    fn test_effective_proxy_credential_overrides_global() {
        let global = ProxyConfig::new("http://global:8080");
        let mut creds = KiroCredentials::default();
        creds.proxy_url = Some("socks5://cred:1080".to_string());

        let result = creds.effective_proxy(Some(&global));
        assert_eq!(result, Some(ProxyConfig::new("socks5://cred:1080")));
    }

    #[test]
    fn test_effective_proxy_credential_with_auth() {
        let global = ProxyConfig::new("http://global:8080");
        let mut creds = KiroCredentials::default();
        creds.proxy_url = Some("http://proxy:3128".to_string());
        creds.proxy_username = Some("user".to_string());
        creds.proxy_password = Some("pass".to_string());

        let result = creds.effective_proxy(Some(&global));
        let expected = ProxyConfig::new("http://proxy:3128").with_auth("user", "pass");
        assert_eq!(result, Some(expected));
    }

    #[test]
    fn test_effective_proxy_direct_bypasses_global() {
        let global = ProxyConfig::new("http://global:8080");
        let mut creds = KiroCredentials::default();
        creds.proxy_url = Some("direct".to_string());

        let result = creds.effective_proxy(Some(&global));
        assert_eq!(result, None);
    }

    #[test]
    fn test_effective_proxy_direct_case_insensitive() {
        let global = ProxyConfig::new("http://global:8080");
        let mut creds = KiroCredentials::default();
        creds.proxy_url = Some("DIRECT".to_string());

        let result = creds.effective_proxy(Some(&global));
        assert_eq!(result, None);
    }

    #[test]
    fn test_effective_proxy_fallback_to_global() {
        let global = ProxyConfig::new("http://global:8080");
        let creds = KiroCredentials::default();

        let result = creds.effective_proxy(Some(&global));
        assert_eq!(result, Some(ProxyConfig::new("http://global:8080")));
    }

    #[test]
    fn test_effective_proxy_none_when_no_proxy() {
        let creds = KiroCredentials::default();
        let result = creds.effective_proxy(None);
        assert_eq!(result, None);
    }

    #[test]
    fn test_supports_real_opus_4_7_allows_power_tier_as_candidate() {
        let creds = KiroCredentials {
            subscription_title: Some("KIRO POWER".to_string()),
            ..Default::default()
        };

        assert!(creds.supports_real_opus_4_7());
        assert_eq!(creds.opus_4_7_preference_rank(), 1);
    }

    #[test]
    fn test_supports_real_opus_4_7_prefers_pro_plus_tier() {
        let creds = KiroCredentials {
            subscription_title: Some("KIRO PRO+".to_string()),
            ..Default::default()
        };

        assert!(creds.supports_real_opus_4_7());
        assert_eq!(creds.opus_4_7_preference_rank(), 0);
    }

    #[test]
    fn test_supports_real_opus_4_7_still_rejects_free_tier() {
        let creds = KiroCredentials {
            subscription_title: Some("KIRO FREE".to_string()),
            ..Default::default()
        };

        assert!(!creds.supports_real_opus_4_7());
        assert_eq!(creds.opus_4_7_preference_rank(), 2);
    }

    #[test]
    fn test_subscription_type_drives_model_support_when_title_missing() {
        let free = KiroCredentials {
            subscription_type: Some("Q_DEVELOPER_STANDALONE_FREE".to_string()),
            ..Default::default()
        };
        assert!(!free.supports_opus());
        assert!(!free.supports_real_opus_4_7());
        assert_eq!(free.opus_4_7_preference_rank(), 2);

        let pro_plus = KiroCredentials {
            subscription_type: Some("Q_DEVELOPER_STANDALONE_PRO_PLUS".to_string()),
            ..Default::default()
        };
        assert!(pro_plus.supports_opus());
        assert!(pro_plus.supports_real_opus_4_7());
        assert_eq!(pro_plus.opus_4_7_preference_rank(), 0);
    }
}
