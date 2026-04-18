//! Token 管理模块
//!
//! 负责 Token 过期检测和刷新，支持 Social 和 IdC 认证方式
//! 支持多凭据 (MultiTokenManager) 管理

use anyhow::bail;
use chrono::{DateTime, Duration, Utc};
use parking_lot::Mutex;
use serde::Serialize;
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex as TokioMutex, Notify, oneshot};

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration as StdDuration, Instant};

use crate::common::logging::summarize_upstream_error;
use crate::http_client::{ProxyConfig, build_client};
use crate::kiro::machine_id;
use crate::kiro::model::credentials::KiroCredentials;
use crate::kiro::model::token_refresh::{
    IdcRefreshRequest, IdcRefreshResponse, RefreshRequest, RefreshResponse,
};
use crate::kiro::model::usage_limits::UsageLimitsResponse;
use crate::model::config::{Config, RequestWeightingConfig};
use crate::model::model_policy::{
    ModelSupportPolicy, normalize_account_type_policies, normalize_model_selector,
};
use crate::state::{
    DispatchLeaseReservationStatus, DispatchRuntimeBucketPolicy, DispatchRuntimeCredential,
    DispatchRuntimeSnapshot, PersistedDispatchConfig, RuntimeCoordinationStatus, StateChangeKind,
    StateChangeRevisions, StateStore, StatsEntryRecord, StatsMergeRecord, current_epoch_ms,
};

const DEFAULT_REQUEST_WEIGHT: f64 = 1.0;
const UPSTREAM_ERROR_EXCERPT_CHARS: usize = 240;

/// 检查 Token 是否在指定时间内过期
pub(crate) fn is_token_expiring_within(
    credentials: &KiroCredentials,
    minutes: i64,
) -> Option<bool> {
    credentials
        .expires_at
        .as_ref()
        .and_then(|expires_at| DateTime::parse_from_rfc3339(expires_at).ok())
        .map(|expires| expires <= Utc::now() + Duration::minutes(minutes))
}

/// 检查 Token 是否已过期（提前 5 分钟判断）
pub(crate) fn is_token_expired(credentials: &KiroCredentials) -> bool {
    is_token_expiring_within(credentials, 5).unwrap_or(true)
}

/// 检查 Token 是否即将过期（10分钟内）
pub(crate) fn is_token_expiring_soon(credentials: &KiroCredentials) -> bool {
    is_token_expiring_within(credentials, 10).unwrap_or(false)
}

fn sha256_hex(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    let result = hasher.finalize();
    format!("{:x}", result)
}

fn newer_timestamp(current: Option<String>, candidate: Option<String>) -> Option<String> {
    match (current, candidate) {
        (None, other) | (other, None) => other,
        (Some(current), Some(candidate)) => {
            let current_parsed = DateTime::parse_from_rfc3339(&current).ok();
            let candidate_parsed = DateTime::parse_from_rfc3339(&candidate).ok();

            match (current_parsed, candidate_parsed) {
                (Some(current_parsed), Some(candidate_parsed)) => {
                    if candidate_parsed > current_parsed {
                        Some(candidate)
                    } else {
                        Some(current)
                    }
                }
                _ => {
                    if candidate > current {
                        Some(candidate)
                    } else {
                        Some(current)
                    }
                }
            }
        }
    }
}

/// 验证 refreshToken 的基本有效性
pub(crate) fn validate_refresh_token(credentials: &KiroCredentials) -> anyhow::Result<()> {
    let refresh_token = credentials
        .refresh_token
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("缺少 refreshToken"))?;

    if refresh_token.is_empty() {
        bail!("refreshToken 为空");
    }

    if refresh_token.len() < 100 || refresh_token.ends_with("...") || refresh_token.contains("...")
    {
        bail!(
            "refreshToken 已被截断（长度: {} 字符）。\n\
             这通常是 Kiro IDE 为了防止凭证被第三方工具使用而故意截断的。",
            refresh_token.len()
        );
    }

    Ok(())
}

/// Refresh Token 永久失效错误
///
/// 当服务端返回 400 + `invalid_grant` 时，表示 refreshToken 已被撤销或过期，
/// 不应重试，需立即禁用对应凭据。
#[derive(Debug)]
pub(crate) struct RefreshTokenInvalidError {
    pub message: String,
}

impl fmt::Display for RefreshTokenInvalidError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for RefreshTokenInvalidError {}

#[derive(Debug)]
pub(crate) struct RuntimeRefreshLeaderRequiredError {
    pub instance_id: String,
    pub leader_id: Option<String>,
}

impl fmt::Display for RuntimeRefreshLeaderRequiredError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.leader_id {
            Some(leader_id) => write!(
                f,
                "当前实例不是运行时 leader，无法刷新共享凭据（instanceId={}, leaderId={}）",
                self.instance_id, leader_id
            ),
            None => write!(
                f,
                "当前未观察到运行时 leader，无法刷新共享凭据（instanceId={}）",
                self.instance_id
            ),
        }
    }
}

impl std::error::Error for RuntimeRefreshLeaderRequiredError {}

/// 刷新 Token
pub(crate) async fn refresh_token(
    credentials: &KiroCredentials,
    config: &Config,
    proxy: Option<&ProxyConfig>,
) -> anyhow::Result<KiroCredentials> {
    validate_refresh_token(credentials)?;

    // 根据 auth_method 选择刷新方式
    // 如果未指定 auth_method，根据是否有 clientId/clientSecret 自动判断
    let auth_method = credentials.auth_method.as_deref().unwrap_or_else(|| {
        if credentials.client_id.is_some() && credentials.client_secret.is_some() {
            "idc"
        } else {
            "social"
        }
    });

    if auth_method.eq_ignore_ascii_case("idc")
        || auth_method.eq_ignore_ascii_case("builder-id")
        || auth_method.eq_ignore_ascii_case("iam")
    {
        refresh_idc_token(credentials, config, proxy).await
    } else {
        refresh_social_token(credentials, config, proxy).await
    }
}

/// 刷新 Social Token
async fn refresh_social_token(
    credentials: &KiroCredentials,
    config: &Config,
    proxy: Option<&ProxyConfig>,
) -> anyhow::Result<KiroCredentials> {
    tracing::info!("正在刷新 Social Token...");

    let refresh_token = credentials.refresh_token.as_ref().unwrap();
    // 优先级：凭据.auth_region > 凭据.region > config.auth_region > config.region
    let region = credentials.effective_auth_region(config);

    let refresh_url = format!("https://prod.{}.auth.desktop.kiro.dev/refreshToken", region);
    let refresh_domain = format!("prod.{}.auth.desktop.kiro.dev", region);
    let machine_id = machine_id::generate_from_credentials(credentials, config)
        .ok_or_else(|| anyhow::anyhow!("无法生成 machineId"))?;
    let kiro_version = &config.kiro_version;

    let client = build_client(proxy, 60, config.tls_backend)?;
    let body = RefreshRequest {
        refresh_token: refresh_token.to_string(),
    };

    let response = client
        .post(&refresh_url)
        .header("Accept", "application/json, text/plain, */*")
        .header("Content-Type", "application/json")
        .header(
            "User-Agent",
            format!("KiroIDE-{}-{}", kiro_version, machine_id),
        )
        .header("Accept-Encoding", "gzip, compress, deflate, br")
        .header("host", &refresh_domain)
        .json(&body)
        .send()
        .await?;

    let status = response.status();
    if !status.is_success() {
        let body_text = response.text().await.unwrap_or_default();
        let error_summary =
            summarize_upstream_error(status.as_u16(), &body_text, UPSTREAM_ERROR_EXCERPT_CHARS);

        // 400 + invalid_grant + Invalid refresh token provided → refreshToken 永久失效
        if status.as_u16() == 400
            && body_text.contains("\"invalid_grant\"")
            && body_text.contains("Invalid refresh token provided")
        {
            return Err(RefreshTokenInvalidError {
                message: format!(
                    "Social refreshToken 已失效 (invalid_grant): {}",
                    error_summary
                ),
            }
            .into());
        }

        let error_msg = match status.as_u16() {
            401 => "OAuth 凭证已过期或无效，需要重新认证",
            403 => "权限不足，无法刷新 Token",
            429 => "请求过于频繁，已被限流",
            500..=599 => "服务器错误，AWS OAuth 服务暂时不可用",
            _ => "Token 刷新失败",
        };
        bail!("{}: {}", error_msg, error_summary);
    }

    let data: RefreshResponse = response.json().await?;

    let mut new_credentials = credentials.clone();
    new_credentials.access_token = Some(data.access_token);

    if let Some(new_refresh_token) = data.refresh_token {
        new_credentials.refresh_token = Some(new_refresh_token);
    }

    if let Some(profile_arn) = data.profile_arn {
        new_credentials.profile_arn = Some(profile_arn);
    }

    if let Some(expires_in) = data.expires_in {
        let expires_at = Utc::now() + Duration::seconds(expires_in);
        new_credentials.expires_at = Some(expires_at.to_rfc3339());
    }

    Ok(new_credentials)
}

/// 刷新 IdC Token (AWS SSO OIDC)
async fn refresh_idc_token(
    credentials: &KiroCredentials,
    config: &Config,
    proxy: Option<&ProxyConfig>,
) -> anyhow::Result<KiroCredentials> {
    tracing::info!("正在刷新 IdC Token...");

    let refresh_token = credentials.refresh_token.as_ref().unwrap();
    let client_id = credentials
        .client_id
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("IdC 刷新需要 clientId"))?;
    let client_secret = credentials
        .client_secret
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("IdC 刷新需要 clientSecret"))?;

    // 优先级：凭据.auth_region > 凭据.region > config.auth_region > config.region
    let region = credentials.effective_auth_region(config);
    let refresh_url = format!("https://oidc.{}.amazonaws.com/token", region);
    let os_name = &config.system_version;
    let node_version = &config.node_version;

    let x_amz_user_agent = "aws-sdk-js/3.980.0 KiroIDE";
    let user_agent = format!(
        "aws-sdk-js/3.980.0 ua/2.1 os/{} lang/js md/nodejs#{} api/sso-oidc#3.980.0 m/E KiroIDE",
        os_name, node_version
    );

    let client = build_client(proxy, 60, config.tls_backend)?;
    let body = IdcRefreshRequest {
        client_id: client_id.to_string(),
        client_secret: client_secret.to_string(),
        refresh_token: refresh_token.to_string(),
        grant_type: "refresh_token".to_string(),
    };

    let response = client
        .post(&refresh_url)
        .header("content-type", "application/json")
        .header("x-amz-user-agent", x_amz_user_agent)
        .header("user-agent", &user_agent)
        .header("host", format!("oidc.{}.amazonaws.com", region))
        .header("amz-sdk-invocation-id", uuid::Uuid::new_v4().to_string())
        .header("amz-sdk-request", "attempt=1; max=4")
        .json(&body)
        .send()
        .await?;

    let status = response.status();
    if !status.is_success() {
        let body_text = response.text().await.unwrap_or_default();
        let error_summary =
            summarize_upstream_error(status.as_u16(), &body_text, UPSTREAM_ERROR_EXCERPT_CHARS);

        // 400 + invalid_grant + Invalid refresh token provided → refreshToken 永久失效
        if status.as_u16() == 400
            && body_text.contains("\"invalid_grant\"")
            && body_text.contains("Invalid refresh token provided")
        {
            return Err(RefreshTokenInvalidError {
                message: format!("IdC refreshToken 已失效 (invalid_grant): {}", error_summary),
            }
            .into());
        }

        let error_msg = match status.as_u16() {
            401 => "IdC 凭证已过期或无效，需要重新认证",
            403 => "权限不足，无法刷新 Token",
            429 => "请求过于频繁，已被限流",
            500..=599 => "服务器错误，AWS OIDC 服务暂时不可用",
            _ => "IdC Token 刷新失败",
        };
        bail!("{}: {}", error_msg, error_summary);
    }

    let data: IdcRefreshResponse = response.json().await?;

    let mut new_credentials = credentials.clone();
    new_credentials.access_token = Some(data.access_token);

    if let Some(new_refresh_token) = data.refresh_token {
        new_credentials.refresh_token = Some(new_refresh_token);
    }

    if let Some(expires_in) = data.expires_in {
        let expires_at = Utc::now() + Duration::seconds(expires_in);
        new_credentials.expires_at = Some(expires_at.to_rfc3339());
    }

    // 同步更新 profile_arn（如果 IdC 响应中包含）
    if let Some(profile_arn) = data.profile_arn {
        new_credentials.profile_arn = Some(profile_arn);
    }

    Ok(new_credentials)
}

/// 获取使用额度信息
pub(crate) async fn get_usage_limits(
    credentials: &KiroCredentials,
    config: &Config,
    token: &str,
    proxy: Option<&ProxyConfig>,
) -> anyhow::Result<UsageLimitsResponse> {
    tracing::debug!("正在获取使用额度信息...");

    // 优先级：凭据.api_region > config.api_region > config.region
    let region = credentials.effective_api_region(config);
    let host = format!("q.{}.amazonaws.com", region);
    let machine_id = machine_id::generate_from_credentials(credentials, config)
        .ok_or_else(|| anyhow::anyhow!("无法生成 machineId"))?;
    let kiro_version = &config.kiro_version;
    let os_name = &config.system_version;
    let node_version = &config.node_version;

    // 构建 URL
    let mut url = format!(
        "https://{}/getUsageLimits?origin=AI_EDITOR&resourceType=AGENTIC_REQUEST",
        host
    );

    // profileArn 是可选的
    if let Some(profile_arn) = &credentials.profile_arn {
        url.push_str(&format!("&profileArn={}", urlencoding::encode(profile_arn)));
    }

    // 构建 User-Agent headers
    let user_agent = format!(
        "aws-sdk-js/1.0.0 ua/2.1 os/{} lang/js md/nodejs#{} api/codewhispererruntime#1.0.0 m/N,E KiroIDE-{}-{}",
        os_name, node_version, kiro_version, machine_id
    );
    let amz_user_agent = format!("aws-sdk-js/1.0.0 KiroIDE-{}-{}", kiro_version, machine_id);

    let client = build_client(proxy, 60, config.tls_backend)?;

    let response = client
        .get(&url)
        .header("x-amz-user-agent", &amz_user_agent)
        .header("user-agent", &user_agent)
        .header("host", &host)
        .header("amz-sdk-invocation-id", uuid::Uuid::new_v4().to_string())
        .header("amz-sdk-request", "attempt=1; max=1")
        .header("Authorization", format!("Bearer {}", token))
        .send()
        .await?;

    let status = response.status();
    if !status.is_success() {
        let body_text = response.text().await.unwrap_or_default();
        let error_summary =
            summarize_upstream_error(status.as_u16(), &body_text, UPSTREAM_ERROR_EXCERPT_CHARS);
        let error_msg = match status.as_u16() {
            401 => "认证失败，Token 无效或已过期",
            403 => "权限不足，无法获取使用额度",
            429 => "请求过于频繁，已被限流",
            500..=599 => "服务器错误，AWS 服务暂时不可用",
            _ => "获取使用额度失败",
        };
        bail!("{}: {}", error_msg, error_summary);
    }

    let data: UsageLimitsResponse = response.json().await?;
    Ok(data)
}

// ============================================================================
// 多凭据 Token 管理器
// ============================================================================

/// 单个凭据条目的状态
struct CredentialEntry {
    /// 凭据唯一 ID
    id: u64,
    /// 凭据信息
    credentials: KiroCredentials,
    /// API 调用连续失败次数
    failure_count: u32,
    /// Token 刷新连续失败次数
    refresh_failure_count: u32,
    /// 是否已禁用
    disabled: bool,
    /// 禁用原因（用于区分手动禁用 vs 自动禁用，便于自愈）
    disabled_reason: Option<DisabledReason>,
    /// API 调用成功次数
    success_count: u64,
    /// 自上次成功落盘后的新增成功次数，用于跨实例合并统计
    pending_success_count_delta: u64,
    /// 最后一次 API 调用时间（RFC3339 格式）
    last_used_at: Option<String>,
    /// 当前运行中的请求数
    active_requests: usize,
    /// 限流或临时避让冷却到期时间
    rate_limit_cooldown_until: Option<Instant>,
    /// 本地 token bucket 与自适应退避状态
    rate_limit_bucket: Option<AdaptiveTokenBucket>,
    /// 连续 429 次数，用于放大冷却时间
    rate_limit_hit_streak: u32,
    /// 凭据级刷新锁，避免不同账号刷新 token 时互相串行阻塞
    refresh_lock: Arc<TokioMutex<()>>,
}

/// 禁用原因
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DisabledReason {
    /// Admin API 手动禁用
    Manual,
    /// 连续失败达到阈值后自动禁用
    TooManyFailures,
    /// Token 刷新连续失败达到阈值后自动禁用
    TooManyRefreshFailures,
    /// 额度已用尽（如 MONTHLY_REQUEST_COUNT）
    QuotaExceeded,
    /// Refresh Token 永久失效（服务端返回 invalid_grant）
    InvalidRefreshToken,
}

/// 统计数据持久化条目
#[derive(Debug, Clone, Copy)]
struct TokenBucketPolicy {
    capacity: f64,
    refill_per_second: f64,
    min_refill_per_second: f64,
    recovery_step_per_success: f64,
    backoff_factor: f64,
}

#[derive(Debug, Clone)]
struct AdaptiveTokenBucket {
    policy: TokenBucketPolicy,
    tokens: f64,
    current_refill_per_second: f64,
    last_refill_at: Instant,
}

impl AdaptiveTokenBucket {
    fn new(policy: TokenBucketPolicy, now: Instant) -> Self {
        Self {
            tokens: policy.capacity,
            current_refill_per_second: policy.refill_per_second,
            policy,
            last_refill_at: now,
        }
    }

    fn refill(&mut self, now: Instant) {
        let elapsed = now
            .checked_duration_since(self.last_refill_at)
            .unwrap_or_default()
            .as_secs_f64();
        if elapsed > 0.0 {
            self.tokens =
                (self.tokens + elapsed * self.current_refill_per_second).min(self.policy.capacity);
            self.last_refill_at = now;
        }
    }

    fn requested_tokens(&self, requested_tokens: f64) -> f64 {
        let requested_tokens = if requested_tokens.is_finite() && requested_tokens > 0.0 {
            requested_tokens
        } else {
            DEFAULT_REQUEST_WEIGHT
        };

        requested_tokens.min(self.policy.capacity.max(0.0))
    }

    fn has_available_token(&mut self, now: Instant, requested_tokens: f64) -> bool {
        self.refill(now);
        self.tokens >= self.requested_tokens(requested_tokens)
    }

    fn consume(&mut self, now: Instant, requested_tokens: f64) -> bool {
        let requested_tokens = self.requested_tokens(requested_tokens);
        if !self.has_available_token(now, requested_tokens) {
            return false;
        }
        self.tokens = (self.tokens - requested_tokens).max(0.0);
        true
    }

    fn ready_at(&mut self, now: Instant, requested_tokens: f64) -> Option<Instant> {
        let requested_tokens = self.requested_tokens(requested_tokens);
        self.refill(now);
        if self.tokens >= requested_tokens {
            return None;
        }

        let missing_tokens = requested_tokens - self.tokens;
        let wait_seconds = missing_tokens / self.current_refill_per_second;
        Some(now + StdDuration::from_secs_f64(wait_seconds.max(0.0)))
    }

    fn on_rate_limited(&mut self, now: Instant) {
        self.refill(now);
        self.tokens = 0.0;
        self.current_refill_per_second =
            (self.current_refill_per_second * self.policy.backoff_factor).clamp(
                self.policy.min_refill_per_second,
                self.policy.refill_per_second,
            );
        self.last_refill_at = now;
    }

    fn on_success(&mut self, now: Instant) {
        self.refill(now);
        self.current_refill_per_second = (self.current_refill_per_second
            + self.policy.recovery_step_per_success)
            .min(self.policy.refill_per_second);
    }

    fn reconfigure(&mut self, policy: TokenBucketPolicy, now: Instant) {
        self.refill(now);

        let token_ratio = if self.policy.capacity > 0.0 {
            (self.tokens / self.policy.capacity).clamp(0.0, 1.0)
        } else {
            1.0
        };
        let refill_ratio = if self.policy.refill_per_second > 0.0 {
            (self.current_refill_per_second / self.policy.refill_per_second).clamp(0.0, 1.0)
        } else {
            1.0
        };

        self.policy = policy;
        self.tokens = (token_ratio * policy.capacity).clamp(0.0, policy.capacity);
        self.current_refill_per_second = (policy.refill_per_second * refill_ratio)
            .clamp(policy.min_refill_per_second, policy.refill_per_second);
        self.last_refill_at = now;
    }
}

// ============================================================================
// Admin API 公开结构
// ============================================================================

/// 凭据条目快照（用于 Admin API 读取）
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CredentialEntrySnapshot {
    /// 凭据唯一 ID
    pub id: u64,
    /// 优先级
    pub priority: u32,
    /// 是否被禁用
    pub disabled: bool,
    /// 连续失败次数
    pub failure_count: u32,
    /// 认证方式
    pub auth_method: Option<String>,
    /// 是否有 Profile ARN
    pub has_profile_arn: bool,
    /// Token 过期时间
    pub expires_at: Option<String>,
    /// refreshToken 的 SHA-256 哈希（用于前端重复检测）
    pub refresh_token_hash: Option<String>,
    /// 用户邮箱（用于前端显示）
    pub email: Option<String>,
    /// 订阅等级（KIRO PRO+ / KIRO FREE 等）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subscription_title: Option<String>,
    /// 账号类型
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account_type: Option<String>,
    /// 账号级额外允许模型
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub allowed_models: Vec<String>,
    /// 账号级额外禁用模型
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub blocked_models: Vec<String>,
    /// 运行时探测到的临时模型限制
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub runtime_model_restrictions: Vec<crate::model::model_policy::RuntimeModelRestriction>,
    /// 导入时间（RFC3339 格式）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub imported_at: Option<String>,
    /// API 调用成功次数
    pub success_count: u64,
    /// 最后一次 API 调用时间（RFC3339 格式）
    pub last_used_at: Option<String>,
    /// 当前运行中的请求数
    pub active_requests: usize,
    /// 单账号并发上限（空表示不限制）
    pub max_concurrency: Option<u32>,
    /// 是否配置了凭据级代理
    pub has_proxy: bool,
    /// 代理 URL（用于前端展示）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proxy_url: Option<String>,
    /// Token 刷新连续失败次数
    pub refresh_failure_count: u32,
    /// 禁用原因
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disabled_reason: Option<String>,
    /// 429 限流冷却剩余时间（毫秒）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cooldown_remaining_ms: Option<u64>,
    /// 当前 bucket 可用 token 数
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limit_bucket_tokens: Option<f64>,
    /// 当前 bucket 容量
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limit_bucket_capacity: Option<f64>,
    /// 凭据级 bucket 容量覆盖
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limit_bucket_capacity_override: Option<f64>,
    /// 当前生效回填速率（token/s）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limit_refill_per_second: Option<f64>,
    /// 凭据级回填速率覆盖
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limit_refill_per_second_override: Option<f64>,
    /// 配置的基础回填速率（token/s）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limit_refill_base_per_second: Option<f64>,
    /// 连续 429 次数
    pub rate_limit_hit_streak: u32,
    /// 当前账号再次可被调度的剩余时间（毫秒）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_ready_in_ms: Option<u64>,
}

/// 凭据管理器状态快照
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ManagerSnapshot {
    /// 凭据条目列表
    pub entries: Vec<CredentialEntrySnapshot>,
    /// 当前活跃凭据 ID
    pub current_id: u64,
    /// 总凭据数量
    pub total: usize,
    /// 可用凭据数量
    pub available: usize,
    /// 当前可立即被调度的凭据数量
    pub dispatchable: usize,
}

#[derive(Debug, Clone)]
pub struct LoadBalancingConfigSnapshot {
    pub mode: String,
    pub queue_max_size: usize,
    pub queue_max_wait_ms: u64,
    pub rate_limit_cooldown_ms: u64,
    pub default_max_concurrency: Option<u32>,
    pub rate_limit_bucket_capacity: f64,
    pub rate_limit_refill_per_second: f64,
    pub rate_limit_refill_min_per_second: f64,
    pub rate_limit_refill_recovery_step_per_success: f64,
    pub rate_limit_refill_backoff_factor: f64,
    pub request_weighting: RequestWeightingConfig,
    pub waiting_requests: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ExternalStateSyncReport {
    pub credentials_reloaded: bool,
    pub dispatch_config_reloaded: bool,
    pub stats_reloaded: bool,
}

#[derive(Debug, Clone, PartialEq)]
struct DispatchConfig {
    mode: String,
    queue_max_size: usize,
    queue_max_wait_ms: u64,
    rate_limit_cooldown_ms: u64,
    default_max_concurrency: Option<u32>,
    rate_limit_bucket_capacity: f64,
    rate_limit_refill_per_second: f64,
    rate_limit_refill_min_per_second: f64,
    rate_limit_refill_recovery_step_per_success: f64,
    rate_limit_refill_backoff_factor: f64,
    request_weighting: RequestWeightingConfig,
    account_type_policies: BTreeMap<String, ModelSupportPolicy>,
}

impl DispatchConfig {
    fn from_config(config: &Config) -> Self {
        let normalize_non_negative = |value: f64, fallback: f64| {
            if value.is_finite() && value >= 0.0 {
                value
            } else {
                fallback
            }
        };
        let normalize_backoff = |value: f64| {
            if value.is_finite() && value > 0.0 {
                value.clamp(0.05, 1.0)
            } else {
                0.5
            }
        };
        let mut account_type_policies = config.account_type_policies.clone();
        normalize_account_type_policies(&mut account_type_policies);

        Self {
            mode: config.load_balancing_mode.clone(),
            queue_max_size: config.queue_max_size,
            queue_max_wait_ms: config.queue_max_wait_ms,
            rate_limit_cooldown_ms: config.rate_limit_cooldown_ms,
            default_max_concurrency: config.default_max_concurrency.filter(|limit| *limit > 0),
            rate_limit_bucket_capacity: normalize_non_negative(
                config.rate_limit_bucket_capacity,
                3.0,
            ),
            rate_limit_refill_per_second: normalize_non_negative(
                config.rate_limit_refill_per_second,
                1.0,
            ),
            rate_limit_refill_min_per_second: normalize_non_negative(
                config.rate_limit_refill_min_per_second,
                0.2,
            ),
            rate_limit_refill_recovery_step_per_success: normalize_non_negative(
                config.rate_limit_refill_recovery_step_per_success,
                0.1,
            ),
            rate_limit_refill_backoff_factor: normalize_backoff(
                config.rate_limit_refill_backoff_factor,
            ),
            request_weighting: config.request_weighting.clone(),
            account_type_policies,
        }
    }

    fn queue_enabled(&self) -> bool {
        self.queue_max_size > 0 && self.queue_max_wait_ms > 0
    }

    fn queue_wait_duration(&self) -> StdDuration {
        StdDuration::from_millis(self.queue_max_wait_ms)
    }

    fn account_type_policy_for<'a>(
        &'a self,
        credentials: &'a KiroCredentials,
    ) -> Option<&'a ModelSupportPolicy> {
        credentials.account_type_policy(&self.account_type_policies)
    }

    fn bucket_policy_for(&self, credentials: &KiroCredentials) -> Option<TokenBucketPolicy> {
        let capacity = credentials
            .rate_limit_bucket_capacity_override()
            .unwrap_or(self.rate_limit_bucket_capacity);
        let refill_per_second = credentials
            .rate_limit_refill_per_second_override()
            .unwrap_or(self.rate_limit_refill_per_second);

        if !capacity.is_finite()
            || !refill_per_second.is_finite()
            || capacity <= 0.0
            || refill_per_second <= 0.0
        {
            return None;
        }

        let min_refill_per_second = if self.rate_limit_refill_min_per_second.is_finite() {
            self.rate_limit_refill_min_per_second
                .clamp(0.0, refill_per_second)
        } else {
            0.0
        };
        let recovery_step_per_success =
            if self.rate_limit_refill_recovery_step_per_success.is_finite() {
                self.rate_limit_refill_recovery_step_per_success.max(0.0)
            } else {
                0.0
            };
        let backoff_factor = if self.rate_limit_refill_backoff_factor.is_finite() {
            self.rate_limit_refill_backoff_factor.clamp(0.05, 1.0)
        } else {
            0.5
        };

        Some(TokenBucketPolicy {
            capacity,
            refill_per_second,
            min_refill_per_second,
            recovery_step_per_success,
            backoff_factor,
        })
    }

    fn shared_bucket_policy_for(
        &self,
        credentials: &KiroCredentials,
    ) -> Option<DispatchRuntimeBucketPolicy> {
        self.bucket_policy_for(credentials)
            .map(|policy| DispatchRuntimeBucketPolicy {
                capacity: policy.capacity,
                refill_per_second: policy.refill_per_second,
                min_refill_per_second: policy.min_refill_per_second,
                recovery_step_per_success: policy.recovery_step_per_success,
                backoff_factor: policy.backoff_factor,
            })
    }
}

/// 多凭据 Token 管理器
///
/// 支持多个凭据的管理，实现固定优先级 + 故障转移策略
/// 故障统计基于 API 调用结果，而非 Token 刷新结果
pub struct MultiTokenManager {
    config: Config,
    proxy: Option<ProxyConfig>,
    state_store: StateStore,
    /// 凭据条目列表
    entries: Arc<Mutex<Vec<CredentialEntry>>>,
    /// 当前活动凭据 ID
    current_id: Mutex<u64>,
    /// 是否为多凭据格式（数组格式才回写）
    is_multiple_format: bool,
    /// 调度配置（负载均衡模式、排队参数）
    dispatch_config: Mutex<DispatchConfig>,
    /// 可用性变更通知（并发释放、凭据启用、配置变更等）
    availability_notify: Arc<Notify>,
    /// 当前正在等待可用槽位的请求数
    waiting_requests: Arc<std::sync::atomic::AtomicUsize>,
    /// 串行化本实例内所有会写入共享凭据/调度状态的操作，避免快照覆盖。
    state_write_lock: Mutex<()>,
    /// 最近一次已观察到的共享状态修订号。
    last_state_change_revisions: Mutex<StateChangeRevisions>,
    /// 热路径共享状态检查的单调时钟原点。
    hot_path_state_sync_origin: Instant,
    /// 最近一次热路径 revision 检查时间戳（相对 origin 的毫秒数）。
    last_hot_path_state_sync_check_ms: AtomicU64,
    /// 最近一次统计持久化时间（用于 debounce）
    last_stats_save_at: Mutex<Option<Instant>>,
    /// 统计数据是否有未落盘更新
    stats_dirty: AtomicBool,
}

/// 每个凭据最大 API 调用失败次数
const MAX_FAILURES_PER_CREDENTIAL: u32 = 3;
/// follower 观察到共享凭据需要由 leader 刷新时，临时避让该凭据的冷却时间
const SHARED_REFRESH_DEFER_COOLDOWN: StdDuration = StdDuration::from_secs(2);
/// 共享调度占位 TTL。请求异常退出时依赖该 TTL 自动回收全局并发占位。
const SHARED_DISPATCH_LEASE_TTL_MS: u64 = 120_000;
/// 共享调度占位续租心跳间隔。
const SHARED_DISPATCH_LEASE_HEARTBEAT_INTERVAL_MS: u64 = 30_000;
/// 共享调度运行态下，等待队列需要短周期轮询 Redis 以观察跨副本释放。
const SHARED_DISPATCH_WAIT_POLL_INTERVAL_MS: u64 = 200;
/// 统计数据持久化防抖间隔
const STATS_SAVE_DEBOUNCE: StdDuration = StdDuration::from_secs(30);

/// API 调用上下文
///
/// 绑定特定凭据的调用上下文，确保 token、credentials 和 id 的一致性
/// 用于解决并发调用时 current_id 竞态问题
pub struct CallContext {
    /// 凭据 ID（用于 report_success/report_failure）
    pub id: u64,
    /// 凭据信息（用于构建请求头）
    pub credentials: KiroCredentials,
    /// 访问 Token
    pub token: String,
    /// 请求生命周期租约，用于自动释放并发占位
    lease: CallLease,
}

impl CallContext {
    pub(crate) fn into_parts(self) -> (u64, KiroCredentials, String, CallLease) {
        (self.id, self.credentials, self.token, self.lease)
    }
}

#[derive(Clone)]
pub(crate) struct CallLease {
    // Keep the shared state alive for the full request/response lifecycle.
    _state: Arc<CallLeaseState>,
}

struct CallLeaseState {
    entries: Arc<Mutex<Vec<CredentialEntry>>>,
    availability_notify: Arc<Notify>,
    id: u64,
    shared_dispatch: Option<SharedDispatchLease>,
}

struct SharedDispatchLease {
    state_store: StateStore,
    lease_id: String,
    renew_shutdown_tx: Mutex<Option<oneshot::Sender<()>>>,
}

impl Drop for CallLeaseState {
    fn drop(&mut self) {
        if let Some(shared_dispatch) = &self.shared_dispatch {
            if let Some(shutdown_tx) = shared_dispatch.renew_shutdown_tx.lock().take() {
                let _ = shutdown_tx.send(());
            }
            if let Err(err) = shared_dispatch.state_store.release_dispatch_lease(
                self.id,
                &shared_dispatch.lease_id,
                current_epoch_ms(),
            ) {
                tracing::warn!(
                    "释放共享调度占位失败（credentialId={}, leaseId={}）: {}",
                    self.id,
                    shared_dispatch.lease_id,
                    err
                );
            }
        }

        let mut entries = self.entries.lock();
        if let Some(entry) = entries.iter_mut().find(|e| e.id == self.id) {
            entry.active_requests = entry.active_requests.saturating_sub(1);
            tracing::debug!(
                "释放凭据 #{} 并发占位，当前运行中请求数: {}",
                self.id,
                entry.active_requests
            );
        }
        drop(entries);
        self.availability_notify.notify_one();
    }
}

enum ReservationFailure {
    NoCredentials,
    AllDisabled,
    NoModelSupport,
    AllTemporarilyUnavailable { next_ready_at: Option<Instant> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ModelRequirement {
    Any,
    PaidOpus,
    RealOpus47,
}

struct WaitQueueGuard {
    waiting_requests: Arc<std::sync::atomic::AtomicUsize>,
}

impl Drop for WaitQueueGuard {
    fn drop(&mut self) {
        self.waiting_requests.fetch_sub(1, Ordering::SeqCst);
    }
}

impl MultiTokenManager {
    /// 创建多凭据 Token 管理器
    ///
    /// # Arguments
    /// * `config` - 应用配置
    /// * `credentials` - 凭据列表
    /// * `proxy` - 可选的代理配置
    /// * `credentials_path` - 凭据文件路径（用于回写）
    /// * `is_multiple_format` - 是否为多凭据格式（数组格式才回写）
    pub fn new(
        config: Config,
        credentials: Vec<KiroCredentials>,
        proxy: Option<ProxyConfig>,
        credentials_path: Option<PathBuf>,
        is_multiple_format: bool,
    ) -> anyhow::Result<Self> {
        let state_store = StateStore::from_config(&config, credentials_path.clone())?;
        let initial_state_change_revisions = state_store.state_change_revisions()?;
        let dispatch_config = DispatchConfig::from_config(&config);
        let now = Instant::now();

        // 计算当前最大 ID，为没有 ID 的凭据分配新 ID
        let max_existing_id = credentials.iter().filter_map(|c| c.id).max().unwrap_or(0);
        let mut next_id = max_existing_id + 1;
        let mut has_new_ids = false;
        let mut has_new_machine_ids = false;
        let config_ref = &config;

        let entries: Vec<CredentialEntry> = credentials
            .into_iter()
            .map(|mut cred| {
                cred.canonicalize_auth_method();
                cred.normalize_model_capabilities();
                let id = cred.id.unwrap_or_else(|| {
                    let id = next_id;
                    next_id += 1;
                    cred.id = Some(id);
                    has_new_ids = true;
                    id
                });
                if cred.machine_id.is_none() {
                    if let Some(machine_id) =
                        machine_id::generate_from_credentials(&cred, config_ref)
                    {
                        cred.machine_id = Some(machine_id);
                        has_new_machine_ids = true;
                    }
                }
                CredentialEntry {
                    id,
                    credentials: cred.clone(),
                    failure_count: 0,
                    refresh_failure_count: 0,
                    disabled: cred.disabled, // 从配置文件读取 disabled 状态
                    disabled_reason: if cred.disabled {
                        Some(DisabledReason::Manual)
                    } else {
                        None
                    },
                    success_count: 0,
                    pending_success_count_delta: 0,
                    last_used_at: None,
                    active_requests: 0,
                    rate_limit_cooldown_until: None,
                    rate_limit_bucket: dispatch_config
                        .bucket_policy_for(&cred)
                        .map(|policy| AdaptiveTokenBucket::new(policy, now)),
                    rate_limit_hit_streak: 0,
                    refresh_lock: Arc::new(TokioMutex::new(())),
                }
            })
            .collect();

        // 检测重复 ID
        let mut seen_ids = std::collections::HashSet::new();
        let mut duplicate_ids = Vec::new();
        for entry in &entries {
            if !seen_ids.insert(entry.id) {
                duplicate_ids.push(entry.id);
            }
        }
        if !duplicate_ids.is_empty() {
            anyhow::bail!("检测到重复的凭据 ID: {:?}", duplicate_ids);
        }

        // 选择初始凭据：优先级最高（priority 最小）的凭据，无凭据时为 0
        let initial_id = entries
            .iter()
            .min_by_key(|e| e.credentials.priority)
            .map(|e| e.id)
            .unwrap_or(0);

        let manager = Self {
            config,
            proxy,
            state_store,
            entries: Arc::new(Mutex::new(entries)),
            current_id: Mutex::new(initial_id),
            is_multiple_format,
            dispatch_config: Mutex::new(dispatch_config),
            availability_notify: Arc::new(Notify::new()),
            waiting_requests: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            state_write_lock: Mutex::new(()),
            last_state_change_revisions: Mutex::new(initial_state_change_revisions),
            hot_path_state_sync_origin: Instant::now(),
            last_hot_path_state_sync_check_ms: AtomicU64::new(u64::MAX),
            last_stats_save_at: Mutex::new(None),
            stats_dirty: AtomicBool::new(false),
        };

        // 如果有新分配的 ID 或新生成的 machineId，立即持久化到配置文件
        if has_new_ids || has_new_machine_ids {
            if let Err(e) = manager.persist_credentials() {
                tracing::warn!("补全凭据 ID/machineId 后持久化失败: {}", e);
            } else {
                tracing::info!("已补全凭据 ID/machineId 并写回配置文件");
            }
        }

        // 加载持久化的统计数据（success_count, last_used_at）
        manager.load_stats();

        Ok(manager)
    }

    /// 获取配置的引用
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// 获取凭据总数
    pub fn total_count(&self) -> usize {
        self.entries.lock().len()
    }

    /// 获取可用凭据数量
    pub fn available_count(&self) -> usize {
        self.entries.lock().iter().filter(|e| !e.disabled).count()
    }

    pub fn should_fast_fail_runtime_leader_refresh(&self) -> bool {
        self.shared_dispatch_runtime_enabled()
            && self.state_store.runtime_coordination_enabled()
            && self.available_count() <= 1
    }

    pub fn leader_http_base_url_for_single_shared_credential_mode(
        &self,
    ) -> anyhow::Result<Option<String>> {
        if !self.should_fast_fail_runtime_leader_refresh() {
            return Ok(None);
        }

        let Some(mut status) = self.runtime_refresh_coordination_status()? else {
            return Ok(None);
        };

        if !status.is_leader && status.leader_http_base_url.is_none() {
            if let Some(updated_status) = self.state_store.runtime_coordination_tick()? {
                status = updated_status;
            }
        }

        if status.is_leader {
            return Ok(None);
        }

        Ok(status.leader_http_base_url)
    }

    fn dispatch_config(&self) -> DispatchConfig {
        self.dispatch_config.lock().clone()
    }

    fn queue_depth(&self) -> usize {
        self.waiting_requests.load(Ordering::SeqCst)
    }

    fn model_requirement(model: Option<&str>) -> ModelRequirement {
        let Some(model) = model.map(|name| name.to_ascii_lowercase()) else {
            return ModelRequirement::Any;
        };

        if model.contains("claude-opus-4.7") || model.contains("claude-opus-4-7") {
            ModelRequirement::RealOpus47
        } else if model.contains("opus") {
            ModelRequirement::PaidOpus
        } else {
            ModelRequirement::Any
        }
    }

    fn policy_allows_model(
        dispatch: &DispatchConfig,
        credentials: &KiroCredentials,
        model: Option<&str>,
    ) -> bool {
        let Some(model) = model else {
            return true;
        };
        credentials.policy_allows_model(dispatch.account_type_policy_for(credentials), model)
    }

    fn is_model_supported(
        dispatch: &DispatchConfig,
        credentials: &KiroCredentials,
        model: Option<&str>,
        requirement: ModelRequirement,
    ) -> bool {
        if !Self::policy_allows_model(dispatch, credentials, model) {
            return false;
        }
        match requirement {
            ModelRequirement::Any => true,
            ModelRequirement::PaidOpus => credentials.supports_opus(),
            ModelRequirement::RealOpus47 => credentials.supports_real_opus_4_7(),
        }
    }

    fn model_preference_rank(credentials: &KiroCredentials, requirement: ModelRequirement) -> u8 {
        match requirement {
            ModelRequirement::RealOpus47 => credentials.opus_4_7_preference_rank(),
            ModelRequirement::Any | ModelRequirement::PaidOpus => 0,
        }
    }

    fn has_capacity(
        credentials: &KiroCredentials,
        active_requests: usize,
        default_max_concurrency: Option<u32>,
    ) -> bool {
        match credentials.effective_max_concurrency_with_default(default_max_concurrency) {
            Some(limit) => active_requests < limit,
            None => true,
        }
    }

    fn refresh_runtime_state(entries: &mut [CredentialEntry], now: Instant) {
        for entry in entries {
            Self::refresh_entry_runtime(entry, now);
        }
    }

    fn refresh_entry_runtime(entry: &mut CredentialEntry, now: Instant) {
        if entry
            .rate_limit_cooldown_until
            .is_some_and(|until| until <= now)
        {
            entry.rate_limit_cooldown_until = None;
        }
        if let Some(bucket) = entry.rate_limit_bucket.as_mut() {
            bucket.refill(now);
        }
    }

    fn clear_all_rate_limit_cooldowns(&self) {
        if self.shared_dispatch_runtime_enabled() {
            let ids: Vec<u64> = {
                let entries = self.entries.lock();
                entries.iter().map(|entry| entry.id).collect()
            };
            if let Err(err) = self.state_store.clear_dispatch_cooldowns(&ids) {
                tracing::warn!("清理共享调度冷却失败: {}", err);
            }
        }

        let mut entries = self.entries.lock();
        for entry in entries.iter_mut() {
            entry.rate_limit_cooldown_until = None;
        }
    }

    fn is_rate_limited(entry: &CredentialEntry, now: Instant) -> bool {
        entry
            .rate_limit_cooldown_until
            .is_some_and(|until| until > now)
    }

    fn bucket_is_ready(entry: &CredentialEntry) -> bool {
        Self::bucket_is_ready_for(entry, DEFAULT_REQUEST_WEIGHT)
    }

    fn bucket_is_ready_for(entry: &CredentialEntry, request_weight: f64) -> bool {
        entry.rate_limit_bucket.as_ref().map_or(true, |bucket| {
            bucket.tokens >= bucket.requested_tokens(request_weight)
        })
    }

    fn combined_ready_at(entry: &mut CredentialEntry, now: Instant) -> Option<Instant> {
        Self::combined_ready_at_for(entry, now, DEFAULT_REQUEST_WEIGHT)
    }

    fn combined_ready_at_for(
        entry: &mut CredentialEntry,
        now: Instant,
        request_weight: f64,
    ) -> Option<Instant> {
        let cooldown_ready_at = entry.rate_limit_cooldown_until.filter(|until| *until > now);
        let bucket_ready_at = entry
            .rate_limit_bucket
            .as_mut()
            .and_then(|bucket| bucket.ready_at(now, request_weight));

        match (cooldown_ready_at, bucket_ready_at) {
            (Some(cooldown_ready_at), Some(bucket_ready_at)) => {
                Some(cooldown_ready_at.max(bucket_ready_at))
            }
            (Some(cooldown_ready_at), None) => Some(cooldown_ready_at),
            (None, Some(bucket_ready_at)) => Some(bucket_ready_at),
            (None, None) => None,
        }
    }

    fn update_min_ready_at_for(
        current: &mut Option<Instant>,
        entry: &mut CredentialEntry,
        now: Instant,
        request_weight: f64,
    ) {
        if let Some(ready_at) = Self::combined_ready_at_for(entry, now, request_weight) {
            *current = Some(match *current {
                Some(existing) => existing.min(ready_at),
                None => ready_at,
            });
        }
    }

    fn next_ready_at_for(
        dispatch: &DispatchConfig,
        entries: &mut [CredentialEntry],
        model: Option<&str>,
        model_requirement: ModelRequirement,
        now: Instant,
        request_weight: f64,
    ) -> Option<Instant> {
        entries
            .iter_mut()
            .filter(|entry| {
                !entry.disabled
                    && Self::is_model_supported(
                        dispatch,
                        &entry.credentials,
                        model,
                        model_requirement,
                    )
            })
            .filter_map(|entry| Self::combined_ready_at_for(entry, now, request_weight))
            .min()
    }

    fn refresh_lock_for(&self, id: u64) -> anyhow::Result<Arc<TokioMutex<()>>> {
        let entries = self.entries.lock();
        entries
            .iter()
            .find(|entry| entry.id == id)
            .map(|entry| Arc::clone(&entry.refresh_lock))
            .ok_or_else(|| anyhow::anyhow!("凭据 #{} 不存在", id))
    }

    fn sync_rate_limit_bucket_runtime(
        entry: &mut CredentialEntry,
        dispatch: &DispatchConfig,
        now: Instant,
    ) {
        match dispatch.bucket_policy_for(&entry.credentials) {
            Some(policy) => {
                if let Some(bucket) = entry.rate_limit_bucket.as_mut() {
                    bucket.reconfigure(policy, now);
                } else {
                    entry.rate_limit_bucket = Some(AdaptiveTokenBucket::new(policy, now));
                }
            }
            None => {
                entry.rate_limit_bucket = None;
            }
        }
    }

    fn reset_rate_limit_runtime(
        entry: &mut CredentialEntry,
        dispatch: &DispatchConfig,
        now: Instant,
    ) {
        entry.rate_limit_cooldown_until = None;
        entry.rate_limit_hit_streak = 0;
        entry.rate_limit_bucket = dispatch
            .bucket_policy_for(&entry.credentials)
            .map(|policy| AdaptiveTokenBucket::new(policy, now));
    }

    fn reconfigure_rate_limit_runtime(&self, dispatch: &DispatchConfig) {
        let now = Instant::now();
        let mut entries = self.entries.lock();
        for entry in entries.iter_mut() {
            Self::sync_rate_limit_bucket_runtime(entry, dispatch, now);
        }
    }

    fn validate_non_negative_finite(name: &str, value: f64) -> anyhow::Result<()> {
        if !value.is_finite() || value < 0.0 {
            anyhow::bail!("{} 必须是大于等于 0 的有限数字", name);
        }
        Ok(())
    }

    fn validate_dispatch_rate_limit_config(dispatch: &DispatchConfig) -> anyhow::Result<()> {
        for (name, value) in [
            (
                "rateLimitBucketCapacity",
                dispatch.rate_limit_bucket_capacity,
            ),
            (
                "rateLimitRefillPerSecond",
                dispatch.rate_limit_refill_per_second,
            ),
            (
                "rateLimitRefillMinPerSecond",
                dispatch.rate_limit_refill_min_per_second,
            ),
            (
                "rateLimitRefillRecoveryStepPerSuccess",
                dispatch.rate_limit_refill_recovery_step_per_success,
            ),
            (
                "rateLimitRefillBackoffFactor",
                dispatch.rate_limit_refill_backoff_factor,
            ),
        ] {
            Self::validate_non_negative_finite(name, value)?;
        }

        if dispatch.rate_limit_refill_backoff_factor < 0.05
            || dispatch.rate_limit_refill_backoff_factor > 1.0
        {
            anyhow::bail!("rateLimitRefillBackoffFactor 必须在 [0.05, 1] 范围内");
        }

        if dispatch.rate_limit_refill_per_second > 0.0
            && dispatch.rate_limit_refill_min_per_second > dispatch.rate_limit_refill_per_second
        {
            anyhow::bail!("rateLimitRefillMinPerSecond 不能大于 rateLimitRefillPerSecond");
        }

        dispatch.request_weighting.validate()?;

        Ok(())
    }

    fn shared_dispatch_runtime_enabled(&self) -> bool {
        self.state_store.dispatch_runtime_enabled()
    }

    fn load_shared_dispatch_runtime_snapshots(
        &self,
        dispatch: &DispatchConfig,
        now_epoch_ms: u64,
    ) -> anyhow::Result<HashMap<u64, DispatchRuntimeSnapshot>> {
        let credentials: Vec<DispatchRuntimeCredential> = {
            let entries = self.entries.lock();
            entries
                .iter()
                .map(|entry| DispatchRuntimeCredential {
                    id: entry.id,
                    bucket_policy: dispatch.shared_bucket_policy_for(&entry.credentials),
                })
                .collect()
        };

        self.state_store
            .load_dispatch_runtime_snapshots(&credentials, now_epoch_ms)
    }

    fn reserve_call_lease(&self, id: u64, shared_lease_id: Option<String>) -> CallLease {
        let shared_dispatch = shared_lease_id.map(|lease_id| {
            let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
            let state_store = self.state_store.clone();
            let task_state_store = state_store.clone();
            let task_lease_id = lease_id.clone();

            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(StdDuration::from_millis(
                    SHARED_DISPATCH_LEASE_HEARTBEAT_INTERVAL_MS,
                ));
                ticker.tick().await;

                loop {
                    tokio::select! {
                        _ = &mut shutdown_rx => break,
                        _ = ticker.tick() => {
                            match task_state_store.renew_dispatch_lease(
                                id,
                                &task_lease_id,
                                SHARED_DISPATCH_LEASE_TTL_MS,
                            ) {
                                Ok(true) => {}
                                Ok(false) => {
                                    tracing::warn!(
                                        "共享调度占位续租失败：占位已丢失（credentialId={}, leaseId={}）",
                                        id,
                                        task_lease_id
                                    );
                                    break;
                                }
                                Err(err) => {
                                    tracing::warn!(
                                        "共享调度占位续租失败（credentialId={}, leaseId={}）: {}",
                                        id,
                                        task_lease_id,
                                        err
                                    );
                                    break;
                                }
                            }
                        }
                    }
                }
            });

            SharedDispatchLease {
                state_store,
                lease_id,
                renew_shutdown_tx: Mutex::new(Some(shutdown_tx)),
            }
        });

        CallLease {
            _state: Arc::new(CallLeaseState {
                entries: Arc::clone(&self.entries),
                availability_notify: Arc::clone(&self.availability_notify),
                id,
                shared_dispatch,
            }),
        }
    }

    fn try_enter_wait_queue(&self, max_queue_size: usize) -> anyhow::Result<WaitQueueGuard> {
        loop {
            let current = self.waiting_requests.load(Ordering::SeqCst);
            if current >= max_queue_size {
                anyhow::bail!("等待队列已满（{}/{})", current, max_queue_size);
            }

            if self
                .waiting_requests
                .compare_exchange(current, current + 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                tracing::debug!("请求进入等待队列，当前排队数: {}", current + 1);
                return Ok(WaitQueueGuard {
                    waiting_requests: Arc::clone(&self.waiting_requests),
                });
            }
        }
    }

    async fn wait_for_availability(
        &self,
        deadline: Instant,
        next_ready_at: Option<Instant>,
    ) -> bool {
        let now = Instant::now();
        if now >= deadline {
            return false;
        }

        let wake_at = if self.shared_dispatch_runtime_enabled() {
            let poll_deadline =
                now + StdDuration::from_millis(SHARED_DISPATCH_WAIT_POLL_INTERVAL_MS);
            next_ready_at
                .map(|next| next.min(deadline).min(poll_deadline))
                .unwrap_or_else(|| deadline.min(poll_deadline))
        } else {
            next_ready_at
                .map(|next| next.min(deadline))
                .unwrap_or(deadline)
        };
        if wake_at <= now {
            return true;
        }

        tokio::select! {
            _ = self.availability_notify.notified() => true,
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(wake_at)) => Instant::now() < deadline,
        }
    }

    fn recover_auto_disabled_credentials(&self) -> bool {
        let dispatch = self.dispatch_config();
        let now = Instant::now();
        let mut recovered = false;
        let mut recovered_ids = Vec::new();

        {
            let mut entries = self.entries.lock();
            for entry in entries.iter_mut() {
                if entry.disabled_reason == Some(DisabledReason::TooManyFailures) {
                    entry.disabled = false;
                    entry.disabled_reason = None;
                    entry.failure_count = 0;
                    Self::reset_rate_limit_runtime(entry, &dispatch, now);
                    recovered = true;
                    recovered_ids.push(entry.id);
                }
            }
        }

        if recovered {
            if self.shared_dispatch_runtime_enabled() {
                for id in &recovered_ids {
                    let bucket_policy = {
                        let entries = self.entries.lock();
                        entries
                            .iter()
                            .find(|entry| entry.id == *id)
                            .and_then(|entry| dispatch.shared_bucket_policy_for(&entry.credentials))
                    };
                    if let Err(err) = self.state_store.reset_dispatch_runtime(
                        *id,
                        bucket_policy,
                        current_epoch_ms(),
                    ) {
                        tracing::warn!("重置共享调度运行态失败（credentialId={}）: {}", id, err);
                    }
                }
            }
            tracing::warn!(
                "所有凭据均已被自动禁用，执行自愈：重置失败计数并重新启用（等价于重启）"
            );
            self.availability_notify.notify_waiters();
        }

        recovered
    }

    /// 根据负载均衡模式选择并占用下一个凭据。
    ///
    /// 选择与并发占位在同一把锁内完成，避免并发请求同时命中同一账号。
    fn reserve_next_credential(
        &self,
        model: Option<&str>,
        request_weight: f64,
    ) -> Result<(u64, KiroCredentials, CallLease), ReservationFailure> {
        let dispatch = self.dispatch_config();
        let mode = dispatch.mode.clone();
        let mut entries = self.entries.lock();
        let now = Instant::now();
        let model_requirement = Self::model_requirement(model);

        if entries.is_empty() {
            return Err(ReservationFailure::NoCredentials);
        }

        let mut current_id = self.current_id.lock();
        let current_id_value = *current_id;
        let is_balanced = mode == "balanced";
        let mut has_enabled = false;
        let mut has_supported = false;
        let mut selected_index: Option<usize> = None;
        let mut priority_key: Option<(u8, u32, usize, u8, u64)> = None;
        let mut balanced_key: Option<(u8, usize, u64, u32, u64)> = None;
        let mut next_ready_at: Option<Instant> = None;

        for (index, entry) in entries.iter_mut().enumerate() {
            Self::refresh_entry_runtime(entry, now);

            if entry.disabled {
                continue;
            }
            has_enabled = true;

            if !Self::is_model_supported(&dispatch, &entry.credentials, model, model_requirement) {
                continue;
            }
            has_supported = true;

            let is_dispatchable = !Self::is_rate_limited(entry, now)
                && Self::bucket_is_ready_for(entry, request_weight)
                && Self::has_capacity(
                    &entry.credentials,
                    entry.active_requests,
                    dispatch.default_max_concurrency,
                );

            if !is_dispatchable {
                Self::update_min_ready_at_for(&mut next_ready_at, entry, now, request_weight);
                continue;
            }

            if is_balanced {
                let candidate_key = (
                    Self::model_preference_rank(&entry.credentials, model_requirement),
                    entry.active_requests,
                    entry.success_count,
                    entry.credentials.priority,
                    entry.id,
                );
                let should_select = balanced_key
                    .as_ref()
                    .map(|best_key| candidate_key < *best_key)
                    .unwrap_or(true);
                if should_select {
                    balanced_key = Some(candidate_key);
                    selected_index = Some(index);
                }
                continue;
            }

            let candidate_key = (
                Self::model_preference_rank(&entry.credentials, model_requirement),
                entry.credentials.priority,
                entry.active_requests,
                u8::from(entry.id != current_id_value),
                entry.id,
            );
            let should_select = priority_key
                .as_ref()
                .map(|best_key| candidate_key < *best_key)
                .unwrap_or(true);
            if should_select {
                priority_key = Some(candidate_key);
                selected_index = Some(index);
            }
        }

        if !has_enabled {
            return Err(ReservationFailure::AllDisabled);
        }
        if !has_supported {
            return Err(ReservationFailure::NoModelSupport);
        }

        let entry_index = match selected_index {
            Some(index) => index,
            None => {
                return Err(ReservationFailure::AllTemporarilyUnavailable { next_ready_at });
            }
        };

        let selected_id = entries[entry_index].id;
        let token_consumed = {
            let entry = &mut entries[entry_index];
            entry
                .rate_limit_bucket
                .as_mut()
                .map_or(true, |bucket| bucket.consume(now, request_weight))
        };
        if !token_consumed {
            let next_ready_at = Self::next_ready_at_for(
                &dispatch,
                &mut entries,
                model,
                model_requirement,
                now,
                request_weight,
            );
            return Err(ReservationFailure::AllTemporarilyUnavailable { next_ready_at });
        }
        let entry = &mut entries[entry_index];
        entry.active_requests += 1;
        *current_id = selected_id;

        tracing::debug!(
            "分配凭据 #{} 处理请求，当前运行中请求数: {}{}",
            selected_id,
            entry.active_requests,
            entry
                .credentials
                .effective_max_concurrency_with_default(dispatch.default_max_concurrency)
                .map(|limit| format!("/{}", limit))
                .unwrap_or_default()
        );

        Ok((
            selected_id,
            entry.credentials.clone(),
            self.reserve_call_lease(selected_id, None),
        ))
    }

    fn shared_dispatch_snapshot_for_entry(
        entry: &CredentialEntry,
        dispatch: &DispatchConfig,
        snapshots: &HashMap<u64, DispatchRuntimeSnapshot>,
    ) -> DispatchRuntimeSnapshot {
        snapshots.get(&entry.id).cloned().unwrap_or_else(|| {
            let bucket_policy = dispatch.shared_bucket_policy_for(&entry.credentials);
            DispatchRuntimeSnapshot {
                active_requests: entry.active_requests,
                cooldown_until_epoch_ms: None,
                rate_limit_hit_streak: entry.rate_limit_hit_streak,
                bucket_tokens: bucket_policy.map(|policy| policy.capacity),
                bucket_capacity: bucket_policy.map(|policy| policy.capacity),
                bucket_current_refill_per_second: bucket_policy
                    .map(|policy| policy.refill_per_second),
                bucket_base_refill_per_second: bucket_policy.map(|policy| policy.refill_per_second),
                next_ready_at_epoch_ms: None,
            }
        })
    }

    fn shared_bucket_is_ready(snapshot: &DispatchRuntimeSnapshot) -> bool {
        Self::shared_bucket_is_ready_for(snapshot, DEFAULT_REQUEST_WEIGHT)
    }

    fn shared_bucket_is_ready_for(snapshot: &DispatchRuntimeSnapshot, request_weight: f64) -> bool {
        match snapshot.requested_bucket_tokens(request_weight) {
            Some(requested_tokens) => snapshot
                .bucket_tokens
                .is_some_and(|tokens| tokens >= requested_tokens),
            None => true,
        }
    }

    fn shared_snapshot_ready_at_for(
        snapshot: &DispatchRuntimeSnapshot,
        now: Instant,
        now_epoch_ms: u64,
        request_weight: f64,
    ) -> Option<Instant> {
        snapshot
            .next_ready_at_epoch_ms_for(request_weight, now_epoch_ms)
            .map(|ready_at_epoch_ms| {
                now + StdDuration::from_millis(ready_at_epoch_ms.saturating_sub(now_epoch_ms))
            })
    }

    fn reserve_next_credential_shared(
        &self,
        model: Option<&str>,
        request_weight: f64,
    ) -> Result<(u64, KiroCredentials, CallLease), ReservationFailure> {
        let dispatch = self.dispatch_config();
        let model_requirement = Self::model_requirement(model);
        let mode = dispatch.mode.clone();
        let is_balanced = mode == "balanced";
        let retry_budget = self.total_count().max(1).saturating_mul(2);
        let mut fallback_next_ready_at: Option<Instant> = None;

        for _ in 0..retry_budget {
            let now = Instant::now();
            let now_epoch_ms = current_epoch_ms();
            let snapshots = self
                .load_shared_dispatch_runtime_snapshots(&dispatch, now_epoch_ms)
                .map_err(|err| {
                    tracing::warn!("读取共享调度热态失败: {}", err);
                    ReservationFailure::AllTemporarilyUnavailable {
                        next_ready_at: None,
                    }
                })?;

            let selection = {
                let entries = self.entries.lock();
                if entries.is_empty() {
                    return Err(ReservationFailure::NoCredentials);
                }

                let current_id_value = *self.current_id.lock();
                let mut has_enabled = false;
                let mut has_supported = false;
                let mut selected_id: Option<u64> = None;
                let mut selected_credentials: Option<KiroCredentials> = None;
                let mut selected_max_concurrency: Option<usize> = None;
                let mut selected_bucket_policy: Option<DispatchRuntimeBucketPolicy> = None;
                let mut priority_key: Option<(u8, u32, usize, u8, u64)> = None;
                let mut balanced_key: Option<(u8, usize, u64, u32, u64)> = None;
                let mut next_ready_at: Option<Instant> = fallback_next_ready_at;

                for entry in entries.iter() {
                    if entry.disabled {
                        continue;
                    }
                    has_enabled = true;

                    if !Self::is_model_supported(
                        &dispatch,
                        &entry.credentials,
                        model,
                        model_requirement,
                    ) {
                        continue;
                    }
                    has_supported = true;

                    let runtime =
                        Self::shared_dispatch_snapshot_for_entry(entry, &dispatch, &snapshots);
                    let is_dispatchable = runtime
                        .cooldown_until_epoch_ms
                        .map_or(true, |until| until <= now_epoch_ms)
                        && Self::shared_bucket_is_ready_for(&runtime, request_weight)
                        && Self::has_capacity(
                            &entry.credentials,
                            runtime.active_requests,
                            dispatch.default_max_concurrency,
                        );

                    if !is_dispatchable {
                        if let Some(ready_at) = Self::shared_snapshot_ready_at_for(
                            &runtime,
                            now,
                            now_epoch_ms,
                            request_weight,
                        ) {
                            next_ready_at = Some(match next_ready_at {
                                Some(existing) => existing.min(ready_at),
                                None => ready_at,
                            });
                        }
                        continue;
                    }

                    if is_balanced {
                        let candidate_key = (
                            Self::model_preference_rank(&entry.credentials, model_requirement),
                            runtime.active_requests,
                            entry.success_count,
                            entry.credentials.priority,
                            entry.id,
                        );
                        let should_select = balanced_key
                            .as_ref()
                            .map(|best_key| candidate_key < *best_key)
                            .unwrap_or(true);
                        if should_select {
                            balanced_key = Some(candidate_key);
                            selected_id = Some(entry.id);
                            selected_credentials = Some(entry.credentials.clone());
                            selected_max_concurrency =
                                entry.credentials.effective_max_concurrency_with_default(
                                    dispatch.default_max_concurrency,
                                );
                            selected_bucket_policy =
                                dispatch.shared_bucket_policy_for(&entry.credentials);
                        }
                        continue;
                    }

                    let candidate_key = (
                        Self::model_preference_rank(&entry.credentials, model_requirement),
                        entry.credentials.priority,
                        runtime.active_requests,
                        u8::from(entry.id != current_id_value),
                        entry.id,
                    );
                    let should_select = priority_key
                        .as_ref()
                        .map(|best_key| candidate_key < *best_key)
                        .unwrap_or(true);
                    if should_select {
                        priority_key = Some(candidate_key);
                        selected_id = Some(entry.id);
                        selected_credentials = Some(entry.credentials.clone());
                        selected_max_concurrency =
                            entry.credentials.effective_max_concurrency_with_default(
                                dispatch.default_max_concurrency,
                            );
                        selected_bucket_policy =
                            dispatch.shared_bucket_policy_for(&entry.credentials);
                    }
                }

                if !has_enabled {
                    return Err(ReservationFailure::AllDisabled);
                }
                if !has_supported {
                    return Err(ReservationFailure::NoModelSupport);
                }

                match (selected_id, selected_credentials) {
                    (Some(id), Some(credentials)) => Ok((
                        id,
                        credentials,
                        selected_max_concurrency,
                        selected_bucket_policy,
                    )),
                    _ => Err(ReservationFailure::AllTemporarilyUnavailable { next_ready_at }),
                }
            };

            let (
                selected_id,
                selected_credentials,
                selected_max_concurrency,
                selected_bucket_policy,
            ) = match selection {
                Ok(selection) => selection,
                Err(err) => return Err(err),
            };

            let shared_lease_id = uuid::Uuid::new_v4().to_string();
            let reservation = self
                .state_store
                .try_reserve_dispatch_lease(
                    selected_id,
                    &shared_lease_id,
                    selected_max_concurrency,
                    selected_bucket_policy,
                    request_weight,
                    current_epoch_ms(),
                    SHARED_DISPATCH_LEASE_TTL_MS,
                )
                .map_err(|err| {
                    tracing::warn!("申请共享调度占位失败: {}", err);
                    ReservationFailure::AllTemporarilyUnavailable {
                        next_ready_at: None,
                    }
                })?;

            let Some(reservation) = reservation else {
                return Err(ReservationFailure::AllTemporarilyUnavailable {
                    next_ready_at: None,
                });
            };

            if reservation.status == DispatchLeaseReservationStatus::Reserved {
                {
                    let mut entries = self.entries.lock();
                    if let Some(entry) = entries.iter_mut().find(|entry| entry.id == selected_id) {
                        entry.active_requests += 1;
                    }
                }
                *self.current_id.lock() = selected_id;

                tracing::debug!(
                    "通过共享调度热态分配凭据 #{}，全局运行中请求数: {}{}",
                    selected_id,
                    reservation.snapshot.active_requests,
                    selected_credentials
                        .effective_max_concurrency_with_default(dispatch.default_max_concurrency)
                        .map(|limit| format!("/{}", limit))
                        .unwrap_or_default()
                );

                return Ok((
                    selected_id,
                    selected_credentials,
                    self.reserve_call_lease(selected_id, Some(shared_lease_id)),
                ));
            }

            if let Some(ready_at) = Self::shared_snapshot_ready_at_for(
                &reservation.snapshot,
                now,
                now_epoch_ms,
                request_weight,
            ) {
                fallback_next_ready_at = Some(match fallback_next_ready_at {
                    Some(existing) => existing.min(ready_at),
                    None => ready_at,
                });
            }
        }

        Err(ReservationFailure::AllTemporarilyUnavailable {
            next_ready_at: fallback_next_ready_at,
        })
    }

    /// 获取 API 调用上下文
    ///
    /// 返回绑定了 id、credentials 和 token 的调用上下文
    /// 确保整个 API 调用过程中使用一致的凭据信息
    ///
    /// 如果 Token 过期或即将过期，会自动刷新
    /// Token 刷新失败会累计到当前凭据，达到阈值后禁用并切换
    ///
    /// # 参数
    /// - `model`: 可选的模型名称，用于过滤支持该模型的凭据（如 opus 模型需要付费订阅）
    pub async fn acquire_context(&self, model: Option<&str>) -> anyhow::Result<CallContext> {
        self.acquire_context_with_weight(model, DEFAULT_REQUEST_WEIGHT)
            .await
    }

    pub async fn acquire_context_with_weight(
        &self,
        model: Option<&str>,
        request_weight: f64,
    ) -> anyhow::Result<CallContext> {
        let request_weight = if request_weight.is_finite() && request_weight > 0.0 {
            request_weight
        } else {
            DEFAULT_REQUEST_WEIGHT
        };
        let total = self.total_count();
        let max_attempts = (total * MAX_FAILURES_PER_CREDENTIAL as usize).max(1);
        let mut attempt_count = 0;
        let mut wait_queue_guard: Option<WaitQueueGuard> = None;
        let mut wait_deadline: Option<Instant> = None;
        let mut last_runtime_coordination_error: Option<anyhow::Error> = None;

        loop {
            if self.state_store.is_external() {
                if let Err(err) = self.maybe_sync_external_state_on_hot_path() {
                    tracing::warn!("按需同步外部状态失败，将继续使用本地状态: {}", err);
                }
            }

            if attempt_count >= max_attempts {
                if let Some(err) = last_runtime_coordination_error {
                    return Err(err);
                }
                anyhow::bail!(
                    "所有凭据均无法获取有效 Token（可用: {}/{}）",
                    self.available_count(),
                    total
                );
            }

            let (id, credentials, lease) = match if self.shared_dispatch_runtime_enabled() {
                self.reserve_next_credential_shared(model, request_weight)
            } else {
                self.reserve_next_credential(model, request_weight)
            } {
                Ok(selection) => {
                    wait_queue_guard = None;
                    wait_deadline = None;
                    selection
                }
                Err(ReservationFailure::NoCredentials) => anyhow::bail!("未配置任何凭据"),
                Err(ReservationFailure::AllDisabled) => {
                    if self.recover_auto_disabled_credentials() {
                        continue;
                    }
                    anyhow::bail!("所有凭据均已禁用（0/{})", total);
                }
                Err(ReservationFailure::NoModelSupport) => {
                    anyhow::bail!("当前没有可用凭据支持该模型");
                }
                Err(ReservationFailure::AllTemporarilyUnavailable { next_ready_at }) => {
                    let dispatch = self.dispatch_config();
                    if !dispatch.queue_enabled() {
                        anyhow::bail!(
                            "所有可用凭据已达到并发上限、处于限流冷却或正等待 token bucket 补充"
                        );
                    }

                    if wait_queue_guard.is_none() {
                        wait_queue_guard =
                            Some(self.try_enter_wait_queue(dispatch.queue_max_size)?);
                        wait_deadline = Some(Instant::now() + dispatch.queue_wait_duration());
                    }

                    let deadline = wait_deadline.expect("wait deadline should exist");
                    if !self.wait_for_availability(deadline, next_ready_at).await {
                        anyhow::bail!("等待可用凭据超时");
                    }
                    continue;
                }
            };

            // 尝试获取/刷新 Token
            match self.try_ensure_token(id, &credentials).await {
                Ok((credentials, token)) => {
                    return Ok(CallContext {
                        id,
                        credentials,
                        token,
                        lease,
                    });
                }
                Err(e) => {
                    drop(lease);
                    if e.downcast_ref::<RuntimeRefreshLeaderRequiredError>()
                        .is_some()
                    {
                        tracing::warn!("凭据 #{} 需要由 leader 刷新 Token: {}", id, e);
                        last_runtime_coordination_error = Some(e);
                        attempt_count += 1;
                        let has_available =
                            self.defer_shared_refresh_credential(id, SHARED_REFRESH_DEFER_COOLDOWN);

                        if !has_available {
                            return Err(last_runtime_coordination_error
                                .take()
                                .expect("runtime coordination error should exist"));
                        }
                        continue;
                    }
                    // refreshToken 永久失效 → 立即禁用，不累计重试
                    let has_available = if e.downcast_ref::<RefreshTokenInvalidError>().is_some() {
                        tracing::warn!("凭据 #{} refreshToken 永久失效: {}", id, e);
                        self.report_refresh_token_invalid(id)
                    } else {
                        tracing::warn!("凭据 #{} Token 刷新失败: {}", id, e);
                        self.report_refresh_failure(id)
                    };
                    attempt_count += 1;
                    if !has_available {
                        anyhow::bail!("所有凭据均已禁用（0/{}）", total);
                    }
                }
            }
        }
    }

    /// 选择优先级最高的未禁用凭据作为当前凭据（内部方法）
    ///
    /// 纯粹按优先级选择，不排除当前凭据，用于优先级变更后立即生效
    fn select_highest_priority(&self) {
        let entries = self.entries.lock();
        let mut current_id = self.current_id.lock();

        // 选择优先级最高的未禁用凭据（不排除当前凭据）
        if let Some(best) = entries
            .iter()
            .filter(|e| !e.disabled)
            .min_by_key(|e| e.credentials.priority)
        {
            if best.id != *current_id {
                tracing::info!(
                    "优先级变更后切换凭据: #{} -> #{}（优先级 {}）",
                    *current_id,
                    best.id,
                    best.credentials.priority
                );
                *current_id = best.id;
            }
        }
    }

    /// 尝试使用指定凭据获取有效 Token
    ///
    /// 使用双重检查锁定模式，确保同一凭据同一时间只有一个刷新操作
    ///
    /// # Arguments
    /// * `id` - 凭据 ID，用于更新正确的条目
    /// * `credentials` - 凭据信息
    async fn try_ensure_token(
        &self,
        id: u64,
        credentials: &KiroCredentials,
    ) -> anyhow::Result<(KiroCredentials, String)> {
        // 第一次检查（无锁）：快速判断是否需要刷新
        let needs_refresh = is_token_expired(credentials) || is_token_expiring_soon(credentials);

        let creds = if needs_refresh {
            // 获取凭据级刷新锁，仅串行同一账号的刷新流程
            let refresh_lock = self.refresh_lock_for(id)?;
            let _guard = refresh_lock.lock().await;

            // 第二次检查：获取锁后重新读取凭据，因为其他请求可能已经完成刷新
            let mut current_creds = self.current_credentials(id)?;

            if is_token_expired(&current_creds) || is_token_expiring_soon(&current_creds) {
                if self.state_store.is_external() {
                    if let Err(err) = self.sync_external_state_if_changed() {
                        tracing::warn!("按需同步共享凭据状态失败，将继续使用本地状态: {}", err);
                    }
                    current_creds = self.current_credentials(id)?;
                }

                if is_token_expired(&current_creds) || is_token_expiring_soon(&current_creds) {
                    if let Some(status) = self.runtime_refresh_coordination_status()? {
                        if !status.is_leader {
                            return Err(anyhow::Error::new(RuntimeRefreshLeaderRequiredError {
                                instance_id: status.instance_id,
                                leader_id: status.leader_id,
                            }));
                        }
                    }

                    // 确实需要刷新
                    let effective_proxy = current_creds.effective_proxy(self.proxy.as_ref());
                    let new_creds =
                        refresh_token(&current_creds, &self.config, effective_proxy.as_ref())
                            .await?;

                    if is_token_expired(&new_creds) {
                        anyhow::bail!("刷新后的 Token 仍然无效或已过期");
                    }

                    let _state_write_guard = self.state_write_lock.lock();
                    {
                        let mut entries = self.entries.lock();
                        if let Some(entry) = entries.iter_mut().find(|e| e.id == id) {
                            entry.credentials = new_creds.clone();
                        }
                    }

                    let credentials = self.persisted_credentials_snapshot();
                    if let Err(e) = self.persist_credentials_snapshot(&credentials) {
                        tracing::warn!("Token 刷新后持久化失败（不影响本次请求）: {}", e);
                    }

                    new_creds
                } else {
                    tracing::debug!("Token 已从外部状态更新，跳过本地刷新");
                    current_creds
                }
            } else {
                // 其他请求已经完成刷新，直接使用新凭据
                tracing::debug!("Token 已被其他请求刷新，跳过刷新");
                current_creds
            }
        } else {
            credentials.clone()
        };

        let token = creds
            .access_token
            .clone()
            .ok_or_else(|| anyhow::anyhow!("没有可用的 accessToken"))?;

        {
            let mut entries = self.entries.lock();
            if let Some(entry) = entries.iter_mut().find(|e| e.id == id) {
                entry.refresh_failure_count = 0;
            }
        }

        Ok((creds, token))
    }

    /// 将凭据列表回写到源文件
    ///
    /// 仅在以下条件满足时回写：
    /// - 源文件是多凭据格式（数组）
    /// - credentials_path 已设置
    ///
    /// # Returns
    /// - `Ok(true)` - 成功写入文件
    /// - `Ok(false)` - 跳过写入（非多凭据格式或无路径配置）
    /// - `Err(_)` - 写入失败
    fn persist_credentials(&self) -> anyhow::Result<bool> {
        let _guard = self.state_write_lock.lock();
        let credentials = self.persisted_credentials_snapshot();
        self.persist_credentials_snapshot(&credentials)
    }

    fn persisted_credentials_snapshot(&self) -> Vec<KiroCredentials> {
        let entries = self.entries.lock();
        Self::persisted_credentials_from_entries(&entries)
    }

    fn persisted_credentials_from_entries(entries: &[CredentialEntry]) -> Vec<KiroCredentials> {
        entries
            .iter()
            .map(|entry| {
                let mut credential = entry.credentials.clone();
                credential.canonicalize_auth_method();
                credential.normalize_model_capabilities();
                credential.disabled = entry.disabled;
                credential
            })
            .collect()
    }

    fn persist_credentials_snapshot(
        &self,
        credentials: &[KiroCredentials],
    ) -> anyhow::Result<bool> {
        let mut normalized = credentials.to_vec();
        for credential in &mut normalized {
            credential.canonicalize_auth_method();
            credential.normalize_model_capabilities();
        }

        let persisted = self
            .state_store
            .persist_credentials(&normalized, self.is_multiple_format)?;
        if persisted {
            self.try_bump_state_change_revision(StateChangeKind::Credentials);
        }
        Ok(persisted)
    }

    fn record_state_change_revisions(&self, revisions: StateChangeRevisions) {
        *self.last_state_change_revisions.lock() = revisions;
    }

    fn record_state_change_revision(&self, kind: StateChangeKind, revision: u64) {
        if revision == 0 {
            return;
        }

        let mut revisions = self.last_state_change_revisions.lock();
        match kind {
            StateChangeKind::Credentials => {
                revisions.credentials = revisions.credentials.max(revision)
            }
            StateChangeKind::DispatchConfig => {
                revisions.dispatch_config = revisions.dispatch_config.max(revision)
            }
            StateChangeKind::BalanceCache => {
                revisions.balance_cache = revisions.balance_cache.max(revision)
            }
        }
    }

    fn try_bump_state_change_revision(&self, kind: StateChangeKind) {
        match self.state_store.bump_state_change_revision(kind) {
            Ok(revision) => self.record_state_change_revision(kind, revision),
            Err(err) => tracing::warn!("更新共享状态修订号失败: {}", err),
        }
    }

    fn try_begin_hot_path_state_sync(&self) -> bool {
        let min_interval_ms = self.config.state_hot_path_sync_min_interval_ms;
        if min_interval_ms == 0 {
            return true;
        }

        let now_ms = self.hot_path_state_sync_origin.elapsed().as_millis() as u64;
        loop {
            let previous = self
                .last_hot_path_state_sync_check_ms
                .load(Ordering::Acquire);
            if previous != u64::MAX && now_ms.saturating_sub(previous) < min_interval_ms {
                return false;
            }
            match self.last_hot_path_state_sync_check_ms.compare_exchange(
                previous,
                now_ms,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(_) => continue,
            }
        }
    }

    fn maybe_sync_external_state_on_hot_path(&self) -> anyhow::Result<ExternalStateSyncReport> {
        if !self.state_store.is_external() {
            return Ok(ExternalStateSyncReport::default());
        }
        if !self.try_begin_hot_path_state_sync() {
            return Ok(ExternalStateSyncReport::default());
        }
        self.sync_external_state_if_changed()
    }

    pub fn state_store(&self) -> StateStore {
        self.state_store.clone()
    }

    pub fn sync_from_state(&self) -> anyhow::Result<ExternalStateSyncReport> {
        if !self.state_store.is_external() {
            return Ok(ExternalStateSyncReport::default());
        }

        let dispatch_config_reloaded = self.reload_dispatch_config_from_state()?;
        self.reload_credentials_from_state()?;
        let stats_reloaded = self.reload_stats_from_state()?;
        match self.state_store.state_change_revisions() {
            Ok(revisions) => self.record_state_change_revisions(revisions),
            Err(err) => tracing::warn!("刷新共享状态修订号缓存失败: {}", err),
        }

        Ok(ExternalStateSyncReport {
            credentials_reloaded: true,
            dispatch_config_reloaded,
            stats_reloaded,
        })
    }

    pub fn sync_external_state_if_changed(&self) -> anyhow::Result<ExternalStateSyncReport> {
        if !self.state_store.is_external() {
            return Ok(ExternalStateSyncReport::default());
        }

        let revisions = self.state_store.state_change_revisions()?;
        let previous = *self.last_state_change_revisions.lock();
        if revisions == previous {
            return Ok(ExternalStateSyncReport::default());
        }

        let mut report = ExternalStateSyncReport::default();

        if revisions.dispatch_config > previous.dispatch_config {
            report.dispatch_config_reloaded = self.reload_dispatch_config_from_state()?;
        }
        if revisions.credentials > previous.credentials {
            self.reload_credentials_from_state()?;
            report.credentials_reloaded = true;
        }

        self.record_state_change_revisions(revisions);
        Ok(report)
    }

    fn current_credentials(&self, id: u64) -> anyhow::Result<KiroCredentials> {
        let entries = self.entries.lock();
        entries
            .iter()
            .find(|e| e.id == id)
            .map(|e| e.credentials.clone())
            .ok_or_else(|| anyhow::anyhow!("凭据 #{} 不存在", id))
    }

    fn persisted_credential_mut<'a>(
        credentials: &'a mut [KiroCredentials],
        id: u64,
    ) -> anyhow::Result<&'a mut KiroCredentials> {
        credentials
            .iter_mut()
            .find(|credential| credential.id == Some(id))
            .ok_or_else(|| anyhow::anyhow!("凭据不存在: {}", id))
    }

    fn runtime_refresh_coordination_status(
        &self,
    ) -> anyhow::Result<Option<RuntimeCoordinationStatus>> {
        if !self.state_store.is_external() || !self.state_store.runtime_coordination_enabled() {
            return Ok(None);
        }

        self.state_store.runtime_coordination_status()
    }

    fn reload_dispatch_config_from_state(&self) -> anyhow::Result<bool> {
        let Some(persisted) = self.state_store.load_dispatch_config()? else {
            return Ok(false);
        };

        Ok(self.apply_dispatch_config_from_state(&persisted))
    }

    fn apply_dispatch_config_from_state(&self, persisted: &PersistedDispatchConfig) -> bool {
        let mut config = self.config.clone();
        persisted.apply_to_config(&mut config);
        let next = DispatchConfig::from_config(&config);
        let previous = self.dispatch_config();

        if previous == next {
            return false;
        }

        *self.dispatch_config.lock() = next.clone();

        if previous.rate_limit_cooldown_ms != next.rate_limit_cooldown_ms
            && next.rate_limit_cooldown_ms == 0
        {
            self.clear_all_rate_limit_cooldowns();
        }
        if previous.rate_limit_bucket_capacity != next.rate_limit_bucket_capacity
            || previous.rate_limit_refill_per_second != next.rate_limit_refill_per_second
            || previous.rate_limit_refill_min_per_second != next.rate_limit_refill_min_per_second
            || previous.rate_limit_refill_recovery_step_per_success
                != next.rate_limit_refill_recovery_step_per_success
            || previous.rate_limit_refill_backoff_factor != next.rate_limit_refill_backoff_factor
        {
            self.reconfigure_rate_limit_runtime(&next);
        }

        self.availability_notify.notify_waiters();
        tracing::info!(
            "已从外部状态热加载调度配置: mode={}, queueMaxSize={}, queueMaxWaitMs={}, rateLimitCooldownMs={}, defaultMaxConcurrency={:?}, rateLimitBucketCapacity={}, rateLimitRefillPerSecond={}, rateLimitRefillMinPerSecond={}, rateLimitRefillRecoveryStepPerSuccess={}, rateLimitRefillBackoffFactor={}",
            next.mode,
            next.queue_max_size,
            next.queue_max_wait_ms,
            next.rate_limit_cooldown_ms,
            next.default_max_concurrency,
            next.rate_limit_bucket_capacity,
            next.rate_limit_refill_per_second,
            next.rate_limit_refill_min_per_second,
            next.rate_limit_refill_recovery_step_per_success,
            next.rate_limit_refill_backoff_factor
        );

        true
    }

    fn reload_credentials_from_state(&self) -> anyhow::Result<()> {
        let persisted = self.state_store.load_credentials()?;
        let dispatch = self.dispatch_config();
        let now = Instant::now();
        let mut persisted_by_id = HashMap::new();
        let mut persisted_ids = std::collections::HashSet::new();

        for mut credential in persisted.credentials {
            credential.canonicalize_auth_method();
            credential.normalize_model_capabilities();
            let Some(id) = credential.id else {
                tracing::warn!("外部状态中的凭据缺少 ID，已跳过热重载");
                continue;
            };

            if credential.machine_id.is_none() {
                if let Some(machine_id) =
                    machine_id::generate_from_credentials(&credential, &self.config)
                {
                    credential.machine_id = Some(machine_id);
                }
            }

            if !persisted_ids.insert(id) {
                tracing::warn!("外部状态中的凭据 ID 重复，后写入项将覆盖前一项: {}", id);
            }
            persisted_by_id.insert(id, credential);
        }

        {
            let mut entries = self.entries.lock();

            for entry in entries.iter_mut() {
                let Some(persisted) = persisted_by_id.remove(&entry.id) else {
                    if entry.active_requests == 0 {
                        continue;
                    }

                    entry.disabled = true;
                    if entry.disabled_reason.is_none() {
                        entry.disabled_reason = Some(DisabledReason::Manual);
                    }
                    continue;
                };

                let was_disabled = entry.disabled;
                entry.credentials = persisted.clone();
                entry.disabled = persisted.disabled;

                if persisted.disabled {
                    if entry.disabled_reason.is_none() {
                        entry.disabled_reason = Some(DisabledReason::Manual);
                    }
                } else {
                    entry.disabled_reason = None;
                    if was_disabled {
                        entry.failure_count = 0;
                        entry.refresh_failure_count = 0;
                        Self::reset_rate_limit_runtime(entry, &dispatch, now);
                    } else {
                        Self::sync_rate_limit_bucket_runtime(entry, &dispatch, now);
                    }
                }
            }

            entries.retain(|entry| persisted_ids.contains(&entry.id) || entry.active_requests > 0);

            for (_, credential) in persisted_by_id {
                entries.push(CredentialEntry {
                    id: credential.id.expect("persisted credential id must exist"),
                    disabled: credential.disabled,
                    disabled_reason: credential.disabled.then_some(DisabledReason::Manual),
                    credentials: credential.clone(),
                    failure_count: 0,
                    refresh_failure_count: 0,
                    success_count: 0,
                    pending_success_count_delta: 0,
                    last_used_at: None,
                    active_requests: 0,
                    rate_limit_cooldown_until: None,
                    rate_limit_bucket: dispatch
                        .bucket_policy_for(&credential)
                        .map(|policy| AdaptiveTokenBucket::new(policy, now)),
                    rate_limit_hit_streak: 0,
                    refresh_lock: Arc::new(TokioMutex::new(())),
                });
            }
        }

        {
            let entries = self.entries.lock();
            if entries.is_empty() {
                *self.current_id.lock() = 0;
            }
        }

        if self.total_count() > 0 {
            self.select_highest_priority();
        }
        self.availability_notify.notify_waiters();
        Ok(())
    }

    fn reload_stats_from_state(&self) -> anyhow::Result<bool> {
        let stats = self.state_store.load_stats()?;
        let changed = self.apply_stats_from_state(&stats);

        if changed {
            tracing::info!("已从外部状态热加载 {} 条统计数据", stats.len());
        }

        Ok(changed)
    }

    fn apply_stats_from_state(&self, stats: &HashMap<String, StatsEntryRecord>) -> bool {
        let mut changed = false;
        let mut entries = self.entries.lock();

        for entry in entries.iter_mut() {
            let Some(persisted) = stats.get(&entry.id.to_string()) else {
                continue;
            };

            let next_success_count = persisted
                .success_count
                .saturating_add(entry.pending_success_count_delta);
            let next_last_used_at =
                newer_timestamp(entry.last_used_at.clone(), persisted.last_used_at.clone());

            if entry.success_count != next_success_count || entry.last_used_at != next_last_used_at
            {
                changed = true;
            }

            entry.success_count = next_success_count;
            entry.last_used_at = next_last_used_at;
        }

        changed
    }

    /// 从磁盘加载统计数据并应用到当前条目
    fn load_stats(&self) {
        let stats = match self.state_store.load_stats() {
            Ok(stats) => stats,
            Err(e) => {
                tracing::warn!("加载统计缓存失败，将忽略: {}", e);
                return;
            }
        };

        self.apply_stats_from_state(&stats);
        *self.last_stats_save_at.lock() = Some(Instant::now());
        self.stats_dirty.store(false, Ordering::Relaxed);
        tracing::info!("已从缓存加载 {} 条统计数据", stats.len());
    }

    /// 将当前统计数据持久化到磁盘
    fn save_stats(&self) {
        let updates: HashMap<String, StatsMergeRecord> = {
            let entries = self.entries.lock();
            entries
                .iter()
                .map(|e| {
                    (
                        e.id.to_string(),
                        StatsMergeRecord {
                            success_count_delta: e.pending_success_count_delta,
                            last_used_at: e.last_used_at.clone(),
                        },
                    )
                })
                .collect()
        };

        match self.state_store.merge_stats(&updates) {
            Ok(merged_stats) => {
                let mut entries = self.entries.lock();
                for entry in entries.iter_mut() {
                    if let Some(merged) = merged_stats.get(&entry.id.to_string()) {
                        entry.success_count = merged.success_count;
                        entry.last_used_at = merged.last_used_at.clone();
                    }
                    entry.pending_success_count_delta = 0;
                }
                *self.last_stats_save_at.lock() = Some(Instant::now());
                self.stats_dirty.store(false, Ordering::Relaxed);
            }
            Err(e) => tracing::warn!("保存统计缓存失败: {}", e),
        }
    }

    /// 使用当前内存快照全量覆盖统计缓存。
    ///
    /// 仅用于需要裁剪已删除凭据残留统计的场景。
    fn rewrite_stats_snapshot(&self) {
        let stats: HashMap<String, StatsEntryRecord> = {
            let entries = self.entries.lock();
            entries
                .iter()
                .map(|e| {
                    (
                        e.id.to_string(),
                        StatsEntryRecord {
                            success_count: e.success_count,
                            last_used_at: e.last_used_at.clone(),
                        },
                    )
                })
                .collect()
        };

        match self.state_store.save_stats(&stats) {
            Ok(()) => {
                let mut entries = self.entries.lock();
                for entry in entries.iter_mut() {
                    entry.pending_success_count_delta = 0;
                }
                *self.last_stats_save_at.lock() = Some(Instant::now());
                self.stats_dirty.store(false, Ordering::Relaxed);
            }
            Err(e) => tracing::warn!("全量重写统计缓存失败: {}", e),
        }
    }

    /// 标记统计数据已更新，并按 debounce 策略决定是否立即落盘
    fn save_stats_debounced(&self) {
        self.stats_dirty.store(true, Ordering::Relaxed);

        let should_flush = {
            let last = *self.last_stats_save_at.lock();
            match last {
                Some(last_saved_at) => last_saved_at.elapsed() >= STATS_SAVE_DEBOUNCE,
                None => true,
            }
        };

        if should_flush {
            self.save_stats();
        }
    }

    /// 报告指定凭据 API 调用成功
    ///
    /// 重置该凭据的失败计数
    ///
    /// # Arguments
    /// * `id` - 凭据 ID（来自 CallContext）
    pub fn report_success(&self, id: u64) {
        let now = Instant::now();
        let dispatch = self.dispatch_config();
        let mut shared_bucket_policy = None;
        {
            let mut entries = self.entries.lock();
            if let Some(entry) = entries.iter_mut().find(|e| e.id == id) {
                entry.failure_count = 0;
                entry.refresh_failure_count = 0;
                if entry
                    .rate_limit_cooldown_until
                    .is_some_and(|until| until <= now)
                {
                    entry.rate_limit_cooldown_until = None;
                }
                if let Some(bucket) = entry.rate_limit_bucket.as_mut() {
                    bucket.on_success(now);
                }
                entry.rate_limit_hit_streak = 0;
                entry.success_count += 1;
                entry.pending_success_count_delta += 1;
                entry.last_used_at = Some(Utc::now().to_rfc3339());
                shared_bucket_policy = dispatch.shared_bucket_policy_for(&entry.credentials);
                tracing::debug!(
                    "凭据 #{} API 调用成功（累计 {} 次）",
                    id,
                    entry.success_count
                );
            }
        }
        if self.shared_dispatch_runtime_enabled() {
            if let Err(err) = self.state_store.record_dispatch_success(
                id,
                shared_bucket_policy,
                current_epoch_ms(),
            ) {
                tracing::warn!("更新共享调度成功态失败（credentialId={}）: {}", id, err);
            }
        }
        self.save_stats_debounced();
    }

    /// 报告指定凭据遭遇上游 429 限流。
    ///
    /// 对单账号施加短暂冷却，避免重试流量持续打到同一个受限账号上。
    pub fn report_rate_limited(&self, id: u64) {
        let dispatch = self.dispatch_config();
        let now = Instant::now();
        let mut shared_bucket_policy = None;
        {
            let mut entries = self.entries.lock();
            if let Some(entry) = entries.iter_mut().find(|e| e.id == id) {
                if entry.disabled {
                    return;
                }

                entry.rate_limit_hit_streak = entry.rate_limit_hit_streak.saturating_add(1);
                if let Some(bucket) = entry.rate_limit_bucket.as_mut() {
                    bucket.on_rate_limited(now);
                }
                shared_bucket_policy = dispatch.shared_bucket_policy_for(&entry.credentials);

                let cooldown_ms = dispatch.rate_limit_cooldown_ms;
                entry.rate_limit_cooldown_until =
                    (cooldown_ms > 0).then(|| now + StdDuration::from_millis(cooldown_ms));
                entry.last_used_at = Some(Utc::now().to_rfc3339());

                if let Some(bucket) = entry.rate_limit_bucket.as_ref() {
                    tracing::warn!(
                        "凭据 #{} 遭遇上游 429，固定冷却 {}ms，bucket 速率降至 {:.2}/{:.2} token/s（streak={}）",
                        id,
                        cooldown_ms,
                        bucket.current_refill_per_second,
                        bucket.policy.refill_per_second,
                        entry.rate_limit_hit_streak
                    );
                } else {
                    tracing::warn!(
                        "凭据 #{} 遭遇上游 429，固定冷却 {}ms（streak={}，未启用 token bucket）",
                        id,
                        cooldown_ms,
                        entry.rate_limit_hit_streak
                    );
                }
            }
        }
        if self.shared_dispatch_runtime_enabled() {
            if let Err(err) = self.state_store.record_dispatch_rate_limited(
                id,
                shared_bucket_policy,
                dispatch.rate_limit_cooldown_ms,
                current_epoch_ms(),
            ) {
                tracing::warn!("更新共享调度限流态失败（credentialId={}）: {}", id, err);
            }
        }
        self.save_stats_debounced();
    }

    /// 当 follower 观察到某个凭据需要由 leader 刷新 token 时，临时冷却该凭据，
    /// 让当前请求优先切换到其他可用凭据，避免在同一张共享凭据上反复重试。
    pub fn defer_shared_refresh_credential(&self, id: u64, cooldown: StdDuration) -> bool {
        let now = Instant::now();
        let deferred_until = now + cooldown;
        let result = {
            let mut entries = self.entries.lock();
            let mut current_id = self.current_id.lock();

            let entry = match entries.iter_mut().find(|e| e.id == id) {
                Some(e) => e,
                None => return entries.iter().any(|e| !e.disabled),
            };

            if entry.disabled {
                return entries.iter().any(|e| !e.disabled);
            }

            entry.rate_limit_cooldown_until = Some(
                entry
                    .rate_limit_cooldown_until
                    .map(|until| until.max(deferred_until))
                    .unwrap_or(deferred_until),
            );
            entry.last_used_at = Some(Utc::now().to_rfc3339());

            if *current_id == id {
                if let Some(next) = entries
                    .iter()
                    .filter(|e| !e.disabled && e.id != id)
                    .min_by_key(|e| e.credentials.priority)
                {
                    *current_id = next.id;
                    tracing::info!(
                        "凭据 #{} 需由 leader 刷新 token，已临时冷却 {}ms 并切换到凭据 #{}",
                        id,
                        cooldown.as_millis(),
                        next.id
                    );
                } else {
                    tracing::warn!(
                        "凭据 #{} 需由 leader 刷新 token，已临时冷却 {}ms，当前无其他可切换凭据",
                        id,
                        cooldown.as_millis()
                    );
                }
            } else {
                tracing::warn!(
                    "凭据 #{} 需由 leader 刷新 token，已临时冷却 {}ms",
                    id,
                    cooldown.as_millis()
                );
            }

            entries.iter().any(|e| !e.disabled)
        };
        if self.shared_dispatch_runtime_enabled() {
            let cooldown_ms = cooldown.as_millis().min(u128::from(u64::MAX)) as u64;
            if let Err(err) =
                self.state_store
                    .defer_dispatch_credential(id, cooldown_ms, current_epoch_ms())
            {
                tracing::warn!("更新共享调度临时冷却失败（credentialId={}）: {}", id, err);
            }
        }
        self.save_stats_debounced();
        result
    }

    /// 当上游明确返回 `INVALID_MODEL_ID` 时，
    /// 将当前凭据视为“不支持该模型”，临时避让并记录一段时间的运行时限制。
    pub fn defer_model_unsupported_credential(
        &self,
        id: u64,
        model: &str,
        cooldown: StdDuration,
    ) -> bool {
        let now = Instant::now();
        let deferred_until = now + cooldown;
        let restriction_expires_at = Utc::now() + Duration::minutes(30);
        let dispatch = self.dispatch_config();
        let requirement = Self::model_requirement(Some(model));
        let model_label = normalize_model_selector(model)
            .map(|selector| selector.family)
            .unwrap_or_else(|| model.to_ascii_lowercase());
        let result = {
            let _state_write_guard = self.state_write_lock.lock();
            let mut entries = self.entries.lock();
            let mut current_id = self.current_id.lock();

            let entry = match entries.iter_mut().find(|e| e.id == id) {
                Some(e) => e,
                None => {
                    return entries.iter().any(|e| {
                        !e.disabled
                            && e.id != id
                            && Self::is_model_supported(
                                &dispatch,
                                &e.credentials,
                                Some(model),
                                requirement,
                            )
                    });
                }
            };

            if entry.disabled {
                return entries.iter().any(|e| {
                    !e.disabled
                        && e.id != id
                        && Self::is_model_supported(
                            &dispatch,
                            &e.credentials,
                            Some(model),
                            requirement,
                        )
                });
            }

            entry.rate_limit_cooldown_until = Some(
                entry
                    .rate_limit_cooldown_until
                    .map(|until| until.max(deferred_until))
                    .unwrap_or(deferred_until),
            );
            entry.last_used_at = Some(Utc::now().to_rfc3339());
            let restriction_changed = entry
                .credentials
                .upsert_runtime_model_restriction(model, restriction_expires_at);
            if restriction_changed {
                let credentials = Self::persisted_credentials_from_entries(&entries);
                if let Err(err) = self.persist_credentials_snapshot(&credentials) {
                    tracing::warn!(
                        "持久化模型运行时限制失败（credentialId={}, model={}）: {}",
                        id,
                        model_label,
                        err
                    );
                }
            }

            if *current_id == id {
                if let Some(next) = entries
                    .iter()
                    .filter(|e| {
                        !e.disabled
                            && e.id != id
                            && Self::is_model_supported(
                                &dispatch,
                                &e.credentials,
                                Some(model),
                                requirement,
                            )
                    })
                    .min_by_key(|e| {
                        (
                            Self::model_preference_rank(&e.credentials, requirement),
                            e.credentials.priority,
                            e.id,
                        )
                    })
                {
                    *current_id = next.id;
                    tracing::info!(
                        "凭据 #{} 不支持模型 {}，已临时冷却 {}ms 并切换到凭据 #{}",
                        id,
                        model_label,
                        cooldown.as_millis(),
                        next.id
                    );
                } else {
                    tracing::warn!(
                        "凭据 #{} 不支持模型 {}，已临时冷却 {}ms，当前无其他可切换凭据",
                        id,
                        model_label,
                        cooldown.as_millis()
                    );
                }
            } else {
                tracing::warn!(
                    "凭据 #{} 不支持模型 {}，已临时冷却 {}ms",
                    id,
                    model_label,
                    cooldown.as_millis()
                );
            }

            entries.iter().any(|e| {
                !e.disabled
                    && e.id != id
                    && Self::is_model_supported(&dispatch, &e.credentials, Some(model), requirement)
            })
        };
        if self.shared_dispatch_runtime_enabled() {
            let cooldown_ms = cooldown.as_millis().min(u128::from(u64::MAX)) as u64;
            if let Err(err) =
                self.state_store
                    .defer_dispatch_credential(id, cooldown_ms, current_epoch_ms())
            {
                tracing::warn!(
                    "更新共享调度模型不支持冷却失败（credentialId={}）: {}",
                    id,
                    err
                );
            }
        }
        self.save_stats_debounced();
        result
    }

    /// 报告指定凭据 API 调用失败
    ///
    /// 增加失败计数，达到阈值时禁用凭据并切换到优先级最高的可用凭据
    /// 返回是否还有可用凭据可以重试
    ///
    /// # Arguments
    /// * `id` - 凭据 ID（来自 CallContext）
    pub fn report_failure(&self, id: u64) -> bool {
        let result = {
            let _state_write_guard = self.state_write_lock.lock();
            let mut entries = self.entries.lock();
            let mut current_id = self.current_id.lock();

            let entry = match entries.iter_mut().find(|e| e.id == id) {
                Some(e) => e,
                None => return entries.iter().any(|e| !e.disabled),
            };

            if entry.disabled {
                return entries.iter().any(|e| !e.disabled);
            }

            entry.failure_count += 1;
            entry.last_used_at = Some(Utc::now().to_rfc3339());
            let failure_count = entry.failure_count;

            tracing::warn!(
                "凭据 #{} API 调用失败（{}/{}）",
                id,
                failure_count,
                MAX_FAILURES_PER_CREDENTIAL
            );

            if failure_count >= MAX_FAILURES_PER_CREDENTIAL {
                entry.disabled = true;
                entry.disabled_reason = Some(DisabledReason::TooManyFailures);
                tracing::error!("凭据 #{} 已连续失败 {} 次，已被禁用", id, failure_count);

                // 切换到优先级最高的可用凭据
                if let Some(next) = entries
                    .iter()
                    .filter(|e| !e.disabled)
                    .min_by_key(|e| e.credentials.priority)
                {
                    *current_id = next.id;
                    tracing::info!(
                        "已切换到凭据 #{}（优先级 {}）",
                        next.id,
                        next.credentials.priority
                    );
                } else {
                    tracing::error!("所有凭据均已禁用！");
                }
            }

            entries.iter().any(|e| !e.disabled)
        };
        self.save_stats_debounced();
        result
    }

    /// 报告指定凭据额度已用尽
    ///
    /// 用于处理 402 Payment Required 且 reason 为 `MONTHLY_REQUEST_COUNT` 的场景：
    /// - 立即禁用该凭据（不等待连续失败阈值）
    /// - 切换到下一个可用凭据继续重试
    /// - 返回是否还有可用凭据
    pub fn report_quota_exhausted(&self, id: u64) -> bool {
        let result = {
            let _state_write_guard = self.state_write_lock.lock();
            let mut entries = self.entries.lock();
            let mut current_id = self.current_id.lock();

            let entry = match entries.iter_mut().find(|e| e.id == id) {
                Some(e) => e,
                None => return entries.iter().any(|e| !e.disabled),
            };

            if entry.disabled {
                return entries.iter().any(|e| !e.disabled);
            }

            entry.disabled = true;
            entry.disabled_reason = Some(DisabledReason::QuotaExceeded);
            entry.last_used_at = Some(Utc::now().to_rfc3339());
            // 设为阈值，便于在管理面板中直观看到该凭据已不可用
            entry.failure_count = MAX_FAILURES_PER_CREDENTIAL;

            tracing::error!("凭据 #{} 额度已用尽（MONTHLY_REQUEST_COUNT），已被禁用", id);

            // 切换到优先级最高的可用凭据
            if let Some(next) = entries
                .iter()
                .filter(|e| !e.disabled)
                .min_by_key(|e| e.credentials.priority)
            {
                *current_id = next.id;
                tracing::info!(
                    "已切换到凭据 #{}（优先级 {}）",
                    next.id,
                    next.credentials.priority
                );
                true
            } else {
                tracing::error!("所有凭据均已禁用！");
                false
            }
        };
        self.save_stats_debounced();
        result
    }

    /// 报告指定凭据刷新 Token 失败。
    ///
    /// 连续刷新失败达到阈值后禁用凭据并切换，阈值内保持当前凭据不切换，
    /// 与 API 401/403 的累计失败策略保持一致。
    pub fn report_refresh_failure(&self, id: u64) -> bool {
        let result = {
            let _state_write_guard = self.state_write_lock.lock();
            let mut entries = self.entries.lock();
            let mut current_id = self.current_id.lock();

            let entry = match entries.iter_mut().find(|e| e.id == id) {
                Some(e) => e,
                None => return entries.iter().any(|e| !e.disabled),
            };

            if entry.disabled {
                return entries.iter().any(|e| !e.disabled);
            }

            entry.last_used_at = Some(Utc::now().to_rfc3339());
            entry.refresh_failure_count += 1;
            let refresh_failure_count = entry.refresh_failure_count;

            tracing::warn!(
                "凭据 #{} Token 刷新失败（{}/{}）",
                id,
                refresh_failure_count,
                MAX_FAILURES_PER_CREDENTIAL
            );

            if refresh_failure_count < MAX_FAILURES_PER_CREDENTIAL {
                return entries.iter().any(|e| !e.disabled);
            }

            entry.disabled = true;
            entry.disabled_reason = Some(DisabledReason::TooManyRefreshFailures);

            tracing::error!(
                "凭据 #{} Token 已连续刷新失败 {} 次，已被禁用",
                id,
                refresh_failure_count
            );

            if let Some(next) = entries
                .iter()
                .filter(|e| !e.disabled)
                .min_by_key(|e| e.credentials.priority)
            {
                *current_id = next.id;
                tracing::info!(
                    "已切换到凭据 #{}（优先级 {}）",
                    next.id,
                    next.credentials.priority
                );
                true
            } else {
                tracing::error!("所有凭据均已禁用！");
                false
            }
        };
        self.save_stats_debounced();
        result
    }

    /// 报告指定凭据的 refreshToken 永久失效（invalid_grant）。
    ///
    /// 立即禁用凭据，不累计、不重试。
    /// 返回是否还有可用凭据。
    pub fn report_refresh_token_invalid(&self, id: u64) -> bool {
        let result = {
            let _state_write_guard = self.state_write_lock.lock();
            let mut entries = self.entries.lock();
            let mut current_id = self.current_id.lock();

            let entry = match entries.iter_mut().find(|e| e.id == id) {
                Some(e) => e,
                None => return entries.iter().any(|e| !e.disabled),
            };

            if entry.disabled {
                return entries.iter().any(|e| !e.disabled);
            }

            entry.last_used_at = Some(Utc::now().to_rfc3339());
            entry.disabled = true;
            entry.disabled_reason = Some(DisabledReason::InvalidRefreshToken);

            tracing::error!(
                "凭据 #{} refreshToken 已失效 (invalid_grant)，已立即禁用",
                id
            );

            if let Some(next) = entries
                .iter()
                .filter(|e| !e.disabled)
                .min_by_key(|e| e.credentials.priority)
            {
                *current_id = next.id;
                tracing::info!(
                    "已切换到凭据 #{}（优先级 {}）",
                    next.id,
                    next.credentials.priority
                );
                true
            } else {
                tracing::error!("所有凭据均已禁用！");
                false
            }
        };
        self.save_stats_debounced();
        result
    }

    /// 切换到优先级最高的可用凭据
    ///
    /// 返回是否成功切换
    pub fn switch_to_next(&self) -> bool {
        let entries = self.entries.lock();
        let mut current_id = self.current_id.lock();

        // 选择优先级最高的未禁用凭据（排除当前凭据）
        if let Some(next) = entries
            .iter()
            .filter(|e| !e.disabled && e.id != *current_id)
            .min_by_key(|e| e.credentials.priority)
        {
            *current_id = next.id;
            tracing::info!(
                "已切换到凭据 #{}（优先级 {}）",
                next.id,
                next.credentials.priority
            );
            true
        } else {
            // 没有其他可用凭据，检查当前凭据是否可用
            entries.iter().any(|e| e.id == *current_id && !e.disabled)
        }
    }

    // ========================================================================
    // Admin API 方法
    // ========================================================================

    /// 获取管理器状态快照（用于 Admin API）
    pub fn snapshot(&self) -> ManagerSnapshot {
        let dispatch = self.dispatch_config();
        let now = Instant::now();
        let now_epoch_ms = current_epoch_ms();
        let shared_snapshots = if self.shared_dispatch_runtime_enabled() {
            match self.load_shared_dispatch_runtime_snapshots(&dispatch, now_epoch_ms) {
                Ok(snapshots) => Some(snapshots),
                Err(err) => {
                    tracing::warn!("读取共享调度热态快照失败，将回退到本地视图: {}", err);
                    None
                }
            }
        } else {
            None
        };
        let mut entries = self.entries.lock();
        Self::refresh_runtime_state(&mut entries, now);
        let current_id = *self.current_id.lock();
        let available = entries.iter().filter(|e| !e.disabled).count();
        let dispatchable = entries
            .iter()
            .filter(|e| !e.disabled)
            .filter(|e| {
                if let Some(shared_snapshots) = shared_snapshots.as_ref() {
                    let runtime =
                        Self::shared_dispatch_snapshot_for_entry(e, &dispatch, shared_snapshots);
                    runtime
                        .cooldown_until_epoch_ms
                        .map_or(true, |until| until <= now_epoch_ms)
                        && Self::shared_bucket_is_ready(&runtime)
                        && Self::has_capacity(
                            &e.credentials,
                            runtime.active_requests,
                            dispatch.default_max_concurrency,
                        )
                } else {
                    !Self::is_rate_limited(e, now)
                        && Self::bucket_is_ready(e)
                        && Self::has_capacity(
                            &e.credentials,
                            e.active_requests,
                            dispatch.default_max_concurrency,
                        )
                }
            })
            .count();

        ManagerSnapshot {
            entries: entries
                .iter_mut()
                .map(|e| {
                    let shared_runtime = shared_snapshots.as_ref().map(|snapshots| {
                        Self::shared_dispatch_snapshot_for_entry(e, &dispatch, snapshots)
                    });
                    let active_requests = shared_runtime
                        .as_ref()
                        .map(|runtime| runtime.active_requests)
                        .unwrap_or(e.active_requests);
                    let cooldown_remaining_ms = shared_runtime
                        .as_ref()
                        .and_then(|runtime| runtime.cooldown_until_epoch_ms)
                        .map(|until| until.saturating_sub(now_epoch_ms))
                        .filter(|remaining| *remaining > 0)
                        .or_else(|| {
                            e.rate_limit_cooldown_until
                                .and_then(|until| until.checked_duration_since(now))
                                .map(|remaining| {
                                    remaining.as_millis().min(u128::from(u64::MAX)) as u64
                                })
                        });
                    let next_ready_in_ms = shared_runtime
                        .as_ref()
                        .and_then(|runtime| runtime.next_ready_at_epoch_ms)
                        .map(|ready_at| ready_at.saturating_sub(now_epoch_ms))
                        .filter(|remaining| *remaining > 0)
                        .or_else(|| {
                            Self::combined_ready_at(e, now)
                                .and_then(|ready_at| ready_at.checked_duration_since(now))
                                .map(|remaining| {
                                    remaining.as_millis().min(u128::from(u64::MAX)) as u64
                                })
                        });

                    CredentialEntrySnapshot {
                        id: e.id,
                        priority: e.credentials.priority,
                        disabled: e.disabled,
                        failure_count: e.failure_count,
                        auth_method: e.credentials.auth_method.as_deref().map(|m| {
                            if m.eq_ignore_ascii_case("builder-id") || m.eq_ignore_ascii_case("iam")
                            {
                                "idc".to_string()
                            } else {
                                m.to_string()
                            }
                        }),
                        has_profile_arn: e.credentials.profile_arn.is_some(),
                        expires_at: e.credentials.expires_at.clone(),
                        refresh_token_hash: e.credentials.refresh_token.as_deref().map(sha256_hex),
                        email: e.credentials.email.clone(),
                        subscription_title: e.credentials.subscription_title.clone(),
                        account_type: e.credentials.account_type.clone(),
                        allowed_models: e.credentials.allowed_models.clone(),
                        blocked_models: e.credentials.blocked_models.clone(),
                        runtime_model_restrictions: e
                            .credentials
                            .active_runtime_model_restrictions(),
                        imported_at: e.credentials.imported_at.clone(),
                        success_count: e.success_count,
                        last_used_at: e.last_used_at.clone(),
                        active_requests,
                        max_concurrency: e
                            .credentials
                            .effective_max_concurrency_with_default(
                                dispatch.default_max_concurrency,
                            )
                            .and_then(|limit| u32::try_from(limit).ok()),
                        has_proxy: e.credentials.proxy_url.is_some(),
                        proxy_url: e.credentials.proxy_url.clone(),
                        refresh_failure_count: e.refresh_failure_count,
                        disabled_reason: e.disabled_reason.map(|r| {
                            match r {
                                DisabledReason::Manual => "Manual",
                                DisabledReason::TooManyFailures => "TooManyFailures",
                                DisabledReason::TooManyRefreshFailures => "TooManyRefreshFailures",
                                DisabledReason::QuotaExceeded => "QuotaExceeded",
                                DisabledReason::InvalidRefreshToken => "InvalidRefreshToken",
                            }
                            .to_string()
                        }),
                        cooldown_remaining_ms,
                        rate_limit_bucket_tokens: shared_runtime
                            .as_ref()
                            .and_then(|runtime| runtime.bucket_tokens)
                            .or_else(|| e.rate_limit_bucket.as_ref().map(|bucket| bucket.tokens))
                            .map(|tokens| (tokens * 100.0).round() / 100.0),
                        rate_limit_bucket_capacity: shared_runtime
                            .as_ref()
                            .and_then(|runtime| runtime.bucket_capacity)
                            .or_else(|| {
                                e.rate_limit_bucket
                                    .as_ref()
                                    .map(|bucket| bucket.policy.capacity)
                            }),
                        rate_limit_bucket_capacity_override: e
                            .credentials
                            .rate_limit_bucket_capacity_override(),
                        rate_limit_refill_per_second: shared_runtime
                            .as_ref()
                            .and_then(|runtime| runtime.bucket_current_refill_per_second)
                            .or_else(|| {
                                e.rate_limit_bucket
                                    .as_ref()
                                    .map(|bucket| bucket.current_refill_per_second)
                            })
                            .map(|value| (value * 100.0).round() / 100.0),
                        rate_limit_refill_per_second_override: e
                            .credentials
                            .rate_limit_refill_per_second_override(),
                        rate_limit_refill_base_per_second: shared_runtime
                            .as_ref()
                            .and_then(|runtime| runtime.bucket_base_refill_per_second)
                            .or_else(|| {
                                e.rate_limit_bucket
                                    .as_ref()
                                    .map(|bucket| bucket.policy.refill_per_second)
                            }),
                        rate_limit_hit_streak: shared_runtime
                            .as_ref()
                            .map(|runtime| runtime.rate_limit_hit_streak)
                            .unwrap_or(e.rate_limit_hit_streak),
                        next_ready_in_ms,
                    }
                })
                .collect(),
            current_id,
            total: entries.len(),
            available,
            dispatchable,
        }
    }

    /// 设置凭据禁用状态（Admin API）
    pub fn set_disabled(&self, id: u64, disabled: bool) -> anyhow::Result<()> {
        let dispatch = self.dispatch_config();
        let now = Instant::now();
        let _state_write_guard = self.state_write_lock.lock();

        let mut persisted = self.persisted_credentials_snapshot();
        Self::persisted_credential_mut(&mut persisted, id)?.disabled = disabled;
        self.persist_credentials_snapshot(&persisted)?;

        {
            let mut entries = self.entries.lock();
            let entry = entries
                .iter_mut()
                .find(|e| e.id == id)
                .ok_or_else(|| anyhow::anyhow!("凭据不存在: {}", id))?;
            entry.disabled = disabled;
            if !disabled {
                // 启用时重置失败计数
                entry.failure_count = 0;
                entry.refresh_failure_count = 0;
                entry.disabled_reason = None;
                Self::reset_rate_limit_runtime(entry, &dispatch, now);
            } else {
                entry.disabled_reason = Some(DisabledReason::Manual);
            }
        }
        if !disabled {
            if self.shared_dispatch_runtime_enabled() {
                let bucket_policy = {
                    let entries = self.entries.lock();
                    entries
                        .iter()
                        .find(|entry| entry.id == id)
                        .and_then(|entry| dispatch.shared_bucket_policy_for(&entry.credentials))
                };
                if let Err(err) =
                    self.state_store
                        .reset_dispatch_runtime(id, bucket_policy, current_epoch_ms())
                {
                    tracing::warn!("重置共享调度运行态失败（credentialId={}）: {}", id, err);
                }
            }
            self.availability_notify.notify_waiters();
        }
        Ok(())
    }

    /// 设置凭据优先级（Admin API）
    ///
    /// 修改优先级后会立即按新优先级重新选择当前凭据。
    pub fn set_priority(&self, id: u64, priority: u32) -> anyhow::Result<()> {
        let _state_write_guard = self.state_write_lock.lock();
        let mut persisted = self.persisted_credentials_snapshot();
        Self::persisted_credential_mut(&mut persisted, id)?.priority = priority;
        self.persist_credentials_snapshot(&persisted)?;

        {
            let mut entries = self.entries.lock();
            let entry = entries
                .iter_mut()
                .find(|e| e.id == id)
                .ok_or_else(|| anyhow::anyhow!("凭据不存在: {}", id))?;
            entry.credentials.priority = priority;
        }
        self.select_highest_priority();
        Ok(())
    }

    /// 设置凭据并发上限（Admin API）
    pub fn set_max_concurrency(&self, id: u64, max_concurrency: Option<u32>) -> anyhow::Result<()> {
        let normalized = max_concurrency.filter(|limit| *limit > 0);
        let _state_write_guard = self.state_write_lock.lock();
        let mut persisted = self.persisted_credentials_snapshot();
        Self::persisted_credential_mut(&mut persisted, id)?.max_concurrency = normalized;
        self.persist_credentials_snapshot(&persisted)?;

        {
            let mut entries = self.entries.lock();
            let entry = entries
                .iter_mut()
                .find(|e| e.id == id)
                .ok_or_else(|| anyhow::anyhow!("凭据不存在: {}", id))?;
            entry.credentials.max_concurrency = normalized;
        }
        self.availability_notify.notify_waiters();
        Ok(())
    }

    /// 设置凭据级 token bucket 配置（Admin API）
    pub fn set_rate_limit_config(
        &self,
        id: u64,
        rate_limit_bucket_capacity: Option<Option<f64>>,
        rate_limit_refill_per_second: Option<Option<f64>>,
    ) -> anyhow::Result<()> {
        if let Some(Some(value)) = rate_limit_bucket_capacity {
            Self::validate_non_negative_finite("rateLimitBucketCapacity", value)?;
        }
        if let Some(Some(value)) = rate_limit_refill_per_second {
            Self::validate_non_negative_finite("rateLimitRefillPerSecond", value)?;
        }

        let dispatch = self.dispatch_config();
        let now = Instant::now();
        let _state_write_guard = self.state_write_lock.lock();
        let mut persisted = self.persisted_credentials_snapshot();
        let credential = Self::persisted_credential_mut(&mut persisted, id)?;
        if let Some(rate_limit_bucket_capacity) = rate_limit_bucket_capacity {
            credential.rate_limit_bucket_capacity = rate_limit_bucket_capacity;
        }
        if let Some(rate_limit_refill_per_second) = rate_limit_refill_per_second {
            credential.rate_limit_refill_per_second = rate_limit_refill_per_second;
        }
        self.persist_credentials_snapshot(&persisted)?;

        {
            let mut entries = self.entries.lock();
            let entry = entries
                .iter_mut()
                .find(|e| e.id == id)
                .ok_or_else(|| anyhow::anyhow!("凭据不存在: {}", id))?;
            if let Some(rate_limit_bucket_capacity) = rate_limit_bucket_capacity {
                entry.credentials.rate_limit_bucket_capacity = rate_limit_bucket_capacity;
            }
            if let Some(rate_limit_refill_per_second) = rate_limit_refill_per_second {
                entry.credentials.rate_limit_refill_per_second = rate_limit_refill_per_second;
            }
            Self::sync_rate_limit_bucket_runtime(entry, &dispatch, now);
        }

        self.availability_notify.notify_waiters();
        Ok(())
    }

    /// 设置凭据级模型策略（Admin API）
    pub fn set_credential_model_policy(
        &self,
        id: u64,
        account_type: Option<Option<String>>,
        allowed_models: Option<Option<Vec<String>>>,
        blocked_models: Option<Option<Vec<String>>>,
        clear_runtime_model_restrictions: bool,
    ) -> anyhow::Result<()> {
        let _state_write_guard = self.state_write_lock.lock();
        let mut persisted = self.persisted_credentials_snapshot();
        let credential = Self::persisted_credential_mut(&mut persisted, id)?;

        if let Some(account_type) = account_type {
            credential.account_type = account_type
                .as_deref()
                .and_then(crate::model::model_policy::normalize_account_type);
        }
        if let Some(allowed_models) = allowed_models {
            credential.allowed_models = allowed_models
                .map(|models| crate::model::model_policy::normalize_model_entries(&models))
                .unwrap_or_default();
        }
        if let Some(blocked_models) = blocked_models {
            credential.blocked_models = blocked_models
                .map(|models| crate::model::model_policy::normalize_model_entries(&models))
                .unwrap_or_default();
        }
        if clear_runtime_model_restrictions {
            credential.clear_runtime_model_restrictions();
        }
        credential.normalize_model_capabilities();
        let updated_credential = credential.clone();
        self.persist_credentials_snapshot(&persisted)?;

        {
            let mut entries = self.entries.lock();
            let entry = entries
                .iter_mut()
                .find(|e| e.id == id)
                .ok_or_else(|| anyhow::anyhow!("凭据不存在: {}", id))?;
            entry.credentials = updated_credential;
        }

        self.availability_notify.notify_waiters();
        Ok(())
    }

    /// 重置凭据失败计数并重新启用（Admin API）
    pub fn reset_and_enable(&self, id: u64) -> anyhow::Result<()> {
        let dispatch = self.dispatch_config();
        let now = Instant::now();
        let _state_write_guard = self.state_write_lock.lock();
        let mut persisted = self.persisted_credentials_snapshot();
        Self::persisted_credential_mut(&mut persisted, id)?.disabled = false;
        self.persist_credentials_snapshot(&persisted)?;

        {
            let mut entries = self.entries.lock();
            let entry = entries
                .iter_mut()
                .find(|e| e.id == id)
                .ok_or_else(|| anyhow::anyhow!("凭据不存在: {}", id))?;
            entry.failure_count = 0;
            entry.refresh_failure_count = 0;
            entry.disabled = false;
            entry.disabled_reason = None;
            Self::reset_rate_limit_runtime(entry, &dispatch, now);
        }
        if self.shared_dispatch_runtime_enabled() {
            let bucket_policy = {
                let entries = self.entries.lock();
                entries
                    .iter()
                    .find(|entry| entry.id == id)
                    .and_then(|entry| dispatch.shared_bucket_policy_for(&entry.credentials))
            };
            if let Err(err) =
                self.state_store
                    .reset_dispatch_runtime(id, bucket_policy, current_epoch_ms())
            {
                tracing::warn!("重置共享调度运行态失败（credentialId={}）: {}", id, err);
            }
        }
        self.availability_notify.notify_waiters();
        Ok(())
    }

    /// 获取指定凭据的使用额度（Admin API）
    pub async fn get_usage_limits_for(&self, id: u64) -> anyhow::Result<UsageLimitsResponse> {
        let credentials = self.current_credentials(id)?;
        let (credentials, token) = self.try_ensure_token(id, &credentials).await?;

        let effective_proxy = credentials.effective_proxy(self.proxy.as_ref());
        let usage_limits =
            get_usage_limits(&credentials, &self.config, &token, effective_proxy.as_ref()).await?;

        // 更新订阅等级到凭据（仅在发生变化时持久化）
        if let Some(subscription_title) = usage_limits.subscription_title() {
            {
                let _state_write_guard = self.state_write_lock.lock();
                let mut entries = self.entries.lock();
                if let Some(entry) = entries.iter_mut().find(|e| e.id == id) {
                    let old_title = entry.credentials.subscription_title.clone();
                    if old_title.as_deref() != Some(subscription_title) {
                        entry.credentials.subscription_title = Some(subscription_title.to_string());
                        tracing::info!(
                            "凭据 #{} 订阅等级已更新: {:?} -> {}",
                            id,
                            old_title,
                            subscription_title
                        );
                        let credentials = Self::persisted_credentials_from_entries(&entries);
                        if let Err(e) = self.persist_credentials_snapshot(&credentials) {
                            tracing::warn!("订阅等级更新后持久化失败（不影响本次请求）: {}", e);
                        }
                    }
                }
            }
        }

        Ok(usage_limits)
    }

    /// 添加新凭据（Admin API）
    ///
    /// # 流程
    /// 1. 验证凭据基本字段（refresh_token 不为空）
    /// 2. 基于 refreshToken 的 SHA-256 哈希检测重复
    /// 3. 尝试刷新 Token 验证凭据有效性
    /// 4. 分配新 ID（当前最大 ID + 1）
    /// 5. 添加到 entries 列表
    /// 6. 持久化到配置文件
    ///
    /// # 返回
    /// - `Ok(u64)` - 新凭据 ID
    /// - `Err(_)` - 验证失败或添加失败
    pub async fn add_credential(&self, new_cred: KiroCredentials) -> anyhow::Result<u64> {
        // 1. 基本验证
        validate_refresh_token(&new_cred)?;

        // 2. 基于 refreshToken 的 SHA-256 哈希检测重复
        let new_refresh_token = new_cred
            .refresh_token
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("缺少 refreshToken"))?;
        let new_refresh_token_hash = sha256_hex(new_refresh_token);

        // 3. 先做一次本地去重，避免重复凭据还去触发上游刷新校验
        let duplicate_exists = self
            .persisted_credentials_snapshot()
            .iter()
            .any(|credential| {
                credential
                    .refresh_token
                    .as_deref()
                    .map(sha256_hex)
                    .as_deref()
                    == Some(new_refresh_token_hash.as_str())
            });
        if duplicate_exists {
            anyhow::bail!("凭据已存在（refreshToken 重复）");
        }

        // 4. 尝试刷新 Token 验证凭据有效性
        let effective_proxy = new_cred.effective_proxy(self.proxy.as_ref());
        let mut validated_cred =
            refresh_token(&new_cred, &self.config, effective_proxy.as_ref()).await?;

        // 5. 写入前重新检查一次，避免并发添加时插入重复凭据
        let _state_write_guard = self.state_write_lock.lock();
        let mut persisted = self.persisted_credentials_snapshot();
        let duplicate_exists = persisted.iter().any(|credential| {
            credential
                .refresh_token
                .as_deref()
                .map(sha256_hex)
                .as_deref()
                == Some(new_refresh_token_hash.as_str())
        });
        if duplicate_exists {
            anyhow::bail!("凭据已存在（refreshToken 重复）");
        }

        let new_id = persisted
            .iter()
            .filter_map(|credential| credential.id)
            .max()
            .unwrap_or(0)
            + 1;

        validated_cred.id = Some(new_id);
        validated_cred.priority = new_cred.priority;
        validated_cred.auth_method = new_cred.auth_method.map(|m| {
            if m.eq_ignore_ascii_case("builder-id") || m.eq_ignore_ascii_case("iam") {
                "idc".to_string()
            } else {
                m
            }
        });
        validated_cred.client_id = new_cred.client_id;
        validated_cred.client_secret = new_cred.client_secret;
        validated_cred.region = new_cred.region;
        validated_cred.auth_region = new_cred.auth_region;
        validated_cred.api_region = new_cred.api_region;
        validated_cred.machine_id = new_cred.machine_id;
        validated_cred.email = new_cred.email;
        validated_cred.account_type = new_cred.account_type;
        validated_cred.allowed_models = new_cred.allowed_models;
        validated_cred.blocked_models = new_cred.blocked_models;
        validated_cred.runtime_model_restrictions = new_cred.runtime_model_restrictions;
        validated_cred.imported_at = new_cred
            .imported_at
            .or_else(|| Some(Utc::now().to_rfc3339()));
        validated_cred.max_concurrency = new_cred.max_concurrency;
        validated_cred.rate_limit_bucket_capacity = new_cred.rate_limit_bucket_capacity;
        validated_cred.rate_limit_refill_per_second = new_cred.rate_limit_refill_per_second;
        validated_cred.proxy_url = new_cred.proxy_url;
        validated_cred.proxy_username = new_cred.proxy_username;
        validated_cred.proxy_password = new_cred.proxy_password;
        validated_cred.disabled = false;
        validated_cred.normalize_model_capabilities();

        persisted.push(validated_cred.clone());
        self.persist_credentials_snapshot(&persisted)?;

        let dispatch = self.dispatch_config();
        let now = Instant::now();
        let rate_limit_bucket = dispatch
            .bucket_policy_for(&validated_cred)
            .map(|policy| AdaptiveTokenBucket::new(policy, now));
        {
            let mut entries = self.entries.lock();
            entries.push(CredentialEntry {
                id: new_id,
                credentials: validated_cred,
                failure_count: 0,
                refresh_failure_count: 0,
                disabled: false,
                disabled_reason: None,
                success_count: 0,
                pending_success_count_delta: 0,
                last_used_at: None,
                active_requests: 0,
                rate_limit_cooldown_until: None,
                rate_limit_bucket,
                rate_limit_hit_streak: 0,
                refresh_lock: Arc::new(TokioMutex::new(())),
            });
        }

        self.availability_notify.notify_waiters();

        tracing::info!("成功添加凭据 #{}", new_id);
        Ok(new_id)
    }

    /// 删除凭据（Admin API）
    ///
    /// # 前置条件
    /// - 凭据必须已禁用（disabled = true）
    ///
    /// # 行为
    /// 1. 验证凭据存在
    /// 2. 验证凭据已禁用
    /// 3. 从 entries 移除
    /// 4. 如果删除的是当前凭据，切换到优先级最高的可用凭据
    /// 5. 如果删除后没有凭据，将 current_id 重置为 0
    /// 6. 持久化到文件
    ///
    /// # 返回
    /// - `Ok(())` - 删除成功
    /// - `Err(_)` - 凭据不存在、未禁用或持久化失败
    pub fn delete_credential(&self, id: u64) -> anyhow::Result<()> {
        let _state_write_guard = self.state_write_lock.lock();
        let mut persisted = self.persisted_credentials_snapshot();
        let credential = persisted
            .iter()
            .find(|credential| credential.id == Some(id))
            .ok_or_else(|| anyhow::anyhow!("凭据不存在: {}", id))?;
        if !credential.disabled {
            anyhow::bail!("只能删除已禁用的凭据（请先禁用凭据 #{}）", id);
        }
        persisted.retain(|credential| credential.id != Some(id));
        self.persist_credentials_snapshot(&persisted)?;

        let was_current = {
            let mut entries = self.entries.lock();

            // 查找凭据
            let entry = entries
                .iter()
                .find(|e| e.id == id)
                .ok_or_else(|| anyhow::anyhow!("凭据不存在: {}", id))?;

            // 检查是否已禁用
            if !entry.disabled {
                anyhow::bail!("只能删除已禁用的凭据（请先禁用凭据 #{}）", id);
            }

            // 记录是否是当前凭据
            let current_id = *self.current_id.lock();
            let was_current = current_id == id;

            // 删除凭据
            entries.retain(|e| e.id != id);

            was_current
        };

        // 如果删除的是当前凭据，切换到优先级最高的可用凭据
        if was_current {
            self.select_highest_priority();
        }

        // 如果删除后没有任何凭据，将 current_id 重置为 0（与初始化行为保持一致）
        {
            let entries = self.entries.lock();
            if entries.is_empty() {
                let mut current_id = self.current_id.lock();
                *current_id = 0;
                tracing::info!("所有凭据已删除，current_id 已重置为 0");
            }
        }

        // 立即回写统计数据，清除已删除凭据的残留条目
        self.rewrite_stats_snapshot();

        tracing::info!("已删除凭据 #{}", id);
        Ok(())
    }

    /// 强制刷新指定凭据的 Token（Admin API）
    ///
    /// 无条件调用上游 API 重新获取 access token，不检查是否过期。
    /// 适用于排查问题、Token 异常但未过期、主动更新凭据状态等场景。
    pub async fn force_refresh_token_for(&self, id: u64) -> anyhow::Result<()> {
        // 获取凭据级刷新锁，避免跨账号刷新串行阻塞
        let refresh_lock = self.refresh_lock_for(id)?;
        let _guard = refresh_lock.lock().await;

        if self.state_store.is_external() {
            if let Err(err) = self.sync_external_state_if_changed() {
                tracing::warn!("按需同步共享凭据状态失败，将继续使用本地状态: {}", err);
            }
        }

        let credentials = self.current_credentials(id)?;

        if let Some(status) = self.runtime_refresh_coordination_status()? {
            if !status.is_leader {
                if credentials.access_token.is_some() && !is_token_expired(&credentials) {
                    tracing::debug!(
                        "凭据 #{} 已从外部状态同步到有效 Token，跳过本地强制刷新",
                        id
                    );
                    return Ok(());
                }

                return Err(anyhow::Error::new(RuntimeRefreshLeaderRequiredError {
                    instance_id: status.instance_id,
                    leader_id: status.leader_id,
                }));
            }
        }

        // 无条件调用 refresh_token
        let effective_proxy = credentials.effective_proxy(self.proxy.as_ref());
        let new_creds = refresh_token(&credentials, &self.config, effective_proxy.as_ref()).await?;

        let _state_write_guard = self.state_write_lock.lock();
        {
            let mut entries = self.entries.lock();
            if let Some(entry) = entries.iter_mut().find(|e| e.id == id) {
                entry.credentials = new_creds;
                entry.refresh_failure_count = 0;
            }
        }

        let credentials = self.persisted_credentials_snapshot();
        if let Err(e) = self.persist_credentials_snapshot(&credentials) {
            tracing::warn!("强制刷新 Token 后持久化失败: {}", e);
        }

        tracing::info!("凭据 #{} Token 已强制刷新", id);
        Ok(())
    }

    /// 获取调度配置快照（Admin API）
    pub fn load_balancing_config_snapshot(&self) -> LoadBalancingConfigSnapshot {
        let dispatch = self.dispatch_config();
        LoadBalancingConfigSnapshot {
            mode: dispatch.mode,
            queue_max_size: dispatch.queue_max_size,
            queue_max_wait_ms: dispatch.queue_max_wait_ms,
            rate_limit_cooldown_ms: dispatch.rate_limit_cooldown_ms,
            default_max_concurrency: dispatch.default_max_concurrency,
            rate_limit_bucket_capacity: dispatch.rate_limit_bucket_capacity,
            rate_limit_refill_per_second: dispatch.rate_limit_refill_per_second,
            rate_limit_refill_min_per_second: dispatch.rate_limit_refill_min_per_second,
            rate_limit_refill_recovery_step_per_success: dispatch
                .rate_limit_refill_recovery_step_per_success,
            rate_limit_refill_backoff_factor: dispatch.rate_limit_refill_backoff_factor,
            request_weighting: dispatch.request_weighting.clone(),
            waiting_requests: self.queue_depth(),
        }
    }

    pub fn account_type_policies_snapshot(&self) -> BTreeMap<String, ModelSupportPolicy> {
        self.dispatch_config().account_type_policies
    }

    pub fn supports_model(&self, model: &str) -> bool {
        let dispatch = self.dispatch_config();
        let requirement = Self::model_requirement(Some(model));
        let entries = self.entries.lock();
        entries.iter().any(|entry| {
            !entry.disabled
                && Self::is_model_supported(&dispatch, &entry.credentials, Some(model), requirement)
        })
    }

    pub fn request_weighting_config_snapshot(&self) -> RequestWeightingConfig {
        self.dispatch_config().request_weighting
    }

    /// 获取负载均衡模式（Admin API）
    pub fn get_load_balancing_mode(&self) -> String {
        self.dispatch_config().mode
    }

    fn persist_dispatch_config(&self, dispatch: &DispatchConfig) -> anyhow::Result<()> {
        self.state_store
            .persist_dispatch_config(&PersistedDispatchConfig {
                mode: dispatch.mode.clone(),
                queue_max_size: dispatch.queue_max_size,
                queue_max_wait_ms: dispatch.queue_max_wait_ms,
                rate_limit_cooldown_ms: dispatch.rate_limit_cooldown_ms,
                default_max_concurrency: dispatch.default_max_concurrency,
                rate_limit_bucket_capacity: dispatch.rate_limit_bucket_capacity,
                rate_limit_refill_per_second: dispatch.rate_limit_refill_per_second,
                rate_limit_refill_min_per_second: dispatch.rate_limit_refill_min_per_second,
                rate_limit_refill_recovery_step_per_success: dispatch
                    .rate_limit_refill_recovery_step_per_success,
                rate_limit_refill_backoff_factor: dispatch.rate_limit_refill_backoff_factor,
                request_weighting: dispatch.request_weighting.clone(),
                account_type_policies: dispatch.account_type_policies.clone(),
            })?;
        self.try_bump_state_change_revision(StateChangeKind::DispatchConfig);
        Ok(())
    }

    /// 设置负载均衡模式（Admin API）
    pub fn set_load_balancing_mode(&self, mode: String) -> anyhow::Result<()> {
        self.set_load_balancing_config(
            Some(mode),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    }

    pub fn set_account_type_policies(
        &self,
        mut account_type_policies: BTreeMap<String, ModelSupportPolicy>,
    ) -> anyhow::Result<()> {
        crate::model::model_policy::normalize_account_type_policies(&mut account_type_policies);
        let previous = self.dispatch_config();
        let mut next = previous.clone();
        next.account_type_policies = account_type_policies;

        if previous == next {
            return Ok(());
        }

        let _state_write_guard = self.state_write_lock.lock();
        *self.dispatch_config.lock() = next.clone();

        if let Err(err) = self.persist_dispatch_config(&next) {
            *self.dispatch_config.lock() = previous;
            return Err(err);
        }

        self.availability_notify.notify_waiters();
        Ok(())
    }

    /// 设置调度配置（Admin API）
    pub fn set_load_balancing_config(
        &self,
        mode: Option<String>,
        queue_max_size: Option<usize>,
        queue_max_wait_ms: Option<u64>,
        rate_limit_cooldown_ms: Option<u64>,
        default_max_concurrency: Option<u32>,
        rate_limit_bucket_capacity: Option<f64>,
        rate_limit_refill_per_second: Option<f64>,
        rate_limit_refill_min_per_second: Option<f64>,
        rate_limit_refill_recovery_step_per_success: Option<f64>,
        rate_limit_refill_backoff_factor: Option<f64>,
        request_weighting: Option<RequestWeightingConfig>,
    ) -> anyhow::Result<()> {
        let previous = self.dispatch_config();
        let mut next = previous.clone();

        // 验证模式值
        if let Some(mode) = mode {
            if mode != "priority" && mode != "balanced" {
                anyhow::bail!("无效的负载均衡模式: {}", mode);
            }
            next.mode = mode;
        }

        if let Some(queue_max_size) = queue_max_size {
            next.queue_max_size = queue_max_size;
        }
        if let Some(queue_max_wait_ms) = queue_max_wait_ms {
            next.queue_max_wait_ms = queue_max_wait_ms;
        }
        if let Some(rate_limit_cooldown_ms) = rate_limit_cooldown_ms {
            next.rate_limit_cooldown_ms = rate_limit_cooldown_ms;
        }
        if let Some(default_max_concurrency) = default_max_concurrency {
            next.default_max_concurrency = Some(default_max_concurrency).filter(|limit| *limit > 0);
        }
        if let Some(rate_limit_bucket_capacity) = rate_limit_bucket_capacity {
            next.rate_limit_bucket_capacity = rate_limit_bucket_capacity;
        }
        if let Some(rate_limit_refill_per_second) = rate_limit_refill_per_second {
            next.rate_limit_refill_per_second = rate_limit_refill_per_second;
        }
        if let Some(rate_limit_refill_min_per_second) = rate_limit_refill_min_per_second {
            next.rate_limit_refill_min_per_second = rate_limit_refill_min_per_second;
        }
        if let Some(rate_limit_refill_recovery_step_per_success) =
            rate_limit_refill_recovery_step_per_success
        {
            next.rate_limit_refill_recovery_step_per_success =
                rate_limit_refill_recovery_step_per_success;
        }
        if let Some(rate_limit_refill_backoff_factor) = rate_limit_refill_backoff_factor {
            next.rate_limit_refill_backoff_factor = rate_limit_refill_backoff_factor;
        }
        if let Some(request_weighting) = request_weighting {
            next.request_weighting = request_weighting;
        }

        Self::validate_dispatch_rate_limit_config(&next)?;

        if previous == next {
            return Ok(());
        }

        let _state_write_guard = self.state_write_lock.lock();
        *self.dispatch_config.lock() = next.clone();

        if let Err(err) = self.persist_dispatch_config(&next) {
            *self.dispatch_config.lock() = previous;
            return Err(err);
        }

        if previous.rate_limit_cooldown_ms != next.rate_limit_cooldown_ms
            && next.rate_limit_cooldown_ms == 0
        {
            self.clear_all_rate_limit_cooldowns();
        }
        if previous.rate_limit_bucket_capacity != next.rate_limit_bucket_capacity
            || previous.rate_limit_refill_per_second != next.rate_limit_refill_per_second
            || previous.rate_limit_refill_min_per_second != next.rate_limit_refill_min_per_second
            || previous.rate_limit_refill_recovery_step_per_success
                != next.rate_limit_refill_recovery_step_per_success
            || previous.rate_limit_refill_backoff_factor != next.rate_limit_refill_backoff_factor
        {
            self.reconfigure_rate_limit_runtime(&next);
        }

        self.availability_notify.notify_waiters();
        tracing::info!(
            "调度配置已更新: mode={}, queueMaxSize={}, queueMaxWaitMs={}, rateLimitCooldownMs={}, defaultMaxConcurrency={:?}, rateLimitBucketCapacity={}, rateLimitRefillPerSecond={}, rateLimitRefillMinPerSecond={}, rateLimitRefillRecoveryStepPerSuccess={}, rateLimitRefillBackoffFactor={}, requestWeightingEnabled={}, requestWeightingBaseWeight={}, requestWeightingMaxWeight={}",
            next.mode,
            next.queue_max_size,
            next.queue_max_wait_ms,
            next.rate_limit_cooldown_ms,
            next.default_max_concurrency,
            next.rate_limit_bucket_capacity,
            next.rate_limit_refill_per_second,
            next.rate_limit_refill_min_per_second,
            next.rate_limit_refill_recovery_step_per_success,
            next.rate_limit_refill_backoff_factor,
            next.request_weighting.enabled,
            next.request_weighting.base_weight,
            next.request_weighting.max_weight
        );
        Ok(())
    }
}

impl Drop for MultiTokenManager {
    fn drop(&mut self) {
        if self.stats_dirty.load(Ordering::Relaxed) {
            self.save_stats();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};

    fn temp_credentials_path(test_name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "kiro-token-manager-{test_name}-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("credentials.json")
    }

    fn write_credentials_file(path: &Path, credentials: &[KiroCredentials]) {
        std::fs::write(path, serde_json::to_vec_pretty(credentials).unwrap()).unwrap();
    }

    fn available_credential(priority: u32) -> KiroCredentials {
        let mut credentials = KiroCredentials::default();
        credentials.priority = priority;
        credentials.access_token = Some(format!("token-{priority}"));
        credentials.expires_at = Some((Utc::now() + Duration::hours(1)).to_rfc3339());
        credentials
    }

    fn shared_runtime_test_config() -> Option<Config> {
        let redis_url = std::env::var("TEST_REDIS_URL").ok()?;
        let mut config = Config::default();
        config.state_redis_url = Some(redis_url);
        Some(config)
    }

    fn unique_credential_id() -> u64 {
        let bytes = *uuid::Uuid::new_v4().as_bytes();
        u64::from_be_bytes(bytes[..8].try_into().unwrap())
    }

    #[test]
    fn test_is_token_expired_with_expired_token() {
        let mut credentials = KiroCredentials::default();
        credentials.expires_at = Some("2020-01-01T00:00:00Z".to_string());
        assert!(is_token_expired(&credentials));
    }

    #[test]
    fn test_is_token_expired_with_valid_token() {
        let mut credentials = KiroCredentials::default();
        let future = Utc::now() + Duration::hours(1);
        credentials.expires_at = Some(future.to_rfc3339());
        assert!(!is_token_expired(&credentials));
    }

    #[test]
    fn test_is_token_expired_within_5_minutes() {
        let mut credentials = KiroCredentials::default();
        let expires = Utc::now() + Duration::minutes(3);
        credentials.expires_at = Some(expires.to_rfc3339());
        assert!(is_token_expired(&credentials));
    }

    #[test]
    fn test_is_token_expired_no_expires_at() {
        let credentials = KiroCredentials::default();
        assert!(is_token_expired(&credentials));
    }

    #[test]
    fn test_is_token_expiring_soon_within_10_minutes() {
        let mut credentials = KiroCredentials::default();
        let expires = Utc::now() + Duration::minutes(8);
        credentials.expires_at = Some(expires.to_rfc3339());
        assert!(is_token_expiring_soon(&credentials));
    }

    #[test]
    fn test_is_token_expiring_soon_beyond_10_minutes() {
        let mut credentials = KiroCredentials::default();
        let expires = Utc::now() + Duration::minutes(15);
        credentials.expires_at = Some(expires.to_rfc3339());
        assert!(!is_token_expiring_soon(&credentials));
    }

    #[test]
    fn test_validate_refresh_token_missing() {
        let credentials = KiroCredentials::default();
        let result = validate_refresh_token(&credentials);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_refresh_token_valid() {
        let mut credentials = KiroCredentials::default();
        credentials.refresh_token = Some("a".repeat(150));
        let result = validate_refresh_token(&credentials);
        assert!(result.is_ok());
    }

    #[test]
    fn test_sha256_hex() {
        let result = sha256_hex("test");
        assert_eq!(
            result,
            "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
        );
    }

    #[tokio::test]
    async fn test_add_credential_reject_duplicate_refresh_token() {
        let config = Config::default();

        let mut existing = KiroCredentials::default();
        existing.refresh_token = Some("a".repeat(150));

        let manager = MultiTokenManager::new(config, vec![existing], None, None, false).unwrap();

        let mut duplicate = KiroCredentials::default();
        duplicate.refresh_token = Some("a".repeat(150));

        let result = manager.add_credential(duplicate).await;
        assert!(result.is_err());
        assert!(result.err().unwrap().to_string().contains("凭据已存在"));
    }

    // MultiTokenManager 测试

    #[test]
    fn test_multi_token_manager_new() {
        let config = Config::default();
        let mut cred1 = KiroCredentials::default();
        cred1.priority = 0;
        let mut cred2 = KiroCredentials::default();
        cred2.priority = 1;

        let manager =
            MultiTokenManager::new(config, vec![cred1, cred2], None, None, false).unwrap();
        assert_eq!(manager.total_count(), 2);
        assert_eq!(manager.available_count(), 2);
    }

    #[test]
    fn test_multi_token_manager_empty_credentials() {
        let config = Config::default();
        let result = MultiTokenManager::new(config, vec![], None, None, false);
        // 支持 0 个凭据启动（可通过管理面板添加）
        assert!(result.is_ok());
        let manager = result.unwrap();
        assert_eq!(manager.total_count(), 0);
        assert_eq!(manager.available_count(), 0);
    }

    #[test]
    fn test_multi_token_manager_duplicate_ids() {
        let config = Config::default();
        let mut cred1 = KiroCredentials::default();
        cred1.id = Some(1);
        let mut cred2 = KiroCredentials::default();
        cred2.id = Some(1); // 重复 ID

        let result = MultiTokenManager::new(config, vec![cred1, cred2], None, None, false);
        assert!(result.is_err());
        let err_msg = result.err().unwrap().to_string();
        assert!(
            err_msg.contains("重复的凭据 ID"),
            "错误消息应包含 '重复的凭据 ID'，实际: {}",
            err_msg
        );
    }

    #[tokio::test]
    async fn test_account_type_policy_with_credential_blocklist_skips_higher_priority_card() {
        let mut config = Config::default();
        config.account_type_policies.insert(
            "power".to_string(),
            ModelSupportPolicy {
                allowed_models: vec!["claude-opus-4.6".to_string()],
                blocked_models: vec![],
            },
        );

        let mut blocked = available_credential(0);
        blocked.account_type = Some("power".to_string());
        blocked.blocked_models = vec!["claude-opus-4.6".to_string()];

        let mut allowed = available_credential(1);
        allowed.account_type = Some("power".to_string());

        let manager =
            MultiTokenManager::new(config, vec![blocked, allowed], None, None, false).unwrap();

        let ctx = manager
            .acquire_context(Some("claude-opus-4.6"))
            .await
            .unwrap();
        assert_eq!(ctx.id, 2);
    }

    #[test]
    fn test_runtime_model_restriction_hides_model_until_cache_cleared() {
        let mut config = Config::default();
        config.account_type_policies.insert(
            "power".to_string(),
            ModelSupportPolicy {
                allowed_models: vec!["claude-opus-4.6".to_string()],
                blocked_models: vec![],
            },
        );

        let mut cred = available_credential(0);
        cred.account_type = Some("power".to_string());

        let manager = MultiTokenManager::new(config, vec![cred], None, None, false).unwrap();
        assert!(manager.supports_model("claude-opus-4-6"));

        assert!(!manager.defer_model_unsupported_credential(
            1,
            "claude-opus-4.6",
            StdDuration::from_secs(30)
        ));
        assert!(!manager.supports_model("claude-opus-4-6"));

        manager
            .set_credential_model_policy(1, None, None, None, true)
            .unwrap();
        assert!(manager.supports_model("claude-opus-4-6"));
    }

    #[test]
    fn test_multi_token_manager_report_failure() {
        let config = Config::default();
        let cred1 = KiroCredentials::default();
        let cred2 = KiroCredentials::default();

        let manager =
            MultiTokenManager::new(config, vec![cred1, cred2], None, None, false).unwrap();

        // 凭据会自动分配 ID（从 1 开始）
        // 前两次失败不会禁用（使用 ID 1）
        assert!(manager.report_failure(1));
        assert!(manager.report_failure(1));
        assert_eq!(manager.available_count(), 2);

        // 第三次失败会禁用第一个凭据
        assert!(manager.report_failure(1));
        assert_eq!(manager.available_count(), 1);

        // 继续失败第二个凭据（使用 ID 2）
        assert!(manager.report_failure(2));
        assert!(manager.report_failure(2));
        assert!(!manager.report_failure(2)); // 所有凭据都禁用了
        assert_eq!(manager.available_count(), 0);
    }

    #[test]
    fn test_multi_token_manager_report_success() {
        let config = Config::default();
        let cred = KiroCredentials::default();

        let manager = MultiTokenManager::new(config, vec![cred], None, None, false).unwrap();

        // 失败两次（使用 ID 1）
        manager.report_failure(1);
        manager.report_failure(1);

        // 成功后重置计数（使用 ID 1）
        manager.report_success(1);

        // 再失败两次不会禁用
        manager.report_failure(1);
        manager.report_failure(1);
        assert_eq!(manager.available_count(), 1);
    }

    #[test]
    fn test_multi_token_manager_switch_to_next() {
        let config = Config::default();
        let mut cred1 = KiroCredentials::default();
        cred1.refresh_token = Some("token1".to_string());
        let mut cred2 = KiroCredentials::default();
        cred2.refresh_token = Some("token2".to_string());

        let manager =
            MultiTokenManager::new(config, vec![cred1, cred2], None, None, false).unwrap();

        let initial_id = manager.snapshot().current_id;

        // 切换到下一个
        assert!(manager.switch_to_next());
        assert_ne!(manager.snapshot().current_id, initial_id);
    }

    #[test]
    fn test_hot_path_state_sync_slot_throttles_rapid_checks() {
        let mut config = Config::default();
        config.state_hot_path_sync_min_interval_ms = 20;
        let manager =
            MultiTokenManager::new(config, vec![available_credential(0)], None, None, false)
                .unwrap();

        assert!(manager.try_begin_hot_path_state_sync());
        assert!(
            !manager.try_begin_hot_path_state_sync(),
            "连续热路径检查应命中最小间隔限频"
        );

        std::thread::sleep(StdDuration::from_millis(25));
        assert!(manager.try_begin_hot_path_state_sync());
    }

    #[tokio::test]
    async fn test_priority_mode_respects_per_account_concurrency_limit() {
        let config = Config::default();
        let mut primary = available_credential(0);
        primary.max_concurrency = Some(1);
        let secondary = available_credential(1);

        let manager =
            MultiTokenManager::new(config, vec![primary, secondary], None, None, false).unwrap();

        let first = manager.acquire_context(None).await.unwrap();
        assert_eq!(first.id, 1);

        let second = manager.acquire_context(None).await.unwrap();
        assert_eq!(second.id, 2);

        let snapshot = manager.snapshot();
        let first_entry = snapshot.entries.iter().find(|e| e.id == 1).unwrap();
        let second_entry = snapshot.entries.iter().find(|e| e.id == 2).unwrap();
        assert_eq!(first_entry.active_requests, 1);
        assert_eq!(second_entry.active_requests, 1);
    }

    #[tokio::test]
    async fn test_priority_mode_returns_to_highest_priority_after_fallback_recovers() {
        let config = Config::default();
        let mut primary = available_credential(0);
        primary.max_concurrency = Some(1);
        let secondary = available_credential(1);

        let manager =
            MultiTokenManager::new(config, vec![primary, secondary], None, None, false).unwrap();

        let first = manager.acquire_context(None).await.unwrap();
        assert_eq!(first.id, 1);

        let second = manager.acquire_context(None).await.unwrap();
        assert_eq!(second.id, 2);

        drop(first);

        let third = manager.acquire_context(None).await.unwrap();
        assert_eq!(
            third.id, 1,
            "高优先级账号恢复可调度后，应重新优先分配高优先级账号"
        );

        drop(second);
        drop(third);
    }

    #[tokio::test]
    async fn test_priority_mode_skips_free_tier_for_opus_models_case_insensitively() {
        let config = Config::default();
        let mut free = available_credential(0);
        free.subscription_title = Some("KIRO FREE".to_string());
        let mut paid = available_credential(1);
        paid.subscription_title = Some("KIRO PRO+".to_string());

        let manager = MultiTokenManager::new(config, vec![free, paid], None, None, false).unwrap();

        let ctx = manager
            .acquire_context(Some("claude-OPUS-4"))
            .await
            .unwrap();
        assert_eq!(ctx.id, 2);
    }

    #[tokio::test]
    async fn test_priority_mode_deprioritizes_power_tier_for_real_opus_4_7() {
        let config = Config::default();
        let mut power = available_credential(0);
        power.subscription_title = Some("KIRO POWER".to_string());
        let mut pro_plus = available_credential(9);
        pro_plus.subscription_title = Some("KIRO PRO+".to_string());

        let manager =
            MultiTokenManager::new(config, vec![power, pro_plus], None, None, false).unwrap();

        let ctx = manager
            .acquire_context(Some("claude-opus-4.7"))
            .await
            .unwrap();
        assert_eq!(ctx.id, 2);
    }

    #[tokio::test]
    async fn test_real_opus_4_7_allows_power_when_only_power_available() {
        let config = Config::default();
        let mut power = available_credential(0);
        power.subscription_title = Some("KIRO POWER".to_string());

        let manager = MultiTokenManager::new(config, vec![power], None, None, false).unwrap();

        let ctx = manager
            .acquire_context(Some("claude-opus-4.7"))
            .await
            .unwrap();
        assert_eq!(ctx.id, 1);
    }

    #[tokio::test]
    async fn test_balanced_mode_spreads_concurrent_reservations() {
        let mut config = Config::default();
        config.load_balancing_mode = "balanced".to_string();

        let cred1 = available_credential(0);
        let cred2 = available_credential(1);

        let manager =
            MultiTokenManager::new(config, vec![cred1, cred2], None, None, false).unwrap();

        let first = manager.acquire_context(None).await.unwrap();
        let second = manager.acquire_context(None).await.unwrap();

        assert_eq!(first.id, 1);
        assert_eq!(second.id, 2);

        let snapshot = manager.snapshot();
        let first_entry = snapshot.entries.iter().find(|e| e.id == 1).unwrap();
        let second_entry = snapshot.entries.iter().find(|e| e.id == 2).unwrap();
        assert_eq!(first_entry.active_requests, 1);
        assert_eq!(second_entry.active_requests, 1);
    }

    #[tokio::test]
    async fn test_acquire_context_returns_error_when_all_credentials_at_capacity() {
        let config = Config::default();
        let mut cred = available_credential(0);
        cred.max_concurrency = Some(1);

        let manager = MultiTokenManager::new(config, vec![cred], None, None, false).unwrap();

        let _ctx = manager.acquire_context(None).await.unwrap();
        let err = manager
            .acquire_context(None)
            .await
            .err()
            .unwrap()
            .to_string();
        assert!(
            err.contains("并发上限"),
            "错误应提示并发上限，实际: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_acquire_context_with_weight_distinguishes_heavy_and_light_requests() {
        let mut config = Config::default();
        config.rate_limit_cooldown_ms = 0;
        config.rate_limit_bucket_capacity = 3.0;
        config.rate_limit_refill_per_second = 0.2;
        config.rate_limit_refill_min_per_second = 0.2;
        config.rate_limit_refill_recovery_step_per_success = 0.0;
        config.rate_limit_refill_backoff_factor = 0.5;

        let credential = available_credential(0);
        let manager = MultiTokenManager::new(config, vec![credential], None, None, false).unwrap();

        let heavy = manager
            .acquire_context_with_weight(None, 2.0)
            .await
            .unwrap();
        drop(heavy);

        let err = manager
            .acquire_context_with_weight(None, 2.0)
            .await
            .err()
            .unwrap()
            .to_string();
        assert!(
            err.contains("token bucket") || err.contains("等待可用凭据"),
            "重请求应被 bucket 阻塞，实际: {}",
            err
        );

        let light = manager
            .acquire_context_with_weight(None, 1.0)
            .await
            .unwrap();
        assert_eq!(light.id, 1);
    }

    #[tokio::test]
    async fn test_shared_dispatch_runtime_enforces_global_max_concurrency_when_test_redis_is_set() {
        let Some(config) = shared_runtime_test_config() else {
            return;
        };

        let credential_id = unique_credential_id();
        let mut credential = available_credential(0);
        credential.id = Some(credential_id);
        credential.max_concurrency = Some(1);

        let manager_a =
            MultiTokenManager::new(config.clone(), vec![credential.clone()], None, None, false)
                .unwrap();
        let manager_b =
            MultiTokenManager::new(config, vec![credential], None, None, false).unwrap();

        let first = manager_a.acquire_context(None).await.unwrap();
        assert_eq!(first.id, credential_id);

        let err = manager_b
            .acquire_context(None)
            .await
            .err()
            .unwrap()
            .to_string();
        assert!(
            err.contains("并发上限"),
            "共享调度热态应阻止跨 manager 超出全局并发上限，实际: {}",
            err
        );

        let snapshot = manager_b.snapshot();
        let entry = snapshot
            .entries
            .iter()
            .find(|entry| entry.id == credential_id)
            .unwrap();
        assert_eq!(entry.active_requests, 1);

        drop(first);

        let second = manager_b.acquire_context(None).await.unwrap();
        assert_eq!(second.id, credential_id);
    }

    #[tokio::test]
    async fn test_shared_dispatch_runtime_shares_rate_limit_bucket_when_test_redis_is_set() {
        let Some(mut config) = shared_runtime_test_config() else {
            return;
        };
        config.rate_limit_cooldown_ms = 0;
        config.rate_limit_bucket_capacity = 1.0;
        config.rate_limit_refill_per_second = 0.2;
        config.rate_limit_refill_min_per_second = 0.2;
        config.rate_limit_refill_recovery_step_per_success = 0.0;
        config.rate_limit_refill_backoff_factor = 0.5;

        let credential_id = unique_credential_id();
        let mut credential = available_credential(0);
        credential.id = Some(credential_id);

        let manager_a =
            MultiTokenManager::new(config.clone(), vec![credential.clone()], None, None, false)
                .unwrap();
        let manager_b =
            MultiTokenManager::new(config, vec![credential], None, None, false).unwrap();

        let first = manager_a.acquire_context(None).await.unwrap();
        assert_eq!(first.id, credential_id);
        drop(first);

        let err = manager_b
            .acquire_context(None)
            .await
            .err()
            .unwrap()
            .to_string();
        assert!(
            err.contains("token bucket") || err.contains("等待可用凭据"),
            "跨 manager 应共享 bucket 消耗，实际: {}",
            err
        );

        let snapshot = manager_b.snapshot();
        let entry = snapshot
            .entries
            .iter()
            .find(|entry| entry.id == credential_id)
            .unwrap();
        assert!(
            entry.rate_limit_bucket_tokens.unwrap_or_default() < 1.0,
            "共享 bucket 应在其他 manager 中可见"
        );
        assert!(
            entry.next_ready_in_ms.unwrap_or_default() > 0,
            "共享 bucket 不可用时应暴露下次可调度时间"
        );
    }

    #[tokio::test]
    async fn test_shared_dispatch_runtime_respects_weighted_bucket_when_test_redis_is_set() {
        let Some(mut config) = shared_runtime_test_config() else {
            return;
        };
        config.rate_limit_cooldown_ms = 0;
        config.rate_limit_bucket_capacity = 3.0;
        config.rate_limit_refill_per_second = 0.2;
        config.rate_limit_refill_min_per_second = 0.2;
        config.rate_limit_refill_recovery_step_per_success = 0.0;
        config.rate_limit_refill_backoff_factor = 0.5;

        let credential_id = unique_credential_id();
        let mut credential = available_credential(0);
        credential.id = Some(credential_id);

        let manager_a =
            MultiTokenManager::new(config.clone(), vec![credential.clone()], None, None, false)
                .unwrap();
        let manager_b =
            MultiTokenManager::new(config, vec![credential], None, None, false).unwrap();

        let heavy = manager_a
            .acquire_context_with_weight(None, 2.0)
            .await
            .unwrap();
        assert_eq!(heavy.id, credential_id);
        drop(heavy);

        let err = manager_b
            .acquire_context_with_weight(None, 2.0)
            .await
            .err()
            .unwrap()
            .to_string();
        assert!(
            err.contains("token bucket") || err.contains("等待可用凭据"),
            "跨 manager 的重请求应看到共享 bucket 剩余不足，实际: {}",
            err
        );

        let light = manager_b
            .acquire_context_with_weight(None, 1.0)
            .await
            .unwrap();
        assert_eq!(light.id, credential_id);
    }

    #[tokio::test]
    async fn test_shared_dispatch_runtime_shares_rate_limit_cooldown_when_test_redis_is_set() {
        let Some(mut config) = shared_runtime_test_config() else {
            return;
        };
        config.rate_limit_cooldown_ms = 2_000;

        let primary_id = unique_credential_id();
        let secondary_id = unique_credential_id();

        let mut primary = available_credential(0);
        primary.id = Some(primary_id);
        let mut secondary = available_credential(1);
        secondary.id = Some(secondary_id);

        let manager_a = MultiTokenManager::new(
            config.clone(),
            vec![primary.clone(), secondary.clone()],
            None,
            None,
            false,
        )
        .unwrap();
        let manager_b =
            MultiTokenManager::new(config, vec![primary, secondary], None, None, false).unwrap();

        manager_a.report_rate_limited(primary_id);

        let fallback = manager_b.acquire_context(None).await.unwrap();
        assert_eq!(fallback.id, secondary_id);

        let snapshot = manager_b.snapshot();
        let primary_entry = snapshot
            .entries
            .iter()
            .find(|entry| entry.id == primary_id)
            .unwrap();
        assert_eq!(primary_entry.rate_limit_hit_streak, 1);
        assert!(
            primary_entry.cooldown_remaining_ms.unwrap_or_default() > 0,
            "共享 429 冷却应反映到其他 manager 的快照"
        );
    }

    #[tokio::test]
    async fn test_acquire_context_waits_for_capacity_when_queue_enabled() {
        let mut config = Config::default();
        config.queue_max_size = 1;
        config.queue_max_wait_ms = 200;

        let mut cred = available_credential(0);
        cred.max_concurrency = Some(1);

        let manager =
            Arc::new(MultiTokenManager::new(config, vec![cred], None, None, false).unwrap());

        let first = manager.acquire_context(None).await.unwrap();
        let waiter_manager = Arc::clone(&manager);
        let waiter =
            tokio::spawn(async move { waiter_manager.acquire_context(None).await.unwrap() });

        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
        assert!(!waiter.is_finished(), "第二个请求应进入等待队列");

        drop(first);
        let second = waiter.await.unwrap();
        assert_eq!(second.id, 1);
    }

    #[tokio::test]
    async fn test_acquire_context_returns_error_when_wait_queue_is_full() {
        let mut config = Config::default();
        config.queue_max_size = 1;
        config.queue_max_wait_ms = 200;

        let mut cred = available_credential(0);
        cred.max_concurrency = Some(1);

        let manager =
            Arc::new(MultiTokenManager::new(config, vec![cred], None, None, false).unwrap());

        let first = manager.acquire_context(None).await.unwrap();
        let waiter_manager = Arc::clone(&manager);
        let waiter = tokio::spawn(async move { waiter_manager.acquire_context(None).await });

        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;

        let err = manager
            .acquire_context(None)
            .await
            .err()
            .unwrap()
            .to_string();
        assert!(
            err.contains("等待队列已满"),
            "错误应提示等待队列已满，实际: {}",
            err
        );

        drop(first);
        waiter.abort();
    }

    #[tokio::test]
    async fn test_acquire_context_returns_error_when_queue_wait_times_out() {
        let mut config = Config::default();
        config.queue_max_size = 1;
        config.queue_max_wait_ms = 50;

        let mut cred = available_credential(0);
        cred.max_concurrency = Some(1);

        let manager = MultiTokenManager::new(config, vec![cred], None, None, false).unwrap();

        let _ctx = manager.acquire_context(None).await.unwrap();
        let err = manager
            .acquire_context(None)
            .await
            .err()
            .unwrap()
            .to_string();
        assert!(
            err.contains("等待可用凭据超时"),
            "错误应提示等待超时，实际: {}",
            err
        );
    }

    #[tokio::test]
    async fn test_rate_limited_credential_enters_cooldown_and_falls_back() {
        let mut config = Config::default();
        config.rate_limit_cooldown_ms = 3_500;
        let primary = available_credential(0);
        let secondary = available_credential(1);

        let manager =
            MultiTokenManager::new(config, vec![primary, secondary], None, None, false).unwrap();

        manager.report_rate_limited(1);
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        manager.report_rate_limited(1);

        let ctx = manager.acquire_context(None).await.unwrap();
        assert_eq!(ctx.id, 2);

        let snapshot = manager.snapshot();
        let primary_entry = snapshot.entries.iter().find(|entry| entry.id == 1).unwrap();
        assert_eq!(primary_entry.rate_limit_hit_streak, 2);
        assert!(
            primary_entry.cooldown_remaining_ms.unwrap_or_default() > 0,
            "主账号应处于限流冷却中"
        );
        assert!(
            primary_entry.cooldown_remaining_ms.unwrap_or_default() <= 3_500,
            "固定冷却时间不应因连续 429 被额外放大"
        );
    }

    #[test]
    fn test_set_rate_limit_config_updates_only_requested_fields_and_preserves_cooldown() {
        let mut config = Config::default();
        config.rate_limit_cooldown_ms = 4_000;

        let mut cred = available_credential(0);
        cred.rate_limit_bucket_capacity = Some(5.0);
        cred.rate_limit_refill_per_second = Some(0.8);

        let manager = MultiTokenManager::new(config, vec![cred], None, None, false).unwrap();

        manager.report_rate_limited(1);
        manager
            .set_rate_limit_config(1, Some(Some(7.0)), None)
            .unwrap();

        let snapshot = manager.snapshot();
        let entry = snapshot.entries.iter().find(|entry| entry.id == 1).unwrap();
        assert_eq!(entry.rate_limit_bucket_capacity_override, Some(7.0));
        assert_eq!(entry.rate_limit_refill_per_second_override, Some(0.8));
        assert_eq!(entry.rate_limit_bucket_capacity, Some(7.0));
        assert_eq!(entry.rate_limit_hit_streak, 1);
        assert!(
            entry.cooldown_remaining_ms.unwrap_or_default() > 0,
            "修改 bucket 参数不应清空现有 429 冷却"
        );
    }

    #[test]
    fn test_set_disabled_does_not_mutate_memory_when_persist_fails() {
        let credentials_dir = std::env::temp_dir().join(format!(
            "kiro-set-disabled-persist-fail-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&credentials_dir).unwrap();

        let mut credential = available_credential(0);
        credential.id = Some(1);
        credential.machine_id = Some("machine-1".to_string());

        let manager = MultiTokenManager::new(
            Config::default(),
            vec![credential],
            None,
            Some(credentials_dir.clone()),
            true,
        )
        .unwrap();

        let err = manager.set_disabled(1, true).unwrap_err().to_string();
        assert!(
            err.contains("写入状态文件失败"),
            "错误应来自持久化失败，实际: {}",
            err
        );

        let snapshot = manager.snapshot();
        let entry = snapshot.entries.iter().find(|entry| entry.id == 1).unwrap();
        assert!(!entry.disabled, "持久化失败后内存状态不应提前变为 disabled");

        std::fs::remove_dir_all(credentials_dir).unwrap();
    }

    #[test]
    fn test_reload_credentials_from_state_merges_updates_and_preserves_active_entries() {
        let credentials_path = temp_credentials_path("reload-state-merge");

        let mut primary = available_credential(0);
        primary.id = Some(1);
        primary.machine_id = Some("machine-1".to_string());
        primary.priority = 5;

        let mut in_flight = available_credential(1);
        in_flight.id = Some(2);
        in_flight.machine_id = Some("machine-2".to_string());
        in_flight.priority = 9;

        write_credentials_file(&credentials_path, &[primary.clone(), in_flight.clone()]);

        let manager = MultiTokenManager::new(
            Config::default(),
            vec![primary.clone(), in_flight.clone()],
            None,
            Some(credentials_path.clone()),
            true,
        )
        .unwrap();

        {
            let mut entries = manager.entries.lock();
            let retained = entries.iter_mut().find(|entry| entry.id == 2).unwrap();
            retained.active_requests = 1;
            retained.failure_count = 2;
        }

        let mut updated_primary = primary.clone();
        updated_primary.priority = 3;
        updated_primary.access_token = Some("token-updated".to_string());
        updated_primary.expires_at = Some((Utc::now() + Duration::hours(2)).to_rfc3339());
        updated_primary.disabled = true;

        let mut added = available_credential(0);
        added.id = Some(3);
        added.machine_id = Some("machine-3".to_string());
        added.priority = 1;

        write_credentials_file(&credentials_path, &[updated_primary.clone(), added.clone()]);

        manager.reload_credentials_from_state().unwrap();

        let snapshot = manager.snapshot();
        assert_eq!(snapshot.entries.len(), 3);
        assert_eq!(snapshot.current_id, 3);

        let primary_entry = snapshot.entries.iter().find(|entry| entry.id == 1).unwrap();
        assert_eq!(primary_entry.priority, 3);
        assert_eq!(primary_entry.expires_at, updated_primary.expires_at);
        assert!(primary_entry.disabled);
        assert_eq!(primary_entry.disabled_reason.as_deref(), Some("Manual"));
        assert_eq!(
            manager
                .current_credentials(1)
                .unwrap()
                .access_token
                .as_deref(),
            Some("token-updated")
        );

        let retained_entry = snapshot.entries.iter().find(|entry| entry.id == 2).unwrap();
        assert!(retained_entry.disabled);
        assert_eq!(retained_entry.disabled_reason.as_deref(), Some("Manual"));
        assert_eq!(retained_entry.active_requests, 1);
        assert_eq!(retained_entry.failure_count, 2);

        let added_entry = snapshot.entries.iter().find(|entry| entry.id == 3).unwrap();
        assert_eq!(added_entry.priority, 1);
        assert!(!added_entry.disabled);
        assert_eq!(added_entry.expires_at, added.expires_at);

        {
            let mut entries = manager.entries.lock();
            let retained = entries.iter_mut().find(|entry| entry.id == 2).unwrap();
            retained.active_requests = 0;
        }

        manager.reload_credentials_from_state().unwrap();

        let snapshot = manager.snapshot();
        assert_eq!(snapshot.entries.len(), 2);
        assert!(snapshot.entries.iter().all(|entry| entry.id != 2));

        std::fs::remove_file(credentials_path).unwrap();
    }

    #[test]
    fn test_reload_dispatch_config_from_state_updates_runtime_config() {
        let config = Config::default();
        let manager =
            MultiTokenManager::new(config, vec![available_credential(0)], None, None, false)
                .unwrap();

        let persisted = PersistedDispatchConfig {
            mode: "balanced".to_string(),
            queue_max_size: 8,
            queue_max_wait_ms: 1500,
            rate_limit_cooldown_ms: 4500,
            default_max_concurrency: Some(3),
            rate_limit_bucket_capacity: 4.0,
            rate_limit_refill_per_second: 1.2,
            rate_limit_refill_min_per_second: 0.3,
            rate_limit_refill_recovery_step_per_success: 0.15,
            rate_limit_refill_backoff_factor: 0.6,
            request_weighting: RequestWeightingConfig {
                max_weight: 4.0,
                tools_bonus: 1.0,
                ..RequestWeightingConfig::default()
            },
            account_type_policies: BTreeMap::new(),
        };

        assert!(manager.apply_dispatch_config_from_state(&persisted));

        let snapshot = manager.load_balancing_config_snapshot();
        assert_eq!(snapshot.mode, "balanced");
        assert_eq!(snapshot.queue_max_size, 8);
        assert_eq!(snapshot.queue_max_wait_ms, 1500);
        assert_eq!(snapshot.rate_limit_cooldown_ms, 4500);
        assert_eq!(snapshot.default_max_concurrency, Some(3));
        assert_eq!(snapshot.rate_limit_bucket_capacity, 4.0);
        assert_eq!(snapshot.rate_limit_refill_per_second, 1.2);
        assert_eq!(snapshot.rate_limit_refill_min_per_second, 0.3);
        assert_eq!(snapshot.rate_limit_refill_recovery_step_per_success, 0.15);
        assert_eq!(snapshot.rate_limit_refill_backoff_factor, 0.6);
        assert_eq!(snapshot.request_weighting.max_weight, 4.0);
        assert_eq!(snapshot.request_weighting.tools_bonus, 1.0);

        assert!(!manager.apply_dispatch_config_from_state(&persisted));
    }

    #[test]
    fn test_save_stats_merges_success_counts_across_managers() {
        let credentials_path = temp_credentials_path("merge-success-counts");
        let mut credential = available_credential(0);
        credential.id = Some(1);
        credential.machine_id = Some("machine-1".to_string());

        let manager_a = MultiTokenManager::new(
            Config::default(),
            vec![credential.clone()],
            None,
            Some(credentials_path.clone()),
            false,
        )
        .unwrap();
        let manager_b = MultiTokenManager::new(
            Config::default(),
            vec![credential],
            None,
            Some(credentials_path.clone()),
            false,
        )
        .unwrap();

        manager_a.report_success(1);
        manager_b.report_success(1);

        drop(manager_a);
        drop(manager_b);

        let store = StateStore::file(None, Some(credentials_path.clone()));
        let stats = store.load_stats().unwrap();
        assert_eq!(stats.get("1").unwrap().success_count, 2);

        let stats_path = credentials_path.parent().unwrap().join("kiro_stats.json");
        std::fs::remove_file(stats_path).unwrap();
    }

    #[test]
    fn test_reload_stats_from_state_preserves_pending_local_deltas() {
        let credentials_path = temp_credentials_path("reload-stats-preserve-local");
        let mut credential = available_credential(0);
        credential.id = Some(1);
        credential.machine_id = Some("machine-1".to_string());

        let manager = MultiTokenManager::new(
            Config::default(),
            vec![credential],
            None,
            Some(credentials_path.clone()),
            false,
        )
        .unwrap();

        {
            let mut entries = manager.entries.lock();
            let entry = entries.iter_mut().find(|entry| entry.id == 1).unwrap();
            entry.success_count = 2;
            entry.pending_success_count_delta = 2;
            entry.last_used_at = Some("2026-04-15T02:00:00Z".to_string());
        }

        let store = StateStore::file(None, Some(credentials_path.clone()));
        let mut persisted = HashMap::new();
        persisted.insert(
            "1".to_string(),
            StatsEntryRecord {
                success_count: 5,
                last_used_at: Some("2026-04-15T01:00:00Z".to_string()),
            },
        );
        store.save_stats(&persisted).unwrap();

        assert!(manager.reload_stats_from_state().unwrap());

        let snapshot = manager.snapshot();
        let entry = snapshot.entries.iter().find(|entry| entry.id == 1).unwrap();
        assert_eq!(entry.success_count, 7);
        assert_eq!(entry.last_used_at.as_deref(), Some("2026-04-15T02:00:00Z"));

        let stats_path = credentials_path.parent().unwrap().join("kiro_stats.json");
        std::fs::remove_file(stats_path).unwrap();
    }

    #[test]
    fn test_set_load_balancing_config_rejects_min_refill_above_base_refill() {
        let manager = MultiTokenManager::new(
            Config::default(),
            vec![available_credential(0)],
            None,
            None,
            false,
        )
        .unwrap();

        let err = manager
            .set_load_balancing_config(
                None,
                None,
                None,
                None,
                None,
                None,
                Some(1.0),
                Some(1.2),
                None,
                None,
                None,
            )
            .unwrap_err()
            .to_string();

        assert!(
            err.contains("rateLimitRefillMinPerSecond 不能大于 rateLimitRefillPerSecond"),
            "错误应明确提示最小回填速率不能大于基础回填速率，实际: {}",
            err
        );
    }

    #[test]
    fn test_set_load_balancing_mode_persists_to_config_file() {
        let config_path =
            std::env::temp_dir().join(format!("kiro-load-balancing-{}.json", uuid::Uuid::new_v4()));
        std::fs::write(
            &config_path,
            r#"{"loadBalancingMode":"priority","queueMaxSize":0,"queueMaxWaitMs":0,"rateLimitCooldownMs":2000,"defaultMaxConcurrency":2}"#,
        )
        .unwrap();

        let config = Config::load(&config_path).unwrap();
        let manager =
            MultiTokenManager::new(config, vec![KiroCredentials::default()], None, None, false)
                .unwrap();

        manager
            .set_load_balancing_config(
                Some("balanced".to_string()),
                Some(8),
                Some(1500),
                Some(4500),
                Some(3),
                Some(4.0),
                Some(1.2),
                Some(0.3),
                Some(0.15),
                Some(0.6),
                Some(RequestWeightingConfig {
                    max_weight: 4.0,
                    tools_bonus: 1.0,
                    ..RequestWeightingConfig::default()
                }),
            )
            .unwrap();

        let persisted = Config::load(&config_path).unwrap();
        assert_eq!(persisted.load_balancing_mode, "balanced");
        assert_eq!(persisted.queue_max_size, 8);
        assert_eq!(persisted.queue_max_wait_ms, 1500);
        assert_eq!(persisted.rate_limit_cooldown_ms, 4500);
        assert_eq!(persisted.default_max_concurrency, Some(3));
        assert_eq!(persisted.rate_limit_bucket_capacity, 4.0);
        assert_eq!(persisted.rate_limit_refill_per_second, 1.2);
        assert_eq!(persisted.rate_limit_refill_min_per_second, 0.3);
        assert_eq!(persisted.rate_limit_refill_recovery_step_per_success, 0.15);
        assert_eq!(persisted.rate_limit_refill_backoff_factor, 0.6);
        assert_eq!(persisted.request_weighting.max_weight, 4.0);
        assert_eq!(persisted.request_weighting.tools_bonus, 1.0);
        assert_eq!(manager.get_load_balancing_mode(), "balanced");

        std::fs::remove_file(&config_path).unwrap();
    }

    #[tokio::test]
    async fn test_priority_mode_uses_default_max_concurrency_when_credential_has_no_override() {
        let mut config = Config::default();
        config.default_max_concurrency = Some(1);

        let primary = available_credential(0);
        let secondary = available_credential(1);

        let manager =
            MultiTokenManager::new(config, vec![primary, secondary], None, None, false).unwrap();

        let first = manager.acquire_context(None).await.unwrap();
        assert_eq!(first.id, 1);

        let second = manager.acquire_context(None).await.unwrap();
        assert_eq!(second.id, 2);

        let snapshot = manager.snapshot();
        let first_entry = snapshot.entries.iter().find(|e| e.id == 1).unwrap();
        let second_entry = snapshot.entries.iter().find(|e| e.id == 2).unwrap();
        assert_eq!(first_entry.active_requests, 1);
        assert_eq!(second_entry.active_requests, 1);
    }

    #[tokio::test]
    async fn test_multi_token_manager_acquire_context_auto_recovers_all_disabled() {
        let config = Config::default();
        let mut cred1 = KiroCredentials::default();
        cred1.access_token = Some("t1".to_string());
        cred1.expires_at = Some((Utc::now() + Duration::hours(1)).to_rfc3339());
        let mut cred2 = KiroCredentials::default();
        cred2.access_token = Some("t2".to_string());
        cred2.expires_at = Some((Utc::now() + Duration::hours(1)).to_rfc3339());

        let manager =
            MultiTokenManager::new(config, vec![cred1, cred2], None, None, false).unwrap();

        // 凭据会自动分配 ID（从 1 开始）
        for _ in 0..MAX_FAILURES_PER_CREDENTIAL {
            manager.report_failure(1);
        }
        for _ in 0..MAX_FAILURES_PER_CREDENTIAL {
            manager.report_failure(2);
        }

        assert_eq!(manager.available_count(), 0);

        // 应触发自愈：重置失败计数并重新启用，避免必须重启进程
        let ctx = manager.acquire_context(None).await.unwrap();
        assert!(ctx.token == "t1" || ctx.token == "t2");
        assert_eq!(manager.available_count(), 2);
    }

    #[tokio::test]
    async fn test_multi_token_manager_acquire_context_balanced_retries_until_bad_credential_disabled()
     {
        let mut config = Config::default();
        config.load_balancing_mode = "balanced".to_string();

        let mut bad_cred = KiroCredentials::default();
        bad_cred.priority = 0;
        bad_cred.refresh_token = Some("bad".to_string());

        let mut good_cred = KiroCredentials::default();
        good_cred.priority = 1;
        good_cred.access_token = Some("good-token".to_string());
        good_cred.expires_at = Some((Utc::now() + Duration::hours(1)).to_rfc3339());

        let manager =
            MultiTokenManager::new(config, vec![bad_cred, good_cred], None, None, false).unwrap();

        let ctx = manager.acquire_context(None).await.unwrap();
        assert_eq!(ctx.id, 2);
        assert_eq!(ctx.token, "good-token");
    }

    #[test]
    fn test_multi_token_manager_report_refresh_failure() {
        let config = Config::default();
        let cred1 = KiroCredentials::default();
        let cred2 = KiroCredentials::default();

        let manager =
            MultiTokenManager::new(config, vec![cred1, cred2], None, None, false).unwrap();

        assert_eq!(manager.available_count(), 2);
        for _ in 0..(MAX_FAILURES_PER_CREDENTIAL - 1) {
            assert!(manager.report_refresh_failure(1));
        }
        assert_eq!(manager.available_count(), 2);

        assert!(manager.report_refresh_failure(1));
        assert_eq!(manager.available_count(), 1);

        let snapshot = manager.snapshot();
        let first = snapshot.entries.iter().find(|e| e.id == 1).unwrap();
        assert!(first.disabled);
        assert_eq!(first.refresh_failure_count, MAX_FAILURES_PER_CREDENTIAL);
        assert_eq!(snapshot.current_id, 2);
    }

    #[tokio::test]
    async fn test_multi_token_manager_refresh_failure_disabled_is_not_auto_recovered() {
        let config = Config::default();
        let cred1 = KiroCredentials::default();
        let cred2 = KiroCredentials::default();

        let manager =
            MultiTokenManager::new(config, vec![cred1, cred2], None, None, false).unwrap();

        for _ in 0..MAX_FAILURES_PER_CREDENTIAL {
            manager.report_refresh_failure(1);
            manager.report_refresh_failure(2);
        }
        assert_eq!(manager.available_count(), 0);

        let err = manager
            .acquire_context(None)
            .await
            .err()
            .unwrap()
            .to_string();
        assert!(
            err.contains("所有凭据均已禁用"),
            "错误应提示所有凭据禁用，实际: {}",
            err
        );
    }

    #[test]
    fn test_multi_token_manager_report_quota_exhausted() {
        let config = Config::default();
        let cred1 = KiroCredentials::default();
        let cred2 = KiroCredentials::default();

        let manager =
            MultiTokenManager::new(config, vec![cred1, cred2], None, None, false).unwrap();

        // 凭据会自动分配 ID（从 1 开始）
        assert_eq!(manager.available_count(), 2);
        assert!(manager.report_quota_exhausted(1));
        assert_eq!(manager.available_count(), 1);

        // 再禁用第二个后，无可用凭据
        assert!(!manager.report_quota_exhausted(2));
        assert_eq!(manager.available_count(), 0);
    }

    #[tokio::test]
    async fn test_multi_token_manager_quota_disabled_is_not_auto_recovered() {
        let config = Config::default();
        let cred1 = KiroCredentials::default();
        let cred2 = KiroCredentials::default();

        let manager =
            MultiTokenManager::new(config, vec![cred1, cred2], None, None, false).unwrap();

        manager.report_quota_exhausted(1);
        manager.report_quota_exhausted(2);
        assert_eq!(manager.available_count(), 0);

        let err = manager
            .acquire_context(None)
            .await
            .err()
            .unwrap()
            .to_string();
        assert!(
            err.contains("所有凭据均已禁用"),
            "错误应提示所有凭据禁用，实际: {}",
            err
        );
        assert_eq!(manager.available_count(), 0);
    }

    // ============ 凭据级 Region 优先级测试 ============

    #[test]
    fn test_credential_region_priority_uses_credential_auth_region() {
        // 凭据配置了 auth_region 时，应使用凭据的 auth_region
        let mut config = Config::default();
        config.region = "us-west-2".to_string();

        let mut credentials = KiroCredentials::default();
        credentials.auth_region = Some("eu-west-1".to_string());

        let region = credentials.effective_auth_region(&config);
        assert_eq!(region, "eu-west-1");
    }

    #[test]
    fn test_credential_region_priority_fallback_to_credential_region() {
        // 凭据未配置 auth_region 但配置了 region 时，应回退到凭据.region
        let mut config = Config::default();
        config.region = "us-west-2".to_string();

        let mut credentials = KiroCredentials::default();
        credentials.region = Some("eu-central-1".to_string());

        let region = credentials.effective_auth_region(&config);
        assert_eq!(region, "eu-central-1");
    }

    #[test]
    fn test_credential_region_priority_fallback_to_config() {
        // 凭据未配置 auth_region 和 region 时，应回退到 config
        let mut config = Config::default();
        config.region = "us-west-2".to_string();

        let credentials = KiroCredentials::default();
        assert!(credentials.auth_region.is_none());
        assert!(credentials.region.is_none());

        let region = credentials.effective_auth_region(&config);
        assert_eq!(region, "us-west-2");
    }

    #[test]
    fn test_multiple_credentials_use_respective_regions() {
        // 多凭据场景下，不同凭据使用各自的 auth_region
        let mut config = Config::default();
        config.region = "ap-northeast-1".to_string();

        let mut cred1 = KiroCredentials::default();
        cred1.auth_region = Some("us-east-1".to_string());

        let mut cred2 = KiroCredentials::default();
        cred2.region = Some("eu-west-1".to_string());

        let cred3 = KiroCredentials::default(); // 无 region，使用 config

        assert_eq!(cred1.effective_auth_region(&config), "us-east-1");
        assert_eq!(cred2.effective_auth_region(&config), "eu-west-1");
        assert_eq!(cred3.effective_auth_region(&config), "ap-northeast-1");
    }

    #[test]
    fn test_idc_oidc_endpoint_uses_credential_auth_region() {
        // 验证 IdC OIDC endpoint URL 使用凭据 auth_region
        let mut config = Config::default();
        config.region = "us-west-2".to_string();

        let mut credentials = KiroCredentials::default();
        credentials.auth_region = Some("eu-central-1".to_string());

        let region = credentials.effective_auth_region(&config);
        let refresh_url = format!("https://oidc.{}.amazonaws.com/token", region);

        assert_eq!(refresh_url, "https://oidc.eu-central-1.amazonaws.com/token");
    }

    #[test]
    fn test_social_refresh_endpoint_uses_credential_auth_region() {
        // 验证 Social refresh endpoint URL 使用凭据 auth_region
        let mut config = Config::default();
        config.region = "us-west-2".to_string();

        let mut credentials = KiroCredentials::default();
        credentials.auth_region = Some("ap-southeast-1".to_string());

        let region = credentials.effective_auth_region(&config);
        let refresh_url = format!("https://prod.{}.auth.desktop.kiro.dev/refreshToken", region);

        assert_eq!(
            refresh_url,
            "https://prod.ap-southeast-1.auth.desktop.kiro.dev/refreshToken"
        );
    }

    #[test]
    fn test_api_call_uses_effective_api_region() {
        // 验证 API 调用使用 effective_api_region
        let mut config = Config::default();
        config.region = "us-west-2".to_string();

        let mut credentials = KiroCredentials::default();
        credentials.region = Some("eu-west-1".to_string());

        // 凭据.region 不参与 api_region 回退链
        let api_region = credentials.effective_api_region(&config);
        let api_host = format!("q.{}.amazonaws.com", api_region);

        assert_eq!(api_host, "q.us-west-2.amazonaws.com");
    }

    #[test]
    fn test_api_call_uses_credential_api_region() {
        // 凭据配置了 api_region 时，API 调用应使用凭据的 api_region
        let mut config = Config::default();
        config.region = "us-west-2".to_string();

        let mut credentials = KiroCredentials::default();
        credentials.api_region = Some("eu-central-1".to_string());

        let api_region = credentials.effective_api_region(&config);
        let api_host = format!("q.{}.amazonaws.com", api_region);

        assert_eq!(api_host, "q.eu-central-1.amazonaws.com");
    }

    #[test]
    fn test_credential_region_empty_string_treated_as_set() {
        // 空字符串 auth_region 被视为已设置（虽然不推荐，但行为应一致）
        let mut config = Config::default();
        config.region = "us-west-2".to_string();

        let mut credentials = KiroCredentials::default();
        credentials.auth_region = Some("".to_string());

        let region = credentials.effective_auth_region(&config);
        // 空字符串被视为已设置，不会回退到 config
        assert_eq!(region, "");
    }

    #[test]
    fn test_auth_and_api_region_independent() {
        // auth_region 和 api_region 互不影响
        let mut config = Config::default();
        config.region = "default".to_string();

        let mut credentials = KiroCredentials::default();
        credentials.auth_region = Some("auth-only".to_string());
        credentials.api_region = Some("api-only".to_string());

        assert_eq!(credentials.effective_auth_region(&config), "auth-only");
        assert_eq!(credentials.effective_api_region(&config), "api-only");
    }
}
