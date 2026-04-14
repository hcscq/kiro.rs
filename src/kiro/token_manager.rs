//! Token 管理模块
//!
//! 负责 Token 过期检测和刷新，支持 Social 和 IdC 认证方式
//! 支持多凭据 (MultiTokenManager) 管理

use anyhow::bail;
use chrono::{DateTime, Duration, Utc};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex as TokioMutex, Notify};

use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration as StdDuration, Instant};

use crate::http_client::{ProxyConfig, build_client};
use crate::kiro::machine_id;
use crate::kiro::model::credentials::KiroCredentials;
use crate::kiro::model::token_refresh::{
    IdcRefreshRequest, IdcRefreshResponse, RefreshRequest, RefreshResponse,
};
use crate::kiro::model::usage_limits::UsageLimitsResponse;
use crate::model::config::Config;

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

        // 400 + invalid_grant + Invalid refresh token provided → refreshToken 永久失效
        if status.as_u16() == 400
            && body_text.contains("\"invalid_grant\"")
            && body_text.contains("Invalid refresh token provided")
        {
            return Err(RefreshTokenInvalidError {
                message: format!("Social refreshToken 已失效 (invalid_grant): {}", body_text),
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
        bail!("{}: {} {}", error_msg, status, body_text);
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

        // 400 + invalid_grant + Invalid refresh token provided → refreshToken 永久失效
        if status.as_u16() == 400
            && body_text.contains("\"invalid_grant\"")
            && body_text.contains("Invalid refresh token provided")
        {
            return Err(RefreshTokenInvalidError {
                message: format!("IdC refreshToken 已失效 (invalid_grant): {}", body_text),
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
        bail!("{}: {} {}", error_msg, status, body_text);
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
        let error_msg = match status.as_u16() {
            401 => "认证失败，Token 无效或已过期",
            403 => "权限不足，无法获取使用额度",
            429 => "请求过于频繁，已被限流",
            500..=599 => "服务器错误，AWS 服务暂时不可用",
            _ => "获取使用额度失败",
        };
        bail!("{}: {} {}", error_msg, status, body_text);
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
    /// 最后一次 API 调用时间（RFC3339 格式）
    last_used_at: Option<String>,
    /// 当前运行中的请求数
    active_requests: usize,
    /// 429 限流冷却到期时间
    rate_limit_cooldown_until: Option<Instant>,
    /// 本地 token bucket 与自适应退避状态
    rate_limit_bucket: Option<AdaptiveTokenBucket>,
    /// 连续 429 次数，用于放大冷却时间
    rate_limit_hit_streak: u32,
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
#[derive(Serialize, Deserialize)]
struct StatsEntry {
    success_count: u64,
    last_used_at: Option<String>,
}

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

    fn has_available_token(&mut self, now: Instant) -> bool {
        self.refill(now);
        self.tokens >= 1.0
    }

    fn consume(&mut self, now: Instant) -> bool {
        if !self.has_available_token(now) {
            return false;
        }
        self.tokens = (self.tokens - 1.0).max(0.0);
        true
    }

    fn ready_at(&mut self, now: Instant) -> Option<Instant> {
        self.refill(now);
        if self.tokens >= 1.0 {
            return None;
        }

        let missing_tokens = 1.0 - self.tokens;
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
    pub waiting_requests: usize,
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
        }
    }

    fn queue_enabled(&self) -> bool {
        self.queue_max_size > 0 && self.queue_max_wait_ms > 0
    }

    fn queue_wait_duration(&self) -> StdDuration {
        StdDuration::from_millis(self.queue_max_wait_ms)
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
}

/// 多凭据 Token 管理器
///
/// 支持多个凭据的管理，实现固定优先级 + 故障转移策略
/// 故障统计基于 API 调用结果，而非 Token 刷新结果
pub struct MultiTokenManager {
    config: Config,
    proxy: Option<ProxyConfig>,
    /// 凭据条目列表
    entries: Arc<Mutex<Vec<CredentialEntry>>>,
    /// 当前活动凭据 ID
    current_id: Mutex<u64>,
    /// Token 刷新锁，确保同一时间只有一个刷新操作
    refresh_lock: TokioMutex<()>,
    /// 凭据文件路径（用于回写）
    credentials_path: Option<PathBuf>,
    /// 是否为多凭据格式（数组格式才回写）
    is_multiple_format: bool,
    /// 调度配置（负载均衡模式、排队参数）
    dispatch_config: Mutex<DispatchConfig>,
    /// 可用性变更通知（并发释放、凭据启用、配置变更等）
    availability_notify: Arc<Notify>,
    /// 当前正在等待可用槽位的请求数
    waiting_requests: Arc<std::sync::atomic::AtomicUsize>,
    /// 最近一次统计持久化时间（用于 debounce）
    last_stats_save_at: Mutex<Option<Instant>>,
    /// 统计数据是否有未落盘更新
    stats_dirty: AtomicBool,
}

/// 每个凭据最大 API 调用失败次数
const MAX_FAILURES_PER_CREDENTIAL: u32 = 3;
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
}

impl Drop for CallLeaseState {
    fn drop(&mut self) {
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
                    last_used_at: None,
                    active_requests: 0,
                    rate_limit_cooldown_until: None,
                    rate_limit_bucket: dispatch_config
                        .bucket_policy_for(&cred)
                        .map(|policy| AdaptiveTokenBucket::new(policy, now)),
                    rate_limit_hit_streak: 0,
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
            entries: Arc::new(Mutex::new(entries)),
            current_id: Mutex::new(initial_id),
            refresh_lock: TokioMutex::new(()),
            credentials_path,
            is_multiple_format,
            dispatch_config: Mutex::new(dispatch_config),
            availability_notify: Arc::new(Notify::new()),
            waiting_requests: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
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

    fn dispatch_config(&self) -> DispatchConfig {
        self.dispatch_config.lock().clone()
    }

    fn queue_depth(&self) -> usize {
        self.waiting_requests.load(Ordering::SeqCst)
    }

    fn is_model_supported(credentials: &KiroCredentials, model: Option<&str>) -> bool {
        let is_opus = model
            .map(|m| m.to_lowercase().contains("opus"))
            .unwrap_or(false);

        !is_opus || credentials.supports_opus()
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
    }

    fn clear_all_rate_limit_cooldowns(&self) {
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
        entry
            .rate_limit_bucket
            .as_ref()
            .map_or(true, |bucket| bucket.tokens >= 1.0)
    }

    fn combined_ready_at(entry: &mut CredentialEntry, now: Instant) -> Option<Instant> {
        let cooldown_ready_at = entry.rate_limit_cooldown_until.filter(|until| *until > now);
        let bucket_ready_at = entry
            .rate_limit_bucket
            .as_mut()
            .and_then(|bucket| bucket.ready_at(now));

        match (cooldown_ready_at, bucket_ready_at) {
            (Some(cooldown_ready_at), Some(bucket_ready_at)) => {
                Some(cooldown_ready_at.max(bucket_ready_at))
            }
            (Some(cooldown_ready_at), None) => Some(cooldown_ready_at),
            (None, Some(bucket_ready_at)) => Some(bucket_ready_at),
            (None, None) => None,
        }
    }

    fn next_ready_at(
        entries: &mut [CredentialEntry],
        model: Option<&str>,
        now: Instant,
    ) -> Option<Instant> {
        entries
            .iter_mut()
            .filter(|entry| !entry.disabled && Self::is_model_supported(&entry.credentials, model))
            .filter_map(|entry| Self::combined_ready_at(entry, now))
            .min()
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

        Ok(())
    }

    fn reserve_call_lease(&self, id: u64) -> CallLease {
        CallLease {
            _state: Arc::new(CallLeaseState {
                entries: Arc::clone(&self.entries),
                availability_notify: Arc::clone(&self.availability_notify),
                id,
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

        let wake_at = next_ready_at
            .map(|next| next.min(deadline))
            .unwrap_or(deadline);
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
        let mut entries = self.entries.lock();
        let mut recovered = false;

        for entry in entries.iter_mut() {
            if entry.disabled_reason == Some(DisabledReason::TooManyFailures) {
                entry.disabled = false;
                entry.disabled_reason = None;
                entry.failure_count = 0;
                Self::reset_rate_limit_runtime(entry, &dispatch, now);
                recovered = true;
            }
        }

        if recovered {
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
    ) -> Result<(u64, KiroCredentials, CallLease), ReservationFailure> {
        let dispatch = self.dispatch_config();
        let mode = dispatch.mode;
        let mut entries = self.entries.lock();
        let now = Instant::now();

        if entries.is_empty() {
            return Err(ReservationFailure::NoCredentials);
        }

        Self::refresh_runtime_state(&mut entries, now);

        let enabled_count = entries.iter().filter(|e| !e.disabled).count();
        if enabled_count == 0 {
            return Err(ReservationFailure::AllDisabled);
        }

        let supported_count = entries
            .iter()
            .filter(|e| !e.disabled && Self::is_model_supported(&e.credentials, model))
            .count();
        if supported_count == 0 {
            return Err(ReservationFailure::NoModelSupport);
        }

        let mut current_id = self.current_id.lock();

        let selected_id = if mode == "balanced" {
            entries
                .iter()
                .filter(|e| {
                    !e.disabled
                        && Self::is_model_supported(&e.credentials, model)
                        && !Self::is_rate_limited(e, now)
                        && Self::bucket_is_ready(e)
                        && Self::has_capacity(
                            &e.credentials,
                            e.active_requests,
                            dispatch.default_max_concurrency,
                        )
                })
                .min_by_key(|e| {
                    (
                        e.active_requests,
                        e.success_count,
                        e.credentials.priority,
                        e.id,
                    )
                })
                .map(|e| e.id)
        } else {
            let current_candidate = entries.iter().find(|e| {
                e.id == *current_id
                    && !e.disabled
                    && Self::is_model_supported(&e.credentials, model)
                    && !Self::is_rate_limited(e, now)
                    && Self::bucket_is_ready(e)
                    && Self::has_capacity(
                        &e.credentials,
                        e.active_requests,
                        dispatch.default_max_concurrency,
                    )
            });

            current_candidate.map(|e| e.id).or_else(|| {
                entries
                    .iter()
                    .filter(|e| {
                        !e.disabled
                            && Self::is_model_supported(&e.credentials, model)
                            && !Self::is_rate_limited(e, now)
                            && Self::bucket_is_ready(e)
                            && Self::has_capacity(
                                &e.credentials,
                                e.active_requests,
                                dispatch.default_max_concurrency,
                            )
                    })
                    .min_by_key(|e| (e.credentials.priority, e.active_requests, e.id))
                    .map(|e| e.id)
            })
        };

        let selected_id = match selected_id {
            Some(id) => id,
            None => {
                let next_ready_at = Self::next_ready_at(&mut entries, model, now);
                return Err(ReservationFailure::AllTemporarilyUnavailable { next_ready_at });
            }
        };

        let entry_index = entries
            .iter()
            .position(|e| e.id == selected_id)
            .expect("selected credential should exist");
        let token_consumed = {
            let entry = &mut entries[entry_index];
            entry
                .rate_limit_bucket
                .as_mut()
                .map_or(true, |bucket| bucket.consume(now))
        };
        if !token_consumed {
            let next_ready_at = Self::next_ready_at(&mut entries, model, now);
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
            self.reserve_call_lease(selected_id),
        ))
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
        let total = self.total_count();
        let max_attempts = (total * MAX_FAILURES_PER_CREDENTIAL as usize).max(1);
        let mut attempt_count = 0;
        let mut wait_queue_guard: Option<WaitQueueGuard> = None;
        let mut wait_deadline: Option<Instant> = None;

        loop {
            if attempt_count >= max_attempts {
                anyhow::bail!(
                    "所有凭据均无法获取有效 Token（可用: {}/{}）",
                    self.available_count(),
                    total
                );
            }

            let (id, credentials, lease) = match self.reserve_next_credential(model) {
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
    /// 使用双重检查锁定模式，确保同一时间只有一个刷新操作
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
            // 获取刷新锁，确保同一时间只有一个刷新操作
            let _guard = self.refresh_lock.lock().await;

            // 第二次检查：获取锁后重新读取凭据，因为其他请求可能已经完成刷新
            let current_creds = {
                let entries = self.entries.lock();
                entries
                    .iter()
                    .find(|e| e.id == id)
                    .map(|e| e.credentials.clone())
                    .ok_or_else(|| anyhow::anyhow!("凭据 #{} 不存在", id))?
            };

            if is_token_expired(&current_creds) || is_token_expiring_soon(&current_creds) {
                // 确实需要刷新
                let effective_proxy = current_creds.effective_proxy(self.proxy.as_ref());
                let new_creds =
                    refresh_token(&current_creds, &self.config, effective_proxy.as_ref()).await?;

                if is_token_expired(&new_creds) {
                    anyhow::bail!("刷新后的 Token 仍然无效或已过期");
                }

                // 更新凭据
                {
                    let mut entries = self.entries.lock();
                    if let Some(entry) = entries.iter_mut().find(|e| e.id == id) {
                        entry.credentials = new_creds.clone();
                    }
                }

                // 回写凭据到文件（仅多凭据格式），失败只记录警告
                if let Err(e) = self.persist_credentials() {
                    tracing::warn!("Token 刷新后持久化失败（不影响本次请求）: {}", e);
                }

                new_creds
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
        use anyhow::Context;

        // 仅多凭据格式才回写
        if !self.is_multiple_format {
            return Ok(false);
        }

        let path = match &self.credentials_path {
            Some(p) => p,
            None => return Ok(false),
        };

        // 收集所有凭据
        let credentials: Vec<KiroCredentials> = {
            let entries = self.entries.lock();
            entries
                .iter()
                .map(|e| {
                    let mut cred = e.credentials.clone();
                    cred.canonicalize_auth_method();
                    // 同步 disabled 状态到凭据对象
                    cred.disabled = e.disabled;
                    cred
                })
                .collect()
        };

        // 序列化为 pretty JSON
        let json = serde_json::to_string_pretty(&credentials).context("序列化凭据失败")?;

        // 写入文件（在 Tokio runtime 内使用 block_in_place 避免阻塞 worker）
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::task::block_in_place(|| std::fs::write(path, &json))
                .with_context(|| format!("回写凭据文件失败: {:?}", path))?;
        } else {
            std::fs::write(path, &json).with_context(|| format!("回写凭据文件失败: {:?}", path))?;
        }

        tracing::debug!("已回写凭据到文件: {:?}", path);
        Ok(true)
    }

    /// 获取缓存目录（凭据文件所在目录）
    pub fn cache_dir(&self) -> Option<PathBuf> {
        self.credentials_path
            .as_ref()
            .and_then(|p| p.parent().map(|d| d.to_path_buf()))
    }

    /// 统计数据文件路径
    fn stats_path(&self) -> Option<PathBuf> {
        self.cache_dir().map(|d| d.join("kiro_stats.json"))
    }

    /// 从磁盘加载统计数据并应用到当前条目
    fn load_stats(&self) {
        let path = match self.stats_path() {
            Some(p) => p,
            None => return,
        };

        let content = match std::fs::read_to_string(&path) {
            Ok(c) => c,
            Err(_) => return, // 首次运行时文件不存在
        };

        let stats: HashMap<String, StatsEntry> = match serde_json::from_str(&content) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("解析统计缓存失败，将忽略: {}", e);
                return;
            }
        };

        let mut entries = self.entries.lock();
        for entry in entries.iter_mut() {
            if let Some(s) = stats.get(&entry.id.to_string()) {
                entry.success_count = s.success_count;
                entry.last_used_at = s.last_used_at.clone();
            }
        }
        *self.last_stats_save_at.lock() = Some(Instant::now());
        self.stats_dirty.store(false, Ordering::Relaxed);
        tracing::info!("已从缓存加载 {} 条统计数据", stats.len());
    }

    /// 将当前统计数据持久化到磁盘
    fn save_stats(&self) {
        let path = match self.stats_path() {
            Some(p) => p,
            None => return,
        };

        let stats: HashMap<String, StatsEntry> = {
            let entries = self.entries.lock();
            entries
                .iter()
                .map(|e| {
                    (
                        e.id.to_string(),
                        StatsEntry {
                            success_count: e.success_count,
                            last_used_at: e.last_used_at.clone(),
                        },
                    )
                })
                .collect()
        };

        match serde_json::to_string_pretty(&stats) {
            Ok(json) => {
                if let Err(e) = std::fs::write(&path, json) {
                    tracing::warn!("保存统计缓存失败: {}", e);
                } else {
                    *self.last_stats_save_at.lock() = Some(Instant::now());
                    self.stats_dirty.store(false, Ordering::Relaxed);
                }
            }
            Err(e) => tracing::warn!("序列化统计数据失败: {}", e),
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
                entry.last_used_at = Some(Utc::now().to_rfc3339());
                tracing::debug!(
                    "凭据 #{} API 调用成功（累计 {} 次）",
                    id,
                    entry.success_count
                );
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
        self.save_stats_debounced();
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
        let mut entries = self.entries.lock();
        Self::refresh_runtime_state(&mut entries, now);
        let current_id = *self.current_id.lock();
        let available = entries.iter().filter(|e| !e.disabled).count();
        let dispatchable = entries
            .iter()
            .filter(|e| !e.disabled)
            .filter(|e| !Self::is_rate_limited(e, now))
            .filter(|e| Self::bucket_is_ready(e))
            .filter(|e| {
                Self::has_capacity(
                    &e.credentials,
                    e.active_requests,
                    dispatch.default_max_concurrency,
                )
            })
            .count();

        ManagerSnapshot {
            entries: entries
                .iter_mut()
                .map(|e| CredentialEntrySnapshot {
                    id: e.id,
                    priority: e.credentials.priority,
                    disabled: e.disabled,
                    failure_count: e.failure_count,
                    auth_method: e.credentials.auth_method.as_deref().map(|m| {
                        if m.eq_ignore_ascii_case("builder-id") || m.eq_ignore_ascii_case("iam") {
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
                    imported_at: e.credentials.imported_at.clone(),
                    success_count: e.success_count,
                    last_used_at: e.last_used_at.clone(),
                    active_requests: e.active_requests,
                    max_concurrency: e
                        .credentials
                        .effective_max_concurrency_with_default(dispatch.default_max_concurrency)
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
                    cooldown_remaining_ms: e
                        .rate_limit_cooldown_until
                        .and_then(|until| until.checked_duration_since(now))
                        .map(|remaining| remaining.as_millis().min(u128::from(u64::MAX)) as u64),
                    rate_limit_bucket_tokens: e
                        .rate_limit_bucket
                        .as_ref()
                        .map(|bucket| (bucket.tokens * 100.0).round() / 100.0),
                    rate_limit_bucket_capacity: e
                        .rate_limit_bucket
                        .as_ref()
                        .map(|bucket| bucket.policy.capacity),
                    rate_limit_bucket_capacity_override: e
                        .credentials
                        .rate_limit_bucket_capacity_override(),
                    rate_limit_refill_per_second: e
                        .rate_limit_bucket
                        .as_ref()
                        .map(|bucket| (bucket.current_refill_per_second * 100.0).round() / 100.0),
                    rate_limit_refill_per_second_override: e
                        .credentials
                        .rate_limit_refill_per_second_override(),
                    rate_limit_refill_base_per_second: e
                        .rate_limit_bucket
                        .as_ref()
                        .map(|bucket| bucket.policy.refill_per_second),
                    rate_limit_hit_streak: e.rate_limit_hit_streak,
                    next_ready_in_ms: Self::combined_ready_at(e, now)
                        .and_then(|ready_at| ready_at.checked_duration_since(now))
                        .map(|remaining| remaining.as_millis().min(u128::from(u64::MAX)) as u64),
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
        // 持久化更改
        self.persist_credentials()?;
        if !disabled {
            self.availability_notify.notify_waiters();
        }
        Ok(())
    }

    /// 设置凭据优先级（Admin API）
    ///
    /// 修改优先级后会立即按新优先级重新选择当前凭据。
    /// 即使持久化失败，内存中的优先级和当前凭据选择也会生效。
    pub fn set_priority(&self, id: u64, priority: u32) -> anyhow::Result<()> {
        {
            let mut entries = self.entries.lock();
            let entry = entries
                .iter_mut()
                .find(|e| e.id == id)
                .ok_or_else(|| anyhow::anyhow!("凭据不存在: {}", id))?;
            entry.credentials.priority = priority;
        }
        // 立即按新优先级重新选择当前凭据（无论持久化是否成功）
        self.select_highest_priority();
        // 持久化更改
        self.persist_credentials()?;
        Ok(())
    }

    /// 设置凭据并发上限（Admin API）
    pub fn set_max_concurrency(&self, id: u64, max_concurrency: Option<u32>) -> anyhow::Result<()> {
        {
            let mut entries = self.entries.lock();
            let entry = entries
                .iter_mut()
                .find(|e| e.id == id)
                .ok_or_else(|| anyhow::anyhow!("凭据不存在: {}", id))?;
            entry.credentials.max_concurrency = max_concurrency.filter(|limit| *limit > 0);
        }
        self.persist_credentials()?;
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

        self.persist_credentials()?;
        self.availability_notify.notify_waiters();
        Ok(())
    }

    /// 重置凭据失败计数并重新启用（Admin API）
    pub fn reset_and_enable(&self, id: u64) -> anyhow::Result<()> {
        let dispatch = self.dispatch_config();
        let now = Instant::now();
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
        // 持久化更改
        self.persist_credentials()?;
        self.availability_notify.notify_waiters();
        Ok(())
    }

    /// 获取指定凭据的使用额度（Admin API）
    pub async fn get_usage_limits_for(&self, id: u64) -> anyhow::Result<UsageLimitsResponse> {
        let credentials = {
            let entries = self.entries.lock();
            entries
                .iter()
                .find(|e| e.id == id)
                .map(|e| e.credentials.clone())
                .ok_or_else(|| anyhow::anyhow!("凭据不存在: {}", id))?
        };

        // 检查是否需要刷新 token
        let needs_refresh = is_token_expired(&credentials) || is_token_expiring_soon(&credentials);

        let token = if needs_refresh {
            let _guard = self.refresh_lock.lock().await;
            let current_creds = {
                let entries = self.entries.lock();
                entries
                    .iter()
                    .find(|e| e.id == id)
                    .map(|e| e.credentials.clone())
                    .ok_or_else(|| anyhow::anyhow!("凭据不存在: {}", id))?
            };

            if is_token_expired(&current_creds) || is_token_expiring_soon(&current_creds) {
                let effective_proxy = current_creds.effective_proxy(self.proxy.as_ref());
                let new_creds =
                    refresh_token(&current_creds, &self.config, effective_proxy.as_ref()).await?;
                {
                    let mut entries = self.entries.lock();
                    if let Some(entry) = entries.iter_mut().find(|e| e.id == id) {
                        entry.credentials = new_creds.clone();
                    }
                }
                // 持久化失败只记录警告，不影响本次请求
                if let Err(e) = self.persist_credentials() {
                    tracing::warn!("Token 刷新后持久化失败（不影响本次请求）: {}", e);
                }
                new_creds
                    .access_token
                    .ok_or_else(|| anyhow::anyhow!("刷新后无 access_token"))?
            } else {
                current_creds
                    .access_token
                    .ok_or_else(|| anyhow::anyhow!("凭据无 access_token"))?
            }
        } else {
            credentials
                .access_token
                .ok_or_else(|| anyhow::anyhow!("凭据无 access_token"))?
        };

        let credentials = {
            let entries = self.entries.lock();
            entries
                .iter()
                .find(|e| e.id == id)
                .map(|e| e.credentials.clone())
                .ok_or_else(|| anyhow::anyhow!("凭据不存在: {}", id))?
        };

        let effective_proxy = credentials.effective_proxy(self.proxy.as_ref());
        let usage_limits =
            get_usage_limits(&credentials, &self.config, &token, effective_proxy.as_ref()).await?;

        // 更新订阅等级到凭据（仅在发生变化时持久化）
        if let Some(subscription_title) = usage_limits.subscription_title() {
            let changed = {
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
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            };

            if changed {
                if let Err(e) = self.persist_credentials() {
                    tracing::warn!("订阅等级更新后持久化失败（不影响本次请求）: {}", e);
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
        let duplicate_exists = {
            let entries = self.entries.lock();
            entries.iter().any(|entry| {
                entry
                    .credentials
                    .refresh_token
                    .as_deref()
                    .map(sha256_hex)
                    .as_deref()
                    == Some(new_refresh_token_hash.as_str())
            })
        };
        if duplicate_exists {
            anyhow::bail!("凭据已存在（refreshToken 重复）");
        }

        // 3. 尝试刷新 Token 验证凭据有效性
        let effective_proxy = new_cred.effective_proxy(self.proxy.as_ref());
        let mut validated_cred =
            refresh_token(&new_cred, &self.config, effective_proxy.as_ref()).await?;

        // 4. 分配新 ID
        let new_id = {
            let entries = self.entries.lock();
            entries.iter().map(|e| e.id).max().unwrap_or(0) + 1
        };

        // 5. 设置 ID 并保留用户输入的元数据
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
        validated_cred.imported_at = new_cred
            .imported_at
            .or_else(|| Some(Utc::now().to_rfc3339()));
        validated_cred.max_concurrency = new_cred.max_concurrency;
        validated_cred.rate_limit_bucket_capacity = new_cred.rate_limit_bucket_capacity;
        validated_cred.rate_limit_refill_per_second = new_cred.rate_limit_refill_per_second;
        validated_cred.proxy_url = new_cred.proxy_url;
        validated_cred.proxy_username = new_cred.proxy_username;
        validated_cred.proxy_password = new_cred.proxy_password;

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
                last_used_at: None,
                active_requests: 0,
                rate_limit_cooldown_until: None,
                rate_limit_bucket,
                rate_limit_hit_streak: 0,
            });
        }

        // 6. 持久化
        self.persist_credentials()?;
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

        // 持久化更改
        self.persist_credentials()?;

        // 立即回写统计数据，清除已删除凭据的残留条目
        self.save_stats();

        tracing::info!("已删除凭据 #{}", id);
        Ok(())
    }

    /// 强制刷新指定凭据的 Token（Admin API）
    ///
    /// 无条件调用上游 API 重新获取 access token，不检查是否过期。
    /// 适用于排查问题、Token 异常但未过期、主动更新凭据状态等场景。
    pub async fn force_refresh_token_for(&self, id: u64) -> anyhow::Result<()> {
        let credentials = {
            let entries = self.entries.lock();
            entries
                .iter()
                .find(|e| e.id == id)
                .map(|e| e.credentials.clone())
                .ok_or_else(|| anyhow::anyhow!("凭据不存在: {}", id))?
        };

        // 获取刷新锁防止并发刷新
        let _guard = self.refresh_lock.lock().await;

        // 无条件调用 refresh_token
        let effective_proxy = credentials.effective_proxy(self.proxy.as_ref());
        let new_creds = refresh_token(&credentials, &self.config, effective_proxy.as_ref()).await?;

        // 更新 entries 中对应凭据
        {
            let mut entries = self.entries.lock();
            if let Some(entry) = entries.iter_mut().find(|e| e.id == id) {
                entry.credentials = new_creds;
                entry.refresh_failure_count = 0;
            }
        }

        // 持久化
        if let Err(e) = self.persist_credentials() {
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
            waiting_requests: self.queue_depth(),
        }
    }

    /// 获取负载均衡模式（Admin API）
    pub fn get_load_balancing_mode(&self) -> String {
        self.dispatch_config().mode
    }

    fn persist_dispatch_config(&self, dispatch: &DispatchConfig) -> anyhow::Result<()> {
        use anyhow::Context;

        let config_path = match self.config.config_path() {
            Some(path) => path.to_path_buf(),
            None => {
                tracing::warn!(
                    "配置文件路径未知，调度配置仅在当前进程生效: mode={}, queueMaxSize={}, queueMaxWaitMs={}, rateLimitCooldownMs={}, defaultMaxConcurrency={:?}, rateLimitBucketCapacity={}, rateLimitRefillPerSecond={}, rateLimitRefillMinPerSecond={}, rateLimitRefillRecoveryStepPerSuccess={}, rateLimitRefillBackoffFactor={}",
                    dispatch.mode,
                    dispatch.queue_max_size,
                    dispatch.queue_max_wait_ms,
                    dispatch.rate_limit_cooldown_ms,
                    dispatch.default_max_concurrency,
                    dispatch.rate_limit_bucket_capacity,
                    dispatch.rate_limit_refill_per_second,
                    dispatch.rate_limit_refill_min_per_second,
                    dispatch.rate_limit_refill_recovery_step_per_success,
                    dispatch.rate_limit_refill_backoff_factor
                );
                return Ok(());
            }
        };

        let mut config = Config::load(&config_path)
            .with_context(|| format!("重新加载配置失败: {}", config_path.display()))?;
        config.load_balancing_mode = dispatch.mode.clone();
        config.queue_max_size = dispatch.queue_max_size;
        config.queue_max_wait_ms = dispatch.queue_max_wait_ms;
        config.rate_limit_cooldown_ms = dispatch.rate_limit_cooldown_ms;
        config.default_max_concurrency = dispatch.default_max_concurrency;
        config.rate_limit_bucket_capacity = dispatch.rate_limit_bucket_capacity;
        config.rate_limit_refill_per_second = dispatch.rate_limit_refill_per_second;
        config.rate_limit_refill_min_per_second = dispatch.rate_limit_refill_min_per_second;
        config.rate_limit_refill_recovery_step_per_success =
            dispatch.rate_limit_refill_recovery_step_per_success;
        config.rate_limit_refill_backoff_factor = dispatch.rate_limit_refill_backoff_factor;
        config
            .save()
            .with_context(|| format!("持久化调度配置失败: {}", config_path.display()))?;

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
        )
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

        Self::validate_dispatch_rate_limit_config(&next)?;

        if previous == next {
            return Ok(());
        }

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
            "调度配置已更新: mode={}, queueMaxSize={}, queueMaxWaitMs={}, rateLimitCooldownMs={}, defaultMaxConcurrency={:?}, rateLimitBucketCapacity={}, rateLimitRefillPerSecond={}, rateLimitRefillMinPerSecond={}, rateLimitRefillRecoveryStepPerSuccess={}, rateLimitRefillBackoffFactor={}",
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

    fn available_credential(priority: u32) -> KiroCredentials {
        let mut credentials = KiroCredentials::default();
        credentials.priority = priority;
        credentials.access_token = Some(format!("token-{priority}"));
        credentials.expires_at = Some((Utc::now() + Duration::hours(1)).to_rfc3339());
        credentials
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
