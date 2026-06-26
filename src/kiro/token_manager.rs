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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::error::Error as StdError;
use std::fmt;
use std::future::Future;
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration as StdDuration, Instant};

use crate::admin::types::BalanceResponse;
use crate::common::auth::CredentialGroupScope;
use crate::common::logging::summarize_upstream_error;
use crate::http_client::{ProxyConfig, build_client, build_client_no_redirect};
use crate::kiro::machine_id;
use crate::kiro::model::available_models::ListAvailableModelsResponse;
use crate::kiro::model::available_profiles::{AvailableProfile, ListAvailableProfilesResponse};
use crate::kiro::model::credentials::KiroCredentials;
use crate::kiro::model::token_refresh::{
    ExternalIdpDiscoveryResponse, ExternalIdpRefreshResponse, IdcRefreshRequest,
    IdcRefreshResponse, RefreshRequest, RefreshResponse,
};
use crate::kiro::model::usage_limits::UsageLimitsResponse;
use crate::model::config::{
    Config, CredentialGroupConfig, KiroRequestBodyGuardConfig, NonStreamBodyReadTimeoutConfig,
    ProxyPoolConfig, ProxyPoolEntry, RequestWeightingConfig, StreamPreSseFailoverConfig,
    ThinkingSignatureValidationMode,
};
use crate::model::model_policy::{
    AccountTypeDispatchPolicy, ModelSupportPolicy, normalize_account_type_dispatch_policies,
    normalize_account_type_policies, normalize_model_entries, normalize_model_selector,
};
use crate::state::{
    CachedBalanceRecord, CredentialCompareAndSwapResult, CredentialHealthPatch,
    CredentialMetadataPatch, DispatchLeaseReservationStatus, DispatchRuntimeBucketPolicy,
    DispatchRuntimeCredential, DispatchRuntimeSnapshot, PersistedDispatchConfig,
    RuntimeCoordinationStatus, RuntimeRefreshLeaseAcquisition, StateChangeKind,
    StateChangeRevisions, StateStore, StatsEntryRecord, StatsMergeRecord, current_epoch_ms,
};

const DEFAULT_REQUEST_WEIGHT: f64 = 1.0;
const UPSTREAM_ERROR_EXCERPT_CHARS: usize = 240;
const SESSION_AFFINITY_TTL_MS: u64 = 60 * 60 * 1000;
const SESSION_AFFINITY_LOCAL_TTL: StdDuration = StdDuration::from_secs(60 * 60);
const SESSION_AFFINITY_LOCAL_MAX_ENTRIES: usize = 10_000;

#[derive(Debug)]
struct KiroManagementApiError {
    api: &'static str,
    status_code: u16,
    message: String,
}

impl KiroManagementApiError {
    fn is_auth_error(&self) -> bool {
        matches!(self.status_code, 401 | 403)
    }
}

impl fmt::Display for KiroManagementApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.api, self.message)
    }
}

impl StdError for KiroManagementApiError {}

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

fn normalize_optional_metadata(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn normalized_duplicate_identity(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_lowercase)
}

fn normalized_duplicate_url(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.trim_end_matches('/').to_lowercase())
}

fn external_idp_duplicate_reason(
    existing: &KiroCredentials,
    candidate: &KiroCredentials,
) -> Option<&'static str> {
    if !existing.is_external_idp_auth() || !candidate.is_external_idp_auth() {
        return None;
    }

    let same_issuer = match (
        normalized_duplicate_url(existing.issuer_url.as_deref()),
        normalized_duplicate_url(candidate.issuer_url.as_deref()),
    ) {
        (Some(existing), Some(candidate)) => existing == candidate,
        _ => false,
    };
    let same_client = match (
        normalized_duplicate_identity(existing.client_id.as_deref()),
        normalized_duplicate_identity(candidate.client_id.as_deref()),
    ) {
        (Some(existing), Some(candidate)) => existing == candidate,
        _ => false,
    };
    if !same_issuer || !same_client {
        return None;
    }

    let existing_user_id = normalized_duplicate_identity(existing.user_id.as_deref());
    let candidate_user_id = normalized_duplicate_identity(candidate.user_id.as_deref());
    if existing_user_id.is_some() && existing_user_id == candidate_user_id {
        return Some("issuerUrl/clientId/userId 重复");
    }

    let existing_email = normalized_duplicate_identity(existing.email.as_deref());
    let candidate_email = normalized_duplicate_identity(candidate.email.as_deref());
    if existing_email.is_some() && existing_email == candidate_email {
        return Some("issuerUrl/clientId/email 重复");
    }

    None
}

fn normalized_auth_account_type(credentials: &KiroCredentials) -> Option<String> {
    let account_type =
        normalized_duplicate_identity(credentials.detected_auth_account_type().as_deref())?;
    if account_type == "builder-id" {
        return Some("idc".to_string());
    }
    Some(account_type)
}

fn non_external_duplicate_scope_matches(
    existing: &KiroCredentials,
    candidate: &KiroCredentials,
) -> bool {
    if existing.is_external_idp_auth() || candidate.is_external_idp_auth() {
        return false;
    }

    let existing_account_type = normalized_auth_account_type(existing);
    let candidate_account_type = normalized_auth_account_type(candidate);
    if existing_account_type.is_none() || existing_account_type != candidate_account_type {
        return false;
    }

    if let (Some(existing_provider), Some(candidate_provider)) = (
        normalized_duplicate_identity(existing.provider.as_deref()),
        normalized_duplicate_identity(candidate.provider.as_deref()),
    ) {
        if existing_provider != candidate_provider {
            return false;
        }
    }

    if existing_account_type.as_deref() == Some("enterprise") {
        let existing_start_url = normalized_duplicate_url(existing.start_url.as_deref());
        let candidate_start_url = normalized_duplicate_url(candidate.start_url.as_deref());
        if existing_start_url.is_some()
            && candidate_start_url.is_some()
            && existing_start_url != candidate_start_url
        {
            return false;
        }
    }

    true
}

fn credential_duplicate_reason(
    existing: &KiroCredentials,
    candidate: &KiroCredentials,
) -> Option<&'static str> {
    if let Some(reason) = external_idp_duplicate_reason(existing, candidate) {
        return Some(reason);
    }

    if !non_external_duplicate_scope_matches(existing, candidate) {
        return None;
    }

    let existing_user_id = normalized_duplicate_identity(existing.user_id.as_deref());
    let candidate_user_id = normalized_duplicate_identity(candidate.user_id.as_deref());
    if existing_user_id.is_some() && existing_user_id == candidate_user_id {
        return Some("authAccountType/userId 重复");
    }

    let existing_email = normalized_duplicate_identity(existing.email.as_deref());
    let candidate_email = normalized_duplicate_identity(candidate.email.as_deref());
    if existing_email.is_some() && existing_email == candidate_email {
        return Some("authAccountType/email 重复");
    }

    None
}

fn apply_usage_limits_metadata_to_credentials(
    credentials: &mut KiroCredentials,
    usage_limits: &UsageLimitsResponse,
) {
    if let Some(subscription_title) = normalize_optional_metadata(usage_limits.subscription_title())
    {
        credentials.subscription_title = Some(subscription_title);
    }
    if let Some(subscription_type) = normalize_optional_metadata(usage_limits.subscription_type()) {
        credentials.subscription_type = Some(subscription_type);
    }
    if let Some(email) = normalize_optional_metadata(usage_limits.email()) {
        credentials.email = Some(email);
    }
    if let Some(user_id) = normalize_optional_metadata(usage_limits.user_id()) {
        credentials.user_id = Some(user_id);
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

#[derive(Debug)]
pub(crate) struct RuntimeRefreshLeaseBusyError {
    pub instance_id: String,
    pub owner_instance_id: Option<String>,
}

impl fmt::Display for RuntimeRefreshLeaseBusyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.owner_instance_id {
            Some(owner_instance_id) => write!(
                f,
                "共享凭据正在由其他实例刷新（instanceId={}, ownerInstanceId={}）",
                self.instance_id, owner_instance_id
            ),
            None => write!(
                f,
                "共享凭据刷新结果尚未同步完成（instanceId={}）",
                self.instance_id
            ),
        }
    }
}

impl std::error::Error for RuntimeRefreshLeaseBusyError {}

#[derive(Debug, Clone)]
pub struct CredentialScopeForbiddenError;

impl fmt::Display for CredentialScopeForbiddenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "当前 API key 的凭据分组范围内没有可用凭据")
    }
}

impl StdError for CredentialScopeForbiddenError {}

/// 刷新 Token
pub(crate) async fn refresh_token(
    credentials: &KiroCredentials,
    config: &Config,
    proxy: Option<&ProxyConfig>,
) -> anyhow::Result<KiroCredentials> {
    validate_refresh_token(credentials)?;

    // 根据 auth_method 选择刷新方式；如果未指定，则根据 clientId/clientSecret 自动判断。
    let auth_method = credentials.effective_auth_method();

    if auth_method.eq_ignore_ascii_case("external_idp")
        || auth_method.eq_ignore_ascii_case("external-idp")
    {
        refresh_external_idp_token(credentials, config, proxy).await
    } else if auth_method.eq_ignore_ascii_case("idc")
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

fn oidc_discovery_url(issuer_url: &str) -> anyhow::Result<url::Url> {
    let mut url = normalize_external_idp_issuer_url(issuer_url)?;
    let mut path = url.path().trim_end_matches('/').to_string();
    path.push_str("/.well-known/openid-configuration");
    url.set_path(&path);
    url.set_query(None);
    url.set_fragment(None);
    Ok(url)
}

fn normalize_external_idp_url(raw_url: &str, label: &str) -> anyhow::Result<url::Url> {
    let parsed = url::Url::parse(raw_url.trim())
        .map_err(|err| anyhow::anyhow!("ExternalIdP {label} 无效: {err}"))?;
    if parsed.scheme() != "https" {
        bail!("ExternalIdP {label} 必须使用 https");
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        bail!("ExternalIdP {label} 不能包含用户名或密码");
    }
    if parsed.fragment().is_some() {
        bail!("ExternalIdP {label} 不能包含 fragment");
    }
    let host = parsed
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("ExternalIdP {label} 必须包含主机名"))?
        .trim()
        .to_ascii_lowercase();
    if host == "localhost" || host.ends_with(".localhost") {
        bail!("ExternalIdP {label} 不能指向 localhost");
    }
    if host.trim_matches(['[', ']']).parse::<IpAddr>().is_ok() {
        bail!("ExternalIdP {label} 不能使用 IP literal 主机");
    }
    Ok(parsed)
}

fn normalize_external_idp_issuer_url(raw_url: &str) -> anyhow::Result<url::Url> {
    let mut parsed = normalize_external_idp_url(raw_url, "issuerUrl")?;
    parsed.set_query(None);
    parsed.set_fragment(None);
    Ok(parsed)
}

fn normalize_external_idp_endpoint(raw_url: &str, label: &str) -> anyhow::Result<String> {
    Ok(normalize_external_idp_url(raw_url, label)?.to_string())
}

fn external_idp_token_type_header(
    request: reqwest::RequestBuilder,
    credentials: &KiroCredentials,
) -> reqwest::RequestBuilder {
    if credentials.is_external_idp_auth() {
        request.header("TokenType", "EXTERNAL_IDP")
    } else {
        request
    }
}

async fn discover_external_idp_token_endpoint(
    client: &reqwest::Client,
    issuer_url: &str,
) -> anyhow::Result<String> {
    let discovery_url = oidc_discovery_url(issuer_url)?;
    let response = client
        .get(discovery_url)
        .header("accept", "application/json")
        .send()
        .await?;
    let status = response.status();
    if !status.is_success() {
        let body_text = response.text().await.unwrap_or_default();
        let error_summary =
            summarize_upstream_error(status.as_u16(), &body_text, UPSTREAM_ERROR_EXCERPT_CHARS);
        bail!("ExternalIdP OIDC discovery 失败: {}", error_summary);
    }

    let data: ExternalIdpDiscoveryResponse = response.json().await?;
    let token_endpoint = data
        .token_endpoint
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow::anyhow!("ExternalIdP OIDC discovery 未返回 token_endpoint"))?;
    normalize_external_idp_endpoint(&token_endpoint, "token_endpoint")
}

/// 刷新 External IdP Token
async fn refresh_external_idp_token(
    credentials: &KiroCredentials,
    config: &Config,
    proxy: Option<&ProxyConfig>,
) -> anyhow::Result<KiroCredentials> {
    tracing::info!("正在刷新 External IdP Token...");

    let refresh_token = credentials.refresh_token.as_ref().unwrap();
    let issuer_url = credentials
        .issuer_url
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow::anyhow!("ExternalIdP 刷新需要 issuerUrl"))?;
    let client_id = credentials
        .client_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow::anyhow!("ExternalIdP 刷新需要 clientId"))?;

    let client = build_client_no_redirect(proxy, 60, config.tls_backend)?;
    let token_endpoint = match discover_external_idp_token_endpoint(&client, issuer_url).await {
        Ok(endpoint) => endpoint,
        Err(err) => {
            if let Some(endpoint) = credentials
                .token_endpoint
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                tracing::warn!(
                    "ExternalIdP OIDC discovery 失败，将回退到缓存的 tokenEndpoint: {}",
                    err
                );
                normalize_external_idp_endpoint(endpoint, "token_endpoint")?
            } else {
                return Err(err);
            }
        }
    };

    let mut form = vec![
        ("grant_type", "refresh_token".to_string()),
        ("refresh_token", refresh_token.to_string()),
        ("client_id", client_id.to_string()),
    ];
    if let Some(scopes) = credentials
        .scopes
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        form.push(("scope", scopes.to_string()));
    }

    let response = client
        .post(&token_endpoint)
        .header("accept", "application/json")
        .form(&form)
        .send()
        .await?;

    let status = response.status();
    if !status.is_success() {
        let body_text = response.text().await.unwrap_or_default();
        let error_summary =
            summarize_upstream_error(status.as_u16(), &body_text, UPSTREAM_ERROR_EXCERPT_CHARS);

        if status.as_u16() == 400 && body_text.contains("invalid_grant") {
            return Err(RefreshTokenInvalidError {
                message: format!(
                    "ExternalIdP refreshToken 已失效 (invalid_grant): {}",
                    error_summary
                ),
            }
            .into());
        }

        let error_msg = match status.as_u16() {
            401 => "ExternalIdP 凭证已过期或无效，需要重新认证",
            403 => "权限不足，无法刷新 ExternalIdP Token",
            429 => "请求过于频繁，已被限流",
            500..=599 => "ExternalIdP 服务暂时不可用",
            _ => "ExternalIdP Token 刷新失败",
        };
        bail!("{}: {}", error_msg, error_summary);
    }

    let data: ExternalIdpRefreshResponse = response.json().await?;
    let access_token = data.access_token;
    let mut new_credentials = credentials.clone();
    new_credentials.access_token = Some(access_token.clone());
    new_credentials.auth_method = Some("external_idp".to_string());
    new_credentials.provider = Some("ExternalIdp".to_string());
    new_credentials.token_endpoint = Some(token_endpoint);

    if let Some(new_refresh_token) = data.refresh_token {
        new_credentials.refresh_token = Some(new_refresh_token);
    }

    if let Some(expires_in) = data.expires_in {
        let expires_at = Utc::now() + Duration::seconds(expires_in);
        new_credentials.expires_at = Some(expires_at.to_rfc3339());
    }

    let has_saved_profile_arn = new_credentials
        .profile_arn
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_some();
    if !has_saved_profile_arn {
        match discover_available_profile_arn(&new_credentials, config, &access_token, proxy).await {
            Ok(Some(profile_arn)) => {
                tracing::info!("ExternalIdP 凭据已通过 ListAvailableProfiles 发现可用 profileArn");
                new_credentials.profile_arn = Some(profile_arn);
            }
            Ok(None) => {
                tracing::warn!("ExternalIdP 凭据 ListAvailableProfiles 未返回可用 profileArn");
            }
            Err(err) => {
                tracing::warn!(
                    "ExternalIdP 凭据 ListAvailableProfiles 发现 profileArn 失败: {}",
                    err
                );
            }
        }
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

    let access_token = data.access_token;
    let mut new_credentials = credentials.clone();
    new_credentials.access_token = Some(access_token.clone());

    if let Some(new_refresh_token) = data.refresh_token {
        new_credentials.refresh_token = Some(new_refresh_token);
    }

    if let Some(expires_in) = data.expires_in {
        let expires_at = Utc::now() + Duration::seconds(expires_in);
        new_credentials.expires_at = Some(expires_at.to_rfc3339());
    }

    let is_enterprise = credentials.detected_auth_account_type().as_deref() == Some("enterprise");
    let has_saved_profile_arn = new_credentials
        .profile_arn
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_some();
    if is_enterprise && !has_saved_profile_arn {
        match discover_available_profile_arn(&new_credentials, config, &access_token, proxy).await {
            Ok(Some(profile_arn)) => {
                tracing::info!("Enterprise 凭据已通过 ListAvailableProfiles 发现可用 profileArn");
                new_credentials.profile_arn = Some(profile_arn);
            }
            Ok(None) => {
                tracing::warn!("Enterprise 凭据 ListAvailableProfiles 未返回可用 profileArn");
            }
            Err(err) => {
                tracing::warn!(
                    "Enterprise 凭据 ListAvailableProfiles 发现 profileArn 失败: {}",
                    err
                );
            }
        }
    } else if is_enterprise {
        tracing::debug!("Enterprise 凭据已有保存的 profileArn，跳过自动发现");
    } else if let Some(profile_arn) = data.profile_arn {
        new_credentials.profile_arn = Some(profile_arn);
    }

    Ok(new_credentials)
}

fn configured_api_region_for_profile_discovery<'a>(
    credentials: &'a KiroCredentials,
    config: &'a Config,
) -> &'a str {
    credentials
        .api_region
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .or_else(|| {
            credentials
                .region
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
        })
        .unwrap_or(config.effective_api_region())
}

fn effective_management_endpoint_base_for_credentials(
    credentials: &KiroCredentials,
    config: &Config,
    region: &str,
) -> String {
    if credentials.detected_auth_account_type().as_deref() == Some("enterprise") {
        Config::q_endpoint_base(region)
    } else {
        config.effective_management_endpoint_base(region)
    }
}

fn profile_discovery_regions(credentials: &KiroCredentials, config: &Config) -> Vec<String> {
    let mut regions = Vec::new();
    let mut push_region = |region: &str| {
        let region = region.trim();
        if !region.is_empty() && !regions.iter().any(|existing| existing == region) {
            regions.push(region.to_string());
        }
    };

    push_region(configured_api_region_for_profile_discovery(
        credentials,
        config,
    ));
    if credentials.is_external_idp_auth() {
        // Matches Kiro's commercial default profile scan for external IdP logins.
        push_region("us-east-1");
        push_region("eu-central-1");
    }

    regions
}

fn normalized_next_token(next_token: Option<String>) -> Option<String> {
    next_token
        .map(|token| token.trim().to_string())
        .filter(|token| !token.is_empty())
}

fn append_available_profiles_page(
    aggregated: &mut ListAvailableProfilesResponse,
    page: ListAvailableProfilesResponse,
) -> Option<String> {
    let next_token = normalized_next_token(page.next_token);
    aggregated.profiles.extend(page.profiles);
    next_token
}

async fn list_available_profiles_page(
    credentials: &KiroCredentials,
    config: &Config,
    region: &str,
    token: &str,
    next_token: Option<&str>,
    proxy: Option<&ProxyConfig>,
) -> anyhow::Result<ListAvailableProfilesResponse> {
    tracing::debug!("正在获取可用 Profile 列表...");

    let management_endpoint =
        effective_management_endpoint_base_for_credentials(credentials, config, region);
    let host = Config::endpoint_host(&management_endpoint);
    let machine_id = machine_id::generate_from_credentials(credentials, config)
        .ok_or_else(|| anyhow::anyhow!("无法生成 machineId"))?;
    let kiro_version = &config.kiro_version;
    let os_name = &config.system_version;
    let node_version = &config.node_version;

    let url = format!(
        "{}/ListAvailableProfiles",
        management_endpoint.trim_end_matches('/')
    );
    let user_agent = format!(
        "aws-sdk-js/1.0.0 ua/2.1 os/{} lang/js md/nodejs#{} api/codewhispererruntime#1.0.0 m/N,E KiroIDE-{}-{}",
        os_name, node_version, kiro_version, machine_id
    );
    let amz_user_agent = format!("aws-sdk-js/1.0.0 KiroIDE-{}-{}", kiro_version, machine_id);

    let client = build_client(proxy, 60, config.tls_backend)?;
    let mut body = serde_json::Map::new();
    if let Some(next_token) = next_token.map(str::trim).filter(|value| !value.is_empty()) {
        body.insert(
            "nextToken".to_string(),
            serde_json::Value::String(next_token.to_string()),
        );
    }
    let request = client
        .post(&url)
        .header("content-type", "application/json")
        .header("accept", "application/json")
        .header("x-amz-user-agent", &amz_user_agent)
        .header("user-agent", &user_agent)
        .header("host", &host)
        .header("amz-sdk-invocation-id", uuid::Uuid::new_v4().to_string())
        .header("amz-sdk-request", "attempt=1; max=1")
        .header("Authorization", format!("Bearer {}", token))
        .json(&body);
    let response = external_idp_token_type_header(request, credentials)
        .send()
        .await?;

    let status = response.status();
    if !status.is_success() {
        let body_text = response.text().await.unwrap_or_default();
        let error_summary =
            summarize_upstream_error(status.as_u16(), &body_text, UPSTREAM_ERROR_EXCERPT_CHARS);
        let error_msg = match status.as_u16() {
            401 => "认证失败，Token 无效或已过期",
            403 => "权限不足，无法获取可用 Profile 列表",
            429 => "请求过于频繁，已被限流",
            500..=599 => "服务器错误，AWS 服务暂时不可用",
            _ => "获取可用 Profile 列表失败",
        };
        bail!("{}: {}", error_msg, error_summary);
    }

    Ok(response.json().await?)
}

async fn list_available_profiles(
    credentials: &KiroCredentials,
    config: &Config,
    token: &str,
    proxy: Option<&ProxyConfig>,
) -> anyhow::Result<ListAvailableProfilesResponse> {
    let mut aggregated = ListAvailableProfilesResponse {
        profiles: Vec::new(),
        next_token: None,
    };
    let mut errors = Vec::new();

    for region in profile_discovery_regions(credentials, config) {
        let mut next_token: Option<String> = None;
        let mut seen_next_tokens = HashSet::new();

        loop {
            let page = match list_available_profiles_page(
                credentials,
                config,
                &region,
                token,
                next_token.as_deref(),
                proxy,
            )
            .await
            {
                Ok(page) => page,
                Err(err) => {
                    errors.push(format!("{}: {}", region, err));
                    break;
                }
            };
            next_token = append_available_profiles_page(&mut aggregated, page);

            let Some(token) = next_token.as_deref() else {
                break;
            };
            if !seen_next_tokens.insert(token.to_string()) {
                tracing::warn!("ListAvailableProfiles 返回重复 nextToken，已停止分页避免循环");
                break;
            }
        }
    }

    aggregated.next_token = None;
    if aggregated.profiles.is_empty() && !errors.is_empty() {
        bail!("获取可用 Profile 列表失败: {}", errors.join("; "));
    }
    Ok(aggregated)
}

async fn discover_available_profile_arn(
    credentials: &KiroCredentials,
    config: &Config,
    token: &str,
    proxy: Option<&ProxyConfig>,
) -> anyhow::Result<Option<String>> {
    if credentials.detected_auth_account_type().as_deref() != Some("enterprise") {
        return Ok(None);
    }

    let response = list_available_profiles(credentials, config, token, proxy).await?;
    let region = configured_api_region_for_profile_discovery(credentials, config);
    Ok(response.selected_profile_arn(region))
}

fn is_enterprise_model_access_denied_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("not authorized to make this call")
        || lower.contains("feature_not_supported")
        || lower.contains("权限不足")
}

async fn validate_enterprise_model_access_on_import(
    credentials: &mut KiroCredentials,
    config: &Config,
    token: &str,
    proxy: Option<&ProxyConfig>,
) -> anyhow::Result<()> {
    if credentials.detected_auth_account_type().as_deref() != Some("enterprise") {
        return Ok(());
    }

    let response = match list_available_models(credentials, config, token, None, proxy).await {
        Ok(response) => response,
        Err(err) => {
            let message = err.to_string();
            if is_enterprise_model_access_denied_error(&message) {
                bail!("Enterprise 凭据没有 Kiro 模型调用权限: {}", message);
            }
            bail!("Enterprise 凭据模型授权验证失败: {}", message);
        }
    };

    let model_ids = response.model_ids();
    if model_ids.is_empty() {
        bail!("Enterprise 凭据没有返回可用模型，无法确认 Kiro 模型调用权限");
    }

    let model_count = model_ids.len();
    credentials.available_model_ids = model_ids;
    credentials.available_models_cached_at = Some(Utc::now().to_rfc3339());
    credentials.normalize_model_capabilities();
    tracing::info!(
        model_count,
        "Enterprise 凭据已通过 ListAvailableModels 验证模型授权"
    );

    Ok(())
}

/// 获取使用额度信息
pub(crate) async fn get_usage_limits(
    credentials: &KiroCredentials,
    config: &Config,
    token: &str,
    proxy: Option<&ProxyConfig>,
) -> anyhow::Result<UsageLimitsResponse> {
    tracing::debug!("正在获取使用额度信息...");

    let region = credentials.effective_usage_limits_api_region(config);
    let management_endpoint =
        effective_management_endpoint_base_for_credentials(credentials, config, region);
    let host = Config::endpoint_host(&management_endpoint);
    let machine_id = machine_id::generate_from_credentials(credentials, config)
        .ok_or_else(|| anyhow::anyhow!("无法生成 machineId"))?;
    let kiro_version = &config.kiro_version;
    let os_name = &config.system_version;
    let node_version = &config.node_version;

    // 构建 URL
    let mut url = format!(
        "{}/getUsageLimits?isEmailRequired=true&origin=AI_EDITOR",
        management_endpoint
    );

    // getUsageLimits 只使用显式保存或自动发现到的 profileArn。不要注入
    // BuilderId 固定默认 profileArn；该默认值可能把付费账号查成 FREE 额度。
    if let Some(profile_arn) = credentials.explicit_profile_arn_for_kiro_requests() {
        url.push_str(&format!("&profileArn={}", urlencoding::encode(profile_arn)));
    }

    url.push_str("&resourceType=AGENTIC_REQUEST");

    // 构建 User-Agent headers
    let user_agent = format!(
        "aws-sdk-js/1.0.0 ua/2.1 os/{} lang/js md/nodejs#{} api/codewhispererruntime#1.0.0 m/N,E KiroIDE-{}-{}",
        os_name, node_version, kiro_version, machine_id
    );
    let amz_user_agent = format!("aws-sdk-js/1.0.0 KiroIDE-{}-{}", kiro_version, machine_id);

    let client = build_client(proxy, 60, config.tls_backend)?;

    let request = client
        .get(&url)
        .header("x-amz-user-agent", &amz_user_agent)
        .header("user-agent", &user_agent)
        .header("host", &host)
        .header("amz-sdk-invocation-id", uuid::Uuid::new_v4().to_string())
        .header("amz-sdk-request", "attempt=1; max=1")
        .header("Authorization", format!("Bearer {}", token));
    let response = external_idp_token_type_header(request, credentials)
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

pub(crate) async fn list_available_models_page(
    credentials: &KiroCredentials,
    config: &Config,
    token: &str,
    model_provider: Option<&str>,
    next_token: Option<&str>,
    proxy: Option<&ProxyConfig>,
) -> anyhow::Result<ListAvailableModelsResponse> {
    tracing::debug!("正在获取可用模型列表...");

    let region = credentials.effective_api_region(config);
    let management_endpoint =
        effective_management_endpoint_base_for_credentials(credentials, config, region);
    let host = Config::endpoint_host(&management_endpoint);
    let machine_id = machine_id::generate_from_credentials(credentials, config)
        .ok_or_else(|| anyhow::anyhow!("无法生成 machineId"))?;
    let kiro_version = &config.kiro_version;
    let os_name = &config.system_version;
    let node_version = &config.node_version;

    let mut url = url::Url::parse(&format!(
        "{}/ListAvailableModels",
        management_endpoint.trim_end_matches('/')
    ))?;
    {
        let mut pairs = url.query_pairs_mut();
        pairs.append_pair("origin", "AI_EDITOR");
        pairs.append_pair("maxResults", "50");
        if let Some(profile_arn) = credentials.effective_profile_arn_for_kiro_requests() {
            pairs.append_pair("profileArn", profile_arn);
        }
        if let Some(model_provider) = model_provider
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            pairs.append_pair("modelProvider", model_provider);
        }
        if let Some(next_token) = next_token.map(str::trim).filter(|value| !value.is_empty()) {
            pairs.append_pair("nextToken", next_token);
        }
    }

    let user_agent = format!(
        "aws-sdk-js/1.0.0 ua/2.1 os/{} lang/js md/nodejs#{} api/codewhispererruntime#1.0.0 m/N,E KiroIDE-{}-{}",
        os_name, node_version, kiro_version, machine_id
    );
    let amz_user_agent = format!("aws-sdk-js/1.0.0 KiroIDE-{}-{}", kiro_version, machine_id);

    let client = build_client(proxy, 60, config.tls_backend)?;

    let request = client
        .get(url)
        .header("accept", "application/json")
        .header("x-amz-user-agent", &amz_user_agent)
        .header("user-agent", &user_agent)
        .header("host", &host)
        .header("amz-sdk-invocation-id", uuid::Uuid::new_v4().to_string())
        .header("amz-sdk-request", "attempt=1; max=1")
        .header("Authorization", format!("Bearer {}", token));
    let response = external_idp_token_type_header(request, credentials)
        .send()
        .await?;

    let status = response.status();
    if !status.is_success() {
        let body_text = response.text().await.unwrap_or_default();
        let error_summary =
            summarize_upstream_error(status.as_u16(), &body_text, UPSTREAM_ERROR_EXCERPT_CHARS);
        let error_msg = match status.as_u16() {
            401 => "认证失败，Token 无效或已过期",
            403 => "权限不足，无法获取可用模型列表",
            429 => "请求过于频繁，已被限流",
            500..=599 => "服务器错误，AWS 服务暂时不可用",
            _ => "获取可用模型列表失败",
        };
        bail!("{}: {}", error_msg, error_summary);
    }

    Ok(response.json().await?)
}

pub(crate) async fn list_available_models(
    credentials: &KiroCredentials,
    config: &Config,
    token: &str,
    model_provider: Option<&str>,
    proxy: Option<&ProxyConfig>,
) -> anyhow::Result<ListAvailableModelsResponse> {
    let mut aggregated = ListAvailableModelsResponse {
        available_models: Vec::new(),
        next_token: None,
        default_model: None,
    };
    let mut next_token: Option<String> = None;

    loop {
        let page = list_available_models_page(
            credentials,
            config,
            token,
            model_provider,
            next_token.as_deref(),
            proxy,
        )
        .await?;

        if aggregated.default_model.is_none() {
            aggregated.default_model = page.default_model.clone();
        }
        aggregated.available_models.extend(page.available_models);
        next_token = page.next_token;
        if next_token.is_none() {
            break;
        }
    }

    aggregated.next_token = None;
    Ok(aggregated)
}

pub(crate) async fn set_user_preference_overage_status(
    credentials: &KiroCredentials,
    config: &Config,
    token: &str,
    enabled: bool,
    proxy: Option<&ProxyConfig>,
) -> anyhow::Result<()> {
    tracing::debug!("正在设置超额使用开关...");

    let region = credentials.effective_api_region(config);
    let management_endpoint =
        effective_management_endpoint_base_for_credentials(credentials, config, region);
    let host = Config::endpoint_host(&management_endpoint);
    let machine_id = machine_id::generate_from_credentials(credentials, config)
        .ok_or_else(|| anyhow::anyhow!("无法生成 machineId"))?;
    let kiro_version = &config.kiro_version;
    let os_name = &config.system_version;
    let node_version = &config.node_version;
    let url = format!("{}/setUserPreference", management_endpoint);
    let overage_status = if enabled { "ENABLED" } else { "DISABLED" };

    let user_agent = format!(
        "aws-sdk-js/1.0.0 ua/2.1 os/{} lang/js md/nodejs#{} api/codewhispererruntime#1.0.0 m/N,E KiroIDE-{}-{}",
        os_name, node_version, kiro_version, machine_id
    );
    let amz_user_agent = format!("aws-sdk-js/1.0.0 KiroIDE-{}-{}", kiro_version, machine_id);

    let mut body = serde_json::json!({
        "overageConfiguration": {
            "overageStatus": overage_status
        }
    });

    if let Some(profile_arn) = credentials.effective_profile_arn_for_kiro_requests() {
        body["profileArn"] = serde_json::Value::String(profile_arn.to_string());
    }

    let client = build_client(proxy, 60, config.tls_backend)?;

    let request = client
        .post(&url)
        .header("content-type", "application/json")
        .header("accept", "application/json")
        .header("x-amz-user-agent", &amz_user_agent)
        .header("user-agent", &user_agent)
        .header("host", &host)
        .header("amz-sdk-invocation-id", uuid::Uuid::new_v4().to_string())
        .header("amz-sdk-request", "attempt=1; max=1")
        .header("Authorization", format!("Bearer {}", token))
        .json(&body);
    let response = external_idp_token_type_header(request, credentials)
        .send()
        .await?;

    let status = response.status();
    if !status.is_success() {
        let body_text = response.text().await.unwrap_or_default();
        let error_summary =
            summarize_upstream_error(status.as_u16(), &body_text, UPSTREAM_ERROR_EXCERPT_CHARS);
        let error_msg = match status.as_u16() {
            401 => "认证失败，Token 无效或已过期",
            403 => "权限不足，无法设置超额使用开关",
            429 => "请求过于频繁，已被限流",
            500..=599 => "服务器错误，AWS 服务暂时不可用",
            _ => "设置超额使用开关失败",
        };

        return Err(KiroManagementApiError {
            api: "setUserPreference",
            status_code: status.as_u16(),
            message: format!("{}: {}", error_msg, error_summary),
        }
        .into());
    }

    Ok(())
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
    /// 完整响应 token 用量记录次数
    token_usage_count: u64,
    /// 自上次落盘后的完整响应 token 用量记录次数
    pending_token_usage_count_delta: u64,
    /// 完整响应累计输入 tokens
    input_tokens: u64,
    /// 自上次落盘后的新增输入 tokens
    pending_input_tokens_delta: u64,
    /// 完整响应累计输出 tokens
    output_tokens: u64,
    /// 自上次落盘后的新增输出 tokens
    pending_output_tokens_delta: u64,
    /// 最后一次 API 调用时间（RFC3339 格式）
    last_used_at: Option<String>,
    /// 当前运行中的请求数
    active_requests: usize,
    /// 限流或临时避让冷却到期时间
    rate_limit_cooldown_until: Option<Instant>,
    /// 后台 Token 刷新期间的本地避让冷却到期时间
    background_refresh_cooldown_until: Option<Instant>,
    /// 本地 token bucket 与自适应退避状态
    rate_limit_bucket: Option<AdaptiveTokenBucket>,
    /// 连续 429 次数，用于放大冷却时间
    rate_limit_hit_streak: u32,
    /// 本实例是否已经为该凭据启动后台刷新
    background_refresh_in_progress: bool,
    /// 是否正在后台刷新该凭据的可用模型列表
    available_models_refresh_in_progress: bool,
    /// 可用模型列表刷新失败后的本地冷却到期时间
    available_models_refresh_cooldown_until: Option<Instant>,
    /// 凭据级刷新锁，避免不同账号刷新 token 时互相串行阻塞
    refresh_lock: Arc<TokioMutex<()>>,
}

struct SuspiciousActivityMarkerUpdate {
    count: u32,
    first_seen_at: Option<String>,
    last_seen_at: Option<String>,
    quarantine_until: Option<String>,
    should_auto_disable: bool,
}

/// 禁用原因
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DisabledReason {
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
    /// 上游明确提示账号被暂停或锁定
    AccountSuspended,
    /// Token 或授权状态无效，刷新后仍不可用
    AuthInvalid,
    /// 上游拒绝访问但未给出更细粒度原因
    PermissionDenied,
    /// 历史禁用原因：Enterprise 凭据缺少账号特定 profileArn
    MissingProfileArn,
    /// 上游 suspicious activity 风控多次命中后自动停调
    SuspiciousActivity,
}

impl DisabledReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::Manual => "Manual",
            Self::TooManyFailures => "TooManyFailures",
            Self::TooManyRefreshFailures => "TooManyRefreshFailures",
            Self::QuotaExceeded => "QuotaExceeded",
            Self::InvalidRefreshToken => "InvalidRefreshToken",
            Self::AccountSuspended => "AccountSuspended",
            Self::AuthInvalid => "AuthInvalid",
            Self::PermissionDenied => "PermissionDenied",
            Self::MissingProfileArn => "MissingProfileArn",
            Self::SuspiciousActivity => "SuspiciousActivity",
        }
    }

    fn from_persisted(value: &str) -> Option<Self> {
        match value {
            "Manual" => Some(Self::Manual),
            "TooManyFailures" => Some(Self::TooManyFailures),
            "TooManyRefreshFailures" => Some(Self::TooManyRefreshFailures),
            "QuotaExceeded" => Some(Self::QuotaExceeded),
            "InvalidRefreshToken" => Some(Self::InvalidRefreshToken),
            "AccountSuspended" => Some(Self::AccountSuspended),
            "AuthInvalid" => Some(Self::AuthInvalid),
            "PermissionDenied" => Some(Self::PermissionDenied),
            "MissingProfileArn" => Some(Self::MissingProfileArn),
            "SuspiciousActivity" => Some(Self::SuspiciousActivity),
            _ => None,
        }
    }
}

fn persisted_disabled_reason(credentials: &KiroCredentials) -> DisabledReason {
    credentials
        .disabled_reason
        .as_deref()
        .and_then(DisabledReason::from_persisted)
        .unwrap_or(DisabledReason::Manual)
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
    /// 登录 Provider（Google / Github / BuilderId / Enterprise）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    /// 是否有当前生效的 Profile ARN
    pub has_profile_arn: bool,
    /// 当前生效的 Profile ARN（显式保存、发现得到或账号类型默认值）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_arn: Option<String>,
    /// Token 过期时间
    pub expires_at: Option<String>,
    /// refreshToken 的 SHA-256 哈希（用于前端重复检测）
    pub refresh_token_hash: Option<String>,
    /// 用户邮箱（用于前端显示）
    pub email: Option<String>,
    /// 用户 ID（企业账号可能没有 email）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
    /// 订阅等级（KIRO PRO+ / KIRO FREE 等）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subscription_title: Option<String>,
    /// 订阅内部类型（如 Q_DEVELOPER_STANDALONE_PRO）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subscription_type: Option<String>,
    /// 识别出的认证账号类型（social / builder-id / enterprise / idc）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_account_type: Option<String>,
    /// 账号类型
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account_type: Option<String>,
    /// 账号来源供应商 ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_supplier_id: Option<String>,
    /// 账号来源供应商名称
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_supplier_name: Option<String>,
    /// 账号来源批次
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_batch: Option<String>,
    /// 凭据分组标记
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub credential_groups: Vec<String>,
    /// 当前命中的账号类型（显式账号类型或由订阅信息推断）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolved_account_type: Option<String>,
    /// 当前账号类型来源：credential / subscription-title / subscription-type
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account_type_source: Option<String>,
    /// 账号级额外允许模型
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub allowed_models: Vec<String>,
    /// 账号级额外禁用模型
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub blocked_models: Vec<String>,
    /// 运行时探测到的临时模型限制
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub runtime_model_restrictions: Vec<crate::model::model_policy::RuntimeModelRestriction>,
    /// 从 ListAvailableModels 拉取到的账号可用模型 ID 缓存
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub available_model_ids: Vec<String>,
    /// 可用模型缓存刷新时间
    #[serde(skip_serializing_if = "Option::is_none")]
    pub available_models_cached_at: Option<String>,
    /// 导入时间（RFC3339 格式）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub imported_at: Option<String>,
    /// API 调用成功次数
    pub success_count: u64,
    /// 完整响应 token 用量记录次数
    pub token_usage_count: u64,
    /// 完整响应累计输入 tokens
    pub input_tokens: u64,
    /// 完整响应累计输出 tokens
    pub output_tokens: u64,
    /// 完整响应累计总 tokens
    pub total_tokens: u64,
    /// 最后一次 API 调用时间（RFC3339 格式）
    pub last_used_at: Option<String>,
    /// 当前运行中的请求数
    pub active_requests: usize,
    /// 当前生效的单账号并发上限（空表示不限制）
    pub max_concurrency: Option<u32>,
    /// 凭据级显式并发覆盖
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_concurrency_override: Option<u32>,
    /// 当前并发上限来源：credential / account-type / global-default
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_concurrency_source: Option<String>,
    /// 是否配置了凭据级代理
    pub has_proxy: bool,
    /// 代理 URL（用于前端展示）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proxy_url: Option<String>,
    /// 代理池绑定 ID（用于前端展示）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proxy_id: Option<String>,
    /// Token 刷新连续失败次数
    pub refresh_failure_count: u32,
    /// 禁用原因
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disabled_reason: Option<String>,
    /// 禁用时间
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disabled_at: Option<String>,
    /// 最近一次异常状态码
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error_status: Option<u16>,
    /// 最近一次异常摘要
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error_summary: Option<String>,
    /// suspicious activity 命中次数（当前统计窗口内）
    pub suspicious_activity_count: u32,
    /// 当前统计窗口内首次命中 suspicious activity 的时间
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suspicious_activity_first_seen_at: Option<String>,
    /// 最近一次命中 suspicious activity 的时间
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suspicious_activity_last_seen_at: Option<String>,
    /// suspicious activity 账号级隔离到期时间
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suspicious_activity_quarantine_until: Option<String>,
    /// 最近一次 suspicious activity 后累计的成功请求次数
    pub suspicious_activity_recovery_success_count: u32,
    /// suspicious activity 隔离剩余时间（毫秒）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub suspicious_activity_quarantine_remaining_ms: Option<u64>,
    /// 当前生效的 429 冷却与 bucket 退避开关
    pub rate_limit_cooldown_enabled: bool,
    /// 凭据级 429 冷却与 bucket 退避开关覆盖
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limit_cooldown_enabled_override: Option<bool>,
    /// 当前 429 冷却与 bucket 退避开关来源：credential / global-default
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limit_cooldown_enabled_source: Option<String>,
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
    /// 当前 bucket 容量来源：credential / account-type / global-default
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limit_bucket_capacity_source: Option<String>,
    /// 当前生效回填速率（token/s）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limit_refill_per_second: Option<f64>,
    /// 凭据级回填速率覆盖
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limit_refill_per_second_override: Option<f64>,
    /// 当前回填速率来源：credential / account-type / global-default
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limit_refill_per_second_source: Option<String>,
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
    pub session_affinity_enabled: bool,
    pub queue_max_size: usize,
    pub queue_max_wait_ms: u64,
    pub rate_limit_cooldown_ms: u64,
    pub rate_limit_cooldown_enabled: bool,
    pub suspicious_activity_cooldown_ms: u64,
    pub suspicious_activity_cooldown_enabled: bool,
    pub suspicious_activity_prefer_clean_credentials: bool,
    pub suspicious_activity_auto_disable_enabled: bool,
    pub suspicious_activity_auto_disable_threshold: u32,
    pub suspicious_activity_auto_disable_window_ms: u64,
    pub suspicious_activity_auto_clear_enabled: bool,
    pub suspicious_activity_auto_clear_success_threshold: u32,
    pub suspicious_activity_auto_clear_after_ms: u64,
    pub model_cooldown_enabled: bool,
    pub default_max_concurrency: Option<u32>,
    pub rate_limit_bucket_capacity: f64,
    pub rate_limit_refill_per_second: f64,
    pub rate_limit_refill_min_per_second: f64,
    pub rate_limit_refill_recovery_step_per_success: f64,
    pub rate_limit_refill_backoff_factor: f64,
    pub request_weighting: RequestWeightingConfig,
    pub stream_dispatch_lease_release_enabled: bool,
    pub stream_pre_sse_failover: StreamPreSseFailoverConfig,
    pub non_stream_body_read_timeout: NonStreamBodyReadTimeoutConfig,
    pub kiro_request_body_guard: KiroRequestBodyGuardConfig,
    pub thinking_signature_validation_mode: ThinkingSignatureValidationMode,
    pub response_thinking_signature_compat_enabled: bool,
    pub proxy_pool: ProxyPoolConfig,
    pub waiting_requests: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct LocalRequestCounts {
    pub active_requests: usize,
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
    session_affinity_enabled: bool,
    queue_max_size: usize,
    queue_max_wait_ms: u64,
    rate_limit_cooldown_ms: u64,
    rate_limit_cooldown_enabled: bool,
    suspicious_activity_cooldown_ms: u64,
    suspicious_activity_cooldown_enabled: bool,
    suspicious_activity_prefer_clean_credentials: bool,
    suspicious_activity_auto_disable_enabled: bool,
    suspicious_activity_auto_disable_threshold: u32,
    suspicious_activity_auto_disable_window_ms: u64,
    suspicious_activity_auto_clear_enabled: bool,
    suspicious_activity_auto_clear_success_threshold: u32,
    suspicious_activity_auto_clear_after_ms: u64,
    model_cooldown_enabled: bool,
    default_max_concurrency: Option<u32>,
    rate_limit_bucket_capacity: f64,
    rate_limit_refill_per_second: f64,
    rate_limit_refill_min_per_second: f64,
    rate_limit_refill_recovery_step_per_success: f64,
    rate_limit_refill_backoff_factor: f64,
    request_weighting: RequestWeightingConfig,
    stream_dispatch_lease_release_enabled: bool,
    stream_pre_sse_failover: StreamPreSseFailoverConfig,
    non_stream_body_read_timeout: NonStreamBodyReadTimeoutConfig,
    kiro_request_body_guard: KiroRequestBodyGuardConfig,
    thinking_signature_validation_mode: ThinkingSignatureValidationMode,
    response_thinking_signature_compat_enabled: bool,
    proxy_pool: ProxyPoolConfig,
    account_type_policies: BTreeMap<String, ModelSupportPolicy>,
    account_type_dispatch_policies: BTreeMap<String, AccountTypeDispatchPolicy>,
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
        let mut account_type_dispatch_policies = config.account_type_dispatch_policies.clone();
        normalize_account_type_dispatch_policies(&mut account_type_dispatch_policies);

        Self {
            mode: config.load_balancing_mode.clone(),
            session_affinity_enabled: config.session_affinity_enabled,
            queue_max_size: config.queue_max_size,
            queue_max_wait_ms: config.queue_max_wait_ms,
            rate_limit_cooldown_ms: config.rate_limit_cooldown_ms,
            rate_limit_cooldown_enabled: config.rate_limit_cooldown_enabled,
            suspicious_activity_cooldown_ms: config.suspicious_activity_cooldown_ms,
            suspicious_activity_cooldown_enabled: config.suspicious_activity_cooldown_enabled,
            suspicious_activity_prefer_clean_credentials: config
                .suspicious_activity_prefer_clean_credentials,
            suspicious_activity_auto_disable_enabled: config
                .suspicious_activity_auto_disable_enabled,
            suspicious_activity_auto_disable_threshold: config
                .suspicious_activity_auto_disable_threshold,
            suspicious_activity_auto_disable_window_ms: config
                .suspicious_activity_auto_disable_window_ms,
            suspicious_activity_auto_clear_enabled: config.suspicious_activity_auto_clear_enabled,
            suspicious_activity_auto_clear_success_threshold: config
                .suspicious_activity_auto_clear_success_threshold,
            suspicious_activity_auto_clear_after_ms: config.suspicious_activity_auto_clear_after_ms,
            model_cooldown_enabled: config.model_cooldown_enabled,
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
            stream_dispatch_lease_release_enabled: config.stream_dispatch_lease_release_enabled,
            stream_pre_sse_failover: config.stream_pre_sse_failover.clone(),
            non_stream_body_read_timeout: config.non_stream_body_read_timeout.clone(),
            kiro_request_body_guard: config.kiro_request_body_guard.clone(),
            thinking_signature_validation_mode: config.thinking_signature_validation_mode,
            response_thinking_signature_compat_enabled: config
                .response_thinking_signature_compat_enabled,
            proxy_pool: config.proxy_pool.clone(),
            account_type_policies,
            account_type_dispatch_policies,
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

    fn account_type_dispatch_policy_for<'a>(
        &'a self,
        credentials: &'a KiroCredentials,
    ) -> Option<&'a AccountTypeDispatchPolicy> {
        credentials.account_type_dispatch_policy(&self.account_type_dispatch_policies)
    }

    fn effective_max_concurrency_for(&self, credentials: &KiroCredentials) -> Option<usize> {
        credentials.effective_max_concurrency_with_policy(
            self.default_max_concurrency,
            self.account_type_dispatch_policy_for(credentials),
        )
    }

    fn effective_max_concurrency_source_for(
        &self,
        credentials: &KiroCredentials,
    ) -> Option<String> {
        credentials
            .effective_max_concurrency_source(
                self.default_max_concurrency,
                self.account_type_dispatch_policy_for(credentials),
            )
            .map(|source| source.as_str().to_string())
    }

    fn rate_limit_cooldown_enabled_for(&self, credentials: &KiroCredentials) -> bool {
        credentials.effective_rate_limit_cooldown_enabled(self.rate_limit_cooldown_enabled)
    }

    fn rate_limit_cooldown_enabled_source_for(
        &self,
        credentials: &KiroCredentials,
    ) -> Option<String> {
        credentials
            .effective_rate_limit_cooldown_enabled_source()
            .map(|source| source.as_str().to_string())
    }

    fn rate_limit_bucket_capacity_source_for(
        &self,
        credentials: &KiroCredentials,
    ) -> Option<String> {
        credentials
            .effective_rate_limit_bucket_capacity_source(
                self.rate_limit_bucket_capacity,
                self.account_type_dispatch_policy_for(credentials),
            )
            .map(|source| source.as_str().to_string())
    }

    fn rate_limit_refill_per_second_source_for(
        &self,
        credentials: &KiroCredentials,
    ) -> Option<String> {
        credentials
            .effective_rate_limit_refill_per_second_source(
                self.rate_limit_refill_per_second,
                self.account_type_dispatch_policy_for(credentials),
            )
            .map(|source| source.as_str().to_string())
    }

    fn bucket_policy_for(&self, credentials: &KiroCredentials) -> Option<TokenBucketPolicy> {
        let account_type_dispatch_policy = self.account_type_dispatch_policy_for(credentials);
        let capacity = credentials
            .rate_limit_bucket_capacity_override()
            .or_else(|| {
                account_type_dispatch_policy
                    .and_then(AccountTypeDispatchPolicy::rate_limit_bucket_capacity_override)
            })
            .unwrap_or(self.rate_limit_bucket_capacity);
        let refill_per_second = credentials
            .rate_limit_refill_per_second_override()
            .or_else(|| {
                account_type_dispatch_policy
                    .and_then(AccountTypeDispatchPolicy::rate_limit_refill_per_second_override)
            })
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
    /// 凭据分组目录只用于管理写入治理，不参与热路径调度匹配。
    credential_group_catalog: Mutex<Vec<CredentialGroupConfig>>,
    /// 可用性变更通知（并发释放、凭据启用、配置变更等）
    availability_notify: Arc<Notify>,
    /// 当前正在等待可用槽位的请求数
    waiting_requests: Arc<std::sync::atomic::AtomicUsize>,
    /// 本地会话到凭据的软亲和缓存；未配置 Redis 时使用。
    session_affinity_cache: Mutex<HashMap<String, SessionAffinityEntry>>,
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
    /// 402 后台用量刷新本地去重冷却
    usage_balance_refresh_cooldowns: Mutex<HashMap<u64, Instant>>,
    /// 代理池节点的本地健康状态；持久化的是凭据绑定关系，不持久化瞬时健康。
    proxy_health: Mutex<HashMap<String, ProxyHealthState>>,
}

#[derive(Debug, Clone)]
struct SessionAffinityEntry {
    credential_id: u64,
    expires_at: Instant,
}

#[derive(Debug, Clone, Default)]
struct ProxyHealthState {
    consecutive_failures: u32,
    unavailable_until: Option<Instant>,
    last_error: Option<String>,
}

/// 每个凭据最大 API 调用失败次数
const MAX_FAILURES_PER_CREDENTIAL: u32 = 3;
/// 共享凭据 refresh 请求的分布式租约 TTL。
/// refresh_token HTTP 客户端默认超时为 60s，这里保留足够缓冲，避免租约过早失效。
const RUNTIME_REFRESH_LEASE_TTL: StdDuration = StdDuration::from_secs(90);
/// 共享调度占位 TTL。请求异常退出时依赖该 TTL 自动回收全局并发占位。
const SHARED_DISPATCH_LEASE_TTL_MS: u64 = 120_000;
/// 共享调度占位续租心跳间隔。
const SHARED_DISPATCH_LEASE_HEARTBEAT_INTERVAL_MS: u64 = 30_000;
/// 共享调度运行态下，等待队列需要短周期轮询 Redis 以观察跨副本释放。
const SHARED_DISPATCH_WAIT_POLL_INTERVAL_MS: u64 = 200;
/// 后台刷新进行中时，本地/共享调度避让该凭据的窗口。
///
/// 刷新 HTTP 客户端默认 60s 超时，使用与分布式 refresh lease 相同的 90s 上限，
/// 刷新完成后会主动清理该冷却。
const BACKGROUND_REFRESH_IN_PROGRESS_COOLDOWN: StdDuration = RUNTIME_REFRESH_LEASE_TTL;
/// 机会性后台刷新遇到瞬态失败时的重试冷却。
///
/// 后台刷新不服务当前请求，失败通常是 429/网络抖动，使用较长冷却避免把刷新端点打成重试风暴。
const BACKGROUND_REFRESH_RETRY_COOLDOWN: StdDuration = StdDuration::from_secs(5 * 60);
/// 可用模型列表刷新失败后的重试冷却。
///
/// 该刷新只用于调度优化，失败不应阻塞请求热路径，也不应在响应结构变化时反复打管理接口。
const AVAILABLE_MODELS_REFRESH_RETRY_COOLDOWN: StdDuration = StdDuration::from_secs(10 * 60);
/// 402 触发的用量缓存刷新冷却，避免同一账号错误风暴时重复打管理接口。
const USAGE_BALANCE_REFRESH_COOLDOWN: StdDuration = StdDuration::from_secs(60);
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

#[derive(Debug)]
enum ReservationFailure {
    NoCredentials,
    AllDisabled,
    NoCredentialScopeMatch,
    NoModelSupport,
    AllTemporarilyUnavailable { next_ready_at: Option<Instant> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProxyDispatchAvailability {
    Available,
    TemporarilyUnavailable { ready_at: Instant },
    Unavailable,
}

#[derive(Debug)]
struct SharedDispatchSnapshotCandidates {
    credentials: Vec<DispatchRuntimeCredential>,
    candidate_ids: HashSet<u64>,
    next_ready_at: Option<Instant>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BackgroundRefreshOutcome {
    Success,
    RetryLater,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AvailableModelsRefreshOutcome {
    Success,
    RetryLater,
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
        let credential_group_catalog = config.credential_groups.clone();
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
                        Some(persisted_disabled_reason(&cred))
                    } else {
                        None
                    },
                    success_count: 0,
                    pending_success_count_delta: 0,
                    token_usage_count: 0,
                    pending_token_usage_count_delta: 0,
                    input_tokens: 0,
                    pending_input_tokens_delta: 0,
                    output_tokens: 0,
                    pending_output_tokens_delta: 0,
                    last_used_at: None,
                    active_requests: 0,
                    rate_limit_cooldown_until: None,
                    background_refresh_cooldown_until: None,
                    rate_limit_bucket: dispatch_config
                        .bucket_policy_for(&cred)
                        .map(|policy| AdaptiveTokenBucket::new(policy, now)),
                    rate_limit_hit_streak: 0,
                    background_refresh_in_progress: false,
                    available_models_refresh_in_progress: false,
                    available_models_refresh_cooldown_until: None,
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
            credential_group_catalog: Mutex::new(credential_group_catalog),
            availability_notify: Arc::new(Notify::new()),
            waiting_requests: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            session_affinity_cache: Mutex::new(HashMap::new()),
            state_write_lock: Mutex::new(()),
            last_state_change_revisions: Mutex::new(initial_state_change_revisions),
            hot_path_state_sync_origin: Instant::now(),
            last_hot_path_state_sync_check_ms: AtomicU64::new(u64::MAX),
            last_stats_save_at: Mutex::new(None),
            stats_dirty: AtomicBool::new(false),
            usage_balance_refresh_cooldowns: Mutex::new(HashMap::new()),
            proxy_health: Mutex::new(HashMap::new()),
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

        let initial_dispatch = manager.dispatch_config();
        manager.clear_disabled_rate_limit_penalties(&initial_dispatch);
        if !initial_dispatch.model_cooldown_enabled {
            if let Err(err) = manager.clear_all_runtime_model_restrictions() {
                tracing::warn!("启动时清理运行时模型限制失败: {}", err);
            }
        }

        Ok(manager)
    }

    /// 获取配置的引用
    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn credential_group_catalog_snapshot(&self) -> Vec<CredentialGroupConfig> {
        self.credential_group_catalog.lock().clone()
    }

    pub fn set_credential_group_catalog_snapshot(&self, groups: Vec<CredentialGroupConfig>) {
        *self.credential_group_catalog.lock() = groups;
    }

    pub fn set_credential_group_catalog_config(
        &self,
        groups: Vec<CredentialGroupConfig>,
    ) -> anyhow::Result<()> {
        let previous = self.credential_group_catalog_snapshot();
        if previous == groups {
            return Ok(());
        }

        let _state_write_guard = self.state_write_lock.lock();
        *self.credential_group_catalog.lock() = groups;

        if let Err(err) = self.persist_dispatch_config(&self.dispatch_config()) {
            *self.credential_group_catalog.lock() = previous;
            return Err(err);
        }

        Ok(())
    }

    /// 获取凭据总数
    pub fn total_count(&self) -> usize {
        self.entries.lock().len()
    }

    /// 获取可用凭据数量
    pub fn available_count(&self) -> usize {
        self.entries.lock().iter().filter(|e| !e.disabled).count()
    }

    fn runtime_refresh_lease_enabled(&self) -> bool {
        self.state_store.is_external() && self.state_store.runtime_refresh_lease_enabled()
    }

    pub fn should_fast_fail_runtime_leader_refresh(&self) -> bool {
        !self.runtime_refresh_lease_enabled()
            && self.shared_dispatch_runtime_enabled()
            && self.state_store.runtime_coordination_enabled()
            && self.available_count() <= 1
    }

    pub fn leader_http_base_url_for_single_shared_credential_mode(
        &self,
    ) -> anyhow::Result<Option<String>> {
        if !self.should_fast_fail_runtime_leader_refresh() {
            return Ok(None);
        }

        self.runtime_leader_http_base_url()
    }

    pub fn runtime_leader_http_base_url(&self) -> anyhow::Result<Option<String>> {
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

    fn truncate_for_log(value: &str, max_chars: usize) -> String {
        if value.chars().count() <= max_chars {
            return value.to_string();
        }
        let keep = max_chars.saturating_sub(3);
        let mut truncated: String = value.chars().take(keep).collect();
        truncated.push_str("...");
        truncated
    }

    pub fn proxy_pool_config_snapshot(&self) -> ProxyPoolConfig {
        self.dispatch_config().proxy_pool
    }

    fn pool_entry_to_proxy_config(entry: &ProxyPoolEntry) -> ProxyConfig {
        let mut proxy = ProxyConfig::new(entry.url.trim());
        if let (Some(username), Some(password)) = (&entry.username, &entry.password) {
            proxy = proxy.with_auth(username, password);
        }
        proxy
    }

    pub fn proxy_config_for_id(&self, proxy_id: &str) -> Option<ProxyConfig> {
        let proxy_id = proxy_id.trim();
        if proxy_id.is_empty() {
            return None;
        }

        let pool = self.dispatch_config().proxy_pool;
        if !pool.enabled {
            return None;
        }

        pool.proxies
            .iter()
            .find(|entry| entry.enabled && entry.id.trim() == proxy_id)
            .map(Self::pool_entry_to_proxy_config)
    }

    fn proxy_pool_unavailable_until(&self, proxy_id: &str, now: Instant) -> Option<Instant> {
        let mut health = self.proxy_health.lock();
        let Some(state) = health.get_mut(proxy_id) else {
            return None;
        };

        if let Some(unavailable_until) = state.unavailable_until {
            if unavailable_until > now {
                return Some(unavailable_until);
            }
            state.unavailable_until = None;
            state.consecutive_failures = 0;
            state.last_error = None;
        }

        None
    }

    fn explicit_proxy_for_credentials(
        credentials: &KiroCredentials,
    ) -> Option<Option<ProxyConfig>> {
        let proxy_url = credentials
            .proxy_url
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())?;

        if proxy_url.eq_ignore_ascii_case(KiroCredentials::PROXY_DIRECT) {
            return Some(None);
        }

        let mut proxy = ProxyConfig::new(proxy_url);
        if let (Some(username), Some(password)) =
            (&credentials.proxy_username, &credentials.proxy_password)
        {
            proxy = proxy.with_auth(username, password);
        }
        Some(Some(proxy))
    }

    pub fn effective_proxy_for_credentials(
        &self,
        credentials: &KiroCredentials,
    ) -> anyhow::Result<Option<ProxyConfig>> {
        let dispatch = self.dispatch_config();
        let pool = &dispatch.proxy_pool;

        if let Some(explicit_proxy) = Self::explicit_proxy_for_credentials(credentials) {
            if explicit_proxy.is_none() && pool.enabled && pool.require_proxy {
                anyhow::bail!("proxyPool.requireProxy=true 时凭据不能使用 direct");
            }
            return Ok(explicit_proxy);
        }

        if let Some(proxy_id) = credentials
            .proxy_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            if !pool.enabled {
                tracing::warn!(
                    proxy_id,
                    credential_id = credentials.id.unwrap_or_default(),
                    "凭据绑定了代理池 ID，但代理池未启用，将回退到全局代理"
                );
                return Ok(self.proxy.clone());
            }

            let Some(entry) = pool
                .proxies
                .iter()
                .find(|entry| entry.enabled && entry.id.trim() == proxy_id)
            else {
                anyhow::bail!(
                    "凭据 #{} 绑定的代理池 ID 不存在或未启用: {}",
                    credentials.id.unwrap_or_default(),
                    proxy_id
                );
            };

            if let Some(unavailable_until) =
                self.proxy_pool_unavailable_until(proxy_id, Instant::now())
            {
                let remaining_ms = unavailable_until
                    .saturating_duration_since(Instant::now())
                    .as_millis();
                anyhow::bail!(
                    "凭据 #{} 绑定的代理池节点 {} 正在冷却中（剩余约 {} ms）",
                    credentials.id.unwrap_or_default(),
                    proxy_id,
                    remaining_ms
                );
            }

            return Ok(Some(Self::pool_entry_to_proxy_config(entry)));
        }

        if pool.enabled && pool.require_proxy {
            anyhow::bail!(
                "proxyPool.requireProxy=true，但凭据 #{} 未绑定代理池节点",
                credentials.id.unwrap_or_default()
            );
        }

        Ok(self.proxy.clone())
    }

    fn proxy_dispatch_availability(
        &self,
        dispatch: &DispatchConfig,
        credentials: &KiroCredentials,
        now: Instant,
    ) -> ProxyDispatchAvailability {
        let pool = &dispatch.proxy_pool;

        if let Some(explicit_proxy) = Self::explicit_proxy_for_credentials(credentials) {
            if explicit_proxy.is_none() && pool.enabled && pool.require_proxy {
                return ProxyDispatchAvailability::Unavailable;
            }
            return ProxyDispatchAvailability::Available;
        }

        let proxy_id = credentials
            .proxy_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());

        let Some(proxy_id) = proxy_id else {
            return if pool.enabled && pool.require_proxy {
                ProxyDispatchAvailability::Unavailable
            } else {
                ProxyDispatchAvailability::Available
            };
        };

        if !pool.enabled {
            return ProxyDispatchAvailability::Available;
        }

        if !pool
            .proxies
            .iter()
            .any(|entry| entry.enabled && entry.id.trim() == proxy_id)
        {
            return ProxyDispatchAvailability::Unavailable;
        }

        if let Some(ready_at) = self.proxy_pool_unavailable_until(proxy_id, now) {
            return ProxyDispatchAvailability::TemporarilyUnavailable { ready_at };
        }

        ProxyDispatchAvailability::Available
    }

    fn proxy_pool_candidate_healthy(&self, proxy_id: &str, now: Instant) -> bool {
        self.proxy_pool_unavailable_until(proxy_id, now).is_none()
    }

    fn select_proxy_id_from_pool(
        &self,
        pool: &ProxyPoolConfig,
        persisted: &[KiroCredentials],
        exclude_proxy_id: Option<&str>,
        hash_key: Option<&str>,
    ) -> Option<String> {
        let now = Instant::now();
        let exclude_proxy_id = exclude_proxy_id
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let mut candidates: Vec<&ProxyPoolEntry> = pool
            .proxies
            .iter()
            .filter(|entry| {
                let id = entry.id.trim();
                entry.enabled
                    && !id.is_empty()
                    && exclude_proxy_id != Some(id)
                    && self.proxy_pool_candidate_healthy(id, now)
            })
            .collect();

        if candidates.is_empty() {
            return None;
        }

        candidates.sort_by(|a, b| a.id.cmp(&b.id));

        if pool.assignment_strategy.trim() == "hash" {
            let hash_key = hash_key.unwrap_or_default();
            let hash = sha256_hex(hash_key);
            let bucket = u64::from_str_radix(&hash[..16], 16).unwrap_or(0) as usize;
            return candidates
                .get(bucket % candidates.len())
                .map(|entry| entry.id.trim().to_string());
        }

        let mut counts: HashMap<String, usize> = HashMap::new();
        for credential in persisted {
            if credential.disabled {
                continue;
            }
            if let Some(proxy_id) = credential
                .proxy_id
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                *counts.entry(proxy_id.to_string()).or_insert(0) += 1;
            }
        }

        candidates
            .into_iter()
            .min_by(|left, right| {
                let left_count = *counts.get(left.id.trim()).unwrap_or(&0) as u128;
                let right_count = *counts.get(right.id.trim()).unwrap_or(&0) as u128;
                let left_weight = u128::from(left.weight.max(1));
                let right_weight = u128::from(right.weight.max(1));
                (left_count * right_weight)
                    .cmp(&(right_count * left_weight))
                    .then_with(|| left.id.cmp(&right.id))
            })
            .map(|entry| entry.id.trim().to_string())
    }

    fn assign_proxy_for_new_credential(
        &self,
        credentials: &mut KiroCredentials,
        persisted: &[KiroCredentials],
    ) -> anyhow::Result<()> {
        let pool = self.dispatch_config().proxy_pool;
        if !pool.enabled {
            return Ok(());
        }

        if let Some(proxy_url) = credentials
            .proxy_url
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            if pool.require_proxy && proxy_url.eq_ignore_ascii_case(KiroCredentials::PROXY_DIRECT) {
                anyhow::bail!("proxyPool.requireProxy=true 时新凭据不能指定 direct");
            }
            return Ok(());
        }

        if let Some(proxy_id) = credentials
            .proxy_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            let exists = pool
                .proxies
                .iter()
                .any(|entry| entry.enabled && entry.id.trim() == proxy_id);
            if exists {
                credentials.proxy_id = Some(proxy_id.to_string());
                return Ok(());
            }
            anyhow::bail!("指定的代理池 ID 不存在或未启用: {}", proxy_id);
        }

        let selected = self.select_proxy_id_from_pool(
            &pool,
            persisted,
            None,
            credentials.refresh_token.as_deref(),
        );

        match selected {
            Some(proxy_id) => {
                credentials.proxy_id = Some(proxy_id.clone());
                tracing::info!(proxy_id, "已按代理池策略为新凭据自动分配代理");
                Ok(())
            }
            None if pool.require_proxy => {
                anyhow::bail!("proxyPool.requireProxy=true，但当前没有可用的代理池节点")
            }
            None => Ok(()),
        }
    }

    fn trim_optional_string(value: Option<String>) -> Option<String> {
        value
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    }

    fn validate_explicit_proxy_url(proxy_url: &str) -> anyhow::Result<()> {
        if proxy_url.eq_ignore_ascii_case(KiroCredentials::PROXY_DIRECT) {
            anyhow::bail!("自定义代理 URL 不能为 direct，请使用 direct 模式");
        }
        reqwest::Proxy::all(proxy_url)
            .map(|_| ())
            .map_err(|err| anyhow::anyhow!("代理 URL 无效: {}", err))
    }

    pub fn set_credential_proxy(
        &self,
        id: u64,
        mode: String,
        proxy_id: Option<String>,
        proxy_url: Option<String>,
        proxy_username: Option<String>,
        proxy_password: Option<String>,
    ) -> anyhow::Result<()> {
        let mode = mode.trim().to_ascii_lowercase();
        let dispatch = self.dispatch_config();
        let pool = &dispatch.proxy_pool;
        let now = Instant::now();

        let _state_write_guard = self.state_write_lock.lock();
        let mut persisted = self.persisted_credentials_snapshot();
        let position = persisted
            .iter()
            .position(|credential| credential.id == Some(id))
            .ok_or_else(|| anyhow::anyhow!("凭据不存在: {}", id))?;

        let mut updated = persisted[position].clone();
        let previous_proxy_id = updated
            .proxy_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);

        match mode.as_str() {
            "auto" => {
                updated.proxy_url = None;
                updated.proxy_username = None;
                updated.proxy_password = None;
                updated.proxy_id = None;

                if pool.enabled {
                    let mut assignment_snapshot = persisted.clone();
                    assignment_snapshot[position].proxy_url = None;
                    assignment_snapshot[position].proxy_username = None;
                    assignment_snapshot[position].proxy_password = None;
                    assignment_snapshot[position].proxy_id = None;

                    let selected = self.select_proxy_id_from_pool(
                        pool,
                        &assignment_snapshot,
                        None,
                        updated.refresh_token.as_deref(),
                    );
                    match selected {
                        Some(proxy_id) => updated.proxy_id = Some(proxy_id),
                        None if pool.require_proxy => {
                            anyhow::bail!("proxyPool.requireProxy=true，但当前没有可用的代理池节点")
                        }
                        None => {}
                    }
                } else if pool.require_proxy {
                    anyhow::bail!("proxyPool.requireProxy=true，但代理池未启用");
                }
            }
            "pool" => {
                if !pool.enabled {
                    anyhow::bail!("代理池未启用，不能绑定代理池节点");
                }
                let proxy_id = Self::trim_optional_string(proxy_id)
                    .ok_or_else(|| anyhow::anyhow!("proxyId 不能为空"))?;
                let exists = pool
                    .proxies
                    .iter()
                    .any(|entry| entry.enabled && entry.id.trim() == proxy_id);
                if !exists {
                    anyhow::bail!("指定的代理池 ID 不存在或未启用: {}", proxy_id);
                }
                if let Some(unavailable_until) = self.proxy_pool_unavailable_until(&proxy_id, now) {
                    let remaining_ms = unavailable_until
                        .saturating_duration_since(Instant::now())
                        .as_millis();
                    anyhow::bail!(
                        "代理池节点 {} 正在冷却中（剩余约 {} ms）",
                        proxy_id,
                        remaining_ms
                    );
                }

                updated.proxy_url = None;
                updated.proxy_username = None;
                updated.proxy_password = None;
                updated.proxy_id = Some(proxy_id);
            }
            "custom" => {
                let proxy_url = Self::trim_optional_string(proxy_url)
                    .ok_or_else(|| anyhow::anyhow!("proxyUrl 不能为空"))?;
                Self::validate_explicit_proxy_url(&proxy_url)?;
                updated.proxy_url = Some(proxy_url);
                updated.proxy_username = Self::trim_optional_string(proxy_username);
                updated.proxy_password = Self::trim_optional_string(proxy_password);
                updated.proxy_id = None;
            }
            "direct" => {
                if pool.enabled && pool.require_proxy {
                    anyhow::bail!("proxyPool.requireProxy=true 时凭据不能使用 direct");
                }
                updated.proxy_url = Some(KiroCredentials::PROXY_DIRECT.to_string());
                updated.proxy_username = None;
                updated.proxy_password = None;
                updated.proxy_id = None;
            }
            "global" => {
                if pool.enabled && pool.require_proxy {
                    anyhow::bail!("proxyPool.requireProxy=true 时凭据不能清空代理绑定");
                }
                updated.proxy_url = None;
                updated.proxy_username = None;
                updated.proxy_password = None;
                updated.proxy_id = None;
            }
            _ => anyhow::bail!("不支持的代理模式: {}", mode),
        }

        let next_proxy_id = updated
            .proxy_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);

        persisted[position] = updated.clone();
        self.persist_credentials_snapshot(&persisted)?;

        {
            let mut entries = self.entries.lock();
            let entry = entries
                .iter_mut()
                .find(|entry| entry.id == id)
                .ok_or_else(|| anyhow::anyhow!("凭据不存在: {}", id))?;
            entry.credentials.proxy_url = updated.proxy_url;
            entry.credentials.proxy_username = updated.proxy_username;
            entry.credentials.proxy_password = updated.proxy_password;
            entry.credentials.proxy_id = updated.proxy_id;
        }

        if let Some(proxy_id) = &next_proxy_id {
            self.proxy_health.lock().remove(proxy_id);
        }
        self.availability_notify.notify_waiters();
        tracing::info!(
            credential_id = id,
            mode,
            previous_proxy_id = ?previous_proxy_id,
            next_proxy_id = ?next_proxy_id,
            "凭据代理配置已更新"
        );
        Ok(())
    }

    fn pool_proxy_id_for_credential(&self, id: u64) -> Option<String> {
        let entries = self.entries.lock();
        let credentials = &entries.iter().find(|entry| entry.id == id)?.credentials;
        if Self::explicit_proxy_for_credentials(credentials).is_some() {
            return None;
        }
        credentials
            .proxy_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    }

    fn record_proxy_success_for_credential(&self, id: u64) {
        let Some(proxy_id) = self.pool_proxy_id_for_credential(id) else {
            return;
        };
        let mut health = self.proxy_health.lock();
        if let Some(state) = health.get_mut(&proxy_id) {
            if state
                .unavailable_until
                .is_some_and(|unavailable_until| unavailable_until > Instant::now())
            {
                return;
            }
            state.consecutive_failures = 0;
            state.unavailable_until = None;
            state.last_error = None;
        }
    }

    pub fn report_proxy_transport_failure(&self, id: u64, error_summary: &str) -> bool {
        let pool = self.dispatch_config().proxy_pool;
        if !pool.enabled || !pool.failover.enabled {
            return false;
        }

        let Some(current_proxy_id) = self.pool_proxy_id_for_credential(id) else {
            return false;
        };

        let now = Instant::now();
        let threshold_reached = {
            let mut health = self.proxy_health.lock();
            let state = health.entry(current_proxy_id.clone()).or_default();
            state.consecutive_failures = state.consecutive_failures.saturating_add(1);
            state.last_error = Some(Self::truncate_for_log(error_summary, 240));

            if state.consecutive_failures < pool.failover.failure_threshold {
                tracing::warn!(
                    credential_id = id,
                    proxy_id = %current_proxy_id,
                    failures = state.consecutive_failures,
                    threshold = pool.failover.failure_threshold,
                    error = %error_summary,
                    "代理池节点出现传输失败"
                );
                false
            } else {
                state.unavailable_until =
                    Some(now + StdDuration::from_secs(pool.failover.cooldown_secs));
                tracing::error!(
                    credential_id = id,
                    proxy_id = %current_proxy_id,
                    failures = state.consecutive_failures,
                    cooldown_secs = pool.failover.cooldown_secs,
                    error = %error_summary,
                    "代理池节点达到故障阈值，准备迁移绑定凭据"
                );
                true
            }
        };

        if !threshold_reached {
            return false;
        }

        self.migrate_credential_proxy(id, &current_proxy_id, &pool, error_summary)
    }

    fn is_proxy_transport_error(err: &anyhow::Error) -> bool {
        err.downcast_ref::<reqwest::Error>()
            .is_some_and(|err| err.is_connect() || err.is_timeout())
    }

    fn report_proxy_transport_failure_for_error(
        &self,
        id: u64,
        operation: &'static str,
        err: &anyhow::Error,
    ) -> bool {
        if !Self::is_proxy_transport_error(err) {
            return false;
        }

        tracing::warn!(
            credential_id = id,
            operation,
            error = %err,
            "代理池管理请求出现传输失败"
        );
        self.report_proxy_transport_failure(id, &err.to_string())
    }

    async fn proxy_pool_retry_management_call<T, F, Fut>(
        &self,
        id: u64,
        credentials: &KiroCredentials,
        operation: &'static str,
        mut call: F,
    ) -> anyhow::Result<(T, KiroCredentials)>
    where
        F: FnMut(KiroCredentials, Option<ProxyConfig>) -> Fut,
        Fut: Future<Output = anyhow::Result<T>>,
    {
        let mut active_credentials = credentials.clone();
        let mut retried_after_proxy_failover = false;

        loop {
            let effective_proxy = self.effective_proxy_for_credentials(&active_credentials)?;
            match call(active_credentials.clone(), effective_proxy).await {
                Ok(value) => return Ok((value, active_credentials)),
                Err(err)
                    if !retried_after_proxy_failover
                        && self.report_proxy_transport_failure_for_error(id, operation, &err) =>
                {
                    retried_after_proxy_failover = true;
                    active_credentials = self.current_credentials(id)?;
                    tracing::info!(
                        credential_id = id,
                        operation,
                        proxy_id = ?active_credentials.proxy_id,
                        "代理池故障迁移后重试管理请求"
                    );
                }
                Err(err) => return Err(err),
            }
        }
    }

    fn migrate_credential_proxy(
        &self,
        id: u64,
        failed_proxy_id: &str,
        pool: &ProxyPoolConfig,
        error_summary: &str,
    ) -> bool {
        let _state_write_guard = self.state_write_lock.lock();
        let mut persisted = self.persisted_credentials_snapshot();
        let mut migrated: Vec<(u64, String)> = Vec::new();

        for position in 0..persisted.len() {
            if Self::explicit_proxy_for_credentials(&persisted[position]).is_some() {
                continue;
            }

            let current_proxy_id = persisted[position]
                .proxy_id
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string);
            if current_proxy_id.as_deref() != Some(failed_proxy_id) {
                continue;
            }

            let credential_id = persisted[position].id.unwrap_or_default();
            let next_proxy_id = self.select_proxy_id_from_pool(
                pool,
                &persisted,
                Some(failed_proxy_id),
                persisted[position].refresh_token.as_deref(),
            );
            let Some(next_proxy_id) = next_proxy_id else {
                tracing::warn!(
                    credential_id,
                    proxy_id = %failed_proxy_id,
                    "代理池故障后没有可迁移的健康代理节点"
                );
                continue;
            };

            persisted[position].proxy_id = Some(next_proxy_id.clone());
            migrated.push((credential_id, next_proxy_id));
        }

        if migrated.is_empty() {
            tracing::warn!(
                credential_id = id,
                proxy_id = %failed_proxy_id,
                "代理池故障后没有完成任何凭据迁移"
            );
            return false;
        }

        if let Err(err) = self.persist_credentials_snapshot(&persisted) {
            tracing::error!(
                credential_id = id,
                from_proxy_id = %failed_proxy_id,
                error = %err,
                "持久化代理池故障迁移失败"
            );
            return false;
        }

        {
            let mut entries = self.entries.lock();
            for (credential_id, next_proxy_id) in &migrated {
                if let Some(entry) = entries.iter_mut().find(|entry| entry.id == *credential_id) {
                    entry.credentials.proxy_id = Some(next_proxy_id.clone());
                    entry.failure_count = 0;
                }
            }
        }

        {
            let mut health = self.proxy_health.lock();
            for (_, next_proxy_id) in &migrated {
                health.remove(next_proxy_id);
            }
        }
        self.availability_notify.notify_waiters();
        for (credential_id, next_proxy_id) in migrated {
            tracing::warn!(
                credential_id,
                from_proxy_id = %failed_proxy_id,
                to_proxy_id = %next_proxy_id,
                error = %error_summary,
                "已完成凭据代理池绑定迁移"
            );
        }
        true
    }

    fn session_affinity_cache_key(model: Option<&str>, raw_key: &str) -> Option<String> {
        let raw_key = raw_key.trim();
        if raw_key.is_empty() {
            return None;
        }
        let model_label = model
            .and_then(normalize_model_selector)
            .map(|selector| selector.family)
            .or_else(|| model.map(|value| value.trim().to_ascii_lowercase()))
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "unknown".to_string());
        Some(sha256_hex(&format!("{model_label}\0{raw_key}")))
    }

    fn session_affinity_cache_key_if_enabled(
        &self,
        model: Option<&str>,
        raw_key: Option<&str>,
    ) -> Option<String> {
        let dispatch = self.dispatch_config();
        if !dispatch.session_affinity_enabled {
            return None;
        }
        raw_key.and_then(|key| Self::session_affinity_cache_key(model, key))
    }

    fn prune_local_session_affinity_cache(
        cache: &mut HashMap<String, SessionAffinityEntry>,
        now: Instant,
    ) {
        cache.retain(|_, entry| entry.expires_at > now);
        if cache.len() <= SESSION_AFFINITY_LOCAL_MAX_ENTRIES {
            return;
        }

        let overflow = cache.len() - SESSION_AFFINITY_LOCAL_MAX_ENTRIES;
        let stale_keys: Vec<String> = cache.keys().take(overflow).cloned().collect();
        for key in stale_keys {
            cache.remove(&key);
        }
    }

    fn load_session_affinity_credential_id(&self, cache_key: &str) -> Option<u64> {
        match self.state_store.load_session_affinity(cache_key) {
            Ok(Some(id)) => return Some(id),
            Ok(None) => {
                if self.state_store.session_affinity_store_enabled() {
                    return None;
                }
            }
            Err(err) => {
                tracing::warn!(
                    session_affinity_key = %cache_key,
                    error = %err,
                    "读取共享会话凭据亲和缓存失败，将回退本地缓存"
                );
            }
        }

        let now = Instant::now();
        let mut cache = self.session_affinity_cache.lock();
        if cache
            .get(cache_key)
            .is_some_and(|entry| entry.expires_at <= now)
        {
            cache.remove(cache_key);
            return None;
        }
        cache.get(cache_key).map(|entry| entry.credential_id)
    }

    fn record_session_affinity_cache_key(&self, cache_key: &str, credential_id: u64) {
        let now = Instant::now();
        {
            let mut cache = self.session_affinity_cache.lock();
            Self::prune_local_session_affinity_cache(&mut cache, now);
            cache.insert(
                cache_key.to_string(),
                SessionAffinityEntry {
                    credential_id,
                    expires_at: now + SESSION_AFFINITY_LOCAL_TTL,
                },
            );
        }

        if let Err(err) = self.state_store.record_session_affinity(
            cache_key,
            credential_id,
            SESSION_AFFINITY_TTL_MS,
        ) {
            tracing::warn!(
                session_affinity_key = %cache_key,
                credential_id,
                error = %err,
                "写入共享会话凭据亲和缓存失败"
            );
        }
    }

    fn clear_session_affinity_cache_key(&self, cache_key: &str) {
        self.session_affinity_cache.lock().remove(cache_key);
        if let Err(err) = self.state_store.clear_session_affinity(cache_key) {
            tracing::warn!(
                session_affinity_key = %cache_key,
                error = %err,
                "清理共享会话凭据亲和缓存失败"
            );
        }
    }

    pub(crate) fn record_session_affinity(
        &self,
        model: Option<&str>,
        raw_key: Option<&str>,
        credential_id: u64,
    ) {
        let Some(cache_key) = self.session_affinity_cache_key_if_enabled(model, raw_key) else {
            return;
        };
        self.record_session_affinity_cache_key(&cache_key, credential_id);
        tracing::debug!(
            session_affinity_key = %cache_key,
            credential_id,
            ttl_ms = SESSION_AFFINITY_TTL_MS,
            "已记录会话凭据亲和"
        );
    }

    fn queue_depth(&self) -> usize {
        self.waiting_requests.load(Ordering::SeqCst)
    }

    pub(crate) fn local_request_counts(&self) -> LocalRequestCounts {
        let active_requests = self
            .entries
            .lock()
            .iter()
            .map(|entry| entry.active_requests)
            .sum();

        LocalRequestCounts {
            active_requests,
            waiting_requests: self.queue_depth(),
        }
    }

    fn model_requirement(model: Option<&str>) -> ModelRequirement {
        let Some(model) = model.map(|name| name.to_ascii_lowercase()) else {
            return ModelRequirement::Any;
        };

        if model.contains("claude-opus-4.8")
            || model.contains("claude-opus-4-8")
            || model.contains("claude-opus-4.7")
            || model.contains("claude-opus-4-7")
        {
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
        credentials.policy_allows_model(
            dispatch.account_type_policy_for(credentials),
            model,
            dispatch.model_cooldown_enabled,
        )
    }

    fn credential_allowed_by_scope(
        credentials: &KiroCredentials,
        credential_group_scope: Option<&CredentialGroupScope>,
    ) -> bool {
        credential_group_scope.map_or(true, |scope| {
            scope.allows_credential_groups(&credentials.credential_groups)
        })
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
        let effective_requirement = model
            .and_then(|model| credentials.effective_model_id_for_request(model))
            .as_deref()
            .map(|model| Self::model_requirement(Some(model)))
            .unwrap_or(requirement);
        match requirement {
            ModelRequirement::Any => true,
            ModelRequirement::PaidOpus => match effective_requirement {
                ModelRequirement::Any => true,
                ModelRequirement::PaidOpus => credentials.supports_opus(),
                ModelRequirement::RealOpus47 => credentials.supports_real_opus_4_7(),
            },
            ModelRequirement::RealOpus47 => match effective_requirement {
                ModelRequirement::Any => true,
                ModelRequirement::PaidOpus => credentials.supports_opus(),
                ModelRequirement::RealOpus47 => credentials.supports_real_opus_4_7(),
            },
        }
    }

    fn needs_token_refresh(credentials: &KiroCredentials) -> bool {
        credentials.access_token.is_none()
            || is_token_expired(credentials)
            || is_token_expiring_soon(credentials)
    }

    fn has_usable_access_token(credentials: &KiroCredentials) -> bool {
        credentials.access_token.is_some()
            && !is_token_expired(credentials)
            && !is_token_expiring_soon(credentials)
    }

    fn model_preference_rank(credentials: &KiroCredentials, requirement: ModelRequirement) -> u8 {
        match requirement {
            ModelRequirement::RealOpus47 => credentials.opus_4_7_preference_rank(),
            ModelRequirement::Any | ModelRequirement::PaidOpus => 0,
        }
    }

    fn has_capacity(
        dispatch: &DispatchConfig,
        credentials: &KiroCredentials,
        active_requests: usize,
    ) -> bool {
        match dispatch.effective_max_concurrency_for(credentials) {
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
        if entry
            .background_refresh_cooldown_until
            .is_some_and(|until| until <= now)
        {
            entry.background_refresh_cooldown_until = None;
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
            entry.background_refresh_cooldown_until = None;
        }
    }

    fn clear_rate_limit_penalty_runtime(
        entry: &mut CredentialEntry,
        dispatch: &DispatchConfig,
        now: Instant,
    ) {
        entry.rate_limit_cooldown_until = None;
        entry.background_refresh_cooldown_until = None;
        entry.rate_limit_hit_streak = 0;
        if let Some(bucket) = entry.rate_limit_bucket.as_mut() {
            bucket.refill(now);
            bucket.current_refill_per_second = bucket.policy.refill_per_second;
            bucket.last_refill_at = now;
        } else {
            entry.rate_limit_bucket = dispatch
                .bucket_policy_for(&entry.credentials)
                .map(|policy| AdaptiveTokenBucket::new(policy, now));
        }
    }

    fn clear_disabled_rate_limit_penalties(&self, dispatch: &DispatchConfig) {
        let now = Instant::now();
        let mut shared_bucket_policies = Vec::new();

        {
            let mut entries = self.entries.lock();
            for entry in entries.iter_mut() {
                if dispatch.rate_limit_cooldown_enabled_for(&entry.credentials) {
                    continue;
                }
                Self::clear_rate_limit_penalty_runtime(entry, dispatch, now);
                shared_bucket_policies.push((
                    entry.id,
                    dispatch.shared_bucket_policy_for(&entry.credentials),
                ));
            }
        }

        if self.shared_dispatch_runtime_enabled() {
            let now_epoch_ms = current_epoch_ms();
            for (id, bucket_policy) in shared_bucket_policies {
                if let Err(err) = self.state_store.clear_dispatch_rate_limit_penalty(
                    id,
                    bucket_policy,
                    now_epoch_ms,
                ) {
                    tracing::warn!("清理共享调度 429 惩罚失败（credentialId={}）: {}", id, err);
                }
            }
        }
    }

    fn clear_rate_limit_penalty_for_credential(&self, id: u64, dispatch: &DispatchConfig) {
        let now = Instant::now();
        let shared_bucket_policy = {
            let mut entries = self.entries.lock();
            let Some(entry) = entries.iter_mut().find(|entry| entry.id == id) else {
                return;
            };
            Self::clear_rate_limit_penalty_runtime(entry, dispatch, now);
            dispatch.shared_bucket_policy_for(&entry.credentials)
        };

        if self.shared_dispatch_runtime_enabled() {
            if let Err(err) = self.state_store.clear_dispatch_rate_limit_penalty(
                id,
                shared_bucket_policy,
                current_epoch_ms(),
            ) {
                tracing::warn!("清理共享调度 429 惩罚失败（credentialId={}）: {}", id, err);
            }
        }
    }

    fn clear_all_runtime_model_restrictions(&self) -> anyhow::Result<bool> {
        let mut entries = self.entries.lock();
        let mut changed = false;
        for entry in entries.iter_mut() {
            changed |= entry.credentials.clear_runtime_model_restrictions();
        }
        if !changed {
            return Ok(false);
        }

        let credentials = Self::persisted_credentials_from_entries(&entries);
        drop(entries);
        self.persist_credentials_snapshot(&credentials)?;
        Ok(true)
    }

    fn enabled_supported_alternate_for_model<'a>(
        dispatch: &DispatchConfig,
        entries: &'a [CredentialEntry],
        excluded_id: u64,
        model: &str,
        requirement: ModelRequirement,
    ) -> Option<&'a CredentialEntry> {
        entries
            .iter()
            .filter(|entry| {
                !entry.disabled
                    && entry.id != excluded_id
                    && Self::is_model_supported(
                        dispatch,
                        &entry.credentials,
                        Some(model),
                        requirement,
                    )
            })
            .min_by_key(|entry| {
                (
                    entry.credentials.priority,
                    Self::model_preference_rank(&entry.credentials, requirement),
                    entry.id,
                )
            })
    }

    fn is_rate_limited(entry: &CredentialEntry, now: Instant) -> bool {
        if entry.background_refresh_in_progress {
            return true;
        }

        entry
            .rate_limit_cooldown_until
            .is_some_and(|until| until > now)
            || entry
                .background_refresh_cooldown_until
                .is_some_and(|until| until > now)
    }

    fn parse_utc_timestamp(value: Option<&str>) -> Option<DateTime<Utc>> {
        value
            .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())
            .map(|timestamp| timestamp.with_timezone(&Utc))
    }

    fn suspicious_activity_seen(credentials: &KiroCredentials) -> bool {
        credentials.suspicious_activity_count > 0
            || credentials.suspicious_activity_last_seen_at.is_some()
    }

    fn clear_suspicious_activity_fields(credentials: &mut KiroCredentials) -> bool {
        let changed = credentials.suspicious_activity_count != 0
            || credentials.suspicious_activity_first_seen_at.is_some()
            || credentials.suspicious_activity_last_seen_at.is_some()
            || credentials.suspicious_activity_quarantine_until.is_some()
            || credentials.suspicious_activity_recovery_success_count != 0;

        credentials.suspicious_activity_count = 0;
        credentials.suspicious_activity_first_seen_at = None;
        credentials.suspicious_activity_last_seen_at = None;
        credentials.suspicious_activity_quarantine_until = None;
        credentials.suspicious_activity_recovery_success_count = 0;

        changed
    }

    fn suspicious_activity_clear_patch() -> CredentialHealthPatch {
        CredentialHealthPatch {
            suspicious_activity_count: Some(0),
            suspicious_activity_first_seen_at: Some(None),
            suspicious_activity_last_seen_at: Some(None),
            suspicious_activity_quarantine_until: Some(None),
            suspicious_activity_recovery_success_count: Some(0),
            ..CredentialHealthPatch::default()
        }
    }

    fn suspicious_activity_preference_rank(
        dispatch: &DispatchConfig,
        credentials: &KiroCredentials,
    ) -> u8 {
        if dispatch.suspicious_activity_prefer_clean_credentials
            && Self::suspicious_activity_seen(credentials)
        {
            1
        } else {
            0
        }
    }

    fn suspicious_activity_quarantine_until(
        credentials: &KiroCredentials,
    ) -> Option<DateTime<Utc>> {
        Self::parse_utc_timestamp(credentials.suspicious_activity_quarantine_until.as_deref())
    }

    fn suspicious_activity_quarantine_remaining_ms_at(
        credentials: &KiroCredentials,
        now_utc: &DateTime<Utc>,
    ) -> Option<u64> {
        let until = Self::suspicious_activity_quarantine_until(credentials)?;
        let remaining = until.signed_duration_since(*now_utc);
        if remaining <= Duration::zero() {
            return None;
        }
        remaining.num_milliseconds().try_into().ok()
    }

    fn suspicious_activity_quarantine_ready_at(
        credentials: &KiroCredentials,
        now: Instant,
        now_utc: &DateTime<Utc>,
    ) -> Option<Instant> {
        Self::suspicious_activity_quarantine_remaining_ms_at(credentials, now_utc)
            .map(|remaining_ms| now + StdDuration::from_millis(remaining_ms))
    }

    fn is_suspicious_activity_quarantined_at(
        credentials: &KiroCredentials,
        now_utc: &DateTime<Utc>,
    ) -> bool {
        Self::suspicious_activity_quarantine_remaining_ms_at(credentials, now_utc).is_some()
    }

    fn build_suspicious_activity_marker_update(
        credentials: &KiroCredentials,
        dispatch: &DispatchConfig,
        now_utc: DateTime<Utc>,
        cooldown_ms: u64,
    ) -> SuspiciousActivityMarkerUpdate {
        let window_ms = dispatch.suspicious_activity_auto_disable_window_ms;
        let existing_first_seen =
            Self::parse_utc_timestamp(credentials.suspicious_activity_first_seen_at.as_deref());
        let within_window = existing_first_seen.as_ref().is_some_and(|first_seen| {
            if window_ms == 0 {
                return true;
            }
            let elapsed = now_utc.signed_duration_since(*first_seen);
            elapsed >= Duration::zero()
                && elapsed.num_milliseconds() <= i64::try_from(window_ms).unwrap_or(i64::MAX)
        });

        let first_seen = if within_window {
            existing_first_seen.unwrap_or(now_utc)
        } else {
            now_utc
        };
        let previous_count = if within_window {
            credentials.suspicious_activity_count
        } else {
            0
        };
        let count = previous_count.saturating_add(1).max(1);

        let existing_quarantine = Self::suspicious_activity_quarantine_until(credentials)
            .filter(|until| *until > now_utc);
        let next_quarantine = if cooldown_ms > 0 {
            let cooldown_ms = cooldown_ms.min(i64::MAX as u64) as i64;
            Some(now_utc + Duration::milliseconds(cooldown_ms))
        } else {
            None
        };
        let quarantine_until = match (existing_quarantine, next_quarantine) {
            (Some(existing), Some(next)) => Some(existing.max(next)),
            (Some(existing), None) => Some(existing),
            (None, Some(next)) => Some(next),
            (None, None) => None,
        };
        let should_auto_disable = dispatch.suspicious_activity_auto_disable_enabled
            && dispatch.suspicious_activity_auto_disable_threshold > 0
            && count >= dispatch.suspicious_activity_auto_disable_threshold;

        SuspiciousActivityMarkerUpdate {
            count,
            first_seen_at: Some(first_seen.to_rfc3339()),
            last_seen_at: Some(now_utc.to_rfc3339()),
            quarantine_until: quarantine_until.map(|until| until.to_rfc3339()),
            should_auto_disable,
        }
    }

    fn suspicious_activity_auto_clear_due_to_age(
        credentials: &KiroCredentials,
        dispatch: &DispatchConfig,
        now_utc: &DateTime<Utc>,
    ) -> bool {
        if !dispatch.suspicious_activity_auto_clear_enabled
            || dispatch.suspicious_activity_auto_clear_after_ms == 0
            || !Self::suspicious_activity_seen(credentials)
            || Self::is_suspicious_activity_quarantined_at(credentials, now_utc)
        {
            return false;
        }

        let Some(last_seen) =
            Self::parse_utc_timestamp(credentials.suspicious_activity_last_seen_at.as_deref())
        else {
            return false;
        };

        let elapsed = now_utc.signed_duration_since(last_seen);
        elapsed >= Duration::zero()
            && elapsed.num_milliseconds()
                >= i64::try_from(dispatch.suspicious_activity_auto_clear_after_ms)
                    .unwrap_or(i64::MAX)
    }

    fn suspicious_activity_auto_clear_due_to_success(
        credentials: &KiroCredentials,
        dispatch: &DispatchConfig,
        now_utc: &DateTime<Utc>,
        next_recovery_success_count: u32,
    ) -> bool {
        dispatch.suspicious_activity_auto_clear_enabled
            && dispatch.suspicious_activity_auto_clear_success_threshold > 0
            && Self::suspicious_activity_seen(credentials)
            && !Self::is_suspicious_activity_quarantined_at(credentials, now_utc)
            && next_recovery_success_count
                >= dispatch.suspicious_activity_auto_clear_success_threshold
    }

    fn suspicious_activity_recovery_count_patch(count: u32) -> CredentialHealthPatch {
        CredentialHealthPatch {
            suspicious_activity_recovery_success_count: Some(count),
            ..CredentialHealthPatch::default()
        }
    }

    fn maybe_clear_stale_suspicious_activity_markers(&self, dispatch: &DispatchConfig) -> bool {
        if !dispatch.suspicious_activity_auto_clear_enabled
            || dispatch.suspicious_activity_auto_clear_after_ms == 0
        {
            return false;
        }

        let now_utc = Utc::now();
        let cleared_ids: Vec<u64> = {
            let mut entries = self.entries.lock();
            entries
                .iter_mut()
                .filter_map(|entry| {
                    if entry.disabled
                        || !Self::suspicious_activity_auto_clear_due_to_age(
                            &entry.credentials,
                            dispatch,
                            &now_utc,
                        )
                    {
                        return None;
                    }
                    Self::clear_suspicious_activity_fields(&mut entry.credentials)
                        .then_some(entry.id)
                })
                .collect()
        };

        for id in &cleared_ids {
            if let Err(err) = self
                .state_store
                .patch_credential_health(*id, &Self::suspicious_activity_clear_patch())
            {
                tracing::warn!(
                    credential_id = id,
                    "自动清除过期 suspicious activity 标记持久化失败: {}",
                    err
                );
            }
        }

        if !cleared_ids.is_empty() {
            self.try_bump_state_change_revision(StateChangeKind::Credentials);
            self.availability_notify.notify_waiters();
            tracing::info!(
                cleared_credentials = cleared_ids.len(),
                "已自动清除过期 suspicious activity 标记"
            );
            return true;
        }

        false
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
        let background_refresh_ready_at = entry
            .background_refresh_cooldown_until
            .filter(|until| *until > now);
        let bucket_ready_at = entry
            .rate_limit_bucket
            .as_mut()
            .and_then(|bucket| bucket.ready_at(now, request_weight));

        [
            cooldown_ready_at,
            background_refresh_ready_at,
            bucket_ready_at,
        ]
        .into_iter()
        .flatten()
        .max()
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

    fn entry_dispatchable_for_background_refresh(
        &self,
        dispatch: &DispatchConfig,
        entry: &CredentialEntry,
        snapshots: Option<&HashMap<u64, DispatchRuntimeSnapshot>>,
        now: Instant,
        now_epoch_ms: u64,
        request_weight: f64,
    ) -> bool {
        if entry.background_refresh_in_progress {
            return false;
        }

        if entry
            .background_refresh_cooldown_until
            .is_some_and(|until| until > now)
        {
            return false;
        }

        if self.proxy_dispatch_availability(dispatch, &entry.credentials, now)
            != ProxyDispatchAvailability::Available
        {
            return false;
        }

        if let Some(snapshots) = snapshots {
            let runtime = Self::shared_dispatch_snapshot_for_entry(entry, dispatch, snapshots);
            runtime
                .cooldown_until_epoch_ms
                .map_or(true, |until| until <= now_epoch_ms)
                && Self::shared_bucket_is_ready_for(&runtime, request_weight)
                && Self::has_capacity(dispatch, &entry.credentials, runtime.active_requests)
        } else {
            !Self::is_rate_limited(entry, now)
                && Self::bucket_is_ready_for(entry, request_weight)
                && Self::has_capacity(dispatch, &entry.credentials, entry.active_requests)
        }
    }

    fn background_refresh_exclusions_for_request(
        self: &Arc<Self>,
        model: Option<&str>,
        request_weight: f64,
        excluded_credential_ids: &HashSet<u64>,
        credential_group_scope: Option<&CredentialGroupScope>,
    ) -> HashSet<u64> {
        let dispatch = self.dispatch_config();
        let model_requirement = Self::model_requirement(model);
        let is_balanced = dispatch.mode == "balanced";
        let now = Instant::now();
        let now_epoch_ms = current_epoch_ms();
        let shared_snapshots = if self.shared_dispatch_runtime_enabled() {
            match self.load_shared_dispatch_runtime_snapshots(&dispatch, now_epoch_ms) {
                Ok(snapshots) => Some(snapshots),
                Err(err) => {
                    tracing::warn!(
                        "后台刷新预选读取共享调度热态失败，跳过本轮异步刷新: {}",
                        err
                    );
                    return HashSet::new();
                }
            }
        } else {
            None
        };

        let mut request_exclusions = HashSet::new();
        let mut spawn_id: Option<u64> = None;
        {
            let mut entries = self.entries.lock();
            Self::refresh_runtime_state(&mut entries, now);

            let has_fresh_dispatchable_alternate = entries.iter().any(|entry| {
                !entry.disabled
                    && !excluded_credential_ids.contains(&entry.id)
                    && Self::credential_allowed_by_scope(&entry.credentials, credential_group_scope)
                    && Self::is_model_supported(
                        &dispatch,
                        &entry.credentials,
                        model,
                        model_requirement,
                    )
                    && Self::has_usable_access_token(&entry.credentials)
                    && self.entry_dispatchable_for_background_refresh(
                        &dispatch,
                        entry,
                        shared_snapshots.as_ref(),
                        now,
                        now_epoch_ms,
                        request_weight,
                    )
            });

            if !has_fresh_dispatchable_alternate {
                return HashSet::new();
            }

            let mut selected_candidate_key: Option<(u32, u8, usize, u64, u64)> = None;
            let mut selected_candidate_id: Option<u64> = None;
            let cooldown_until = now + BACKGROUND_REFRESH_IN_PROGRESS_COOLDOWN;
            for entry in entries.iter() {
                if entry.disabled
                    || excluded_credential_ids.contains(&entry.id)
                    || !Self::credential_allowed_by_scope(
                        &entry.credentials,
                        credential_group_scope,
                    )
                    || !Self::is_model_supported(
                        &dispatch,
                        &entry.credentials,
                        model,
                        model_requirement,
                    )
                    || !Self::needs_token_refresh(&entry.credentials)
                    || !self.entry_dispatchable_for_background_refresh(
                        &dispatch,
                        entry,
                        shared_snapshots.as_ref(),
                        now,
                        now_epoch_ms,
                        request_weight,
                    )
                {
                    continue;
                }

                request_exclusions.insert(entry.id);

                if entry.background_refresh_in_progress {
                    continue;
                }

                let candidate_key = if is_balanced {
                    (
                        u32::from(Self::model_preference_rank(
                            &entry.credentials,
                            model_requirement,
                        )),
                        0,
                        entry.active_requests,
                        entry.success_count,
                        entry.id,
                    )
                } else {
                    (
                        entry.credentials.priority,
                        Self::model_preference_rank(&entry.credentials, model_requirement),
                        entry.active_requests,
                        entry.success_count,
                        entry.id,
                    )
                };
                let should_select = selected_candidate_key
                    .as_ref()
                    .map(|best_key| candidate_key < *best_key)
                    .unwrap_or(true);
                if should_select {
                    selected_candidate_key = Some(candidate_key);
                    selected_candidate_id = Some(entry.id);
                }
            }

            if let Some(id) = selected_candidate_id {
                if let Some(entry) = entries.iter_mut().find(|entry| entry.id == id) {
                    entry.background_refresh_cooldown_until = Some(
                        entry
                            .background_refresh_cooldown_until
                            .map(|until| until.max(cooldown_until))
                            .unwrap_or(cooldown_until),
                    );
                    entry.last_used_at = Some(Utc::now().to_rfc3339());
                    entry.background_refresh_in_progress = true;
                    spawn_id = Some(id);
                }
            }
        }

        if request_exclusions.is_empty() {
            return HashSet::new();
        }

        if let Some(id) = spawn_id {
            if self.shared_dispatch_runtime_enabled() {
                let cooldown_ms = BACKGROUND_REFRESH_IN_PROGRESS_COOLDOWN
                    .as_millis()
                    .min(u128::from(u64::MAX)) as u64;
                if let Err(err) =
                    self.state_store
                        .defer_dispatch_credential(id, cooldown_ms, current_epoch_ms())
                {
                    tracing::warn!(
                        "更新后台刷新共享调度冷却失败（credentialId={}）: {}",
                        id,
                        err
                    );
                }
            }

            self.spawn_background_token_refresh(id);
        }

        if self.shared_dispatch_runtime_enabled() && spawn_id.is_none() {
            let cooldown_ms = self
                .runtime_refresh_coordination_cooldown()
                .as_millis()
                .min(u128::from(u64::MAX)) as u64;
            let now_epoch_ms = current_epoch_ms();
            for id in &request_exclusions {
                if let Err(err) =
                    self.state_store
                        .defer_dispatch_credential(*id, cooldown_ms, now_epoch_ms)
                {
                    tracing::warn!(
                        "更新后台刷新共享调度冷却失败（credentialId={}）: {}",
                        id,
                        err
                    );
                }
            }
        }

        tracing::info!(
            scheduled = usize::from(spawn_id.is_some()),
            excluded = request_exclusions.len(),
            model = model.unwrap_or("unknown"),
            "当前请求跳过需刷新凭据并择机后台刷新"
        );

        request_exclusions
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
        entry.background_refresh_cooldown_until = None;
        entry.available_models_refresh_in_progress = false;
        entry.available_models_refresh_cooldown_until = None;
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

    fn apply_subscription_info_update(
        &self,
        id: u64,
        subscription_title: Option<&str>,
        subscription_type: Option<&str>,
    ) {
        self.apply_credential_metadata_update(
            id,
            subscription_title,
            subscription_type,
            None,
            None,
            None,
            None,
        );
    }

    fn apply_usage_limits_metadata_update(&self, id: u64, usage_limits: &UsageLimitsResponse) {
        self.apply_credential_metadata_update(
            id,
            usage_limits.subscription_title(),
            usage_limits.subscription_type(),
            usage_limits.email(),
            usage_limits.user_id(),
            None,
            None,
        );
    }

    fn apply_available_models_update(&self, id: u64, model_ids: Vec<String>) {
        if model_ids.is_empty() {
            return;
        }
        self.apply_credential_metadata_update(
            id,
            None,
            None,
            None,
            None,
            Some(model_ids),
            Some(Utc::now().to_rfc3339()),
        );
    }

    fn apply_credential_metadata_update(
        &self,
        id: u64,
        subscription_title: Option<&str>,
        subscription_type: Option<&str>,
        email: Option<&str>,
        user_id: Option<&str>,
        available_model_ids: Option<Vec<String>>,
        available_models_cached_at: Option<String>,
    ) {
        let next_email = normalize_optional_metadata(email);
        let next_user_id = normalize_optional_metadata(user_id);
        let next_available_model_ids = available_model_ids.map(|ids| normalize_model_entries(&ids));

        if subscription_title.is_none()
            && subscription_type.is_none()
            && next_email.is_none()
            && next_user_id.is_none()
            && next_available_model_ids.is_none()
            && available_models_cached_at.is_none()
        {
            return;
        }

        let dispatch = self.dispatch_config();
        let now = Instant::now();
        {
            let _state_write_guard = self.state_write_lock.lock();
            let mut entries = self.entries.lock();
            let Some(entry) = entries.iter_mut().find(|entry| entry.id == id) else {
                return;
            };

            let old_title = entry.credentials.subscription_title.clone();
            let old_type = entry.credentials.subscription_type.clone();
            let old_email = entry.credentials.email.clone();
            let old_user_id = entry.credentials.user_id.clone();
            let old_available_model_ids = entry.credentials.available_model_ids.clone();
            let old_available_models_cached_at =
                entry.credentials.available_models_cached_at.clone();
            let next_title = subscription_title
                .map(str::to_string)
                .or_else(|| old_title.clone());
            let next_type = subscription_type
                .map(str::to_string)
                .or_else(|| old_type.clone());
            let next_email = next_email.clone().or_else(|| old_email.clone());
            let next_user_id = next_user_id.clone().or_else(|| old_user_id.clone());
            let next_available_model_ids = next_available_model_ids
                .clone()
                .unwrap_or_else(|| old_available_model_ids.clone());
            let next_available_models_cached_at = available_models_cached_at
                .clone()
                .or_else(|| old_available_models_cached_at.clone());

            if old_title == next_title
                && old_type == next_type
                && old_email == next_email
                && old_user_id == next_user_id
                && old_available_model_ids == next_available_model_ids
                && old_available_models_cached_at == next_available_models_cached_at
            {
                return;
            }

            entry.credentials.subscription_title = next_title.clone();
            entry.credentials.subscription_type = next_type.clone();
            entry.credentials.email = next_email.clone();
            entry.credentials.user_id = next_user_id.clone();
            entry.credentials.available_model_ids = next_available_model_ids.clone();
            entry.credentials.available_models_cached_at = next_available_models_cached_at.clone();
            entry.credentials.normalize_model_capabilities();
            if !entry.credentials.available_model_ids.is_empty() {
                entry.available_models_refresh_in_progress = false;
                entry.available_models_refresh_cooldown_until = None;
            }
            Self::sync_rate_limit_bucket_runtime(entry, &dispatch, now);

            tracing::info!(
                "凭据 #{} 元数据已更新: title {:?} -> {:?}, type {:?} -> {:?}, email {:?} -> {:?}, userId {:?} -> {:?}, availableModels {} -> {}",
                id,
                old_title,
                next_title,
                old_type,
                next_type,
                old_email,
                next_email,
                old_user_id,
                next_user_id,
                old_available_model_ids.len(),
                next_available_model_ids.len()
            );
            let metadata_patch = CredentialMetadataPatch {
                subscription_title: (old_title != next_title).then(|| next_title.clone()),
                subscription_type: (old_type != next_type).then(|| next_type.clone()),
                email: (old_email != next_email).then(|| next_email.clone()),
                user_id: (old_user_id != next_user_id).then(|| next_user_id.clone()),
                available_model_ids: (old_available_model_ids != next_available_model_ids)
                    .then(|| next_available_model_ids.clone()),
                available_models_cached_at: (old_available_models_cached_at
                    != next_available_models_cached_at)
                    .then(|| next_available_models_cached_at.clone()),
            };

            if self.state_store.is_external() {
                match self
                    .state_store
                    .patch_credential_metadata(id, &metadata_patch)
                {
                    Ok(true) => self.try_bump_state_change_revision(StateChangeKind::Credentials),
                    Ok(false) => {}
                    Err(err) => {
                        tracing::warn!("凭据元数据更新后持久化失败（不影响本次请求）: {}", err)
                    }
                }
            } else {
                let credentials = Self::persisted_credentials_from_entries(&entries);
                if let Err(err) = self.persist_credentials_snapshot(&credentials) {
                    tracing::warn!("凭据元数据更新后持久化失败（不影响本次请求）: {}", err);
                }
            }
        }

        self.availability_notify.notify_waiters();
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

        if dispatch.suspicious_activity_auto_disable_enabled
            && dispatch.suspicious_activity_auto_disable_threshold == 0
        {
            anyhow::bail!(
                "suspiciousActivityAutoDisableThreshold 必须大于 0，或关闭 suspiciousActivityAutoDisableEnabled"
            );
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

    fn load_shared_dispatch_runtime_snapshots_for(
        &self,
        credentials: &[DispatchRuntimeCredential],
        now_epoch_ms: u64,
    ) -> anyhow::Result<HashMap<u64, DispatchRuntimeSnapshot>> {
        self.state_store
            .load_dispatch_runtime_snapshots(credentials, now_epoch_ms)
    }

    fn collect_shared_dispatch_snapshot_candidates(
        &self,
        dispatch: &DispatchConfig,
        model: Option<&str>,
        model_requirement: ModelRequirement,
        excluded_credential_ids: &HashSet<u64>,
        credential_group_scope: Option<&CredentialGroupScope>,
        now: Instant,
        now_utc: &DateTime<Utc>,
        fallback_next_ready_at: Option<Instant>,
    ) -> Result<SharedDispatchSnapshotCandidates, ReservationFailure> {
        let entries = self.entries.lock();
        if entries.is_empty() {
            return Err(ReservationFailure::NoCredentials);
        }

        let mut has_enabled = false;
        let mut has_scope_match = false;
        let mut has_supported = false;
        let mut next_ready_at = fallback_next_ready_at;
        let mut credentials = Vec::new();
        let mut candidate_ids = HashSet::new();

        for entry in entries.iter() {
            if entry.disabled {
                continue;
            }
            has_enabled = true;

            if !Self::credential_allowed_by_scope(&entry.credentials, credential_group_scope) {
                continue;
            }
            has_scope_match = true;

            if excluded_credential_ids.contains(&entry.id) {
                continue;
            }

            if !Self::is_model_supported(&dispatch, &entry.credentials, model, model_requirement) {
                continue;
            }
            has_supported = true;

            match self.proxy_dispatch_availability(&dispatch, &entry.credentials, now) {
                ProxyDispatchAvailability::Available => {}
                ProxyDispatchAvailability::TemporarilyUnavailable { ready_at } => {
                    next_ready_at = Some(match next_ready_at {
                        Some(existing) => existing.min(ready_at),
                        None => ready_at,
                    });
                    continue;
                }
                ProxyDispatchAvailability::Unavailable => continue,
            }

            if let Some(ready_at) =
                Self::suspicious_activity_quarantine_ready_at(&entry.credentials, now, now_utc)
            {
                next_ready_at = Some(match next_ready_at {
                    Some(existing) => existing.min(ready_at),
                    None => ready_at,
                });
                continue;
            }

            if let Some(ready_at) = entry
                .background_refresh_cooldown_until
                .filter(|until| *until > now)
            {
                next_ready_at = Some(match next_ready_at {
                    Some(existing) => existing.min(ready_at),
                    None => ready_at,
                });
                continue;
            }

            candidate_ids.insert(entry.id);
            credentials.push(DispatchRuntimeCredential {
                id: entry.id,
                bucket_policy: dispatch.shared_bucket_policy_for(&entry.credentials),
            });
        }

        if !has_enabled {
            return Err(ReservationFailure::AllDisabled);
        }
        if !has_scope_match {
            return Err(ReservationFailure::NoCredentialScopeMatch);
        }
        if !has_supported {
            return Err(ReservationFailure::NoModelSupport);
        }
        if credentials.is_empty() {
            return Err(ReservationFailure::AllTemporarilyUnavailable { next_ready_at });
        }

        Ok(SharedDispatchSnapshotCandidates {
            credentials,
            candidate_ids,
            next_ready_at,
        })
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
        excluded_credential_ids: &HashSet<u64>,
        session_affinity_cache_key: Option<&str>,
        credential_group_scope: Option<&CredentialGroupScope>,
    ) -> Result<(u64, KiroCredentials, CallLease), ReservationFailure> {
        let dispatch = self.dispatch_config();
        let mode = dispatch.mode.clone();
        let affinity_candidate_id = session_affinity_cache_key
            .and_then(|key| self.load_session_affinity_credential_id(key));
        let mut entries = self.entries.lock();
        let now = Instant::now();
        let now_utc = Utc::now();
        let model_requirement = Self::model_requirement(model);

        if entries.is_empty() {
            return Err(ReservationFailure::NoCredentials);
        }

        let mut current_id = self.current_id.lock();
        let current_id_value = *current_id;
        let is_balanced = mode == "balanced";
        let mut has_enabled = false;
        let mut has_scope_match = false;
        let mut has_supported = false;
        let mut selected_index: Option<usize> = None;
        let mut priority_key: Option<(u8, u32, u8, usize, u8, u8, u64)> = None;
        let mut balanced_key: Option<(u8, u8, u8, usize, u64, u32, u64)> = None;
        let mut next_ready_at: Option<Instant> = None;

        for (index, entry) in entries.iter_mut().enumerate() {
            Self::refresh_entry_runtime(entry, now);

            if entry.disabled {
                continue;
            }
            has_enabled = true;
            if !Self::credential_allowed_by_scope(&entry.credentials, credential_group_scope) {
                continue;
            }
            has_scope_match = true;
            if excluded_credential_ids.contains(&entry.id) {
                continue;
            }

            if !Self::is_model_supported(&dispatch, &entry.credentials, model, model_requirement) {
                continue;
            }
            has_supported = true;

            match self.proxy_dispatch_availability(&dispatch, &entry.credentials, now) {
                ProxyDispatchAvailability::Available => {}
                ProxyDispatchAvailability::TemporarilyUnavailable { ready_at } => {
                    next_ready_at = Some(match next_ready_at {
                        Some(existing) => existing.min(ready_at),
                        None => ready_at,
                    });
                    continue;
                }
                ProxyDispatchAvailability::Unavailable => continue,
            }

            if let Some(ready_at) =
                Self::suspicious_activity_quarantine_ready_at(&entry.credentials, now, &now_utc)
            {
                next_ready_at = Some(match next_ready_at {
                    Some(existing) => existing.min(ready_at),
                    None => ready_at,
                });
                continue;
            }

            let is_dispatchable = !Self::is_rate_limited(entry, now)
                && Self::bucket_is_ready_for(entry, request_weight)
                && Self::has_capacity(&dispatch, &entry.credentials, entry.active_requests);

            if !is_dispatchable {
                Self::update_min_ready_at_for(&mut next_ready_at, entry, now, request_weight);
                continue;
            }

            let suspicious_rank =
                Self::suspicious_activity_preference_rank(&dispatch, &entry.credentials);
            if is_balanced {
                let candidate_key = (
                    suspicious_rank,
                    Self::model_preference_rank(&entry.credentials, model_requirement),
                    u8::from(affinity_candidate_id != Some(entry.id)),
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
                suspicious_rank,
                entry.credentials.priority,
                u8::from(affinity_candidate_id != Some(entry.id)),
                entry.active_requests,
                Self::model_preference_rank(&entry.credentials, model_requirement),
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
        if !has_scope_match {
            return Err(ReservationFailure::NoCredentialScopeMatch);
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
        if let Some(cache_key) = session_affinity_cache_key {
            if affinity_candidate_id.is_some()
                && !entries
                    .iter()
                    .any(|entry| Some(entry.id) == affinity_candidate_id)
            {
                self.clear_session_affinity_cache_key(cache_key);
            }
            tracing::debug!(
                session_affinity_key = %cache_key,
                preferred_credential_id = ?affinity_candidate_id,
                selected_id,
                session_affinity_hit = affinity_candidate_id == Some(selected_id),
                "会话凭据亲和调度结果"
            );
        }
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
                .effective_max_concurrency_with_policy(
                    dispatch.default_max_concurrency,
                    dispatch.account_type_dispatch_policy_for(&entry.credentials),
                )
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
        excluded_credential_ids: &HashSet<u64>,
        session_affinity_cache_key: Option<&str>,
        credential_group_scope: Option<&CredentialGroupScope>,
    ) -> Result<(u64, KiroCredentials, CallLease), ReservationFailure> {
        let dispatch = self.dispatch_config();
        let model_requirement = Self::model_requirement(model);
        let mode = dispatch.mode.clone();
        let is_balanced = mode == "balanced";
        let affinity_candidate_id = session_affinity_cache_key
            .and_then(|key| self.load_session_affinity_credential_id(key));
        let retry_budget = self.total_count().max(1).saturating_mul(2);
        let mut fallback_next_ready_at: Option<Instant> = None;

        for _ in 0..retry_budget {
            let now = Instant::now();
            let now_utc = Utc::now();
            let now_epoch_ms = current_epoch_ms();
            let snapshot_candidates = self.collect_shared_dispatch_snapshot_candidates(
                &dispatch,
                model,
                model_requirement,
                excluded_credential_ids,
                credential_group_scope,
                now,
                &now_utc,
                fallback_next_ready_at,
            )?;
            let snapshot_candidate_count = snapshot_candidates.credentials.len();
            let snapshot_started_at = Instant::now();
            let snapshots = self
                .load_shared_dispatch_runtime_snapshots_for(
                    &snapshot_candidates.credentials,
                    now_epoch_ms,
                )
                .map_err(|err| {
                    tracing::warn!("读取共享调度热态失败: {}", err);
                    ReservationFailure::AllTemporarilyUnavailable {
                        next_ready_at: None,
                    }
                })?;
            let snapshot_elapsed_ms = snapshot_started_at.elapsed().as_millis();
            if snapshot_elapsed_ms >= 100 {
                tracing::warn!(
                    shared_dispatch_candidate_count = snapshot_candidate_count,
                    shared_dispatch_snapshot_ms = snapshot_elapsed_ms,
                    total_credentials = self.total_count(),
                    "共享调度候选热态读取较慢"
                );
            } else {
                tracing::debug!(
                    shared_dispatch_candidate_count = snapshot_candidate_count,
                    shared_dispatch_snapshot_ms = snapshot_elapsed_ms,
                    "共享调度候选热态读取完成"
                );
            }

            let selection = {
                let entries = self.entries.lock();
                if entries.is_empty() {
                    return Err(ReservationFailure::NoCredentials);
                }

                let current_id_value = *self.current_id.lock();
                let mut has_enabled = false;
                let mut has_scope_match = false;
                let mut has_supported = false;
                let mut selected_id: Option<u64> = None;
                let mut selected_credentials: Option<KiroCredentials> = None;
                let mut selected_max_concurrency: Option<usize> = None;
                let mut selected_bucket_policy: Option<DispatchRuntimeBucketPolicy> = None;
                let mut priority_key: Option<(u8, u32, u8, usize, u8, u8, u64)> = None;
                let mut balanced_key: Option<(u8, u8, u8, usize, u64, u32, u64)> = None;
                let mut next_ready_at: Option<Instant> = snapshot_candidates.next_ready_at;

                for entry in entries.iter() {
                    if entry.disabled {
                        continue;
                    }
                    has_enabled = true;
                    if !Self::credential_allowed_by_scope(
                        &entry.credentials,
                        credential_group_scope,
                    ) {
                        continue;
                    }
                    has_scope_match = true;
                    if excluded_credential_ids.contains(&entry.id) {
                        continue;
                    }

                    if !Self::is_model_supported(
                        &dispatch,
                        &entry.credentials,
                        model,
                        model_requirement,
                    ) {
                        continue;
                    }
                    has_supported = true;

                    match self.proxy_dispatch_availability(&dispatch, &entry.credentials, now) {
                        ProxyDispatchAvailability::Available => {}
                        ProxyDispatchAvailability::TemporarilyUnavailable { ready_at } => {
                            next_ready_at = Some(match next_ready_at {
                                Some(existing) => existing.min(ready_at),
                                None => ready_at,
                            });
                            continue;
                        }
                        ProxyDispatchAvailability::Unavailable => continue,
                    }

                    // 候选预筛选和 Redis snapshot 读取之间可能发生 Admin 写入或热重载；
                    // 这里保留防御性复核，确保最终选择仍以最新本地状态为准。
                    if !snapshot_candidates.candidate_ids.contains(&entry.id) {
                        continue;
                    }

                    if let Some(ready_at) = Self::suspicious_activity_quarantine_ready_at(
                        &entry.credentials,
                        now,
                        &now_utc,
                    ) {
                        next_ready_at = Some(match next_ready_at {
                            Some(existing) => existing.min(ready_at),
                            None => ready_at,
                        });
                        continue;
                    }

                    if let Some(ready_at) = entry
                        .background_refresh_cooldown_until
                        .filter(|until| *until > now)
                    {
                        next_ready_at = Some(match next_ready_at {
                            Some(existing) => existing.min(ready_at),
                            None => ready_at,
                        });
                        continue;
                    }

                    let runtime =
                        Self::shared_dispatch_snapshot_for_entry(entry, &dispatch, &snapshots);
                    let is_dispatchable = runtime
                        .cooldown_until_epoch_ms
                        .map_or(true, |until| until <= now_epoch_ms)
                        && Self::shared_bucket_is_ready_for(&runtime, request_weight)
                        && Self::has_capacity(
                            &dispatch,
                            &entry.credentials,
                            runtime.active_requests,
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

                    let suspicious_rank =
                        Self::suspicious_activity_preference_rank(&dispatch, &entry.credentials);
                    if is_balanced {
                        let candidate_key = (
                            suspicious_rank,
                            Self::model_preference_rank(&entry.credentials, model_requirement),
                            u8::from(affinity_candidate_id != Some(entry.id)),
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
                                dispatch.effective_max_concurrency_for(&entry.credentials);
                            selected_bucket_policy =
                                dispatch.shared_bucket_policy_for(&entry.credentials);
                        }
                        continue;
                    }

                    let candidate_key = (
                        suspicious_rank,
                        entry.credentials.priority,
                        u8::from(affinity_candidate_id != Some(entry.id)),
                        runtime.active_requests,
                        Self::model_preference_rank(&entry.credentials, model_requirement),
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
                            dispatch.effective_max_concurrency_for(&entry.credentials);
                        selected_bucket_policy =
                            dispatch.shared_bucket_policy_for(&entry.credentials);
                    }
                }

                if !has_enabled {
                    return Err(ReservationFailure::AllDisabled);
                }
                if !has_scope_match {
                    return Err(ReservationFailure::NoCredentialScopeMatch);
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

                if let Some(cache_key) = session_affinity_cache_key {
                    tracing::debug!(
                        session_affinity_key = %cache_key,
                        preferred_credential_id = ?affinity_candidate_id,
                        selected_id,
                        session_affinity_hit = affinity_candidate_id == Some(selected_id),
                        "会话凭据亲和共享调度结果"
                    );
                }

                tracing::debug!(
                    "通过共享调度热态分配凭据 #{}，全局运行中请求数: {}{}",
                    selected_id,
                    reservation.snapshot.active_requests,
                    selected_credentials
                        .effective_max_concurrency_with_policy(
                            dispatch.default_max_concurrency,
                            dispatch.account_type_dispatch_policy_for(&selected_credentials),
                        )
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
        let excluded_credential_ids = HashSet::new();
        self.acquire_context_with_weight_excluding(model, request_weight, &excluded_credential_ids)
            .await
    }

    pub(crate) async fn acquire_context_with_weight_excluding(
        &self,
        model: Option<&str>,
        request_weight: f64,
        excluded_credential_ids: &HashSet<u64>,
    ) -> anyhow::Result<CallContext> {
        self.acquire_context_with_weight_excluding_and_affinity(
            model,
            request_weight,
            excluded_credential_ids,
            None,
        )
        .await
    }

    pub(crate) async fn acquire_context_with_weight_excluding_and_affinity(
        &self,
        model: Option<&str>,
        request_weight: f64,
        excluded_credential_ids: &HashSet<u64>,
        session_affinity_key: Option<&str>,
    ) -> anyhow::Result<CallContext> {
        self.acquire_context_with_weight_excluding_and_affinity_for_scope(
            model,
            request_weight,
            excluded_credential_ids,
            session_affinity_key,
            None,
        )
        .await
    }

    pub(crate) async fn acquire_context_with_weight_excluding_and_affinity_for_scope(
        &self,
        model: Option<&str>,
        request_weight: f64,
        excluded_credential_ids: &HashSet<u64>,
        session_affinity_key: Option<&str>,
        credential_group_scope: Option<&CredentialGroupScope>,
    ) -> anyhow::Result<CallContext> {
        self.acquire_context_with_weight_excluding_and_affinity_inner(
            model,
            request_weight,
            excluded_credential_ids,
            session_affinity_key,
            credential_group_scope,
            None,
        )
        .await
    }

    pub(crate) async fn acquire_context_with_background_refresh(
        self: &Arc<Self>,
        model: Option<&str>,
        request_weight: f64,
        excluded_credential_ids: &HashSet<u64>,
        session_affinity_key: Option<&str>,
        credential_group_scope: Option<&CredentialGroupScope>,
    ) -> anyhow::Result<CallContext> {
        self.acquire_context_with_weight_excluding_and_affinity_inner(
            model,
            request_weight,
            excluded_credential_ids,
            session_affinity_key,
            credential_group_scope,
            Some(self),
        )
        .await
    }

    async fn acquire_context_with_weight_excluding_and_affinity_inner(
        &self,
        model: Option<&str>,
        request_weight: f64,
        excluded_credential_ids: &HashSet<u64>,
        session_affinity_key: Option<&str>,
        credential_group_scope: Option<&CredentialGroupScope>,
        background_owner: Option<&Arc<Self>>,
    ) -> anyhow::Result<CallContext> {
        let request_weight = if request_weight.is_finite() && request_weight > 0.0 {
            request_weight
        } else {
            DEFAULT_REQUEST_WEIGHT
        };
        let session_affinity_cache_key =
            self.session_affinity_cache_key_if_enabled(model, session_affinity_key);
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
            let dispatch_for_recovery = self.dispatch_config();
            self.maybe_clear_stale_suspicious_activity_markers(&dispatch_for_recovery);

            let background_refresh_exclusions = background_owner
                .map(|owner| {
                    owner.background_refresh_exclusions_for_request(
                        model,
                        request_weight,
                        excluded_credential_ids,
                        credential_group_scope,
                    )
                })
                .unwrap_or_default();
            let effective_excluded_credential_ids;
            let reservation_excluded_credential_ids = if background_refresh_exclusions.is_empty() {
                excluded_credential_ids
            } else {
                effective_excluded_credential_ids = {
                    let mut ids = excluded_credential_ids.clone();
                    ids.extend(background_refresh_exclusions);
                    ids
                };
                &effective_excluded_credential_ids
            };

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
                self.reserve_next_credential_shared(
                    model,
                    request_weight,
                    reservation_excluded_credential_ids,
                    session_affinity_cache_key.as_deref(),
                    credential_group_scope,
                )
            } else {
                self.reserve_next_credential(
                    model,
                    request_weight,
                    reservation_excluded_credential_ids,
                    session_affinity_cache_key.as_deref(),
                    credential_group_scope,
                )
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
                Err(ReservationFailure::NoCredentialScopeMatch) => {
                    return Err(anyhow::Error::new(CredentialScopeForbiddenError));
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
                    if Self::is_runtime_refresh_coordination_error(&e) {
                        if e.downcast_ref::<RuntimeRefreshLeaseBusyError>().is_some() {
                            tracing::info!("凭据 #{} 正等待其他实例刷新 Token: {}", id, e);
                        } else {
                            tracing::warn!("凭据 #{} 需要由 leader 刷新 Token: {}", id, e);
                        }
                        last_runtime_coordination_error = Some(e);
                        attempt_count += 1;
                        let has_available = self.defer_runtime_refresh_credential(
                            id,
                            self.runtime_refresh_coordination_cooldown(),
                        );

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

    pub(crate) fn enabled_supported_credential_count(&self, model: Option<&str>) -> usize {
        self.enabled_supported_credential_count_for_scope(model, None)
    }

    pub(crate) fn enabled_supported_credential_count_for_scope(
        &self,
        model: Option<&str>,
        credential_group_scope: Option<&CredentialGroupScope>,
    ) -> usize {
        let dispatch = self.dispatch_config();
        let model_requirement = Self::model_requirement(model);
        let mut entries = self.entries.lock();
        let now = Instant::now();
        Self::refresh_runtime_state(&mut entries, now);

        entries
            .iter()
            .filter(|entry| {
                !entry.disabled
                    && Self::credential_allowed_by_scope(&entry.credentials, credential_group_scope)
                    && Self::is_model_supported(
                        &dispatch,
                        &entry.credentials,
                        model,
                        model_requirement,
                    )
            })
            .count()
    }

    pub(crate) fn enabled_supported_credential_ids_at_priority(
        &self,
        model: Option<&str>,
        priority: u32,
    ) -> Vec<u64> {
        self.enabled_supported_credential_ids_at_priority_for_scope(model, priority, None)
    }

    pub(crate) fn enabled_supported_credential_ids_at_priority_for_scope(
        &self,
        model: Option<&str>,
        priority: u32,
        credential_group_scope: Option<&CredentialGroupScope>,
    ) -> Vec<u64> {
        let dispatch = self.dispatch_config();
        let model_requirement = Self::model_requirement(model);
        let mut entries = self.entries.lock();
        let now = Instant::now();
        Self::refresh_runtime_state(&mut entries, now);

        entries
            .iter()
            .filter(|entry| {
                !entry.disabled
                    && entry.credentials.priority == priority
                    && Self::credential_allowed_by_scope(&entry.credentials, credential_group_scope)
                    && Self::is_model_supported(
                        &dispatch,
                        &entry.credentials,
                        model,
                        model_requirement,
                    )
            })
            .map(|entry| entry.id)
            .collect()
    }

    pub(crate) fn has_enabled_supported_credential_below_priority(
        &self,
        model: Option<&str>,
        priority: u32,
        excluded_credential_ids: &HashSet<u64>,
        credential_group_scope: Option<&CredentialGroupScope>,
    ) -> bool {
        let dispatch = self.dispatch_config();
        let model_requirement = Self::model_requirement(model);
        let mut entries = self.entries.lock();
        let now = Instant::now();
        Self::refresh_runtime_state(&mut entries, now);

        entries.iter().any(|entry| {
            !entry.disabled
                && entry.credentials.priority > priority
                && !excluded_credential_ids.contains(&entry.id)
                && Self::credential_allowed_by_scope(&entry.credentials, credential_group_scope)
                && Self::is_model_supported(&dispatch, &entry.credentials, model, model_requirement)
        })
    }

    fn best_enabled_current_candidate<'a>(
        dispatch: &DispatchConfig,
        entries: &'a [CredentialEntry],
        now_utc: &DateTime<Utc>,
    ) -> Option<&'a CredentialEntry> {
        entries
            .iter()
            .filter(|entry| {
                !entry.disabled
                    && !Self::is_suspicious_activity_quarantined_at(&entry.credentials, now_utc)
            })
            .min_by_key(|entry| {
                (
                    Self::suspicious_activity_preference_rank(dispatch, &entry.credentials),
                    entry.credentials.priority,
                    entry.id,
                )
            })
            .or_else(|| {
                entries
                    .iter()
                    .filter(|entry| !entry.disabled)
                    .min_by_key(|entry| {
                        (
                            Self::suspicious_activity_preference_rank(dispatch, &entry.credentials),
                            entry.credentials.priority,
                            entry.id,
                        )
                    })
            })
    }

    /// 选择当前最佳未禁用凭据作为当前凭据（内部方法）
    ///
    /// 优先选择未命中过 suspicious activity 且不在隔离期内的账号。
    fn select_highest_priority(&self) {
        let dispatch = self.dispatch_config();
        let entries = self.entries.lock();
        let mut current_id = self.current_id.lock();
        let now_utc = Utc::now();

        if let Some(best) = Self::best_enabled_current_candidate(&dispatch, &entries, &now_utc) {
            if best.id != *current_id {
                tracing::info!(
                    "切换到当前最佳凭据: #{} -> #{}（优先级 {}，suspiciousActivityCount={}）",
                    *current_id,
                    best.id,
                    best.credentials.priority,
                    best.credentials.suspicious_activity_count
                );
                *current_id = best.id;
            }
        }
    }

    async fn refresh_credentials_via_upstream(
        &self,
        id: u64,
        current_creds: &KiroCredentials,
    ) -> anyhow::Result<KiroCredentials> {
        let mut active_credentials = current_creds.clone();
        let mut retried_after_proxy_failover = false;
        let refreshed = loop {
            let effective_proxy = self.effective_proxy_for_credentials(&active_credentials)?;
            match refresh_token(&active_credentials, &self.config, effective_proxy.as_ref()).await {
                Ok(refreshed) => break refreshed,
                Err(err) => {
                    if err.downcast_ref::<RefreshTokenInvalidError>().is_some()
                        && self.runtime_refresh_lease_enabled()
                        && !self.refresh_token_still_current_in_shared_state(
                            id,
                            current_creds.refresh_token.as_deref(),
                        )
                    {
                        tracing::warn!(
                            "凭据 #{} refresh 返回 invalid_grant，但共享状态中的 refreshToken 已变化，等待最新刷新结果同步",
                            id
                        );
                        return Err(self.runtime_refresh_busy_error(None));
                    }

                    if !retried_after_proxy_failover
                        && self.report_proxy_transport_failure_for_error(id, "refreshToken", &err)
                    {
                        retried_after_proxy_failover = true;
                        active_credentials = self.current_credentials(id)?;
                        tracing::info!(
                            credential_id = id,
                            proxy_id = ?active_credentials.proxy_id,
                            "代理池故障迁移后重试 Token 刷新"
                        );
                        continue;
                    }

                    return Err(err);
                }
            }
        };

        if is_token_expired(&refreshed) {
            anyhow::bail!("刷新后的 Token 仍然无效或已过期");
        }

        let committed = self.commit_refreshed_credential(
            id,
            current_creds.refresh_token.as_deref(),
            refreshed,
        )?;
        if committed.access_token.is_none() || is_token_expired(&committed) {
            return Err(self.runtime_refresh_busy_error(None));
        }
        Ok(committed)
    }

    async fn refresh_credentials_with_runtime_coordination(
        &self,
        id: u64,
        current_creds: &KiroCredentials,
    ) -> anyhow::Result<KiroCredentials> {
        if self.runtime_refresh_lease_enabled() {
            let acquisition = self
                .state_store
                .try_acquire_runtime_refresh_lease(
                    id,
                    &self.runtime_refresh_instance_id(),
                    RUNTIME_REFRESH_LEASE_TTL,
                )?
                .expect("runtime refresh lease backend should exist when enabled");

            match acquisition {
                RuntimeRefreshLeaseAcquisition::Acquired(lease) => {
                    let refresh_result = self
                        .refresh_credentials_via_upstream(id, current_creds)
                        .await;
                    match self.state_store.release_runtime_refresh_lease(&lease) {
                        Ok(Some(false)) => {
                            tracing::warn!("凭据 #{} 的 refresh 租约已转移，跳过本地释放", id);
                        }
                        Ok(Some(true)) | Ok(None) => {}
                        Err(err) => {
                            tracing::warn!("释放凭据 #{} 的 refresh 租约失败: {}", id, err);
                        }
                    }
                    return refresh_result;
                }
                RuntimeRefreshLeaseAcquisition::HeldByPeer { owner_instance_id } => {
                    return Err(self.runtime_refresh_busy_error(owner_instance_id));
                }
            }
        }

        if let Some(status) = self.runtime_refresh_coordination_status()? {
            if !status.is_leader {
                return Err(anyhow::Error::new(RuntimeRefreshLeaderRequiredError {
                    instance_id: status.instance_id,
                    leader_id: status.leader_id,
                }));
            }
        }

        self.refresh_credentials_via_upstream(id, current_creds)
            .await
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
        let needs_refresh = Self::needs_token_refresh(credentials);

        let creds = if needs_refresh {
            // 获取凭据级刷新锁，仅串行同一账号的刷新流程
            let refresh_lock = self.refresh_lock_for(id)?;
            let _guard = refresh_lock.lock().await;

            // 第二次检查：获取锁后重新读取凭据，因为其他请求可能已经完成刷新
            let mut current_creds = self.current_credentials(id)?;

            if Self::needs_token_refresh(&current_creds) {
                if self.state_store.is_external() {
                    if let Err(err) = self.sync_external_state_if_changed() {
                        tracing::warn!("按需同步共享凭据状态失败，将继续使用本地状态: {}", err);
                    }
                    current_creds = self.current_credentials(id)?;
                }

                if Self::needs_token_refresh(&current_creds) {
                    self.refresh_credentials_with_runtime_coordination(id, &current_creds)
                        .await?
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
                if entry.disabled {
                    credential.disabled_reason = entry
                        .disabled_reason
                        .map(|reason| reason.as_str().to_string())
                        .or_else(|| credential.disabled_reason.clone());
                    if credential.disabled_at.is_none() {
                        credential.disabled_at = Some(Utc::now().to_rfc3339());
                    }
                } else {
                    credential.disabled_reason = None;
                    credential.disabled_at = None;
                    credential.last_error_status = None;
                    credential.last_error_summary = None;
                }
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

    fn error_summary_for_persistence(error_summary: Option<&str>) -> Option<String> {
        error_summary.map(|summary| {
            const MAX_PERSISTED_ERROR_SUMMARY_CHARS: usize = 512;
            const TRUNCATION_SUFFIX: &str = "...";
            if summary.chars().count() <= MAX_PERSISTED_ERROR_SUMMARY_CHARS {
                summary.to_string()
            } else {
                let mut truncated: String = summary
                    .chars()
                    .take(MAX_PERSISTED_ERROR_SUMMARY_CHARS - TRUNCATION_SUFFIX.len())
                    .collect();
                truncated.push_str(TRUNCATION_SUFFIX);
                truncated
            }
        })
    }

    fn apply_disabled_metadata(
        credential: &mut KiroCredentials,
        reason: DisabledReason,
        disabled_at: &str,
        status_code: Option<u16>,
        error_summary: Option<&str>,
    ) {
        credential.disabled = true;
        credential.disabled_reason = Some(reason.as_str().to_string());
        credential.disabled_at = Some(disabled_at.to_string());
        credential.last_error_status = status_code;
        credential.last_error_summary = Self::error_summary_for_persistence(error_summary);
    }

    fn clear_disabled_metadata(credential: &mut KiroCredentials) {
        credential.disabled = false;
        credential.disabled_reason = None;
        credential.disabled_at = None;
        credential.last_error_status = None;
        credential.last_error_summary = None;
    }

    fn persist_disabled_metadata(
        &self,
        id: u64,
        reason: DisabledReason,
        disabled_at: &str,
        status_code: Option<u16>,
        error_summary: Option<&str>,
    ) {
        let patch = CredentialHealthPatch {
            disabled: Some(true),
            disabled_reason: Some(Some(reason.as_str().to_string())),
            disabled_at: Some(Some(disabled_at.to_string())),
            last_error_status: Some(status_code),
            last_error_summary: Some(Self::error_summary_for_persistence(error_summary)),
            suspicious_activity_count: None,
            suspicious_activity_first_seen_at: None,
            suspicious_activity_last_seen_at: None,
            suspicious_activity_quarantine_until: None,
            suspicious_activity_recovery_success_count: None,
        };

        match self.state_store.patch_credential_health(id, &patch) {
            Ok(true) => self.try_bump_state_change_revision(StateChangeKind::Credentials),
            Ok(false) => {}
            Err(err) => tracing::warn!(
                credential_id = id,
                reason = reason.as_str(),
                "持久化凭据禁用状态失败: {}",
                err
            ),
        }
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

    fn runtime_refresh_instance_id(&self) -> String {
        self.config.resolved_instance_id()
    }

    pub(crate) fn runtime_refresh_coordination_cooldown(&self) -> StdDuration {
        let baseline = StdDuration::from_secs(5);
        let coordinated = self
            .state_store
            .runtime_coordination_interval()
            .and_then(|interval| interval.checked_mul(2))
            .unwrap_or(baseline);
        coordinated.max(baseline)
    }

    fn runtime_refresh_busy_error(&self, owner_instance_id: Option<String>) -> anyhow::Error {
        anyhow::Error::new(RuntimeRefreshLeaseBusyError {
            instance_id: self.runtime_refresh_instance_id(),
            owner_instance_id,
        })
    }

    fn is_runtime_refresh_coordination_error(err: &anyhow::Error) -> bool {
        err.downcast_ref::<RuntimeRefreshLeaderRequiredError>()
            .is_some()
            || err.downcast_ref::<RuntimeRefreshLeaseBusyError>().is_some()
    }

    fn spawn_background_token_refresh(self: &Arc<Self>, id: u64) {
        let manager = Arc::clone(self);
        tokio::spawn(async move {
            manager.run_background_token_refresh(id).await;
        });
    }

    async fn run_background_token_refresh(self: Arc<Self>, id: u64) {
        let started_at = Instant::now();
        tracing::info!("凭据 #{} Token 后台刷新已启动", id);

        let refresh_lock = match self.refresh_lock_for(id) {
            Ok(lock) => lock,
            Err(err) => {
                tracing::warn!("凭据 #{} Token 后台刷新无法获取刷新锁: {}", id, err);
                self.finish_background_token_refresh(id, BackgroundRefreshOutcome::RetryLater);
                return;
            }
        };
        let _guard = refresh_lock.lock().await;

        if self.state_store.is_external() {
            if let Err(err) = self.sync_external_state_if_changed() {
                tracing::warn!("凭据 #{} 后台刷新前同步共享状态失败: {}", id, err);
            }
        }

        let credentials = match self.current_credentials(id) {
            Ok(credentials) => credentials,
            Err(err) => {
                tracing::warn!("凭据 #{} Token 后台刷新读取凭据失败: {}", id, err);
                self.finish_background_token_refresh(id, BackgroundRefreshOutcome::RetryLater);
                return;
            }
        };

        if !Self::needs_token_refresh(&credentials) {
            tracing::debug!("凭据 #{} 已被其他路径刷新，后台刷新跳过", id);
            self.finish_background_token_refresh(id, BackgroundRefreshOutcome::Success);
            return;
        }

        match self
            .refresh_credentials_with_runtime_coordination(id, &credentials)
            .await
        {
            Ok(_) => {
                tracing::info!(
                    credential_id = id,
                    elapsed_ms = started_at.elapsed().as_millis(),
                    "凭据 Token 后台刷新成功"
                );
                self.finish_background_token_refresh(id, BackgroundRefreshOutcome::Success);
            }
            Err(err) => {
                if Self::is_runtime_refresh_coordination_error(&err) {
                    tracing::warn!(
                        credential_id = id,
                        elapsed_ms = started_at.elapsed().as_millis(),
                        error = %err,
                        "凭据 Token 后台刷新等待运行时协调，稍后重试"
                    );
                    self.finish_background_token_refresh(id, BackgroundRefreshOutcome::RetryLater);
                    return;
                }

                if err.downcast_ref::<RefreshTokenInvalidError>().is_some() {
                    tracing::warn!("凭据 #{} 后台刷新发现 refreshToken 永久失效: {}", id, err);
                    self.report_refresh_token_invalid(id);
                } else {
                    tracing::warn!(
                        credential_id = id,
                        elapsed_ms = started_at.elapsed().as_millis(),
                        error = %err,
                        "凭据 Token 后台刷新失败，保留凭据并延后重试"
                    );
                }
                self.finish_background_token_refresh(id, BackgroundRefreshOutcome::RetryLater);
            }
        }
    }

    fn finish_background_token_refresh(&self, id: u64, outcome: BackgroundRefreshOutcome) {
        let retry_cooldown_until = Instant::now() + BACKGROUND_REFRESH_RETRY_COOLDOWN;
        {
            let mut entries = self.entries.lock();
            if let Some(entry) = entries.iter_mut().find(|entry| entry.id == id) {
                entry.background_refresh_in_progress = false;
                match outcome {
                    BackgroundRefreshOutcome::Success => {
                        entry.background_refresh_cooldown_until = None;
                        entry.refresh_failure_count = 0;
                    }
                    BackgroundRefreshOutcome::RetryLater => {
                        if !entry.disabled {
                            entry.background_refresh_cooldown_until = Some(retry_cooldown_until);
                        } else {
                            entry.background_refresh_cooldown_until = None;
                        }
                    }
                }
            }
        }
        self.availability_notify.notify_waiters();
    }

    pub(crate) fn trigger_available_models_refresh_after_model_signal(
        self: &Arc<Self>,
        id: u64,
        token: &str,
        model: &str,
        reason: &'static str,
    ) {
        if !self.try_start_available_models_refresh(id) {
            return;
        }

        let manager = Arc::clone(self);
        let token = token.to_string();
        let model = model.to_string();
        tokio::spawn(async move {
            manager
                .run_available_models_refresh(id, token, model, reason)
                .await;
        });
    }

    fn try_start_available_models_refresh(&self, id: u64) -> bool {
        let now = Instant::now();
        let mut entries = self.entries.lock();
        let Some(entry) = entries.iter_mut().find(|entry| entry.id == id) else {
            return false;
        };

        if entry.disabled || entry.available_models_refresh_in_progress {
            return false;
        }
        if entry
            .available_models_refresh_cooldown_until
            .is_some_and(|until| until > now)
        {
            return false;
        }

        entry.available_models_refresh_in_progress = true;
        true
    }

    async fn run_available_models_refresh(
        self: Arc<Self>,
        id: u64,
        request_token: String,
        model: String,
        reason: &'static str,
    ) {
        let started_at = Instant::now();

        if self.state_store.is_external() {
            if let Err(err) = self.sync_external_state_if_changed() {
                tracing::warn!(
                    credential_id = id,
                    model = %model,
                    reason,
                    error = %err,
                    "刷新可用模型列表前同步共享状态失败，将继续使用本地状态"
                );
            }
        }

        let credentials = match self.current_credentials(id) {
            Ok(credentials) => credentials,
            Err(err) => {
                tracing::warn!(
                    credential_id = id,
                    model = %model,
                    reason,
                    error = %err,
                    "刷新可用模型列表读取凭据失败"
                );
                self.finish_available_models_refresh(id, AvailableModelsRefreshOutcome::RetryLater);
                return;
            }
        };

        let token = credentials
            .access_token
            .as_deref()
            .filter(|token| !token.trim().is_empty())
            .unwrap_or(&request_token);
        let config = self.config.clone();
        let token = token.to_string();
        let response = match self
            .proxy_pool_retry_management_call(
                id,
                &credentials,
                "ListAvailableModels",
                move |credentials, proxy| {
                    let config = config.clone();
                    let token = token.clone();
                    async move {
                        list_available_models(&credentials, &config, &token, None, proxy.as_ref())
                            .await
                    }
                },
            )
            .await
        {
            Ok((response, _credentials)) => response,
            Err(err) => {
                tracing::warn!(
                    credential_id = id,
                    model = %model,
                    reason,
                    elapsed_ms = started_at.elapsed().as_millis(),
                    error = %err,
                    "后台刷新可用模型列表失败，进入冷却"
                );
                self.finish_available_models_refresh(id, AvailableModelsRefreshOutcome::RetryLater);
                return;
            }
        };

        let model_ids = response.model_ids();
        if model_ids.is_empty() {
            tracing::warn!(
                credential_id = id,
                model = %model,
                reason,
                elapsed_ms = started_at.elapsed().as_millis(),
                "后台刷新可用模型列表返回空列表，进入冷却"
            );
            self.finish_available_models_refresh(id, AvailableModelsRefreshOutcome::RetryLater);
            return;
        }

        let model_count = model_ids.len();
        self.apply_available_models_update(id, model_ids);
        tracing::info!(
            credential_id = id,
            model = %model,
            reason,
            model_count,
            elapsed_ms = started_at.elapsed().as_millis(),
            "后台刷新可用模型列表成功"
        );
        self.finish_available_models_refresh(id, AvailableModelsRefreshOutcome::Success);
    }

    fn finish_available_models_refresh(&self, id: u64, outcome: AvailableModelsRefreshOutcome) {
        let retry_cooldown_until = Instant::now() + AVAILABLE_MODELS_REFRESH_RETRY_COOLDOWN;
        {
            let mut entries = self.entries.lock();
            if let Some(entry) = entries.iter_mut().find(|entry| entry.id == id) {
                entry.available_models_refresh_in_progress = false;
                match outcome {
                    AvailableModelsRefreshOutcome::Success => {
                        entry.available_models_refresh_cooldown_until = None;
                    }
                    AvailableModelsRefreshOutcome::RetryLater => {
                        if !entry.disabled {
                            entry.available_models_refresh_cooldown_until =
                                Some(retry_cooldown_until);
                        } else {
                            entry.available_models_refresh_cooldown_until = None;
                        }
                    }
                }
            }
        }
        self.availability_notify.notify_waiters();
    }

    fn refresh_token_still_current_in_shared_state(
        &self,
        id: u64,
        expected_refresh_token: Option<&str>,
    ) -> bool {
        if !self.state_store.is_external() {
            return true;
        }

        if let Err(err) = self.sync_external_state_if_changed() {
            tracing::warn!("刷新 invalid_grant 校验前同步共享状态失败: {}", err);
        }

        self.current_credentials(id)
            .map(|credentials| credentials.refresh_token.as_deref() == expected_refresh_token)
            .unwrap_or(false)
    }

    fn commit_refreshed_credential(
        &self,
        id: u64,
        expected_refresh_token: Option<&str>,
        refreshed: KiroCredentials,
    ) -> anyhow::Result<KiroCredentials> {
        let _state_write_guard = self.state_write_lock.lock();

        if self.state_store.is_external() {
            match self.state_store.compare_and_swap_refreshed_credential(
                id,
                expected_refresh_token,
                &refreshed,
                self.is_multiple_format,
            )? {
                CredentialCompareAndSwapResult::Applied => {
                    let mut entries = self.entries.lock();
                    if let Some(entry) = entries.iter_mut().find(|entry| entry.id == id) {
                        entry.credentials = refreshed.clone();
                    }
                    return Ok(refreshed);
                }
                CredentialCompareAndSwapResult::Conflict { current } => {
                    let mut entries = self.entries.lock();
                    if let Some(entry) = entries.iter_mut().find(|entry| entry.id == id) {
                        entry.credentials = current.clone();
                    }
                    return Ok(current);
                }
                CredentialCompareAndSwapResult::Missing => {
                    anyhow::bail!("共享状态中不存在凭据 #{}", id);
                }
            }
        }

        {
            let mut entries = self.entries.lock();
            if let Some(entry) = entries.iter_mut().find(|entry| entry.id == id) {
                entry.credentials = refreshed.clone();
            }
        }

        let credentials = self.persisted_credentials_snapshot();
        if let Err(err) = self.persist_credentials_snapshot(&credentials) {
            tracing::warn!("Token 刷新后持久化失败（不影响本次请求）: {}", err);
        }

        Ok(refreshed)
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
        let next_catalog = if persisted.credential_groups.is_empty() {
            None
        } else {
            Some(config.credential_groups.clone())
        };
        let catalog_changed = next_catalog
            .as_ref()
            .is_some_and(|groups| *groups != self.credential_group_catalog_snapshot());

        if previous == next && !catalog_changed {
            return false;
        }

        if previous != next {
            *self.dispatch_config.lock() = next.clone();
        }
        if let Some(groups) = next_catalog {
            self.set_credential_group_catalog_snapshot(groups);
        }

        if previous.rate_limit_cooldown_enabled != next.rate_limit_cooldown_enabled {
            self.clear_disabled_rate_limit_penalties(&next);
        } else if previous.rate_limit_cooldown_ms != next.rate_limit_cooldown_ms
            && next.rate_limit_cooldown_ms == 0
        {
            self.clear_all_rate_limit_cooldowns();
        }
        if previous.suspicious_activity_cooldown_enabled
            && !next.suspicious_activity_cooldown_enabled
        {
            self.clear_all_rate_limit_cooldowns();
        } else if previous.suspicious_activity_cooldown_ms != next.suspicious_activity_cooldown_ms
            && next.suspicious_activity_cooldown_ms == 0
        {
            self.clear_all_rate_limit_cooldowns();
        }
        if previous.model_cooldown_enabled && !next.model_cooldown_enabled {
            if let Err(err) = self.clear_all_runtime_model_restrictions() {
                tracing::warn!("外部状态关闭模型冷却后清理运行时模型限制失败: {}", err);
            }
        }
        if previous.rate_limit_bucket_capacity != next.rate_limit_bucket_capacity
            || previous.rate_limit_refill_per_second != next.rate_limit_refill_per_second
            || previous.rate_limit_refill_min_per_second != next.rate_limit_refill_min_per_second
            || previous.rate_limit_refill_recovery_step_per_success
                != next.rate_limit_refill_recovery_step_per_success
            || previous.rate_limit_refill_backoff_factor != next.rate_limit_refill_backoff_factor
            || previous.account_type_dispatch_policies != next.account_type_dispatch_policies
        {
            self.reconfigure_rate_limit_runtime(&next);
        }
        if previous.proxy_pool != next.proxy_pool {
            let active_proxy_ids: HashSet<String> = next
                .proxy_pool
                .proxies
                .iter()
                .filter(|entry| entry.enabled)
                .map(|entry| entry.id.trim().to_string())
                .filter(|id| !id.is_empty())
                .collect();
            let mut health = self.proxy_health.lock();
            if next.proxy_pool.enabled {
                health.retain(|proxy_id, _| active_proxy_ids.contains(proxy_id));
            } else {
                health.clear();
            }
        }

        self.availability_notify.notify_waiters();
        tracing::info!(
            "已从外部状态热加载调度配置: mode={}, sessionAffinityEnabled={}, queueMaxSize={}, queueMaxWaitMs={}, rateLimitCooldownMs={}, rateLimitCooldownEnabled={}, suspiciousActivityCooldownMs={}, suspiciousActivityCooldownEnabled={}, suspiciousActivityAutoClearEnabled={}, suspiciousActivityAutoClearSuccessThreshold={}, suspiciousActivityAutoClearAfterMs={}, modelCooldownEnabled={}, defaultMaxConcurrency={:?}, rateLimitBucketCapacity={}, rateLimitRefillPerSecond={}, rateLimitRefillMinPerSecond={}, rateLimitRefillRecoveryStepPerSuccess={}, rateLimitRefillBackoffFactor={}",
            next.mode,
            next.session_affinity_enabled,
            next.queue_max_size,
            next.queue_max_wait_ms,
            next.rate_limit_cooldown_ms,
            next.rate_limit_cooldown_enabled,
            next.suspicious_activity_cooldown_ms,
            next.suspicious_activity_cooldown_enabled,
            next.suspicious_activity_auto_clear_enabled,
            next.suspicious_activity_auto_clear_success_threshold,
            next.suspicious_activity_auto_clear_after_ms,
            next.model_cooldown_enabled,
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
                    entry.disabled_reason = Some(persisted_disabled_reason(&persisted));
                    entry.background_refresh_in_progress = false;
                    entry.background_refresh_cooldown_until = None;
                    entry.available_models_refresh_in_progress = false;
                    entry.available_models_refresh_cooldown_until = None;
                } else {
                    entry.disabled_reason = None;
                    if was_disabled {
                        entry.failure_count = 0;
                        entry.refresh_failure_count = 0;
                        Self::reset_rate_limit_runtime(entry, &dispatch, now);
                    } else {
                        Self::sync_rate_limit_bucket_runtime(entry, &dispatch, now);
                    }
                    if !Self::needs_token_refresh(&entry.credentials) {
                        entry.background_refresh_in_progress = false;
                        entry.background_refresh_cooldown_until = None;
                    }
                    if !entry.credentials.available_model_ids.is_empty() {
                        entry.available_models_refresh_in_progress = false;
                        entry.available_models_refresh_cooldown_until = None;
                    }
                }
            }

            entries.retain(|entry| persisted_ids.contains(&entry.id) || entry.active_requests > 0);

            for (_, credential) in persisted_by_id {
                entries.push(CredentialEntry {
                    id: credential.id.expect("persisted credential id must exist"),
                    disabled: credential.disabled,
                    disabled_reason: credential
                        .disabled
                        .then(|| persisted_disabled_reason(&credential)),
                    credentials: credential.clone(),
                    failure_count: 0,
                    refresh_failure_count: 0,
                    success_count: 0,
                    pending_success_count_delta: 0,
                    token_usage_count: 0,
                    pending_token_usage_count_delta: 0,
                    input_tokens: 0,
                    pending_input_tokens_delta: 0,
                    output_tokens: 0,
                    pending_output_tokens_delta: 0,
                    last_used_at: None,
                    active_requests: 0,
                    rate_limit_cooldown_until: None,
                    background_refresh_cooldown_until: None,
                    rate_limit_bucket: dispatch
                        .bucket_policy_for(&credential)
                        .map(|policy| AdaptiveTokenBucket::new(policy, now)),
                    rate_limit_hit_streak: 0,
                    background_refresh_in_progress: false,
                    available_models_refresh_in_progress: false,
                    available_models_refresh_cooldown_until: None,
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
            let next_token_usage_count = persisted
                .token_usage_count
                .saturating_add(entry.pending_token_usage_count_delta);
            let next_input_tokens = persisted
                .input_tokens
                .saturating_add(entry.pending_input_tokens_delta);
            let next_output_tokens = persisted
                .output_tokens
                .saturating_add(entry.pending_output_tokens_delta);
            let next_last_used_at =
                newer_timestamp(entry.last_used_at.clone(), persisted.last_used_at.clone());

            if entry.success_count != next_success_count
                || entry.token_usage_count != next_token_usage_count
                || entry.input_tokens != next_input_tokens
                || entry.output_tokens != next_output_tokens
                || entry.last_used_at != next_last_used_at
            {
                changed = true;
            }

            entry.success_count = next_success_count;
            entry.token_usage_count = next_token_usage_count;
            entry.input_tokens = next_input_tokens;
            entry.output_tokens = next_output_tokens;
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
                            token_usage_count_delta: e.pending_token_usage_count_delta,
                            input_tokens_delta: e.pending_input_tokens_delta,
                            output_tokens_delta: e.pending_output_tokens_delta,
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
                        entry.token_usage_count = merged.token_usage_count;
                        entry.input_tokens = merged.input_tokens;
                        entry.output_tokens = merged.output_tokens;
                        entry.last_used_at = merged.last_used_at.clone();
                    }
                    entry.pending_success_count_delta = 0;
                    entry.pending_token_usage_count_delta = 0;
                    entry.pending_input_tokens_delta = 0;
                    entry.pending_output_tokens_delta = 0;
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
                            token_usage_count: e.token_usage_count,
                            input_tokens: e.input_tokens,
                            output_tokens: e.output_tokens,
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
                    entry.pending_token_usage_count_delta = 0;
                    entry.pending_input_tokens_delta = 0;
                    entry.pending_output_tokens_delta = 0;
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
        let now_utc = Utc::now();
        let dispatch = self.dispatch_config();
        let mut shared_bucket_policy = None;
        let mut suspicious_activity_patch = None;
        let mut cleared_suspicious_activity = false;
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
                if entry
                    .background_refresh_cooldown_until
                    .is_some_and(|until| until <= now)
                {
                    entry.background_refresh_cooldown_until = None;
                }
                if let Some(bucket) = entry.rate_limit_bucket.as_mut() {
                    bucket.on_success(now);
                }
                entry.rate_limit_hit_streak = 0;
                entry.success_count += 1;
                entry.pending_success_count_delta += 1;
                entry.last_used_at = Some(now_utc.to_rfc3339());

                if Self::suspicious_activity_seen(&entry.credentials) && !entry.disabled {
                    let next_recovery_success_count = entry
                        .credentials
                        .suspicious_activity_recovery_success_count
                        .saturating_add(1);
                    let should_clear_by_age = Self::suspicious_activity_auto_clear_due_to_age(
                        &entry.credentials,
                        &dispatch,
                        &now_utc,
                    );
                    let should_clear_by_success =
                        Self::suspicious_activity_auto_clear_due_to_success(
                            &entry.credentials,
                            &dispatch,
                            &now_utc,
                            next_recovery_success_count,
                        );

                    if should_clear_by_age || should_clear_by_success {
                        cleared_suspicious_activity =
                            Self::clear_suspicious_activity_fields(&mut entry.credentials);
                        if cleared_suspicious_activity {
                            suspicious_activity_patch =
                                Some(Self::suspicious_activity_clear_patch());
                            tracing::info!(
                                credential_id = id,
                                recovery_success_count = next_recovery_success_count,
                                auto_clear_success_threshold =
                                    dispatch.suspicious_activity_auto_clear_success_threshold,
                                auto_clear_after_ms =
                                    dispatch.suspicious_activity_auto_clear_after_ms,
                                "凭据 suspicious activity 标记已自动恢复清除"
                            );
                        }
                    } else if entry.credentials.suspicious_activity_recovery_success_count
                        != next_recovery_success_count
                    {
                        entry.credentials.suspicious_activity_recovery_success_count =
                            next_recovery_success_count;
                        suspicious_activity_patch =
                            Some(Self::suspicious_activity_recovery_count_patch(
                                next_recovery_success_count,
                            ));
                    }
                }

                shared_bucket_policy = dispatch.shared_bucket_policy_for(&entry.credentials);
                tracing::debug!(
                    "凭据 #{} API 调用成功（累计 {} 次）",
                    id,
                    entry.success_count
                );
            }
        }
        self.record_proxy_success_for_credential(id);
        if self.shared_dispatch_runtime_enabled() {
            if let Err(err) = self.state_store.record_dispatch_success(
                id,
                shared_bucket_policy,
                current_epoch_ms(),
            ) {
                tracing::warn!("更新共享调度成功态失败（credentialId={}）: {}", id, err);
            }
        }
        if let Some(patch) = suspicious_activity_patch {
            match self.state_store.patch_credential_health(id, &patch) {
                Ok(true) => self.try_bump_state_change_revision(StateChangeKind::Credentials),
                Ok(false) => {}
                Err(err) => tracing::warn!(
                    credential_id = id,
                    "持久化 suspicious activity 恢复状态失败: {}",
                    err
                ),
            }
        }
        if cleared_suspicious_activity {
            self.availability_notify.notify_waiters();
        }
        self.save_stats_debounced();
    }

    /// 记录指定凭据完成响应后的 token 用量。
    pub(crate) fn record_token_usage(
        &self,
        id: u64,
        input_tokens: u64,
        output_tokens: u64,
        request_id: Option<&str>,
        model: Option<&str>,
        api_type: &str,
        token_source: &str,
    ) {
        let mut recorded = false;
        {
            let mut entries = self.entries.lock();
            if let Some(entry) = entries.iter_mut().find(|entry| entry.id == id) {
                entry.token_usage_count = entry.token_usage_count.saturating_add(1);
                entry.pending_token_usage_count_delta =
                    entry.pending_token_usage_count_delta.saturating_add(1);
                entry.input_tokens = entry.input_tokens.saturating_add(input_tokens);
                entry.pending_input_tokens_delta = entry
                    .pending_input_tokens_delta
                    .saturating_add(input_tokens);
                entry.output_tokens = entry.output_tokens.saturating_add(output_tokens);
                entry.pending_output_tokens_delta = entry
                    .pending_output_tokens_delta
                    .saturating_add(output_tokens);
                recorded = true;
            }
        }

        if recorded {
            tracing::debug!(
                request_id = request_id.unwrap_or("unknown"),
                api_type,
                model = model.unwrap_or("unknown"),
                credential_id = id,
                input_tokens,
                output_tokens,
                token_source,
                "已记录凭据 token 用量"
            );
            self.save_stats_debounced();
        } else {
            tracing::warn!(
                request_id = request_id.unwrap_or("unknown"),
                api_type,
                model = model.unwrap_or("unknown"),
                credential_id = id,
                input_tokens,
                output_tokens,
                token_source,
                "记录凭据 token 用量失败：凭据不存在"
            );
        }
    }

    /// 报告指定凭据遭遇上游 429 限流。
    ///
    /// 对单账号施加短暂冷却，避免重试流量持续打到同一个受限账号上。
    pub fn report_rate_limited(&self, id: u64) {
        let dispatch = self.dispatch_config();
        let effective_enabled = {
            let entries = self.entries.lock();
            entries
                .iter()
                .find(|entry| entry.id == id)
                .map(|entry| dispatch.rate_limit_cooldown_enabled_for(&entry.credentials))
                .unwrap_or(dispatch.rate_limit_cooldown_enabled)
        };
        if !effective_enabled {
            tracing::info!(
                "凭据 #{} 遭遇上游 429，但 429 冷却与 bucket 退避对该凭据已关闭",
                id
            );
            return;
        }

        self.record_rate_limit_penalty(
            id,
            &dispatch,
            dispatch.rate_limit_cooldown_ms,
            "上游 429",
            "固定冷却",
        );
    }

    /// 报告指定凭据遭遇 Kiro suspicious activity 临时限制。
    ///
    /// 这类限制通常比普通 429 持续更久，使用独立的账号级全局冷却，避免
    /// 该账号在多个并发请求或多个实例中被持续探测。
    pub fn report_suspicious_activity_limited(&self, id: u64, error_summary: Option<&str>) {
        let dispatch = self.dispatch_config();
        if !dispatch.suspicious_activity_cooldown_enabled {
            tracing::info!(
                "凭据 #{} 遭遇上游 suspicious activity 429，但 suspicious activity 全局冷却已关闭，回退普通 429 处理",
                id
            );
            let effective_enabled = {
                let entries = self.entries.lock();
                entries
                    .iter()
                    .find(|entry| entry.id == id)
                    .map(|entry| dispatch.rate_limit_cooldown_enabled_for(&entry.credentials))
                    .unwrap_or(dispatch.rate_limit_cooldown_enabled)
            };
            let cooldown_ms = if effective_enabled {
                self.record_rate_limit_penalty(
                    id,
                    &dispatch,
                    dispatch.rate_limit_cooldown_ms,
                    "上游 429",
                    "固定冷却",
                );
                dispatch.rate_limit_cooldown_ms
            } else {
                0
            };
            self.record_suspicious_activity_marker(id, &dispatch, cooldown_ms, error_summary);
            return;
        }

        self.record_rate_limit_penalty(
            id,
            &dispatch,
            dispatch.suspicious_activity_cooldown_ms,
            "上游 suspicious activity 429",
            "全局隔离冷却",
        );
        self.record_suspicious_activity_marker(
            id,
            &dispatch,
            dispatch.suspicious_activity_cooldown_ms,
            error_summary,
        );
    }

    fn record_suspicious_activity_marker(
        &self,
        id: u64,
        dispatch: &DispatchConfig,
        cooldown_ms: u64,
        error_summary: Option<&str>,
    ) {
        let now_utc = Utc::now();
        let (patch, notify_waiters) = {
            let mut entries = self.entries.lock();
            let Some(entry_index) = entries.iter().position(|entry| entry.id == id) else {
                return;
            };
            if entries[entry_index].disabled {
                return;
            }

            let update = Self::build_suspicious_activity_marker_update(
                &entries[entry_index].credentials,
                dispatch,
                now_utc,
                cooldown_ms,
            );

            let notify_waiters = {
                let entry = &mut entries[entry_index];
                entry.credentials.suspicious_activity_count = update.count;
                entry.credentials.suspicious_activity_first_seen_at = update.first_seen_at.clone();
                entry.credentials.suspicious_activity_last_seen_at = update.last_seen_at.clone();
                entry.credentials.suspicious_activity_quarantine_until =
                    update.quarantine_until.clone();
                entry.credentials.suspicious_activity_recovery_success_count = 0;
                entry.last_used_at = Some(now_utc.to_rfc3339());

                if update.should_auto_disable {
                    let disabled_at = now_utc.to_rfc3339();
                    entry.disabled = true;
                    entry.disabled_reason = Some(DisabledReason::SuspiciousActivity);
                    entry.failure_count = MAX_FAILURES_PER_CREDENTIAL;
                    entry.background_refresh_in_progress = false;
                    entry.background_refresh_cooldown_until = None;
                    Self::apply_disabled_metadata(
                        &mut entry.credentials,
                        DisabledReason::SuspiciousActivity,
                        &disabled_at,
                        Some(429),
                        error_summary,
                    );
                    tracing::error!(
                        credential_id = id,
                        suspicious_activity_count = update.count,
                        auto_disable_threshold =
                            dispatch.suspicious_activity_auto_disable_threshold,
                        auto_disable_window_ms =
                            dispatch.suspicious_activity_auto_disable_window_ms,
                        "凭据多次触发 suspicious activity，已自动停调"
                    );
                    true
                } else {
                    tracing::warn!(
                        credential_id = id,
                        suspicious_activity_count = update.count,
                        quarantine_until = ?update.quarantine_until,
                        "凭据触发 suspicious activity，已标记并进入隔离"
                    );
                    true
                };
                true
            };

            if update.should_auto_disable {
                let mut current_id = self.current_id.lock();
                if *current_id == id {
                    if let Some(next) =
                        Self::best_enabled_current_candidate(dispatch, &entries, &now_utc)
                    {
                        tracing::info!(
                            credential_id = id,
                            alternate_credential_id = next.id,
                            "suspicious activity 自动停调后切换到其他账号"
                        );
                        *current_id = next.id;
                    }
                }
            }

            let patch = CredentialHealthPatch {
                disabled: update.should_auto_disable.then_some(true),
                disabled_reason: update.should_auto_disable.then_some(Some(
                    DisabledReason::SuspiciousActivity.as_str().to_string(),
                )),
                disabled_at: update
                    .should_auto_disable
                    .then_some(Some(now_utc.to_rfc3339())),
                last_error_status: update.should_auto_disable.then_some(Some(429)),
                last_error_summary: update
                    .should_auto_disable
                    .then_some(Self::error_summary_for_persistence(error_summary)),
                suspicious_activity_count: Some(update.count),
                suspicious_activity_first_seen_at: Some(update.first_seen_at),
                suspicious_activity_last_seen_at: Some(update.last_seen_at),
                suspicious_activity_quarantine_until: Some(update.quarantine_until),
                suspicious_activity_recovery_success_count: Some(0),
            };
            (patch, notify_waiters)
        };

        match self.state_store.patch_credential_health(id, &patch) {
            Ok(true) => self.try_bump_state_change_revision(StateChangeKind::Credentials),
            Ok(false) => {}
            Err(err) => tracing::warn!(
                credential_id = id,
                "持久化 suspicious activity 标记失败: {}",
                err
            ),
        }

        if notify_waiters {
            self.availability_notify.notify_waiters();
        }
    }

    fn record_rate_limit_penalty(
        &self,
        id: u64,
        dispatch: &DispatchConfig,
        cooldown_ms: u64,
        reason: &str,
        cooldown_label: &str,
    ) {
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

                if cooldown_ms > 0 {
                    let cooldown_until = now + StdDuration::from_millis(cooldown_ms);
                    entry.rate_limit_cooldown_until = Some(
                        entry
                            .rate_limit_cooldown_until
                            .map(|until| until.max(cooldown_until))
                            .unwrap_or(cooldown_until),
                    );
                }
                entry.last_used_at = Some(Utc::now().to_rfc3339());

                if let Some(bucket) = entry.rate_limit_bucket.as_ref() {
                    tracing::warn!(
                        "凭据 #{} 遭遇{}，{} {}ms，bucket 速率降至 {:.2}/{:.2} token/s（streak={}）",
                        id,
                        reason,
                        cooldown_label,
                        cooldown_ms,
                        bucket.current_refill_per_second,
                        bucket.policy.refill_per_second,
                        entry.rate_limit_hit_streak
                    );
                } else {
                    tracing::warn!(
                        "凭据 #{} 遭遇{}，{} {}ms（streak={}，未启用 token bucket）",
                        id,
                        reason,
                        cooldown_label,
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
                cooldown_ms,
                current_epoch_ms(),
            ) {
                tracing::warn!("更新共享调度限流态失败（credentialId={}）: {}", id, err);
            }
        }
        self.save_stats_debounced();
    }

    /// 上游明确返回模型容量不足时，即使通用 429 冷却未开启，也对该凭据做短暂共享冷却。
    /// 这类错误通常表示账号/区域暂不可用，继续在同一窗口内反复命中会放大首包等待。
    pub fn defer_capacity_limited_credential(
        &self,
        id: u64,
        model: &str,
        cooldown: StdDuration,
    ) -> bool {
        let now = Instant::now();
        let deferred_until = now + cooldown;
        let dispatch = self.dispatch_config();
        let requirement = Self::model_requirement(Some(model));
        let result = {
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

            entry.rate_limit_hit_streak = entry.rate_limit_hit_streak.saturating_add(1);
            if let Some(bucket) = entry.rate_limit_bucket.as_mut() {
                bucket.on_rate_limited(now);
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
                            e.credentials.priority,
                            Self::model_preference_rank(&e.credentials, requirement),
                            e.id,
                        )
                    })
                {
                    *current_id = next.id;
                    tracing::warn!(
                        "凭据 #{} 遭遇上游模型容量不足，已临时冷却 {}ms 并切换到凭据 #{}（model={}）",
                        id,
                        cooldown.as_millis(),
                        next.id,
                        model
                    );
                } else {
                    tracing::warn!(
                        "凭据 #{} 遭遇上游模型容量不足，已临时冷却 {}ms，当前无其他可切换凭据（model={}）",
                        id,
                        cooldown.as_millis(),
                        model
                    );
                }
            } else {
                tracing::warn!(
                    "凭据 #{} 遭遇上游模型容量不足，已临时冷却 {}ms（model={}）",
                    id,
                    cooldown.as_millis(),
                    model
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
                    "更新共享调度模型容量不足冷却失败（credentialId={}）: {}",
                    id,
                    err
                );
            }
        }
        self.save_stats_debounced();
        result
    }

    /// 当共享凭据需要等待其他实例完成 refresh 协调时，临时冷却该凭据，
    /// 让当前请求优先切换到其他可用凭据，避免在同一张共享凭据上反复重试。
    pub fn defer_runtime_refresh_credential(&self, id: u64, cooldown: StdDuration) -> bool {
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
                        "凭据 #{} 正等待运行时 refresh 协调，已临时冷却 {}ms 并切换到凭据 #{}",
                        id,
                        cooldown.as_millis(),
                        next.id
                    );
                } else {
                    tracing::warn!(
                        "凭据 #{} 正等待运行时 refresh 协调，已临时冷却 {}ms，当前无其他可切换凭据",
                        id,
                        cooldown.as_millis()
                    );
                }
            } else {
                tracing::warn!(
                    "凭据 #{} 正等待运行时 refresh 协调，已临时冷却 {}ms",
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

    /// 当某个凭据在流式请求中长期没有产出可转换的首内容块时，临时冷却该凭据。
    /// 这不会禁用账号，只是给当前及并发实例一个短窗口去调度其他候选。
    pub fn defer_slow_first_content_credential(
        &self,
        id: u64,
        model: &str,
        cooldown: StdDuration,
    ) -> bool {
        let now = Instant::now();
        let deferred_until = now + cooldown;
        let dispatch = self.dispatch_config();
        let requirement = Self::model_requirement(Some(model));
        let result = {
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
                            e.credentials.priority,
                            Self::model_preference_rank(&e.credentials, requirement),
                            e.id,
                        )
                    })
                {
                    *current_id = next.id;
                    tracing::warn!(
                        "凭据 #{} 流式首内容块过慢，已临时冷却 {}ms 并切换到凭据 #{}",
                        id,
                        cooldown.as_millis(),
                        next.id
                    );
                } else {
                    tracing::warn!(
                        "凭据 #{} 流式首内容块过慢，已临时冷却 {}ms，当前无其他可切换凭据",
                        id,
                        cooldown.as_millis()
                    );
                }
            } else {
                tracing::warn!(
                    "凭据 #{} 流式首内容块过慢，已临时冷却 {}ms",
                    id,
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
                    "更新共享调度首内容块慢启动冷却失败（credentialId={}）: {}",
                    id,
                    err
                );
            }
        }
        self.save_stats_debounced();
        result
    }

    /// 当真实 Opus 4.7/4.8 在某个凭据上出现明确慢启动时，仅对该模型族做短暂运行时限制。
    ///
    /// 该路径不会设置账号级限流冷却；并且写入前必须确认目标凭据以外仍有至少一个
    /// 已启用且当前支持该模型的候选，避免特殊情况下把整个高阶 Opus 候选池全部打入冷却。
    pub fn defer_slow_model_credential(
        &self,
        id: u64,
        model: &str,
        cooldown: StdDuration,
        reason: &str,
    ) -> bool {
        if cooldown.is_zero() {
            return false;
        }

        let dispatch = self.dispatch_config();
        if !dispatch.model_cooldown_enabled {
            tracing::debug!(
                credential_id = id,
                model,
                reason,
                "模型冷却已关闭，跳过慢模型运行时限制"
            );
            return false;
        }

        let requirement = Self::model_requirement(Some(model));
        let Some(model_label) = normalize_model_selector(model).map(|selector| selector.family)
        else {
            tracing::debug!(
                credential_id = id,
                model,
                reason,
                "模型名称无法规范化，跳过慢模型运行时限制"
            );
            return false;
        };
        let cooldown_ms = cooldown.as_millis().min(i64::MAX as u128) as i64;
        let restriction_expires_at = Utc::now() + Duration::milliseconds(cooldown_ms);

        let applied = {
            let _state_write_guard = self.state_write_lock.lock();
            let mut entries = self.entries.lock();
            let mut current_id = self.current_id.lock();

            let Some(entry_index) = entries.iter().position(|entry| entry.id == id) else {
                tracing::debug!(
                    credential_id = id,
                    model = %model_label,
                    reason,
                    "凭据不存在，跳过慢模型运行时限制"
                );
                return false;
            };

            if entries[entry_index].disabled {
                tracing::debug!(
                    credential_id = id,
                    model = %model_label,
                    reason,
                    "凭据已禁用，跳过慢模型运行时限制"
                );
                return false;
            }

            let alternate_id = Self::enabled_supported_alternate_for_model(
                &dispatch,
                &entries,
                id,
                model,
                requirement,
            )
            .map(|entry| entry.id);
            let Some(alternate_id) = alternate_id else {
                tracing::warn!(
                    credential_id = id,
                    model = %model_label,
                    reason,
                    cooldown_ms,
                    "跳过慢模型运行时限制：没有其他可用高阶 Opus 候选，避免全池冷却"
                );
                return false;
            };

            entries[entry_index].last_used_at = Some(Utc::now().to_rfc3339());
            let restriction_changed = entries[entry_index]
                .credentials
                .upsert_runtime_model_restriction(model, restriction_expires_at);

            if restriction_changed {
                let credentials = Self::persisted_credentials_from_entries(&entries);
                if let Err(err) = self.persist_credentials_snapshot(&credentials) {
                    tracing::warn!(
                        credential_id = id,
                        model = %model_label,
                        reason,
                        error = %err,
                        "持久化慢模型运行时限制失败"
                    );
                }
            }

            if *current_id == id {
                *current_id = alternate_id;
                tracing::warn!(
                    credential_id = id,
                    alternate_credential_id = alternate_id,
                    model = %model_label,
                    reason,
                    cooldown_ms,
                    restriction_changed,
                    "凭据触发慢模型运行时限制，已切换到其他高阶 Opus 候选"
                );
            } else {
                tracing::warn!(
                    credential_id = id,
                    alternate_credential_id = alternate_id,
                    model = %model_label,
                    reason,
                    cooldown_ms,
                    restriction_changed,
                    "凭据触发慢模型运行时限制"
                );
            }

            true
        };

        if applied {
            self.availability_notify.notify_waiters();
            self.save_stats_debounced();
        }
        applied
    }

    /// 当上游明确返回 `INVALID_MODEL_ID` 时，
    /// 将当前凭据视为“不支持该模型”。
    ///
    /// 当模型冷却开启时会记录模型族运行时限制；关闭时仅调整当前实例内的切卡方向，
    /// 不会对账号施加全局冷却，也不会写入运行时模型限制。
    pub fn defer_model_unsupported_credential(&self, id: u64, model: &str) -> bool {
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

            entry.last_used_at = Some(Utc::now().to_rfc3339());
            let restriction_changed = if dispatch.model_cooldown_enabled {
                entry
                    .credentials
                    .upsert_runtime_model_restriction(model, restriction_expires_at)
            } else {
                false
            };
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
                            e.credentials.priority,
                            Self::model_preference_rank(&e.credentials, requirement),
                            e.id,
                        )
                    })
                {
                    *current_id = next.id;
                    if dispatch.model_cooldown_enabled {
                        tracing::info!(
                            "凭据 #{} 不支持模型 {}，已记录运行时限制并切换到凭据 #{}",
                            id,
                            model_label,
                            next.id
                        );
                    } else {
                        tracing::info!(
                            "凭据 #{} 不支持模型 {}，模型冷却已关闭，切换到凭据 #{}",
                            id,
                            model_label,
                            next.id
                        );
                    }
                } else {
                    if dispatch.model_cooldown_enabled {
                        tracing::warn!(
                            "凭据 #{} 不支持模型 {}，已记录运行时限制，当前无其他可切换凭据",
                            id,
                            model_label
                        );
                    } else {
                        tracing::warn!(
                            "凭据 #{} 不支持模型 {}，模型冷却已关闭，当前无其他可切换凭据",
                            id,
                            model_label
                        );
                    }
                }
            } else {
                if dispatch.model_cooldown_enabled {
                    tracing::warn!("凭据 #{} 不支持模型 {}，已记录运行时限制", id, model_label);
                } else {
                    tracing::warn!("凭据 #{} 不支持模型 {}，模型冷却已关闭", id, model_label);
                }
            }

            entries.iter().any(|e| {
                !e.disabled
                    && e.id != id
                    && Self::is_model_supported(&dispatch, &e.credentials, Some(model), requirement)
            })
        };
        self.save_stats_debounced();
        result
    }

    pub fn runtime_leader_refresh_required_for_model_candidates(
        &self,
        model: &str,
    ) -> anyhow::Result<Option<RuntimeRefreshLeaderRequiredError>> {
        if self.runtime_refresh_lease_enabled() {
            return Ok(None);
        }

        let Some(status) = self.runtime_refresh_coordination_status()? else {
            return Ok(None);
        };
        if status.is_leader {
            return Ok(None);
        }

        let dispatch = self.dispatch_config();
        let entries = self.entries.lock();
        if Self::all_supported_model_candidates_need_local_token_refresh(&dispatch, &entries, model)
        {
            return Ok(Some(RuntimeRefreshLeaderRequiredError {
                instance_id: status.instance_id,
                leader_id: status.leader_id,
            }));
        }

        Ok(None)
    }

    fn all_supported_model_candidates_need_local_token_refresh(
        dispatch: &DispatchConfig,
        entries: &[CredentialEntry],
        model: &str,
    ) -> bool {
        let requirement = Self::model_requirement(Some(model));
        let mut has_supported = false;

        for entry in entries {
            if entry.disabled
                || !Self::is_model_supported(dispatch, &entry.credentials, Some(model), requirement)
            {
                continue;
            }

            has_supported = true;

            if !is_token_expired(&entry.credentials) && !is_token_expiring_soon(&entry.credentials)
            {
                return false;
            }
        }

        has_supported
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
    /// 用于处理 402 Payment Required 且 reason 表示额度已用尽的场景：
    /// - 立即禁用该凭据（不等待连续失败阈值）
    /// - 切换到下一个可用凭据继续重试
    /// - 返回是否还有可用凭据
    pub fn report_quota_exhausted(&self, id: u64) -> bool {
        self.report_quota_exhausted_with_error(id, None)
    }

    pub fn report_quota_exhausted_with_error(&self, id: u64, error_summary: Option<&str>) -> bool {
        let persisted_disabled_at: String;
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

            let disabled_at = Utc::now().to_rfc3339();
            entry.disabled = true;
            entry.disabled_reason = Some(DisabledReason::QuotaExceeded);
            Self::apply_disabled_metadata(
                &mut entry.credentials,
                DisabledReason::QuotaExceeded,
                &disabled_at,
                Some(402),
                error_summary,
            );
            entry.last_used_at = Some(Utc::now().to_rfc3339());
            // 设为阈值，便于在管理面板中直观看到该凭据已不可用
            entry.failure_count = MAX_FAILURES_PER_CREDENTIAL;
            persisted_disabled_at = disabled_at;

            tracing::error!("凭据 #{} 额度已用尽，已被禁用", id);

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
        self.persist_disabled_metadata(
            id,
            DisabledReason::QuotaExceeded,
            &persisted_disabled_at,
            Some(402),
            error_summary,
        );
        self.save_stats_debounced();
        result
    }

    /// 报告 401/403 这类明确的认证或账号权限异常。
    ///
    /// 这类错误在 token 强制刷新仍不可恢复后继续调度只会消耗重试次数，
    /// 因此立即停调并把原因持久化到共享凭据状态。
    pub(crate) fn report_auth_or_permission_failure(
        &self,
        id: u64,
        reason: DisabledReason,
        status_code: u16,
        error_summary: &str,
    ) -> bool {
        let disabled_at = Utc::now().to_rfc3339();
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
            entry.disabled_reason = Some(reason);
            entry.last_used_at = Some(Utc::now().to_rfc3339());
            entry.failure_count = MAX_FAILURES_PER_CREDENTIAL;
            Self::apply_disabled_metadata(
                &mut entry.credentials,
                reason,
                &disabled_at,
                Some(status_code),
                Some(error_summary),
            );

            tracing::error!(
                "凭据 #{} 因上游 {} 异常已被停调: {}",
                id,
                status_code,
                error_summary
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

        self.persist_disabled_metadata(
            id,
            reason,
            &disabled_at,
            Some(status_code),
            Some(error_summary),
        );
        self.save_stats_debounced();
        result
    }

    /// 报告指定凭据刷新 Token 失败。
    ///
    /// 连续刷新失败达到阈值后禁用凭据并切换，阈值内保持当前凭据不切换。
    pub fn report_refresh_failure(&self, id: u64) -> bool {
        let persisted_disable: (DisabledReason, String);
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

            let disabled_at = Utc::now().to_rfc3339();
            entry.disabled = true;
            entry.disabled_reason = Some(DisabledReason::TooManyRefreshFailures);
            entry.failure_count = MAX_FAILURES_PER_CREDENTIAL;
            Self::apply_disabled_metadata(
                &mut entry.credentials,
                DisabledReason::TooManyRefreshFailures,
                &disabled_at,
                None,
                None,
            );
            persisted_disable = (DisabledReason::TooManyRefreshFailures, disabled_at);

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
        let (reason, disabled_at) = persisted_disable;
        self.persist_disabled_metadata(id, reason, &disabled_at, None, None);
        self.save_stats_debounced();
        result
    }

    /// 报告指定凭据的 refreshToken 永久失效（invalid_grant）。
    ///
    /// 立即禁用凭据，不累计、不重试。
    /// 返回是否还有可用凭据。
    pub fn report_refresh_token_invalid(&self, id: u64) -> bool {
        let persisted_disable: (DisabledReason, String);
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
            let disabled_at = Utc::now().to_rfc3339();
            entry.disabled = true;
            entry.disabled_reason = Some(DisabledReason::InvalidRefreshToken);
            entry.failure_count = MAX_FAILURES_PER_CREDENTIAL;
            entry.refresh_failure_count = MAX_FAILURES_PER_CREDENTIAL;
            Self::apply_disabled_metadata(
                &mut entry.credentials,
                DisabledReason::InvalidRefreshToken,
                &disabled_at,
                None,
                Some("refreshToken invalid_grant"),
            );
            persisted_disable = (DisabledReason::InvalidRefreshToken, disabled_at);

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
        let (reason, disabled_at) = persisted_disable;
        self.persist_disabled_metadata(
            id,
            reason,
            &disabled_at,
            None,
            Some("refreshToken invalid_grant"),
        );
        self.save_stats_debounced();
        result
    }

    /// 切换到优先级最高的可用凭据
    ///
    /// 返回是否成功切换
    pub fn switch_to_next(&self) -> bool {
        let dispatch = self.dispatch_config();
        let entries = self.entries.lock();
        let mut current_id = self.current_id.lock();
        let now_utc = Utc::now();

        // 选择当前最佳未禁用凭据（排除当前凭据）
        if let Some(next) = entries
            .iter()
            .filter(|e| {
                !e.disabled
                    && e.id != *current_id
                    && !Self::is_suspicious_activity_quarantined_at(&e.credentials, &now_utc)
            })
            .min_by_key(|e| {
                (
                    Self::suspicious_activity_preference_rank(&dispatch, &e.credentials),
                    e.credentials.priority,
                    e.id,
                )
            })
        {
            *current_id = next.id;
            tracing::info!(
                "已切换到凭据 #{}（优先级 {}，suspiciousActivityCount={}）",
                next.id,
                next.credentials.priority,
                next.credentials.suspicious_activity_count
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
        self.maybe_clear_stale_suspicious_activity_markers(&dispatch);
        let now = Instant::now();
        let now_utc = Utc::now();
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
                if self.proxy_dispatch_availability(&dispatch, &e.credentials, now)
                    != ProxyDispatchAvailability::Available
                {
                    return false;
                }

                if let Some(shared_snapshots) = shared_snapshots.as_ref() {
                    let runtime =
                        Self::shared_dispatch_snapshot_for_entry(e, &dispatch, shared_snapshots);
                    !Self::is_suspicious_activity_quarantined_at(&e.credentials, &now_utc)
                        && runtime
                            .cooldown_until_epoch_ms
                            .map_or(true, |until| until <= now_epoch_ms)
                        && Self::shared_bucket_is_ready(&runtime)
                        && Self::has_capacity(&dispatch, &e.credentials, runtime.active_requests)
                } else {
                    !Self::is_suspicious_activity_quarantined_at(&e.credentials, &now_utc)
                        && !Self::is_rate_limited(e, now)
                        && Self::bucket_is_ready(e)
                        && Self::has_capacity(&dispatch, &e.credentials, e.active_requests)
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
                    let suspicious_activity_quarantine_remaining_ms =
                        Self::suspicious_activity_quarantine_remaining_ms_at(
                            &e.credentials,
                            &now_utc,
                        );
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
                        })
                        .into_iter()
                        .chain(suspicious_activity_quarantine_remaining_ms)
                        .max();
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
                        })
                        .into_iter()
                        .chain(suspicious_activity_quarantine_remaining_ms)
                        .max();

                    let effective_profile_arn = e
                        .credentials
                        .effective_profile_arn_for_kiro_requests()
                        .map(str::to_string);

                    CredentialEntrySnapshot {
                        id: e.id,
                        priority: e.credentials.priority,
                        disabled: e.disabled,
                        failure_count: e.failure_count,
                        auth_method: Some(e.credentials.effective_auth_method().to_string()),
                        provider: e.credentials.provider.clone(),
                        has_profile_arn: effective_profile_arn.is_some(),
                        profile_arn: effective_profile_arn,
                        expires_at: e.credentials.expires_at.clone(),
                        refresh_token_hash: e.credentials.refresh_token.as_deref().map(sha256_hex),
                        email: e.credentials.email.clone(),
                        user_id: e.credentials.user_id.clone(),
                        subscription_title: e.credentials.subscription_title.clone(),
                        subscription_type: e.credentials.subscription_type.clone(),
                        auth_account_type: e.credentials.detected_auth_account_type(),
                        account_type: e.credentials.account_type.clone(),
                        source_supplier_id: e.credentials.source_supplier_id.clone(),
                        source_supplier_name: e.credentials.source_supplier_name.clone(),
                        source_batch: e.credentials.source_batch.clone(),
                        credential_groups: e.credentials.credential_groups.clone(),
                        resolved_account_type: e.credentials.resolved_account_type(),
                        account_type_source: e
                            .credentials
                            .resolved_account_type_source()
                            .map(|source| source.as_str().to_string()),
                        allowed_models: e.credentials.allowed_models.clone(),
                        blocked_models: e.credentials.blocked_models.clone(),
                        runtime_model_restrictions: if dispatch.model_cooldown_enabled {
                            e.credentials.active_runtime_model_restrictions()
                        } else {
                            Vec::new()
                        },
                        available_model_ids: e.credentials.available_model_ids.clone(),
                        available_models_cached_at: e
                            .credentials
                            .available_models_cached_at
                            .clone(),
                        imported_at: e.credentials.imported_at.clone(),
                        success_count: e.success_count,
                        token_usage_count: e.token_usage_count,
                        input_tokens: e.input_tokens,
                        output_tokens: e.output_tokens,
                        total_tokens: e.input_tokens.saturating_add(e.output_tokens),
                        last_used_at: e.last_used_at.clone(),
                        active_requests,
                        max_concurrency: e
                            .credentials
                            .effective_max_concurrency_with_policy(
                                dispatch.default_max_concurrency,
                                dispatch.account_type_dispatch_policy_for(&e.credentials),
                            )
                            .and_then(|limit| u32::try_from(limit).ok()),
                        max_concurrency_override: e.credentials.max_concurrency_override(),
                        max_concurrency_source: dispatch
                            .effective_max_concurrency_source_for(&e.credentials),
                        has_proxy: e.credentials.proxy_url.is_some()
                            || e.credentials.proxy_id.is_some(),
                        proxy_url: e.credentials.proxy_url.clone(),
                        proxy_id: e.credentials.proxy_id.clone(),
                        refresh_failure_count: e.refresh_failure_count,
                        disabled_reason: e.disabled_reason.map(|r| r.as_str().to_string()),
                        disabled_at: e.credentials.disabled_at.clone(),
                        last_error_status: e.credentials.last_error_status,
                        last_error_summary: e.credentials.last_error_summary.clone(),
                        suspicious_activity_count: e.credentials.suspicious_activity_count,
                        suspicious_activity_first_seen_at: e
                            .credentials
                            .suspicious_activity_first_seen_at
                            .clone(),
                        suspicious_activity_last_seen_at: e
                            .credentials
                            .suspicious_activity_last_seen_at
                            .clone(),
                        suspicious_activity_quarantine_until: e
                            .credentials
                            .suspicious_activity_quarantine_until
                            .clone(),
                        suspicious_activity_recovery_success_count: e
                            .credentials
                            .suspicious_activity_recovery_success_count,
                        suspicious_activity_quarantine_remaining_ms,
                        rate_limit_cooldown_enabled: dispatch
                            .rate_limit_cooldown_enabled_for(&e.credentials),
                        rate_limit_cooldown_enabled_override: e
                            .credentials
                            .rate_limit_cooldown_enabled_override(),
                        rate_limit_cooldown_enabled_source: dispatch
                            .rate_limit_cooldown_enabled_source_for(&e.credentials),
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
                        rate_limit_bucket_capacity_source: dispatch
                            .rate_limit_bucket_capacity_source_for(&e.credentials),
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
                        rate_limit_refill_per_second_source: dispatch
                            .rate_limit_refill_per_second_source_for(&e.credentials),
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
        let persisted_credential = Self::persisted_credential_mut(&mut persisted, id)?;
        if disabled {
            Self::apply_disabled_metadata(
                persisted_credential,
                DisabledReason::Manual,
                &Utc::now().to_rfc3339(),
                None,
                None,
            );
        } else {
            Self::clear_disabled_metadata(persisted_credential);
        }
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
                Self::clear_disabled_metadata(&mut entry.credentials);
                Self::reset_rate_limit_runtime(entry, &dispatch, now);
            } else {
                entry.disabled_reason = Some(DisabledReason::Manual);
                Self::apply_disabled_metadata(
                    &mut entry.credentials,
                    DisabledReason::Manual,
                    &Utc::now().to_rfc3339(),
                    None,
                    None,
                );
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
        rate_limit_cooldown_enabled: Option<Option<bool>>,
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
        if let Some(rate_limit_cooldown_enabled) = rate_limit_cooldown_enabled {
            credential.rate_limit_cooldown_enabled = rate_limit_cooldown_enabled;
        }
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
            if let Some(rate_limit_cooldown_enabled) = rate_limit_cooldown_enabled {
                entry.credentials.rate_limit_cooldown_enabled = rate_limit_cooldown_enabled;
            }
            if let Some(rate_limit_bucket_capacity) = rate_limit_bucket_capacity {
                entry.credentials.rate_limit_bucket_capacity = rate_limit_bucket_capacity;
            }
            if let Some(rate_limit_refill_per_second) = rate_limit_refill_per_second {
                entry.credentials.rate_limit_refill_per_second = rate_limit_refill_per_second;
            }
            Self::sync_rate_limit_bucket_runtime(entry, &dispatch, now);
        }
        if rate_limit_cooldown_enabled.is_some() {
            let should_clear = {
                let entries = self.entries.lock();
                entries
                    .iter()
                    .find(|entry| entry.id == id)
                    .map(|entry| !dispatch.rate_limit_cooldown_enabled_for(&entry.credentials))
                    .unwrap_or(false)
            };
            if should_clear {
                self.clear_rate_limit_penalty_for_credential(id, &dispatch);
            }
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

    /// 设置凭据来源标记（Admin API）
    pub fn set_credential_source(
        &self,
        id: u64,
        source_supplier_id: Option<Option<String>>,
        source_supplier_name: Option<Option<String>>,
        source_batch: Option<Option<String>>,
    ) -> anyhow::Result<()> {
        let _state_write_guard = self.state_write_lock.lock();
        let mut persisted = self.persisted_credentials_snapshot();
        let credential = Self::persisted_credential_mut(&mut persisted, id)?;

        if let Some(value) = source_supplier_id {
            credential.source_supplier_id = value;
        }
        if let Some(value) = source_supplier_name {
            credential.source_supplier_name = value;
        }
        if let Some(value) = source_batch {
            credential.source_batch = value;
        }
        credential.normalize_source_metadata();
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

    /// 设置凭据分组标记（Admin API）
    pub fn set_credential_groups(
        &self,
        id: u64,
        credential_groups: Vec<String>,
    ) -> anyhow::Result<()> {
        let normalized_groups =
            crate::common::auth::normalize_credential_groups(&credential_groups);
        let _state_write_guard = self.state_write_lock.lock();
        let mut persisted = self.persisted_credentials_snapshot();
        let credential = Self::persisted_credential_mut(&mut persisted, id)?;
        credential.credential_groups = normalized_groups.clone();
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

    /// 设置凭据当前使用的 Profile ARN（Admin API）
    pub fn set_profile_arn(&self, id: u64, profile_arn: String) -> anyhow::Result<()> {
        let profile_arn = profile_arn.trim();
        if profile_arn.is_empty() {
            anyhow::bail!("profileArn 不能为空");
        }

        let _state_write_guard = self.state_write_lock.lock();
        let mut persisted = self.persisted_credentials_snapshot();
        let credential = Self::persisted_credential_mut(&mut persisted, id)?;
        let previous_profile_arn = credential.profile_arn.as_deref().map(str::trim);

        if previous_profile_arn == Some(profile_arn) {
            return Ok(());
        }

        credential.profile_arn = Some(profile_arn.to_string());
        credential.available_model_ids.clear();
        credential.available_models_cached_at = None;
        credential.clear_runtime_model_restrictions();
        credential.normalize_model_capabilities();
        let updated_credential = credential.clone();
        self.persist_credentials_snapshot(&persisted)?;

        {
            let mut entries = self.entries.lock();
            let entry = entries
                .iter_mut()
                .find(|entry| entry.id == id)
                .ok_or_else(|| anyhow::anyhow!("凭据不存在: {}", id))?;
            entry.credentials = updated_credential;
        }

        self.availability_notify.notify_waiters();
        Ok(())
    }

    /// 清除单个凭据的运行时模型限制（Admin API）
    pub fn clear_runtime_model_restrictions_for_credential(&self, id: u64) -> anyhow::Result<bool> {
        let _state_write_guard = self.state_write_lock.lock();
        let mut persisted = self.persisted_credentials_snapshot();
        let credential = Self::persisted_credential_mut(&mut persisted, id)?;
        let changed = credential.clear_runtime_model_restrictions();
        let updated_credential = credential.clone();

        if changed {
            self.persist_credentials_snapshot(&persisted)?;
        }

        {
            let mut entries = self.entries.lock();
            let entry = entries
                .iter_mut()
                .find(|entry| entry.id == id)
                .ok_or_else(|| anyhow::anyhow!("凭据不存在: {}", id))?;
            entry.credentials = updated_credential;
        }

        if changed {
            self.availability_notify.notify_waiters();
        }
        Ok(changed)
    }

    /// 清除单个凭据的 suspicious activity 标记与隔离（Admin API）
    pub fn clear_suspicious_activity_for_credential(&self, id: u64) -> anyhow::Result<bool> {
        let dispatch = self.dispatch_config();
        let now = Instant::now();
        let _state_write_guard = self.state_write_lock.lock();
        let mut persisted = self.persisted_credentials_snapshot();
        let credential = Self::persisted_credential_mut(&mut persisted, id)?;
        let changed = Self::clear_suspicious_activity_fields(credential);
        let updated_credential = credential.clone();

        if changed {
            self.persist_credentials_snapshot(&persisted)?;
        }

        let mut shared_bucket_policy = None;
        {
            let mut entries = self.entries.lock();
            let entry = entries
                .iter_mut()
                .find(|entry| entry.id == id)
                .ok_or_else(|| anyhow::anyhow!("凭据不存在: {}", id))?;
            if changed {
                entry.credentials.suspicious_activity_count =
                    updated_credential.suspicious_activity_count;
                entry.credentials.suspicious_activity_first_seen_at =
                    updated_credential.suspicious_activity_first_seen_at.clone();
                entry.credentials.suspicious_activity_last_seen_at =
                    updated_credential.suspicious_activity_last_seen_at.clone();
                entry.credentials.suspicious_activity_quarantine_until = updated_credential
                    .suspicious_activity_quarantine_until
                    .clone();
                entry.credentials.suspicious_activity_recovery_success_count =
                    updated_credential.suspicious_activity_recovery_success_count;
                Self::clear_rate_limit_penalty_runtime(entry, &dispatch, now);
                shared_bucket_policy = dispatch.shared_bucket_policy_for(&entry.credentials);
            }
        }

        if changed {
            if self.shared_dispatch_runtime_enabled() {
                if let Err(err) = self.state_store.reset_dispatch_runtime(
                    id,
                    shared_bucket_policy,
                    current_epoch_ms(),
                ) {
                    tracing::warn!(
                        "清除 suspicious activity 后重置共享调度运行态失败（credentialId={}）: {}",
                        id,
                        err
                    );
                }
            }
            self.availability_notify.notify_waiters();
        }
        Ok(changed)
    }

    /// 重置凭据失败计数并重新启用（Admin API）
    pub fn reset_and_enable(&self, id: u64) -> anyhow::Result<()> {
        let dispatch = self.dispatch_config();
        let now = Instant::now();
        let _state_write_guard = self.state_write_lock.lock();
        let mut persisted = self.persisted_credentials_snapshot();
        let persisted_credential = Self::persisted_credential_mut(&mut persisted, id)?;
        Self::clear_disabled_metadata(persisted_credential);
        Self::clear_suspicious_activity_fields(persisted_credential);
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
            Self::clear_disabled_metadata(&mut entry.credentials);
            Self::clear_suspicious_activity_fields(&mut entry.credentials);
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

    fn is_enterprise_profile_unauthorized_message(message: &str) -> bool {
        let lower = message.to_ascii_lowercase();
        lower.contains("user is not authorized to make this call")
            || lower.contains("not authorized to make this call")
    }

    fn should_retry_management_call_after_profile_rediscovery(
        credentials: &KiroCredentials,
        err: &anyhow::Error,
    ) -> bool {
        if credentials.detected_auth_account_type().as_deref() != Some("enterprise") {
            return false;
        }

        if let Some(api_err) = err.downcast_ref::<KiroManagementApiError>() {
            return api_err.status_code == 403
                && Self::is_enterprise_profile_unauthorized_message(&api_err.message);
        }

        Self::is_enterprise_profile_unauthorized_message(&err.to_string())
    }

    pub(crate) async fn rediscover_enterprise_profile_for(
        &self,
        id: u64,
        credentials: &KiroCredentials,
        token: &str,
    ) -> anyhow::Result<Option<KiroCredentials>> {
        if credentials.detected_auth_account_type().as_deref() != Some("enterprise") {
            return Ok(None);
        }

        let previous_profile_arn = credentials
            .effective_profile_arn_for_kiro_requests()
            .map(str::to_string);
        let config = self.config.clone();
        let token = token.to_string();
        let (discovered_profile_arn, _active_credentials) = self
            .proxy_pool_retry_management_call(
                id,
                credentials,
                "ListAvailableProfiles",
                move |credentials, proxy| {
                    let config = config.clone();
                    let token = token.clone();
                    async move {
                        discover_available_profile_arn(
                            &credentials,
                            &config,
                            &token,
                            proxy.as_ref(),
                        )
                        .await
                    }
                },
            )
            .await?;
        let Some(discovered_profile_arn) = discovered_profile_arn else {
            return Ok(None);
        };

        let discovered_profile_arn = discovered_profile_arn.trim();
        if discovered_profile_arn.is_empty() {
            return Ok(None);
        }

        if previous_profile_arn.as_deref() == Some(discovered_profile_arn) {
            return Ok(None);
        }

        if self.state_store.is_external() {
            if let Err(err) = self.sync_from_state() {
                tracing::warn!(
                    "凭据 #{} 重新发现 profileArn 前同步共享状态失败，将继续使用本地状态: {}",
                    id,
                    err
                );
            }
        }

        let current = self.current_credentials(id)?;
        if current.detected_auth_account_type().as_deref() != Some("enterprise") {
            return Ok(None);
        }

        if current.effective_profile_arn_for_kiro_requests() == Some(discovered_profile_arn) {
            return Ok(Some(current));
        }

        let mut updated = current.clone();
        updated.profile_arn = Some(discovered_profile_arn.to_string());
        let committed =
            self.commit_refreshed_credential(id, current.refresh_token.as_deref(), updated)?;

        if committed
            .effective_profile_arn_for_kiro_requests()
            .is_some_and(|profile_arn| profile_arn != previous_profile_arn.as_deref().unwrap_or(""))
        {
            tracing::info!("Enterprise 凭据 #{} 已重新发现并更新可用 profileArn", id);
            self.availability_notify.notify_waiters();
            Ok(Some(committed))
        } else {
            Ok(None)
        }
    }

    async fn get_usage_limits_with_enterprise_profile_retry(
        &self,
        id: u64,
        credentials: &KiroCredentials,
        token: &str,
    ) -> anyhow::Result<(UsageLimitsResponse, KiroCredentials)> {
        let config = self.config.clone();
        let token_owned = token.to_string();
        match self
            .proxy_pool_retry_management_call(
                id,
                credentials,
                "getUsageLimits",
                move |credentials, proxy| {
                    let config = config.clone();
                    let token = token_owned.clone();
                    async move { get_usage_limits(&credentials, &config, &token, proxy.as_ref()).await }
                },
            )
            .await
        {
            Ok((usage_limits, active_credentials)) => Ok((usage_limits, active_credentials)),
            Err(err)
                if Self::should_retry_management_call_after_profile_rediscovery(
                    credentials,
                    &err,
                ) =>
            {
                tracing::info!(
                    "凭据 #{} 使用额度查询遇到 Enterprise profileArn 授权失败，尝试重新发现 profileArn",
                    id
                );
                match self
                    .rediscover_enterprise_profile_for(id, credentials, token)
                    .await
                {
                    Ok(Some(updated_credentials)) => {
                        let retry_token =
                            updated_credentials.access_token.as_deref().unwrap_or(token);
                        let config = self.config.clone();
                        let retry_token = retry_token.to_string();
                        let (usage_limits, updated_credentials) = self
                            .proxy_pool_retry_management_call(
                                id,
                                &updated_credentials,
                                "getUsageLimits",
                                move |credentials, proxy| {
                                    let config = config.clone();
                                    let retry_token = retry_token.clone();
                                    async move {
                                        get_usage_limits(
                                            &credentials,
                                            &config,
                                            &retry_token,
                                            proxy.as_ref(),
                                        )
                                        .await
                                    }
                                },
                            )
                            .await?;
                        Ok((usage_limits, updated_credentials))
                    }
                    Ok(None) => Err(err),
                    Err(rediscover_err) => {
                        tracing::warn!(
                            "凭据 #{} 重新发现 Enterprise profileArn 失败，保留原使用额度错误: {}",
                            id,
                            rediscover_err
                        );
                        Err(err)
                    }
                }
            }
            Err(err) => Err(err),
        }
    }

    /// 获取指定凭据当前生效的 Profile ARN（用于额度缓存隔离）
    pub fn effective_profile_arn_for(&self, id: u64) -> anyhow::Result<Option<String>> {
        Ok(self
            .current_credentials(id)?
            .effective_profile_arn_for_kiro_requests()
            .map(str::to_string))
    }

    /// 列出指定凭据可用的 Profile（Admin API）
    pub async fn list_available_profiles_for(
        &self,
        id: u64,
    ) -> anyhow::Result<(Vec<AvailableProfile>, Option<String>)> {
        let credentials = self.current_credentials(id)?;
        let (credentials, token) = self.try_ensure_token(id, &credentials).await?;
        let config = self.config.clone();
        let (response, credentials) = self
            .proxy_pool_retry_management_call(
                id,
                &credentials,
                "ListAvailableProfiles",
                move |credentials, proxy| {
                    let config = config.clone();
                    let token = token.clone();
                    async move {
                        list_available_profiles(&credentials, &config, &token, proxy.as_ref()).await
                    }
                },
            )
            .await?;
        let selected_profile_arn = credentials
            .profile_arn
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);

        Ok((response.profiles, selected_profile_arn))
    }

    /// 获取指定凭据的使用额度（Admin API）
    pub async fn get_usage_limits_for(&self, id: u64) -> anyhow::Result<UsageLimitsResponse> {
        let credentials = self.current_credentials(id)?;
        let (credentials, token) = self.try_ensure_token(id, &credentials).await?;

        let (usage_limits, _credentials) = self
            .get_usage_limits_with_enterprise_profile_retry(id, &credentials, &token)
            .await?;

        // 更新订阅和用户信息到凭据（仅在发生变化时持久化）
        self.apply_usage_limits_metadata_update(id, &usage_limits);
        Ok(usage_limits)
    }

    /// 尝试登记一次后台用量刷新。
    ///
    /// 该方法只做本实例内的短窗口去重；实际刷新失败也会保留冷却，避免上游错误风暴时重复打管理接口。
    pub fn try_schedule_usage_balance_refresh(&self, id: u64) -> bool {
        let now = Instant::now();
        let mut cooldowns = self.usage_balance_refresh_cooldowns.lock();
        cooldowns.retain(|_, cooldown_until| *cooldown_until > now);

        if cooldowns
            .get(&id)
            .is_some_and(|cooldown_until| *cooldown_until > now)
        {
            return false;
        }

        cooldowns.insert(id, now + USAGE_BALANCE_REFRESH_COOLDOWN);
        true
    }

    /// 刷新指定凭据的用量并写入共享余额缓存。
    ///
    /// 用于 402/超额上限错误后的后台观测刷新；不会改变凭据禁用状态。
    pub async fn refresh_usage_balance_cache_for(
        &self,
        id: u64,
        reason: &str,
    ) -> anyhow::Result<BalanceResponse> {
        tracing::info!(
            credential_id = id,
            refresh_reason = reason,
            "开始后台刷新凭据用量缓存"
        );

        let usage_limits = self.get_usage_limits_for(id).await?;
        let profile_arn = self.effective_profile_arn_for(id)?;
        let balance = BalanceResponse::from_usage(id, profile_arn, &usage_limits);

        self.save_usage_balance_cache(id, balance.clone())?;

        tracing::info!(
            credential_id = id,
            refresh_reason = reason,
            current_usage = balance.current_usage,
            effective_usage_limit = balance.effective_usage_limit,
            overage_enabled = ?balance.overage_enabled,
            "后台刷新凭据用量缓存完成"
        );

        Ok(balance)
    }

    fn save_usage_balance_cache(&self, id: u64, balance: BalanceResponse) -> anyhow::Result<()> {
        let mut cache = self.state_store.load_balance_cache()?;
        cache.insert(
            id,
            CachedBalanceRecord {
                profile_arn: balance.profile_arn.clone(),
                cached_at: Utc::now().timestamp() as f64,
                data: balance,
            },
        );
        self.state_store.save_balance_cache(&cache)?;
        self.try_bump_state_change_revision(StateChangeKind::BalanceCache);
        Ok(())
    }

    /// 设置指定凭据的超额使用开关（Admin API）
    pub async fn set_overage_status_for(
        &self,
        id: u64,
        enabled: bool,
    ) -> anyhow::Result<UsageLimitsResponse> {
        let credentials = self.current_credentials(id)?;
        let (mut credentials, mut token) = self.try_ensure_token(id, &credentials).await?;
        let (mut usage_limits, updated_credentials) = self
            .get_usage_limits_with_enterprise_profile_retry(id, &credentials, &token)
            .await?;
        credentials = updated_credentials;
        if let Some(updated_token) = credentials.access_token.clone() {
            token = updated_token;
        }

        self.apply_usage_limits_metadata_update(id, &usage_limits);

        if !usage_limits.is_overage_capable() {
            anyhow::bail!("此账号订阅级别不支持超额使用");
        }

        if usage_limits.overage_enabled() == Some(enabled) {
            return Ok(usage_limits);
        }

        let config = self.config.clone();
        let token_for_set = token.clone();
        let mut result = self
            .proxy_pool_retry_management_call(
                id,
                &credentials,
                "setUserPreference",
                move |credentials, proxy| {
                    let config = config.clone();
                    let token = token_for_set.clone();
                    async move {
                        set_user_preference_overage_status(
                            &credentials,
                            &config,
                            &token,
                            enabled,
                            proxy.as_ref(),
                        )
                        .await
                    }
                },
            )
            .await
            .map(|(_value, active_credentials)| active_credentials);

        if result.as_ref().err().is_some_and(|err| {
            Self::should_retry_management_call_after_profile_rediscovery(&credentials, err)
        }) {
            tracing::info!(
                "设置凭据 #{} 超额使用开关遇到 Enterprise profileArn 授权失败，尝试重新发现 profileArn",
                id
            );
            match self
                .rediscover_enterprise_profile_for(id, &credentials, &token)
                .await
            {
                Ok(Some(updated_credentials)) => {
                    credentials = updated_credentials;
                    if let Some(updated_token) = credentials.access_token.clone() {
                        token = updated_token;
                    }
                    let config = self.config.clone();
                    let token_for_set = token.clone();
                    result = self
                        .proxy_pool_retry_management_call(
                            id,
                            &credentials,
                            "setUserPreference",
                            move |credentials, proxy| {
                                let config = config.clone();
                                let token = token_for_set.clone();
                                async move {
                                    set_user_preference_overage_status(
                                        &credentials,
                                        &config,
                                        &token,
                                        enabled,
                                        proxy.as_ref(),
                                    )
                                    .await
                                }
                            },
                        )
                        .await
                        .map(|(_value, active_credentials)| active_credentials);
                }
                Ok(None) => {}
                Err(err) => {
                    tracing::warn!(
                        "凭据 #{} 设置超额使用开关前重新发现 Enterprise profileArn 失败: {}",
                        id,
                        err
                    );
                }
            }
        }

        let should_refresh_and_retry = result
            .as_ref()
            .err()
            .and_then(|err| err.downcast_ref::<KiroManagementApiError>())
            .is_some_and(KiroManagementApiError::is_auth_error);

        if should_refresh_and_retry {
            tracing::info!(
                "设置凭据 #{} 超额使用开关时 accessToken 失效，刷新后重试",
                id
            );
            let refresh_lock = self.refresh_lock_for(id)?;
            let _guard = refresh_lock.lock().await;
            let current_credentials = self.current_credentials(id)?;
            credentials = self
                .refresh_credentials_with_runtime_coordination(id, &current_credentials)
                .await?;
            token = credentials
                .access_token
                .clone()
                .ok_or_else(|| anyhow::anyhow!("没有可用的 accessToken"))?;
            let config = self.config.clone();
            let token_for_set = token.clone();

            result = self
                .proxy_pool_retry_management_call(
                    id,
                    &credentials,
                    "setUserPreference",
                    move |credentials, proxy| {
                        let config = config.clone();
                        let token = token_for_set.clone();
                        async move {
                            set_user_preference_overage_status(
                                &credentials,
                                &config,
                                &token,
                                enabled,
                                proxy.as_ref(),
                            )
                            .await
                        }
                    },
                )
                .await
                .map(|(_value, active_credentials)| active_credentials);
        }

        result?;
        usage_limits.set_overage_enabled_local(enabled);

        tracing::info!(
            "凭据 #{} 超额使用开关已设置为 {}",
            id,
            if enabled { "ENABLED" } else { "DISABLED" }
        );

        Ok(usage_limits)
    }

    /// 添加新凭据（Admin API）
    ///
    /// # 流程
    /// 1. 验证凭据基本字段（refresh_token 不为空）
    /// 2. 基于 refreshToken 哈希和稳定账号身份检测重复
    /// 3. 尝试刷新 Token 验证凭据有效性
    /// 4. 尝试读取 usage 元数据，补齐 email/userId/subscription
    /// 5. 分配新 ID（当前最大 ID + 1）
    /// 6. 添加到 entries 列表
    /// 7. 持久化到配置文件
    ///
    /// # 返回
    /// - `Ok(u64)` - 新凭据 ID
    /// - `Err(_)` - 验证失败或添加失败
    pub async fn add_credential(&self, mut new_cred: KiroCredentials) -> anyhow::Result<u64> {
        // 1. 基本验证
        validate_refresh_token(&new_cred)?;

        // 2. 基于 refreshToken 的 SHA-256 哈希检测重复
        let new_refresh_token = new_cred
            .refresh_token
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("缺少 refreshToken"))?;
        let new_refresh_token_hash = sha256_hex(new_refresh_token);

        // 3. 先做一次本地去重，避免重复凭据还去触发上游刷新校验
        let persisted_snapshot = self.persisted_credentials_snapshot();
        let duplicate_exists = persisted_snapshot.iter().any(|credential| {
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
        if let Some((existing_id, reason)) = persisted_snapshot.iter().find_map(|credential| {
            credential_duplicate_reason(credential, &new_cred).map(|reason| (credential.id, reason))
        }) {
            match existing_id {
                Some(existing_id) => {
                    anyhow::bail!(
                        "凭据已存在（账号重复：{}，已有凭据 #{}）",
                        reason,
                        existing_id
                    );
                }
                None => {
                    anyhow::bail!("凭据已存在（账号重复：{}）", reason);
                }
            }
        }
        self.assign_proxy_for_new_credential(&mut new_cred, &persisted_snapshot)?;

        let import_is_enterprise =
            new_cred.detected_auth_account_type().as_deref() == Some("enterprise");

        // 4. 尝试刷新 Token 验证凭据有效性
        let effective_proxy = self.effective_proxy_for_credentials(&new_cred)?;
        let mut validated_cred =
            refresh_token(&new_cred, &self.config, effective_proxy.as_ref()).await?;

        validated_cred.priority = new_cred.priority;
        if let Some(provider) = new_cred.provider {
            validated_cred.provider = Some(provider);
        }
        if import_is_enterprise {
            if validated_cred.profile_arn.is_none() {
                validated_cred.profile_arn = new_cred.profile_arn;
            }
        } else if let Some(profile_arn) = new_cred.profile_arn {
            validated_cred.profile_arn = Some(profile_arn);
        }
        if let Some(auth_method) = new_cred.auth_method {
            validated_cred.auth_method = Some(
                if auth_method.eq_ignore_ascii_case("builder-id")
                    || auth_method.eq_ignore_ascii_case("iam")
                {
                    "idc".to_string()
                } else if auth_method.eq_ignore_ascii_case("external-idp")
                    || auth_method.eq_ignore_ascii_case("externalidp")
                {
                    "external_idp".to_string()
                } else {
                    auth_method
                },
            );
        }
        validated_cred.client_id = new_cred.client_id;
        validated_cred.client_secret = new_cred.client_secret;
        if new_cred.token_endpoint.is_some() {
            validated_cred.token_endpoint = new_cred.token_endpoint;
        }
        if new_cred.issuer_url.is_some() {
            validated_cred.issuer_url = new_cred.issuer_url;
        }
        if new_cred.scopes.is_some() {
            validated_cred.scopes = new_cred.scopes;
        }
        if new_cred.audience.is_some() {
            validated_cred.audience = new_cred.audience;
        }
        validated_cred.start_url = new_cred.start_url;
        validated_cred.region = new_cred.region;
        validated_cred.auth_region = new_cred.auth_region;
        validated_cred.api_region = new_cred.api_region;
        validated_cred.machine_id = new_cred.machine_id;
        if new_cred.email.is_some() {
            validated_cred.email = new_cred.email;
        }
        if new_cred.user_id.is_some() {
            validated_cred.user_id = new_cred.user_id;
        }
        if new_cred.subscription_title.is_some() {
            validated_cred.subscription_title = new_cred.subscription_title;
        }
        if new_cred.subscription_type.is_some() {
            validated_cred.subscription_type = new_cred.subscription_type;
        }
        validated_cred.account_type = new_cred.account_type;
        validated_cred.source_supplier_id = new_cred.source_supplier_id;
        validated_cred.source_supplier_name = new_cred.source_supplier_name;
        validated_cred.source_batch = new_cred.source_batch;
        validated_cred.allowed_models = new_cred.allowed_models;
        validated_cred.blocked_models = new_cred.blocked_models;
        validated_cred.runtime_model_restrictions = new_cred.runtime_model_restrictions;
        validated_cred.available_model_ids = new_cred.available_model_ids;
        validated_cred.available_models_cached_at = new_cred.available_models_cached_at;
        validated_cred.imported_at = new_cred
            .imported_at
            .or_else(|| Some(Utc::now().to_rfc3339()));
        validated_cred.max_concurrency = new_cred.max_concurrency;
        validated_cred.rate_limit_cooldown_enabled = new_cred.rate_limit_cooldown_enabled;
        validated_cred.rate_limit_bucket_capacity = new_cred.rate_limit_bucket_capacity;
        validated_cred.rate_limit_refill_per_second = new_cred.rate_limit_refill_per_second;
        validated_cred.proxy_url = new_cred.proxy_url;
        validated_cred.proxy_username = new_cred.proxy_username;
        validated_cred.proxy_password = new_cred.proxy_password;
        validated_cred.proxy_id = new_cred.proxy_id;
        Self::clear_disabled_metadata(&mut validated_cred);
        validated_cred.normalize_model_capabilities();

        if let Some(access_token) = validated_cred.access_token.clone() {
            match get_usage_limits(
                &validated_cred,
                &self.config,
                &access_token,
                effective_proxy.as_ref(),
            )
            .await
            {
                Ok(usage_limits) => {
                    apply_usage_limits_metadata_to_credentials(&mut validated_cred, &usage_limits);
                    validated_cred.normalize_model_capabilities();
                }
                Err(err) => {
                    tracing::warn!(
                        "添加凭据前获取订阅和身份信息失败（将继续添加并仅按已有元数据去重）: {}",
                        err
                    );
                }
            }
        }

        if import_is_enterprise {
            let token = validated_cred
                .access_token
                .clone()
                .ok_or_else(|| anyhow::anyhow!("没有可用的 accessToken"))?;
            validate_enterprise_model_access_on_import(
                &mut validated_cred,
                &self.config,
                &token,
                effective_proxy.as_ref(),
            )
            .await?;
        }

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
        if let Some((existing_id, reason)) = persisted.iter().find_map(|credential| {
            credential_duplicate_reason(credential, &validated_cred)
                .map(|reason| (credential.id, reason))
        }) {
            match existing_id {
                Some(existing_id) => {
                    anyhow::bail!(
                        "凭据已存在（账号重复：{}，已有凭据 #{}）",
                        reason,
                        existing_id
                    );
                }
                None => {
                    anyhow::bail!("凭据已存在（账号重复：{}）", reason);
                }
            }
        }

        let new_id = persisted
            .iter()
            .filter_map(|credential| credential.id)
            .max()
            .unwrap_or(0)
            + 1;

        validated_cred.id = Some(new_id);
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
                token_usage_count: 0,
                pending_token_usage_count_delta: 0,
                input_tokens: 0,
                pending_input_tokens_delta: 0,
                output_tokens: 0,
                pending_output_tokens_delta: 0,
                last_used_at: None,
                active_requests: 0,
                rate_limit_cooldown_until: None,
                background_refresh_cooldown_until: None,
                rate_limit_bucket,
                rate_limit_hit_streak: 0,
                background_refresh_in_progress: false,
                available_models_refresh_in_progress: false,
                available_models_refresh_cooldown_until: None,
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

        if !self.runtime_refresh_lease_enabled() {
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
        }

        self.refresh_credentials_with_runtime_coordination(id, &credentials)
            .await?;

        {
            let mut entries = self.entries.lock();
            if let Some(entry) = entries.iter_mut().find(|e| e.id == id) {
                entry.refresh_failure_count = 0;
            }
        }

        tracing::info!("凭据 #{} Token 已强制刷新", id);
        Ok(())
    }

    /// 获取调度配置快照（Admin API）
    pub fn load_balancing_config_snapshot(&self) -> LoadBalancingConfigSnapshot {
        let dispatch = self.dispatch_config();
        LoadBalancingConfigSnapshot {
            mode: dispatch.mode,
            session_affinity_enabled: dispatch.session_affinity_enabled,
            queue_max_size: dispatch.queue_max_size,
            queue_max_wait_ms: dispatch.queue_max_wait_ms,
            rate_limit_cooldown_ms: dispatch.rate_limit_cooldown_ms,
            rate_limit_cooldown_enabled: dispatch.rate_limit_cooldown_enabled,
            suspicious_activity_cooldown_ms: dispatch.suspicious_activity_cooldown_ms,
            suspicious_activity_cooldown_enabled: dispatch.suspicious_activity_cooldown_enabled,
            suspicious_activity_prefer_clean_credentials: dispatch
                .suspicious_activity_prefer_clean_credentials,
            suspicious_activity_auto_disable_enabled: dispatch
                .suspicious_activity_auto_disable_enabled,
            suspicious_activity_auto_disable_threshold: dispatch
                .suspicious_activity_auto_disable_threshold,
            suspicious_activity_auto_disable_window_ms: dispatch
                .suspicious_activity_auto_disable_window_ms,
            suspicious_activity_auto_clear_enabled: dispatch.suspicious_activity_auto_clear_enabled,
            suspicious_activity_auto_clear_success_threshold: dispatch
                .suspicious_activity_auto_clear_success_threshold,
            suspicious_activity_auto_clear_after_ms: dispatch
                .suspicious_activity_auto_clear_after_ms,
            model_cooldown_enabled: dispatch.model_cooldown_enabled,
            default_max_concurrency: dispatch.default_max_concurrency,
            rate_limit_bucket_capacity: dispatch.rate_limit_bucket_capacity,
            rate_limit_refill_per_second: dispatch.rate_limit_refill_per_second,
            rate_limit_refill_min_per_second: dispatch.rate_limit_refill_min_per_second,
            rate_limit_refill_recovery_step_per_success: dispatch
                .rate_limit_refill_recovery_step_per_success,
            rate_limit_refill_backoff_factor: dispatch.rate_limit_refill_backoff_factor,
            request_weighting: dispatch.request_weighting.clone(),
            stream_dispatch_lease_release_enabled: dispatch.stream_dispatch_lease_release_enabled,
            stream_pre_sse_failover: dispatch.stream_pre_sse_failover.clone(),
            non_stream_body_read_timeout: dispatch.non_stream_body_read_timeout.clone(),
            kiro_request_body_guard: dispatch.kiro_request_body_guard.clone(),
            thinking_signature_validation_mode: dispatch.thinking_signature_validation_mode,
            response_thinking_signature_compat_enabled: dispatch
                .response_thinking_signature_compat_enabled,
            proxy_pool: dispatch.proxy_pool.clone(),
            waiting_requests: self.queue_depth(),
        }
    }

    pub fn account_type_policies_snapshot(&self) -> BTreeMap<String, ModelSupportPolicy> {
        self.dispatch_config().account_type_policies
    }

    pub fn account_type_dispatch_policies_snapshot(
        &self,
    ) -> BTreeMap<String, AccountTypeDispatchPolicy> {
        self.dispatch_config().account_type_dispatch_policies
    }

    pub fn supports_model(&self, model: &str) -> bool {
        self.supports_model_for_scope(model, None)
    }

    pub fn supports_model_for_scope(
        &self,
        model: &str,
        credential_group_scope: Option<&CredentialGroupScope>,
    ) -> bool {
        let dispatch = self.dispatch_config();
        let requirement = Self::model_requirement(Some(model));
        let entries = self.entries.lock();
        entries.iter().any(|entry| {
            !entry.disabled
                && Self::credential_allowed_by_scope(&entry.credentials, credential_group_scope)
                && Self::is_model_supported(&dispatch, &entry.credentials, Some(model), requirement)
        })
    }

    pub fn request_weighting_config_snapshot(&self) -> RequestWeightingConfig {
        self.dispatch_config().request_weighting
    }

    pub fn stream_dispatch_lease_release_enabled(&self) -> bool {
        self.dispatch_config().stream_dispatch_lease_release_enabled
    }

    pub fn stream_pre_sse_failover_config_snapshot(&self) -> StreamPreSseFailoverConfig {
        self.dispatch_config().stream_pre_sse_failover
    }

    pub fn non_stream_body_read_timeout_config_snapshot(&self) -> NonStreamBodyReadTimeoutConfig {
        self.dispatch_config().non_stream_body_read_timeout
    }

    pub fn kiro_request_body_guard_config_snapshot(&self) -> KiroRequestBodyGuardConfig {
        self.dispatch_config().kiro_request_body_guard
    }

    pub fn thinking_signature_validation_mode(&self) -> ThinkingSignatureValidationMode {
        self.dispatch_config().thinking_signature_validation_mode
    }

    pub fn response_thinking_signature_compat_enabled(&self) -> bool {
        self.dispatch_config()
            .response_thinking_signature_compat_enabled
    }

    /// 获取负载均衡模式（Admin API）
    pub fn get_load_balancing_mode(&self) -> String {
        self.dispatch_config().mode
    }

    fn persist_dispatch_config(&self, dispatch: &DispatchConfig) -> anyhow::Result<()> {
        self.state_store
            .persist_dispatch_config(&PersistedDispatchConfig {
                mode: dispatch.mode.clone(),
                session_affinity_enabled: dispatch.session_affinity_enabled,
                queue_max_size: dispatch.queue_max_size,
                queue_max_wait_ms: dispatch.queue_max_wait_ms,
                rate_limit_cooldown_ms: dispatch.rate_limit_cooldown_ms,
                rate_limit_cooldown_enabled: dispatch.rate_limit_cooldown_enabled,
                suspicious_activity_cooldown_ms: dispatch.suspicious_activity_cooldown_ms,
                suspicious_activity_cooldown_enabled: dispatch.suspicious_activity_cooldown_enabled,
                suspicious_activity_prefer_clean_credentials: dispatch
                    .suspicious_activity_prefer_clean_credentials,
                suspicious_activity_auto_disable_enabled: dispatch
                    .suspicious_activity_auto_disable_enabled,
                suspicious_activity_auto_disable_threshold: dispatch
                    .suspicious_activity_auto_disable_threshold,
                suspicious_activity_auto_disable_window_ms: dispatch
                    .suspicious_activity_auto_disable_window_ms,
                suspicious_activity_auto_clear_enabled: dispatch
                    .suspicious_activity_auto_clear_enabled,
                suspicious_activity_auto_clear_success_threshold: dispatch
                    .suspicious_activity_auto_clear_success_threshold,
                suspicious_activity_auto_clear_after_ms: dispatch
                    .suspicious_activity_auto_clear_after_ms,
                model_cooldown_enabled: dispatch.model_cooldown_enabled,
                default_max_concurrency: dispatch.default_max_concurrency,
                rate_limit_bucket_capacity: dispatch.rate_limit_bucket_capacity,
                rate_limit_refill_per_second: dispatch.rate_limit_refill_per_second,
                rate_limit_refill_min_per_second: dispatch.rate_limit_refill_min_per_second,
                rate_limit_refill_recovery_step_per_success: dispatch
                    .rate_limit_refill_recovery_step_per_success,
                rate_limit_refill_backoff_factor: dispatch.rate_limit_refill_backoff_factor,
                request_weighting: dispatch.request_weighting.clone(),
                stream_dispatch_lease_release_enabled: dispatch
                    .stream_dispatch_lease_release_enabled,
                stream_pre_sse_failover: dispatch.stream_pre_sse_failover.clone(),
                non_stream_body_read_timeout: dispatch.non_stream_body_read_timeout.clone(),
                kiro_request_body_guard: dispatch.kiro_request_body_guard.clone(),
                thinking_signature_validation_mode: dispatch.thinking_signature_validation_mode,
                response_thinking_signature_compat_enabled: dispatch
                    .response_thinking_signature_compat_enabled,
                proxy_pool: dispatch.proxy_pool.clone(),
                account_type_policies: dispatch.account_type_policies.clone(),
                account_type_dispatch_policies: dispatch.account_type_dispatch_policies.clone(),
                credential_groups: self.credential_group_catalog_snapshot(),
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
        self.set_account_type_strategy_config(Some(account_type_policies), None)
    }

    pub fn set_account_type_dispatch_policies(
        &self,
        mut account_type_dispatch_policies: BTreeMap<String, AccountTypeDispatchPolicy>,
    ) -> anyhow::Result<()> {
        crate::model::model_policy::normalize_account_type_dispatch_policies(
            &mut account_type_dispatch_policies,
        );
        self.set_account_type_strategy_config(None, Some(account_type_dispatch_policies))
    }

    pub fn set_account_type_strategy_config(
        &self,
        account_type_policies: Option<BTreeMap<String, ModelSupportPolicy>>,
        account_type_dispatch_policies: Option<BTreeMap<String, AccountTypeDispatchPolicy>>,
    ) -> anyhow::Result<()> {
        let previous = self.dispatch_config();
        let mut next = previous.clone();

        if let Some(account_type_policies) = account_type_policies {
            next.account_type_policies = account_type_policies;
        }
        if let Some(account_type_dispatch_policies) = account_type_dispatch_policies {
            next.account_type_dispatch_policies = account_type_dispatch_policies;
        }

        if previous == next {
            return Ok(());
        }

        let _state_write_guard = self.state_write_lock.lock();
        *self.dispatch_config.lock() = next.clone();

        if let Err(err) = self.persist_dispatch_config(&next) {
            *self.dispatch_config.lock() = previous;
            return Err(err);
        }

        if previous.account_type_dispatch_policies != next.account_type_dispatch_policies {
            self.reconfigure_rate_limit_runtime(&next);
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
        rate_limit_cooldown_enabled: Option<bool>,
        suspicious_activity_cooldown_ms: Option<u64>,
        suspicious_activity_cooldown_enabled: Option<bool>,
        suspicious_activity_prefer_clean_credentials: Option<bool>,
        suspicious_activity_auto_disable_enabled: Option<bool>,
        suspicious_activity_auto_disable_threshold: Option<u32>,
        suspicious_activity_auto_disable_window_ms: Option<u64>,
        suspicious_activity_auto_clear_enabled: Option<bool>,
        suspicious_activity_auto_clear_success_threshold: Option<u32>,
        suspicious_activity_auto_clear_after_ms: Option<u64>,
        model_cooldown_enabled: Option<bool>,
        default_max_concurrency: Option<u32>,
        rate_limit_bucket_capacity: Option<f64>,
        rate_limit_refill_per_second: Option<f64>,
        rate_limit_refill_min_per_second: Option<f64>,
        rate_limit_refill_recovery_step_per_success: Option<f64>,
        rate_limit_refill_backoff_factor: Option<f64>,
        request_weighting: Option<RequestWeightingConfig>,
        stream_dispatch_lease_release_enabled: Option<bool>,
        stream_pre_sse_failover: Option<StreamPreSseFailoverConfig>,
        non_stream_body_read_timeout: Option<NonStreamBodyReadTimeoutConfig>,
        kiro_request_body_guard: Option<KiroRequestBodyGuardConfig>,
        session_affinity_enabled: Option<bool>,
        thinking_signature_validation_mode: Option<ThinkingSignatureValidationMode>,
        response_thinking_signature_compat_enabled: Option<bool>,
        proxy_pool: Option<ProxyPoolConfig>,
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
        if let Some(rate_limit_cooldown_enabled) = rate_limit_cooldown_enabled {
            next.rate_limit_cooldown_enabled = rate_limit_cooldown_enabled;
        }
        if let Some(suspicious_activity_cooldown_ms) = suspicious_activity_cooldown_ms {
            next.suspicious_activity_cooldown_ms = suspicious_activity_cooldown_ms;
        }
        if let Some(suspicious_activity_cooldown_enabled) = suspicious_activity_cooldown_enabled {
            next.suspicious_activity_cooldown_enabled = suspicious_activity_cooldown_enabled;
        }
        if let Some(suspicious_activity_prefer_clean_credentials) =
            suspicious_activity_prefer_clean_credentials
        {
            next.suspicious_activity_prefer_clean_credentials =
                suspicious_activity_prefer_clean_credentials;
        }
        if let Some(suspicious_activity_auto_disable_enabled) =
            suspicious_activity_auto_disable_enabled
        {
            next.suspicious_activity_auto_disable_enabled =
                suspicious_activity_auto_disable_enabled;
        }
        if let Some(suspicious_activity_auto_disable_threshold) =
            suspicious_activity_auto_disable_threshold
        {
            next.suspicious_activity_auto_disable_threshold =
                suspicious_activity_auto_disable_threshold;
        }
        if let Some(suspicious_activity_auto_disable_window_ms) =
            suspicious_activity_auto_disable_window_ms
        {
            next.suspicious_activity_auto_disable_window_ms =
                suspicious_activity_auto_disable_window_ms;
        }
        if let Some(suspicious_activity_auto_clear_enabled) = suspicious_activity_auto_clear_enabled
        {
            next.suspicious_activity_auto_clear_enabled = suspicious_activity_auto_clear_enabled;
        }
        if let Some(suspicious_activity_auto_clear_success_threshold) =
            suspicious_activity_auto_clear_success_threshold
        {
            next.suspicious_activity_auto_clear_success_threshold =
                suspicious_activity_auto_clear_success_threshold;
        }
        if let Some(suspicious_activity_auto_clear_after_ms) =
            suspicious_activity_auto_clear_after_ms
        {
            next.suspicious_activity_auto_clear_after_ms = suspicious_activity_auto_clear_after_ms;
        }
        if let Some(model_cooldown_enabled) = model_cooldown_enabled {
            next.model_cooldown_enabled = model_cooldown_enabled;
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
        if let Some(stream_dispatch_lease_release_enabled) = stream_dispatch_lease_release_enabled {
            next.stream_dispatch_lease_release_enabled = stream_dispatch_lease_release_enabled;
        }
        if let Some(stream_pre_sse_failover) = stream_pre_sse_failover {
            next.stream_pre_sse_failover = stream_pre_sse_failover;
        }
        if let Some(non_stream_body_read_timeout) = non_stream_body_read_timeout {
            next.non_stream_body_read_timeout = non_stream_body_read_timeout;
        }
        if let Some(kiro_request_body_guard) = kiro_request_body_guard {
            next.kiro_request_body_guard = kiro_request_body_guard;
        }
        if let Some(session_affinity_enabled) = session_affinity_enabled {
            next.session_affinity_enabled = session_affinity_enabled;
        }
        if let Some(thinking_signature_validation_mode) = thinking_signature_validation_mode {
            next.thinking_signature_validation_mode = thinking_signature_validation_mode;
        }
        if let Some(response_thinking_signature_compat_enabled) =
            response_thinking_signature_compat_enabled
        {
            next.response_thinking_signature_compat_enabled =
                response_thinking_signature_compat_enabled;
        }
        if let Some(proxy_pool) = proxy_pool {
            next.proxy_pool = proxy_pool;
        }

        if next.suspicious_activity_auto_disable_enabled
            && next.suspicious_activity_auto_disable_threshold == 0
        {
            anyhow::bail!(
                "suspiciousActivityAutoDisableThreshold 必须大于 0，或关闭 suspiciousActivityAutoDisableEnabled"
            );
        }
        if next.suspicious_activity_auto_clear_enabled
            && next.suspicious_activity_auto_clear_success_threshold == 0
            && next.suspicious_activity_auto_clear_after_ms == 0
        {
            anyhow::bail!(
                "suspiciousActivityAutoClearSuccessThreshold 和 suspiciousActivityAutoClearAfterMs 不能同时为 0，或关闭 suspiciousActivityAutoClearEnabled"
            );
        }

        Self::validate_dispatch_rate_limit_config(&next)?;
        next.stream_pre_sse_failover.validate()?;
        next.non_stream_body_read_timeout.validate()?;
        next.kiro_request_body_guard.validate()?;
        next.proxy_pool.validate()?;

        if previous == next {
            return Ok(());
        }

        let _state_write_guard = self.state_write_lock.lock();
        *self.dispatch_config.lock() = next.clone();

        if let Err(err) = self.persist_dispatch_config(&next) {
            *self.dispatch_config.lock() = previous;
            return Err(err);
        }

        if previous.rate_limit_cooldown_enabled != next.rate_limit_cooldown_enabled {
            self.clear_disabled_rate_limit_penalties(&next);
        } else if previous.rate_limit_cooldown_ms != next.rate_limit_cooldown_ms
            && next.rate_limit_cooldown_ms == 0
        {
            self.clear_all_rate_limit_cooldowns();
        }
        if previous.suspicious_activity_cooldown_enabled
            && !next.suspicious_activity_cooldown_enabled
        {
            self.clear_all_rate_limit_cooldowns();
        } else if previous.suspicious_activity_cooldown_ms != next.suspicious_activity_cooldown_ms
            && next.suspicious_activity_cooldown_ms == 0
        {
            self.clear_all_rate_limit_cooldowns();
        }
        if previous.model_cooldown_enabled && !next.model_cooldown_enabled {
            if let Err(err) = self.clear_all_runtime_model_restrictions() {
                tracing::warn!("关闭模型冷却后清理运行时模型限制失败: {}", err);
            }
        }
        if previous.rate_limit_bucket_capacity != next.rate_limit_bucket_capacity
            || previous.rate_limit_refill_per_second != next.rate_limit_refill_per_second
            || previous.rate_limit_refill_min_per_second != next.rate_limit_refill_min_per_second
            || previous.rate_limit_refill_recovery_step_per_success
                != next.rate_limit_refill_recovery_step_per_success
            || previous.rate_limit_refill_backoff_factor != next.rate_limit_refill_backoff_factor
            || previous.account_type_dispatch_policies != next.account_type_dispatch_policies
        {
            self.reconfigure_rate_limit_runtime(&next);
        }

        self.availability_notify.notify_waiters();
        tracing::info!(
            "调度配置已更新: mode={}, sessionAffinityEnabled={}, queueMaxSize={}, queueMaxWaitMs={}, rateLimitCooldownMs={}, rateLimitCooldownEnabled={}, suspiciousActivityCooldownMs={}, suspiciousActivityCooldownEnabled={}, suspiciousActivityAutoClearEnabled={}, suspiciousActivityAutoClearSuccessThreshold={}, suspiciousActivityAutoClearAfterMs={}, modelCooldownEnabled={}, defaultMaxConcurrency={:?}, rateLimitBucketCapacity={}, rateLimitRefillPerSecond={}, rateLimitRefillMinPerSecond={}, rateLimitRefillRecoveryStepPerSuccess={}, rateLimitRefillBackoffFactor={}, requestWeightingEnabled={}, requestWeightingBaseWeight={}, requestWeightingMaxWeight={}, streamDispatchLeaseReleaseEnabled={}, streamPreSseFailoverEnabled={}, streamPreSseTotalBudgetMs={}, streamPreSseMaxFastFailovers={}, kiroRequestBodyGuardEnabled={}, kiroRequestBodyGuardMaxBytes={}, thinkingSignatureValidationMode={}, responseThinkingSignatureCompatEnabled={}",
            next.mode,
            next.session_affinity_enabled,
            next.queue_max_size,
            next.queue_max_wait_ms,
            next.rate_limit_cooldown_ms,
            next.rate_limit_cooldown_enabled,
            next.suspicious_activity_cooldown_ms,
            next.suspicious_activity_cooldown_enabled,
            next.suspicious_activity_auto_clear_enabled,
            next.suspicious_activity_auto_clear_success_threshold,
            next.suspicious_activity_auto_clear_after_ms,
            next.model_cooldown_enabled,
            next.default_max_concurrency,
            next.rate_limit_bucket_capacity,
            next.rate_limit_refill_per_second,
            next.rate_limit_refill_min_per_second,
            next.rate_limit_refill_recovery_step_per_success,
            next.rate_limit_refill_backoff_factor,
            next.request_weighting.enabled,
            next.request_weighting.base_weight,
            next.request_weighting.max_weight,
            next.stream_dispatch_lease_release_enabled,
            next.stream_pre_sse_failover.enabled,
            next.stream_pre_sse_failover.total_budget_ms,
            next.stream_pre_sse_failover.max_fast_failovers,
            next.kiro_request_body_guard.enabled,
            next.kiro_request_body_guard.max_bytes,
            next.thinking_signature_validation_mode.as_str(),
            next.response_thinking_signature_compat_enabled
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
    use crate::kiro::model::credentials::CredentialsConfig;
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

    #[test]
    fn external_idp_cached_token_endpoint_is_validated() {
        assert_eq!(
            normalize_external_idp_endpoint(
                "https://login.example.com/tenant/oauth2/v2.0/token",
                "token_endpoint"
            )
            .unwrap(),
            "https://login.example.com/tenant/oauth2/v2.0/token"
        );

        assert!(
            normalize_external_idp_endpoint("http://login.example.com/token", "token_endpoint")
                .is_err()
        );
        assert!(
            normalize_external_idp_endpoint(
                "https://user@login.example.com/token",
                "token_endpoint"
            )
            .is_err()
        );
        assert!(
            normalize_external_idp_endpoint("https://localhost/token", "token_endpoint").is_err()
        );
        assert!(
            normalize_external_idp_endpoint("https://127.0.0.1/token", "token_endpoint").is_err()
        );
    }

    fn proxy_pool_config_for_tests() -> ProxyPoolConfig {
        ProxyPoolConfig {
            enabled: true,
            require_proxy: true,
            assignment_strategy: "weighted_least_assigned".to_string(),
            proxies: vec![
                ProxyPoolEntry {
                    id: "node-a".to_string(),
                    url: "http://proxy-a.local:7890".to_string(),
                    username: None,
                    password: None,
                    weight: 1,
                    enabled: true,
                    expected_egress_ip: None,
                },
                ProxyPoolEntry {
                    id: "node-b".to_string(),
                    url: "http://proxy-b.local:7890".to_string(),
                    username: None,
                    password: None,
                    weight: 1,
                    enabled: true,
                    expected_egress_ip: None,
                },
            ],
            failover: crate::model::config::ProxyPoolFailoverConfig {
                enabled: true,
                failure_threshold: 1,
                cooldown_secs: 300,
                probe_url: None,
            },
        }
    }

    #[test]
    fn proxy_pool_assigns_new_credential_to_least_assigned_proxy() {
        let mut config = Config::default();
        config.proxy_pool = proxy_pool_config_for_tests();

        let manager = MultiTokenManager::new(config, Vec::new(), None, None, false).unwrap();
        let mut existing_a = available_credential(0);
        existing_a.proxy_id = Some("node-a".to_string());
        let mut existing_b = available_credential(1);
        existing_b.proxy_id = Some("node-a".to_string());
        let mut disabled_b = available_credential(2);
        disabled_b.proxy_id = Some("node-b".to_string());
        disabled_b.disabled = true;
        let persisted = vec![existing_a, existing_b, disabled_b];

        let mut new_credential = KiroCredentials::default();
        new_credential.refresh_token = Some("refresh-token-for-proxy-assignment".repeat(4));

        manager
            .assign_proxy_for_new_credential(&mut new_credential, &persisted)
            .unwrap();

        assert_eq!(new_credential.proxy_id.as_deref(), Some("node-b"));
    }

    #[test]
    fn proxy_pool_failover_migrates_credential_and_persists_assignment() {
        let credentials_path = temp_credentials_path("proxy-pool-failover");
        let mut config = Config::default();
        config.proxy_pool = proxy_pool_config_for_tests();

        let mut credential = available_credential(0);
        credential.id = Some(1);
        credential.refresh_token = Some("refresh-token-for-proxy-failover".repeat(4));
        credential.proxy_id = Some("node-a".to_string());
        write_credentials_file(&credentials_path, &[credential.clone()]);

        let manager = MultiTokenManager::new(
            config,
            vec![credential],
            None,
            Some(credentials_path.clone()),
            true,
        )
        .unwrap();

        assert!(manager.report_proxy_transport_failure(1, "proxy connect timeout"));
        assert_eq!(
            manager.current_credentials(1).unwrap().proxy_id.as_deref(),
            Some("node-b")
        );

        let persisted = CredentialsConfig::load(&credentials_path)
            .unwrap()
            .into_sorted_credentials();
        assert_eq!(persisted[0].proxy_id.as_deref(), Some("node-b"));

        std::fs::remove_file(credentials_path).unwrap();
    }

    #[test]
    fn proxy_pool_failover_migrates_all_credentials_on_failed_node() {
        let credentials_path = temp_credentials_path("proxy-pool-failover-all");
        let mut config = Config::default();
        config.proxy_pool = proxy_pool_config_for_tests();

        let mut first = available_credential(0);
        first.id = Some(1);
        first.refresh_token = Some("refresh-token-for-proxy-failover-first".repeat(4));
        first.proxy_id = Some("node-a".to_string());
        let mut second = available_credential(1);
        second.id = Some(2);
        second.refresh_token = Some("refresh-token-for-proxy-failover-second".repeat(4));
        second.proxy_id = Some("node-a".to_string());
        write_credentials_file(&credentials_path, &[first.clone(), second.clone()]);

        let manager = MultiTokenManager::new(
            config,
            vec![first, second],
            None,
            Some(credentials_path.clone()),
            true,
        )
        .unwrap();

        assert!(manager.report_proxy_transport_failure(1, "proxy connect timeout"));
        assert_eq!(
            manager.current_credentials(1).unwrap().proxy_id.as_deref(),
            Some("node-b")
        );
        assert_eq!(
            manager.current_credentials(2).unwrap().proxy_id.as_deref(),
            Some("node-b")
        );

        let persisted = CredentialsConfig::load(&credentials_path)
            .unwrap()
            .into_sorted_credentials();
        assert_eq!(persisted[0].proxy_id.as_deref(), Some("node-b"));
        assert_eq!(persisted[1].proxy_id.as_deref(), Some("node-b"));

        std::fs::remove_file(credentials_path).unwrap();
    }

    #[test]
    fn proxy_pool_bound_missing_id_does_not_fallback_to_global_proxy() {
        let mut config = Config::default();
        config.proxy_pool = proxy_pool_config_for_tests();
        let manager = MultiTokenManager::new(
            config,
            Vec::new(),
            Some(ProxyConfig::new("http://global.local:8080")),
            None,
            false,
        )
        .unwrap();
        let mut credential = available_credential(0);
        credential.id = Some(1);
        credential.proxy_id = Some("missing-node".to_string());

        let err = manager
            .effective_proxy_for_credentials(&credential)
            .unwrap_err()
            .to_string();

        assert!(err.contains("不存在或未启用"));
    }

    #[test]
    fn proxy_pool_cooling_node_is_not_effective_proxy() {
        let mut config = Config::default();
        let mut pool = proxy_pool_config_for_tests();
        pool.proxies.truncate(1);
        config.proxy_pool = pool;

        let mut credential = available_credential(0);
        credential.id = Some(1);
        credential.refresh_token = Some("refresh-token-for-single-proxy".repeat(4));
        credential.proxy_id = Some("node-a".to_string());
        let manager = MultiTokenManager::new(config, vec![credential], None, None, false).unwrap();

        assert!(!manager.report_proxy_transport_failure(1, "proxy connect timeout"));
        let current = manager.current_credentials(1).unwrap();
        let err = manager
            .effective_proxy_for_credentials(&current)
            .unwrap_err()
            .to_string();

        assert!(err.contains("冷却中"));
    }

    #[test]
    fn proxy_pool_validate_rejects_invalid_proxy_url() {
        let mut pool = proxy_pool_config_for_tests();
        pool.proxies[0].url = "not-a-proxy-url".to_string();

        let err = pool.validate().unwrap_err().to_string();

        assert!(err.contains("url 无效"));
    }

    #[test]
    fn set_credential_proxy_auto_assigns_and_persists_pool_binding() {
        let credentials_path = temp_credentials_path("set-proxy-auto");
        let mut config = Config::default();
        config.proxy_pool = proxy_pool_config_for_tests();

        let mut current = available_credential(0);
        current.id = Some(1);
        current.refresh_token = Some("refresh-token-for-current-auto-proxy".repeat(4));
        current.proxy_id = Some("node-a".to_string());
        let mut existing = available_credential(1);
        existing.id = Some(2);
        existing.refresh_token = Some("refresh-token-for-existing-auto-proxy".repeat(4));
        existing.proxy_id = Some("node-a".to_string());
        write_credentials_file(&credentials_path, &[current.clone(), existing.clone()]);

        let manager = MultiTokenManager::new(
            config,
            vec![current, existing],
            None,
            Some(credentials_path.clone()),
            true,
        )
        .unwrap();

        manager
            .set_credential_proxy(1, "auto".to_string(), None, None, None, None)
            .unwrap();

        assert_eq!(
            manager.current_credentials(1).unwrap().proxy_id.as_deref(),
            Some("node-b")
        );
        let persisted = CredentialsConfig::load(&credentials_path)
            .unwrap()
            .into_sorted_credentials();
        assert_eq!(persisted[0].proxy_id.as_deref(), Some("node-b"));
        assert!(persisted[0].proxy_url.is_none());

        std::fs::remove_file(credentials_path).unwrap();
    }

    #[test]
    fn set_credential_groups_normalizes_and_persists() {
        let credentials_path = temp_credentials_path("set-credential-groups");
        let mut credential = available_credential(0);
        credential.id = Some(1);
        write_credentials_file(&credentials_path, &[credential.clone()]);

        let manager = MultiTokenManager::new(
            Config::default(),
            vec![credential],
            None,
            Some(credentials_path.clone()),
            true,
        )
        .unwrap();

        manager
            .set_credential_groups(
                1,
                vec![
                    " Stable ".to_string(),
                    "low-cost".to_string(),
                    "stable".to_string(),
                ],
            )
            .unwrap();

        assert_eq!(
            manager.current_credentials(1).unwrap().credential_groups,
            vec!["low-cost".to_string(), "stable".to_string()]
        );
        let persisted = CredentialsConfig::load(&credentials_path)
            .unwrap()
            .into_sorted_credentials();
        assert_eq!(
            persisted[0].credential_groups,
            vec!["low-cost".to_string(), "stable".to_string()]
        );

        std::fs::remove_file(credentials_path).unwrap();
    }

    #[test]
    fn set_credential_proxy_custom_clears_pool_binding() {
        let credentials_path = temp_credentials_path("set-proxy-custom");
        let mut config = Config::default();
        config.proxy_pool = proxy_pool_config_for_tests();

        let mut credential = available_credential(0);
        credential.id = Some(1);
        credential.proxy_id = Some("node-a".to_string());
        write_credentials_file(&credentials_path, &[credential.clone()]);

        let manager = MultiTokenManager::new(
            config,
            vec![credential],
            None,
            Some(credentials_path.clone()),
            true,
        )
        .unwrap();

        manager
            .set_credential_proxy(
                1,
                "custom".to_string(),
                None,
                Some("http://custom-proxy.local:3128".to_string()),
                Some(" user ".to_string()),
                Some(" pass ".to_string()),
            )
            .unwrap();

        let current = manager.current_credentials(1).unwrap();
        assert_eq!(
            current.proxy_url.as_deref(),
            Some("http://custom-proxy.local:3128")
        );
        assert_eq!(current.proxy_username.as_deref(), Some("user"));
        assert_eq!(current.proxy_password.as_deref(), Some("pass"));
        assert!(current.proxy_id.is_none());

        let persisted = CredentialsConfig::load(&credentials_path)
            .unwrap()
            .into_sorted_credentials();
        assert_eq!(
            persisted[0].proxy_url.as_deref(),
            Some("http://custom-proxy.local:3128")
        );
        assert!(persisted[0].proxy_id.is_none());

        std::fs::remove_file(credentials_path).unwrap();
    }

    #[test]
    fn set_credential_proxy_require_proxy_rejects_direct_and_global() {
        let mut config = Config::default();
        config.proxy_pool = proxy_pool_config_for_tests();

        let mut credential = available_credential(0);
        credential.id = Some(1);
        credential.proxy_id = Some("node-a".to_string());
        let manager = MultiTokenManager::new(config, vec![credential], None, None, false).unwrap();

        let direct_err = manager
            .set_credential_proxy(1, "direct".to_string(), None, None, None, None)
            .unwrap_err()
            .to_string();
        assert!(direct_err.contains("不能使用 direct"));

        let global_err = manager
            .set_credential_proxy(1, "global".to_string(), None, None, None, None)
            .unwrap_err()
            .to_string();
        assert!(global_err.contains("不能清空代理绑定"));
    }

    #[test]
    fn snapshot_exposes_effective_builder_id_profile_arn() {
        let mut credential = available_credential(0);
        credential.auth_method = Some("idc".to_string());
        credential.client_id = Some("client".to_string());
        credential.client_secret = Some("secret".to_string());
        credential.start_url = Some("https://view.awsapps.com/start/".to_string());

        let manager =
            MultiTokenManager::new(Config::default(), vec![credential], None, None, false).unwrap();

        let snapshot = manager.snapshot();
        let entry = snapshot.entries.iter().find(|entry| entry.id == 1).unwrap();
        assert!(entry.has_profile_arn);
        assert_eq!(
            entry.profile_arn.as_deref(),
            Some(crate::kiro::model::credentials::KIRO_BUILDER_ID_PROFILE_ARN)
        );

        let persisted = manager.current_credentials(1).unwrap();
        assert!(persisted.profile_arn.is_none());
    }

    #[test]
    fn set_profile_arn_persists_and_clears_profile_scoped_model_cache() {
        let credentials_path = temp_credentials_path("set-profile-arn");
        let mut credential = available_credential(0);
        credential.id = Some(1);
        credential.profile_arn =
            Some("arn:aws:codewhisperer:us-east-1:123:profile/old".to_string());
        credential.available_model_ids = vec!["old-model".to_string()];
        credential.available_models_cached_at = Some("2026-06-06T00:00:00Z".to_string());
        credential.runtime_model_restrictions =
            vec![crate::model::model_policy::RuntimeModelRestriction {
                model: "old-model".to_string(),
                expires_at: "2026-06-07T00:00:00Z".to_string(),
            }];

        let manager = MultiTokenManager::new(
            Config::default(),
            vec![credential],
            None,
            Some(credentials_path.clone()),
            true,
        )
        .unwrap();

        manager
            .set_profile_arn(
                1,
                "arn:aws:codewhisperer:us-east-1:123:profile/new".to_string(),
            )
            .unwrap();

        let updated = manager.current_credentials(1).unwrap();
        assert_eq!(
            updated.profile_arn.as_deref(),
            Some("arn:aws:codewhisperer:us-east-1:123:profile/new")
        );
        assert!(updated.available_model_ids.is_empty());
        assert!(updated.available_models_cached_at.is_none());
        assert!(updated.runtime_model_restrictions.is_empty());

        let persisted: Vec<KiroCredentials> =
            serde_json::from_slice(&std::fs::read(credentials_path).unwrap()).unwrap();
        assert_eq!(
            persisted[0].profile_arn.as_deref(),
            Some("arn:aws:codewhisperer:us-east-1:123:profile/new")
        );
        assert!(persisted[0].available_model_ids.is_empty());
        assert!(persisted[0].available_models_cached_at.is_none());
    }

    #[test]
    fn available_models_refresh_uses_singleflight_and_failure_cooldown() {
        let mut credential = available_credential(0);
        credential.id = Some(1);
        let manager =
            MultiTokenManager::new(Config::default(), vec![credential], None, None, false).unwrap();

        assert!(manager.try_start_available_models_refresh(1));
        assert!(
            !manager.try_start_available_models_refresh(1),
            "同一凭据刷新进行中时不应重复启动"
        );

        manager.finish_available_models_refresh(1, AvailableModelsRefreshOutcome::RetryLater);
        assert!(
            !manager.try_start_available_models_refresh(1),
            "失败冷却期内不应重复启动"
        );

        {
            let mut entries = manager.entries.lock();
            let entry = entries.iter_mut().find(|entry| entry.id == 1).unwrap();
            entry.available_models_refresh_cooldown_until =
                Some(Instant::now() - StdDuration::from_secs(1));
        }

        assert!(
            manager.try_start_available_models_refresh(1),
            "冷却结束后应允许再次启动"
        );
        manager.finish_available_models_refresh(1, AvailableModelsRefreshOutcome::Success);
        assert!(manager.try_start_available_models_refresh(1));
    }

    #[test]
    fn available_models_update_persists_and_clears_refresh_cooldown() {
        let credentials_path = temp_credentials_path("available-models-update");
        let mut credential = available_credential(0);
        credential.id = Some(1);
        let manager = MultiTokenManager::new(
            Config::default(),
            vec![credential],
            None,
            Some(credentials_path.clone()),
            true,
        )
        .unwrap();

        assert!(manager.try_start_available_models_refresh(1));
        manager.finish_available_models_refresh(1, AvailableModelsRefreshOutcome::RetryLater);

        manager.apply_available_models_update(
            1,
            vec![
                "claude-sonnet-4.5".to_string(),
                "claude-opus-4.7".to_string(),
            ],
        );

        {
            let entries = manager.entries.lock();
            let entry = entries.iter().find(|entry| entry.id == 1).unwrap();
            assert!(!entry.available_models_refresh_in_progress);
            assert!(entry.available_models_refresh_cooldown_until.is_none());
            assert_eq!(entry.credentials.available_model_ids.len(), 2);
            assert!(entry.credentials.available_models_cached_at.is_some());
        }

        let persisted: Vec<KiroCredentials> =
            serde_json::from_slice(&std::fs::read(credentials_path).unwrap()).unwrap();
        assert_eq!(
            persisted[0].available_model_ids,
            vec![
                "claude-opus-4.7".to_string(),
                "claude-sonnet-4.5".to_string()
            ]
        );
        assert!(persisted[0].available_models_cached_at.is_some());
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

    fn shared_candidate_ids(manager: &MultiTokenManager) -> Vec<u64> {
        let dispatch = manager.dispatch_config();
        let candidates = manager
            .collect_shared_dispatch_snapshot_candidates(
                &dispatch,
                None,
                ModelRequirement::Any,
                &HashSet::new(),
                None,
                Instant::now(),
                &Utc::now(),
                None,
            )
            .unwrap();
        candidates
            .credentials
            .into_iter()
            .map(|credential| credential.id)
            .collect()
    }

    #[test]
    fn test_enabled_supported_priority_helpers_detect_lower_priority_fallback() {
        let manager = MultiTokenManager::new(
            Config::default(),
            vec![
                available_credential(0),
                available_credential(0),
                available_credential(10),
            ],
            None,
            None,
            false,
        )
        .unwrap();

        assert_eq!(manager.enabled_supported_credential_count(None), 3);

        let high_priority_ids = manager.enabled_supported_credential_ids_at_priority(None, 0);
        assert_eq!(high_priority_ids.len(), 2);

        let fallback_ids = manager.enabled_supported_credential_ids_at_priority(None, 10);
        assert_eq!(fallback_ids.len(), 1);

        let excluded = HashSet::new();
        assert!(manager.has_enabled_supported_credential_below_priority(None, 0, &excluded, None));

        let excluded_fallback: HashSet<u64> = fallback_ids.into_iter().collect();
        assert!(!manager.has_enabled_supported_credential_below_priority(
            None,
            0,
            &excluded_fallback,
            None
        ));
    }

    #[tokio::test]
    async fn credential_group_scope_limits_acquired_credentials() {
        let mut stable = available_credential(10);
        stable.credential_groups = vec!["stable".to_string()];
        let mut cheap = available_credential(0);
        cheap.credential_groups = vec!["low-cost".to_string()];
        let manager =
            MultiTokenManager::new(Config::default(), vec![stable, cheap], None, None, false)
                .unwrap();
        let scope = CredentialGroupScope::Groups(vec!["stable".to_string()]);

        let ctx = manager
            .acquire_context_with_weight_excluding_and_affinity_for_scope(
                None,
                1.0,
                &HashSet::new(),
                None,
                Some(&scope),
            )
            .await
            .unwrap();

        assert_eq!(ctx.id, 1);
        assert_eq!(ctx.credentials.credential_groups, vec!["stable"]);
        assert_eq!(
            manager.enabled_supported_credential_count_for_scope(None, Some(&scope)),
            1
        );
    }

    #[tokio::test]
    async fn credential_group_scope_reports_forbidden_without_match() {
        let mut stable = available_credential(0);
        stable.credential_groups = vec!["stable".to_string()];
        let manager =
            MultiTokenManager::new(Config::default(), vec![stable], None, None, false).unwrap();
        let scope = CredentialGroupScope::Groups(vec!["low-cost".to_string()]);

        let err = match manager
            .acquire_context_with_weight_excluding_and_affinity_for_scope(
                None,
                1.0,
                &HashSet::new(),
                None,
                Some(&scope),
            )
            .await
        {
            Ok(_) => panic!("expected credential scope mismatch to be rejected"),
            Err(err) => err,
        };

        assert!(
            err.downcast_ref::<CredentialScopeForbiddenError>()
                .is_some()
        );
        assert_eq!(
            manager.enabled_supported_credential_count_for_scope(None, Some(&scope)),
            0
        );
        assert!(!manager.supports_model_for_scope("claude-sonnet-4.5", Some(&scope)));
    }

    #[tokio::test]
    async fn empty_credential_groups_match_default_scope() {
        let credential = available_credential(0);
        let manager =
            MultiTokenManager::new(Config::default(), vec![credential], None, None, false).unwrap();
        let default_scope = CredentialGroupScope::Groups(vec!["default".to_string()]);

        let ctx = manager
            .acquire_context_with_weight_excluding_and_affinity_for_scope(
                None,
                1.0,
                &HashSet::new(),
                None,
                Some(&default_scope),
            )
            .await
            .unwrap();

        assert_eq!(ctx.id, 1);
        assert!(ctx.credentials.credential_groups.is_empty());
        assert!(manager.supports_model_for_scope("claude-sonnet-4.5", Some(&default_scope)));
    }

    #[test]
    fn test_shared_dispatch_snapshot_candidates_skip_disabled_credentials() {
        let mut disabled = available_credential(0);
        disabled.id = Some(1);
        disabled.disabled = true;
        disabled.disabled_reason = Some(DisabledReason::Manual.as_str().to_string());

        let mut enabled = available_credential(1);
        enabled.id = Some(2);

        let manager = MultiTokenManager::new(
            Config::default(),
            vec![disabled, enabled],
            None,
            None,
            false,
        )
        .unwrap();

        assert_eq!(shared_candidate_ids(&manager), vec![2]);
    }

    #[test]
    fn test_shared_dispatch_snapshot_candidates_include_reenabled_credentials() {
        let mut credential = available_credential(0);
        credential.id = Some(1);
        credential.disabled = true;
        credential.disabled_reason = Some(DisabledReason::Manual.as_str().to_string());

        let manager =
            MultiTokenManager::new(Config::default(), vec![credential], None, None, false).unwrap();

        let dispatch = manager.dispatch_config();
        let disabled_result = manager.collect_shared_dispatch_snapshot_candidates(
            &dispatch,
            None,
            ModelRequirement::Any,
            &HashSet::new(),
            None,
            Instant::now(),
            &Utc::now(),
            None,
        );
        assert!(matches!(
            disabled_result,
            Err(ReservationFailure::AllDisabled)
        ));

        manager.set_disabled(1, false).unwrap();

        assert_eq!(shared_candidate_ids(&manager), vec![1]);
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

    #[test]
    fn external_idp_duplicate_reason_matches_stable_identity() {
        let mut existing = KiroCredentials::default();
        existing.auth_method = Some("external_idp".to_string());
        existing.provider = Some("ExternalIdp".to_string());
        existing.issuer_url = Some("https://login.example.com/tenant/v2.0/".to_string());
        existing.client_id = Some("CLIENT-1".to_string());
        existing.email = Some("User@Example.com".to_string());
        existing.user_id = Some("USER-1".to_string());
        existing.profile_arn = Some("arn:aws:codewhisperer:us-east-1:123:profile/p1".to_string());

        let mut same_profile_different_user = KiroCredentials::default();
        same_profile_different_user.auth_method = Some("external-idp".to_string());
        same_profile_different_user.issuer_url =
            Some("https://login.example.com/tenant/v2.0".to_string());
        same_profile_different_user.client_id = Some("client-1".to_string());
        same_profile_different_user.email = Some("other@example.com".to_string());
        same_profile_different_user.user_id = Some("user-2".to_string());
        same_profile_different_user.profile_arn =
            Some("ARN:AWS:CODEWHISPERER:US-EAST-1:123:PROFILE/P1".to_string());
        assert_eq!(
            external_idp_duplicate_reason(&existing, &same_profile_different_user),
            None
        );

        let mut profile_only = KiroCredentials::default();
        profile_only.auth_method = Some("external-idp".to_string());
        profile_only.profile_arn =
            Some("ARN:AWS:CODEWHISPERER:US-EAST-1:123:PROFILE/P1".to_string());
        assert_eq!(
            external_idp_duplicate_reason(&existing, &profile_only),
            None
        );

        let mut by_email = KiroCredentials::default();
        by_email.auth_method = Some("external_idp".to_string());
        by_email.issuer_url = Some("https://login.example.com/tenant/v2.0".to_string());
        by_email.client_id = Some("client-1".to_string());
        by_email.email = Some("user@example.com".to_string());
        assert_eq!(
            external_idp_duplicate_reason(&existing, &by_email),
            Some("issuerUrl/clientId/email 重复")
        );

        let mut by_user_id = KiroCredentials::default();
        by_user_id.auth_method = Some("external_idp".to_string());
        by_user_id.issuer_url = Some("https://login.example.com/tenant/v2.0".to_string());
        by_user_id.client_id = Some("client-1".to_string());
        by_user_id.user_id = Some("user-1".to_string());
        assert_eq!(
            external_idp_duplicate_reason(&existing, &by_user_id),
            Some("issuerUrl/clientId/userId 重复")
        );
    }

    #[test]
    fn external_idp_duplicate_reason_ignores_non_external_accounts() {
        let mut existing = KiroCredentials::default();
        existing.auth_method = Some("social".to_string());
        existing.email = Some("user@example.com".to_string());
        existing.profile_arn = Some("arn:aws:codewhisperer:us-east-1:123:profile/p1".to_string());

        let mut candidate = KiroCredentials::default();
        candidate.auth_method = Some("external_idp".to_string());
        candidate.email = Some("user@example.com".to_string());
        candidate.profile_arn = Some("arn:aws:codewhisperer:us-east-1:123:profile/p1".to_string());

        assert_eq!(external_idp_duplicate_reason(&existing, &candidate), None);
    }

    #[test]
    fn credential_duplicate_reason_matches_idc_user_id_with_rotated_refresh_token() {
        let mut existing = KiroCredentials::default();
        existing.auth_method = Some("idc".to_string());
        existing.refresh_token = Some("old-refresh".to_string());
        existing.email = Some("ProMaxUser@Example.com".to_string());
        existing.user_id = Some("user-123".to_string());
        existing.subscription_title = Some("KIRO MAX".to_string());

        let mut candidate = KiroCredentials::default();
        candidate.auth_method = Some("idc".to_string());
        candidate.refresh_token = Some("new-refresh".to_string());
        candidate.email = Some("other@example.com".to_string());
        candidate.user_id = Some("USER-123".to_string());
        candidate.subscription_title = Some("KIRO MAX".to_string());

        assert_eq!(
            credential_duplicate_reason(&existing, &candidate),
            Some("authAccountType/userId 重复")
        );
    }

    #[test]
    fn credential_duplicate_reason_matches_legacy_builder_id_provider() {
        let mut existing = KiroCredentials::default();
        existing.auth_method = Some("idc".to_string());
        existing.provider = Some("BuilderId".to_string());
        existing.user_id = Some("user-123".to_string());

        let mut candidate = KiroCredentials::default();
        candidate.auth_method = Some("idc".to_string());
        candidate.user_id = Some("user-123".to_string());

        assert_eq!(
            credential_duplicate_reason(&existing, &candidate),
            Some("authAccountType/userId 重复")
        );
    }

    #[test]
    fn credential_duplicate_reason_matches_same_scope_email_when_user_id_missing() {
        let mut existing = KiroCredentials::default();
        existing.auth_method = Some("idc".to_string());
        existing.email = Some("ProMaxUser@Example.com".to_string());
        existing.subscription_title = Some("KIRO MAX".to_string());

        let mut candidate = KiroCredentials::default();
        candidate.auth_method = Some("idc".to_string());
        candidate.email = Some("promaxuser@example.com".to_string());
        candidate.subscription_title = Some("KIRO MAX".to_string());

        assert_eq!(
            credential_duplicate_reason(&existing, &candidate),
            Some("authAccountType/email 重复")
        );
    }

    #[test]
    fn credential_duplicate_reason_allows_same_enterprise_org_different_accounts() {
        let mut existing = KiroCredentials::default();
        existing.auth_method = Some("idc".to_string());
        existing.provider = Some("Enterprise".to_string());
        existing.start_url = Some("https://example.awsapps.com/start".to_string());
        existing.email = Some("alice@example.com".to_string());
        existing.user_id = Some("alice-user-id".to_string());

        let mut candidate = KiroCredentials::default();
        candidate.auth_method = Some("idc".to_string());
        candidate.provider = Some("Enterprise".to_string());
        candidate.start_url = Some("https://example.awsapps.com/start/".to_string());
        candidate.email = Some("bob@example.com".to_string());
        candidate.user_id = Some("bob-user-id".to_string());

        assert_eq!(credential_duplicate_reason(&existing, &candidate), None);
    }

    #[test]
    fn credential_duplicate_reason_does_not_match_email_across_auth_scopes() {
        let mut existing = KiroCredentials::default();
        existing.auth_method = Some("social".to_string());
        existing.provider = Some("Google".to_string());
        existing.email = Some("user@example.com".to_string());

        let mut candidate = KiroCredentials::default();
        candidate.auth_method = Some("idc".to_string());
        candidate.email = Some("user@example.com".to_string());

        assert_eq!(credential_duplicate_reason(&existing, &candidate), None);
    }

    #[test]
    fn apply_usage_limits_metadata_to_credentials_populates_identity_for_duplicate_check() {
        let usage_limits: UsageLimitsResponse = serde_json::from_value(serde_json::json!({
            "subscriptionInfo": {
                "subscriptionTitle": "KIRO MAX",
                "type": "Q_DEVELOPER_STANDALONE_MAX"
            },
            "userInfo": {
                "email": "promax@example.com",
                "userId": "user-123"
            }
        }))
        .unwrap();

        let mut credentials = KiroCredentials::default();
        credentials.auth_method = Some("idc".to_string());
        apply_usage_limits_metadata_to_credentials(&mut credentials, &usage_limits);

        assert_eq!(credentials.email.as_deref(), Some("promax@example.com"));
        assert_eq!(credentials.user_id.as_deref(), Some("user-123"));
        assert_eq!(credentials.subscription_title.as_deref(), Some("KIRO MAX"));
        assert_eq!(
            credentials.subscription_type.as_deref(),
            Some("Q_DEVELOPER_STANDALONE_MAX")
        );
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
    fn test_standard_account_type_dispatch_policy_applies_without_explicit_account_type() {
        let mut config = Config::default();
        config.default_max_concurrency = Some(3);
        config.account_type_dispatch_policies.insert(
            "power".to_string(),
            AccountTypeDispatchPolicy {
                max_concurrency: Some(32),
                rate_limit_bucket_capacity: Some(0.0),
                rate_limit_refill_per_second: Some(0.0),
            },
        );

        let mut cred = available_credential(0);
        cred.subscription_title = Some("KIRO POWER".to_string());

        let manager = MultiTokenManager::new(config, vec![cred], None, None, false).unwrap();
        let snapshot = manager.snapshot();
        let entry = snapshot.entries.iter().find(|entry| entry.id == 1).unwrap();

        assert_eq!(entry.max_concurrency, Some(32));
        assert_eq!(entry.rate_limit_bucket_capacity, None);
        assert_eq!(entry.rate_limit_refill_per_second, None);
    }

    #[test]
    fn test_subscription_title_update_reconfigures_runtime_and_snapshot_sources() {
        let mut config = Config::default();
        config.default_max_concurrency = Some(3);
        config.account_type_dispatch_policies.insert(
            "power".to_string(),
            AccountTypeDispatchPolicy {
                max_concurrency: Some(32),
                rate_limit_bucket_capacity: Some(0.0),
                rate_limit_refill_per_second: Some(0.0),
            },
        );

        let cred = available_credential(0);
        let manager = MultiTokenManager::new(config, vec![cred], None, None, false).unwrap();

        let before = manager.snapshot();
        let before_entry = before.entries.iter().find(|entry| entry.id == 1).unwrap();
        assert_eq!(before_entry.resolved_account_type, None);
        assert_eq!(before_entry.account_type_source, None);
        assert_eq!(before_entry.max_concurrency, Some(3));
        assert_eq!(
            before_entry.max_concurrency_source.as_deref(),
            Some("global-default")
        );
        assert_eq!(before_entry.rate_limit_bucket_capacity, Some(6.0));
        assert_eq!(
            before_entry.rate_limit_bucket_capacity_source.as_deref(),
            Some("global-default")
        );

        manager.apply_subscription_info_update(1, Some("KIRO POWER"), None);

        let after = manager.snapshot();
        let after_entry = after.entries.iter().find(|entry| entry.id == 1).unwrap();
        assert_eq!(
            after_entry.subscription_title.as_deref(),
            Some("KIRO POWER")
        );
        assert_eq!(after_entry.resolved_account_type.as_deref(), Some("power"));
        assert_eq!(
            after_entry.account_type_source.as_deref(),
            Some("subscription-title")
        );
        assert_eq!(after_entry.max_concurrency, Some(32));
        assert_eq!(
            after_entry.max_concurrency_source.as_deref(),
            Some("account-type")
        );
        assert_eq!(after_entry.rate_limit_bucket_capacity, None);
        assert_eq!(
            after_entry.rate_limit_bucket_capacity_source.as_deref(),
            Some("account-type")
        );
        assert_eq!(after_entry.rate_limit_refill_per_second, None);
        assert_eq!(
            after_entry.rate_limit_refill_per_second_source.as_deref(),
            Some("account-type")
        );
    }

    #[test]
    fn test_snapshot_prefers_explicit_overrides_for_sources() {
        let mut config = Config::default();
        config.default_max_concurrency = Some(3);
        config.account_type_dispatch_policies.insert(
            "power".to_string(),
            AccountTypeDispatchPolicy {
                max_concurrency: Some(32),
                rate_limit_bucket_capacity: Some(0.0),
                rate_limit_refill_per_second: Some(0.0),
            },
        );

        let mut cred = available_credential(0);
        cred.subscription_title = Some("KIRO POWER".to_string());
        cred.account_type = Some("power-canary".to_string());
        cred.max_concurrency = Some(7);
        cred.rate_limit_bucket_capacity = Some(4.0);
        cred.rate_limit_refill_per_second = Some(1.5);

        let manager = MultiTokenManager::new(config, vec![cred], None, None, false).unwrap();
        let snapshot = manager.snapshot();
        let entry = snapshot.entries.iter().find(|entry| entry.id == 1).unwrap();

        assert_eq!(entry.resolved_account_type.as_deref(), Some("power-canary"));
        assert_eq!(entry.account_type_source.as_deref(), Some("credential"));
        assert_eq!(entry.max_concurrency, Some(7));
        assert_eq!(entry.max_concurrency_override, Some(7));
        assert_eq!(entry.max_concurrency_source.as_deref(), Some("credential"));
        assert_eq!(entry.rate_limit_bucket_capacity, Some(4.0));
        assert_eq!(entry.rate_limit_bucket_capacity_override, Some(4.0));
        assert_eq!(
            entry.rate_limit_bucket_capacity_source.as_deref(),
            Some("credential")
        );
        assert_eq!(entry.rate_limit_refill_per_second, Some(1.5));
        assert_eq!(entry.rate_limit_refill_per_second_override, Some(1.5));
        assert_eq!(
            entry.rate_limit_refill_per_second_source.as_deref(),
            Some("credential")
        );
    }

    #[test]
    fn test_set_credential_source_updates_snapshot_and_persisted_metadata() {
        let config = Config::default();
        let mut cred = available_credential(0);
        cred.source_supplier_name = Some(" old supplier ".to_string());

        let manager = MultiTokenManager::new(config, vec![cred], None, None, false).unwrap();
        manager
            .set_credential_source(
                1,
                Some(Some(" supplier-1 ".to_string())),
                Some(Some(" Vendor A ".to_string())),
                Some(Some(" 202606181 ".to_string())),
            )
            .unwrap();

        let snapshot = manager.snapshot();
        let entry = snapshot.entries.iter().find(|entry| entry.id == 1).unwrap();
        assert_eq!(entry.source_supplier_id.as_deref(), Some("supplier-1"));
        assert_eq!(entry.source_supplier_name.as_deref(), Some("Vendor A"));
        assert_eq!(entry.source_batch.as_deref(), Some("202606181"));

        manager
            .set_credential_source(1, None, Some(Some("  ".to_string())), Some(None))
            .unwrap();

        let snapshot = manager.snapshot();
        let entry = snapshot.entries.iter().find(|entry| entry.id == 1).unwrap();
        assert_eq!(entry.source_supplier_id.as_deref(), Some("supplier-1"));
        assert_eq!(entry.source_supplier_name, None);
        assert_eq!(entry.source_batch, None);

        let persisted = manager.persisted_credentials_snapshot();
        assert_eq!(
            persisted[0].source_supplier_id.as_deref(),
            Some("supplier-1")
        );
        assert_eq!(persisted[0].source_supplier_name, None);
        assert_eq!(persisted[0].source_batch, None);
    }

    #[test]
    fn test_runtime_model_restriction_hides_model_until_cache_cleared() {
        let mut config = Config::default();
        config.model_cooldown_enabled = true;
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

        assert!(!manager.defer_model_unsupported_credential(1, "claude-opus-4.6"));
        assert!(!manager.supports_model("claude-opus-4-6"));

        let snapshot = manager.snapshot();
        let entry = snapshot.entries.iter().find(|entry| entry.id == 1).unwrap();
        assert_eq!(
            entry.cooldown_remaining_ms, None,
            "模型不支持不应再给账号施加全局冷却"
        );

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
    async fn test_priority_mode_keeps_priority_before_real_opus_4_7_preference() {
        let config = Config::default();
        let mut pro = available_credential(0);
        pro.subscription_title = Some("KIRO PRO".to_string());
        let mut pro_plus = available_credential(9);
        pro_plus.subscription_title = Some("KIRO PRO+".to_string());

        let manager =
            MultiTokenManager::new(config, vec![pro, pro_plus], None, None, false).unwrap();

        let ctx = manager
            .acquire_context(Some("claude-opus-4.7"))
            .await
            .unwrap();
        assert_eq!(ctx.id, 1);
    }

    #[tokio::test]
    async fn test_priority_mode_spills_to_lower_priority_when_high_priority_at_capacity() {
        let config = Config::default();
        let mut primary = available_credential(0);
        primary.subscription_title = Some("KIRO PRO".to_string());
        primary.max_concurrency = Some(1);
        let mut overflow = available_credential(10);
        overflow.subscription_title = Some("KIRO PRO+".to_string());
        overflow.max_concurrency = Some(20);

        let manager =
            MultiTokenManager::new(config, vec![primary, overflow], None, None, false).unwrap();

        let first = manager
            .acquire_context(Some("claude-opus-4.7"))
            .await
            .unwrap();
        assert_eq!(first.id, 1);

        let second = manager
            .acquire_context(Some("claude-opus-4.7"))
            .await
            .unwrap();
        assert_eq!(second.id, 2);
    }

    #[tokio::test]
    async fn test_priority_mode_model_unsupported_switches_to_next_high_priority_before_pro_plus() {
        let mut config = Config::default();
        config.model_cooldown_enabled = true;

        let mut primary = available_credential(0);
        primary.subscription_title = Some("KIRO PRO".to_string());
        let mut secondary = available_credential(0);
        secondary.subscription_title = Some("KIRO PRO".to_string());
        let mut overflow = available_credential(10);
        overflow.subscription_title = Some("KIRO PRO+".to_string());

        let manager = MultiTokenManager::new(
            config,
            vec![primary, secondary, overflow],
            None,
            None,
            false,
        )
        .unwrap();

        assert!(manager.defer_model_unsupported_credential(1, "claude-opus-4.7"));

        let snapshot = manager.snapshot();
        assert_eq!(snapshot.current_id, 2);

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
    async fn test_real_opus_4_8_uses_high_tier_opus_candidate_policy() {
        let config = Config::default();
        let mut power = available_credential(0);
        power.subscription_title = Some("KIRO POWER".to_string());

        let manager = MultiTokenManager::new(config, vec![power], None, None, false).unwrap();

        let ctx = manager
            .acquire_context(Some("claude-opus-4.8"))
            .await
            .unwrap();
        assert_eq!(ctx.id, 1);
    }

    #[tokio::test]
    async fn test_model_unsupported_restriction_does_not_cool_down_account_for_other_models() {
        let mut config = Config::default();
        config.model_cooldown_enabled = true;
        let mut cred = available_credential(0);
        cred.subscription_title = Some("KIRO PRO+".to_string());

        let manager = MultiTokenManager::new(config, vec![cred], None, None, false).unwrap();

        assert!(!manager.defer_model_unsupported_credential(1, "claude-opus-4.7"));
        assert!(
            !manager.supports_model("claude-opus-4-7"),
            "被上游拒绝的模型族应被运行时屏蔽"
        );
        assert!(
            manager.supports_model("claude-opus-4-6"),
            "同一账号仍应可继续承载其他合法模型"
        );

        let snapshot = manager.snapshot();
        let entry = snapshot.entries.iter().find(|entry| entry.id == 1).unwrap();
        assert_eq!(
            entry.cooldown_remaining_ms, None,
            "模型不支持限制不应把账号打进冷却"
        );
        assert_eq!(entry.runtime_model_restrictions.len(), 1);
        assert_eq!(entry.runtime_model_restrictions[0].model, "claude-opus-4.7");

        let ctx = manager
            .acquire_context(Some("claude-opus-4.6"))
            .await
            .expect("其他合法模型不应被前一个 INVALID_MODEL_ID 拖住");
        assert_eq!(ctx.id, 1);
    }

    #[tokio::test]
    async fn test_model_unsupported_restriction_for_opus_4_8_is_model_family_scoped() {
        let mut config = Config::default();
        config.model_cooldown_enabled = true;
        let mut cred = available_credential(0);
        cred.subscription_title = Some("KIRO PRO+".to_string());

        let manager = MultiTokenManager::new(config, vec![cred], None, None, false).unwrap();

        assert!(!manager.defer_model_unsupported_credential(1, "claude-opus-4.8"));
        assert!(
            !manager.supports_model("claude-opus-4-8"),
            "被上游拒绝的 4.8 模型族应被运行时屏蔽"
        );
        assert!(
            manager.supports_model("claude-opus-4-7"),
            "4.8 运行时限制不应影响 4.7 模型族"
        );
    }

    #[tokio::test]
    async fn test_model_unsupported_restriction_is_skipped_when_model_cooldown_disabled() {
        let mut config = Config::default();
        config.model_cooldown_enabled = false;
        let mut cred = available_credential(0);
        cred.subscription_title = Some("KIRO PRO+".to_string());

        let manager = MultiTokenManager::new(config, vec![cred], None, None, false).unwrap();

        assert!(!manager.defer_model_unsupported_credential(1, "claude-opus-4.7"));
        assert!(
            manager.supports_model("claude-opus-4-7"),
            "关闭模型冷却后，不应因为单次 INVALID_MODEL_ID 把模型族屏蔽掉"
        );

        let snapshot = manager.snapshot();
        let entry = snapshot.entries.iter().find(|entry| entry.id == 1).unwrap();
        assert!(entry.runtime_model_restrictions.is_empty());
        assert_eq!(entry.cooldown_remaining_ms, None);
    }

    #[tokio::test]
    async fn test_slow_model_cooldown_restricts_only_target_model_and_switches_candidates() {
        let mut config = Config::default();
        config.model_cooldown_enabled = true;

        let mut primary = available_credential(0);
        primary.subscription_title = Some("KIRO PRO+".to_string());
        let mut alternate = available_credential(0);
        alternate.subscription_title = Some("KIRO PRO+".to_string());

        let manager =
            MultiTokenManager::new(config, vec![primary, alternate], None, None, false).unwrap();

        assert!(manager.defer_slow_model_credential(
            1,
            "claude-opus-4.7",
            StdDuration::from_secs(120),
            "test"
        ));

        let snapshot = manager.snapshot();
        assert_eq!(snapshot.current_id, 2);
        let primary = snapshot.entries.iter().find(|entry| entry.id == 1).unwrap();
        assert_eq!(primary.cooldown_remaining_ms, None);
        assert_eq!(primary.runtime_model_restrictions.len(), 1);
        assert_eq!(
            primary.runtime_model_restrictions[0].model,
            "claude-opus-4.7"
        );

        let ctx = manager
            .acquire_context(Some("claude-opus-4.7"))
            .await
            .expect("4.7 应切换到未冷却的候选");
        assert_eq!(ctx.id, 2);
        drop(ctx);

        assert!(
            manager.supports_model("claude-opus-4.6"),
            "慢 4.7 模型冷却不应影响其他模型族"
        );
    }

    #[test]
    fn test_slow_model_cooldown_skips_last_supported_candidate() {
        let mut config = Config::default();
        config.model_cooldown_enabled = true;
        let mut cred = available_credential(0);
        cred.subscription_title = Some("KIRO PRO+".to_string());

        let manager = MultiTokenManager::new(config, vec![cred], None, None, false).unwrap();

        assert!(!manager.defer_slow_model_credential(
            1,
            "claude-opus-4.7",
            StdDuration::from_secs(120),
            "test"
        ));
        let snapshot = manager.snapshot();
        let entry = snapshot.entries.iter().find(|entry| entry.id == 1).unwrap();
        assert!(entry.runtime_model_restrictions.is_empty());
    }

    #[test]
    fn test_clear_runtime_model_restrictions_for_credential() {
        let mut config = Config::default();
        config.model_cooldown_enabled = true;

        let mut primary = available_credential(0);
        primary.subscription_title = Some("KIRO PRO+".to_string());
        let mut alternate = available_credential(0);
        alternate.subscription_title = Some("KIRO PRO+".to_string());

        let manager =
            MultiTokenManager::new(config, vec![primary, alternate], None, None, false).unwrap();

        assert!(manager.defer_slow_model_credential(
            1,
            "claude-opus-4.7",
            StdDuration::from_secs(120),
            "test"
        ));
        assert!(
            manager
                .clear_runtime_model_restrictions_for_credential(1)
                .unwrap()
        );
        assert!(
            !manager
                .clear_runtime_model_restrictions_for_credential(1)
                .unwrap()
        );

        let snapshot = manager.snapshot();
        let entry = snapshot.entries.iter().find(|entry| entry.id == 1).unwrap();
        assert!(entry.runtime_model_restrictions.is_empty());
    }

    #[tokio::test]
    async fn test_request_scoped_excluded_credentials_are_skipped_in_balanced_mode() {
        let mut config = Config::default();
        config.load_balancing_mode = "balanced".to_string();
        config.model_cooldown_enabled = false;

        let mut first = available_credential(0);
        first.subscription_title = Some("KIRO PRO+".to_string());
        let mut second = available_credential(0);
        second.subscription_title = Some("KIRO PRO+".to_string());

        let manager = MultiTokenManager::new(config, vec![first, second], None, None, false)
            .expect("manager should initialize");

        let initial = manager
            .acquire_context(Some("claude-opus-4.6"))
            .await
            .expect("unexcluded request should pick the first balanced candidate");
        assert_eq!(initial.id, 1);
        drop(initial);

        let mut excluded_credential_ids = HashSet::new();
        excluded_credential_ids.insert(1);
        let fallback = manager
            .acquire_context_with_weight_excluding(
                Some("claude-opus-4.6"),
                1.0,
                &excluded_credential_ids,
            )
            .await
            .expect("request-scoped exclusions should force a different candidate");
        assert_eq!(fallback.id, 2);
    }

    #[tokio::test]
    async fn test_request_scoped_excluded_high_priority_spills_to_lower_priority() {
        let config = Config::default();
        let primary = available_credential(0);
        let fallback = available_credential(10);

        let manager = MultiTokenManager::new(config, vec![primary, fallback], None, None, false)
            .expect("manager should initialize");

        let initial = manager
            .acquire_context(None)
            .await
            .expect("unexcluded request should pick high priority");
        assert_eq!(initial.id, 1);
        drop(initial);

        let mut excluded_credential_ids = HashSet::new();
        excluded_credential_ids.insert(1);
        let fallback = manager
            .acquire_context_with_weight_excluding(None, 1.0, &excluded_credential_ids)
            .await
            .expect("request-scoped high-priority exclusion should spill to lower priority");
        assert_eq!(fallback.id, 2);
    }

    #[test]
    fn test_opus_4_7_remaining_candidates_can_be_detected_as_leader_refresh_only() {
        let mut config = Config::default();
        config.model_cooldown_enabled = true;

        let mut unsupported_primary = available_credential(0);
        unsupported_primary.id = Some(1);
        unsupported_primary.subscription_title = Some("KIRO PRO+".to_string());

        let mut unsupported_secondary = available_credential(1);
        unsupported_secondary.id = Some(2);
        unsupported_secondary.subscription_title = Some("KIRO POWER".to_string());

        let mut stale_power_a = available_credential(2);
        stale_power_a.id = Some(3);
        stale_power_a.subscription_title = Some("KIRO POWER".to_string());
        stale_power_a.expires_at = Some((Utc::now() + Duration::minutes(1)).to_rfc3339());

        let mut stale_power_b = available_credential(3);
        stale_power_b.id = Some(4);
        stale_power_b.subscription_title = Some("KIRO POWER".to_string());
        stale_power_b.expires_at = Some((Utc::now() + Duration::minutes(2)).to_rfc3339());

        let manager = MultiTokenManager::new(
            config,
            vec![
                unsupported_primary,
                unsupported_secondary,
                stale_power_a,
                stale_power_b,
            ],
            None,
            None,
            false,
        )
        .unwrap();

        assert!(manager.defer_model_unsupported_credential(1, "claude-opus-4.7"));
        assert!(manager.defer_model_unsupported_credential(2, "claude-opus-4.7"));

        let dispatch = manager.dispatch_config();
        let entries = manager.entries.lock();
        assert!(
            MultiTokenManager::all_supported_model_candidates_need_local_token_refresh(
                &dispatch,
                &entries,
                "claude-opus-4.7"
            )
        );
    }

    #[test]
    fn test_opus_4_7_remaining_candidates_keep_local_retry_when_fresh_token_exists() {
        let mut config = Config::default();
        config.model_cooldown_enabled = true;

        let mut unsupported_primary = available_credential(0);
        unsupported_primary.id = Some(1);
        unsupported_primary.subscription_title = Some("KIRO PRO+".to_string());

        let mut unsupported_secondary = available_credential(1);
        unsupported_secondary.id = Some(2);
        unsupported_secondary.subscription_title = Some("KIRO POWER".to_string());

        let mut stale_power = available_credential(2);
        stale_power.id = Some(3);
        stale_power.subscription_title = Some("KIRO POWER".to_string());
        stale_power.expires_at = Some((Utc::now() + Duration::minutes(1)).to_rfc3339());

        let mut fresh_power = available_credential(3);
        fresh_power.id = Some(4);
        fresh_power.subscription_title = Some("KIRO POWER".to_string());
        fresh_power.expires_at = Some((Utc::now() + Duration::hours(1)).to_rfc3339());

        let manager = MultiTokenManager::new(
            config,
            vec![
                unsupported_primary,
                unsupported_secondary,
                stale_power,
                fresh_power,
            ],
            None,
            None,
            false,
        )
        .unwrap();

        assert!(manager.defer_model_unsupported_credential(1, "claude-opus-4.7"));
        assert!(manager.defer_model_unsupported_credential(2, "claude-opus-4.7"));

        let dispatch = manager.dispatch_config();
        let entries = manager.entries.lock();
        assert!(
            !MultiTokenManager::all_supported_model_candidates_need_local_token_refresh(
                &dispatch,
                &entries,
                "claude-opus-4.7"
            )
        );
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
    async fn test_session_affinity_reuses_successful_credential_when_enabled() {
        let mut config = Config::default();
        config.load_balancing_mode = "balanced".to_string();
        config.session_affinity_enabled = true;

        let manager = MultiTokenManager::new(
            config,
            vec![available_credential(0), available_credential(1)],
            None,
            None,
            false,
        )
        .unwrap();
        let excluded = HashSet::new();

        let first = manager
            .acquire_context_with_weight_excluding_and_affinity(
                None,
                1.0,
                &excluded,
                Some("session-a"),
            )
            .await
            .unwrap();
        let first_id = first.id;
        drop(first);
        manager.report_success(first_id);
        manager.record_session_affinity(None, Some("session-a"), first_id);

        let sticky = manager
            .acquire_context_with_weight_excluding_and_affinity(
                None,
                1.0,
                &excluded,
                Some("session-a"),
            )
            .await
            .unwrap();
        assert_eq!(sticky.id, first_id);
        drop(sticky);

        let unrelated = manager
            .acquire_context_with_weight_excluding_and_affinity(
                None,
                1.0,
                &excluded,
                Some("session-b"),
            )
            .await
            .unwrap();
        assert_ne!(unrelated.id, first_id);
    }

    #[tokio::test]
    async fn test_session_affinity_is_disabled_by_default() {
        let mut config = Config::default();
        config.load_balancing_mode = "balanced".to_string();

        let manager = MultiTokenManager::new(
            config,
            vec![available_credential(0), available_credential(1)],
            None,
            None,
            false,
        )
        .unwrap();
        let excluded = HashSet::new();

        let first = manager
            .acquire_context_with_weight_excluding_and_affinity(
                None,
                1.0,
                &excluded,
                Some("session-a"),
            )
            .await
            .unwrap();
        let first_id = first.id;
        drop(first);
        manager.report_success(first_id);
        manager.record_session_affinity(None, Some("session-a"), first_id);

        let next = manager
            .acquire_context_with_weight_excluding_and_affinity(
                None,
                1.0,
                &excluded,
                Some("session-a"),
            )
            .await
            .unwrap();
        assert_ne!(next.id, first_id);
    }

    #[tokio::test]
    async fn test_session_affinity_falls_back_when_bound_credential_is_at_capacity() {
        let mut config = Config::default();
        config.load_balancing_mode = "balanced".to_string();
        config.session_affinity_enabled = true;
        config.default_max_concurrency = Some(1);

        let manager = MultiTokenManager::new(
            config,
            vec![available_credential(0), available_credential(1)],
            None,
            None,
            false,
        )
        .unwrap();
        let excluded = HashSet::new();

        manager.record_session_affinity(None, Some("session-a"), 1);

        let first = manager
            .acquire_context_with_weight_excluding_and_affinity(
                None,
                1.0,
                &excluded,
                Some("session-a"),
            )
            .await
            .unwrap();
        assert_eq!(first.id, 1);

        let fallback = manager
            .acquire_context_with_weight_excluding_and_affinity(
                None,
                1.0,
                &excluded,
                Some("session-a"),
            )
            .await
            .unwrap();
        assert_eq!(fallback.id, 2);
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
    async fn test_shared_dispatch_priority_prefers_priority_before_opus_rank_when_redis_is_set() {
        let Some(mut config) = shared_runtime_test_config() else {
            return;
        };
        config.rate_limit_bucket_capacity = 0.0;
        config.rate_limit_refill_per_second = 0.0;

        let primary_id = unique_credential_id();
        let overflow_id = unique_credential_id();

        let mut primary = available_credential(0);
        primary.id = Some(primary_id);
        primary.subscription_title = Some("KIRO PRO".to_string());

        let mut overflow = available_credential(10);
        overflow.id = Some(overflow_id);
        overflow.subscription_title = Some("KIRO PRO+".to_string());

        let manager = MultiTokenManager::new(config, vec![primary, overflow], None, None, false)
            .expect("manager should initialize with shared runtime");

        let ctx = manager
            .acquire_context(Some("claude-opus-4.7"))
            .await
            .unwrap();
        assert_eq!(ctx.id, primary_id);
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
        config.rate_limit_cooldown_enabled = true;
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
        config.rate_limit_cooldown_enabled = true;
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

    #[tokio::test]
    async fn test_suspicious_activity_credential_enters_global_cooldown_when_regular_429_disabled()
    {
        let mut config = Config::default();
        config.rate_limit_cooldown_enabled = false;
        config.suspicious_activity_cooldown_enabled = true;
        config.suspicious_activity_cooldown_ms = 60_000;
        let primary = available_credential(0);
        let secondary = available_credential(1);

        let manager =
            MultiTokenManager::new(config, vec![primary, secondary], None, None, false).unwrap();

        manager.report_suspicious_activity_limited(1, None);

        let ctx = manager.acquire_context(None).await.unwrap();
        assert_eq!(ctx.id, 2);

        let snapshot = manager.snapshot();
        let primary_entry = snapshot.entries.iter().find(|entry| entry.id == 1).unwrap();
        assert_eq!(primary_entry.rate_limit_hit_streak, 1);
        assert_eq!(primary_entry.suspicious_activity_count, 1);
        assert!(
            primary_entry
                .suspicious_activity_quarantine_remaining_ms
                .unwrap_or_default()
                > 50_000,
            "suspicious activity 应写入可观测隔离标记"
        );
        assert!(
            primary_entry.cooldown_remaining_ms.unwrap_or_default() > 50_000,
            "suspicious activity 应使用独立的长冷却"
        );
    }

    #[tokio::test]
    async fn test_suspicious_activity_history_prefers_never_suspicious_credential() {
        let mut config = Config::default();
        config.load_balancing_mode = "priority".to_string();
        config.suspicious_activity_prefer_clean_credentials = true;

        let mut tainted = available_credential(0);
        tainted.suspicious_activity_count = 1;
        tainted.suspicious_activity_last_seen_at = Some(Utc::now().to_rfc3339());
        let clean = available_credential(10);

        let manager =
            MultiTokenManager::new(config, vec![tainted, clean], None, None, false).unwrap();

        let ctx = manager.acquire_context(None).await.unwrap();

        assert_eq!(ctx.id, 2, "历史 suspicious 账号只应作为兜底候选");
    }

    #[tokio::test]
    async fn test_repeated_suspicious_activity_auto_disables_credential() {
        let mut config = Config::default();
        config.suspicious_activity_cooldown_ms = 0;
        config.suspicious_activity_auto_disable_enabled = true;
        config.suspicious_activity_auto_disable_threshold = 2;
        config.suspicious_activity_auto_disable_window_ms = 60_000;

        let manager = MultiTokenManager::new(
            config,
            vec![available_credential(0), available_credential(1)],
            None,
            None,
            false,
        )
        .unwrap();

        manager.report_suspicious_activity_limited(1, Some("suspicious activity"));
        manager.report_suspicious_activity_limited(1, Some("suspicious activity"));

        let snapshot = manager.snapshot();
        let entry = snapshot.entries.iter().find(|entry| entry.id == 1).unwrap();
        assert!(entry.disabled);
        assert_eq!(
            entry.disabled_reason.as_deref(),
            Some(DisabledReason::SuspiciousActivity.as_str())
        );
        assert_eq!(entry.suspicious_activity_count, 2);

        let ctx = manager.acquire_context(None).await.unwrap();
        assert_eq!(ctx.id, 2);

        manager.reset_and_enable(1).unwrap();
        let snapshot = manager.snapshot();
        let entry = snapshot.entries.iter().find(|entry| entry.id == 1).unwrap();
        assert!(!entry.disabled);
        assert_eq!(entry.suspicious_activity_count, 0);
        assert_eq!(entry.suspicious_activity_recovery_success_count, 0);
        assert_eq!(entry.suspicious_activity_quarantine_remaining_ms, None);
    }

    #[test]
    fn test_regular_429_does_not_shrink_suspicious_activity_cooldown() {
        let mut config = Config::default();
        config.rate_limit_cooldown_enabled = true;
        config.rate_limit_cooldown_ms = 2_000;
        config.suspicious_activity_cooldown_enabled = true;
        config.suspicious_activity_cooldown_ms = 60_000;

        let manager =
            MultiTokenManager::new(config, vec![available_credential(0)], None, None, false)
                .unwrap();

        manager.report_suspicious_activity_limited(1, None);
        manager.report_rate_limited(1);

        let snapshot = manager.snapshot();
        let entry = snapshot.entries.iter().find(|entry| entry.id == 1).unwrap();
        assert!(
            entry.cooldown_remaining_ms.unwrap_or_default() > 50_000,
            "后续普通 429 不应缩短 suspicious activity 的长冷却"
        );
    }

    #[test]
    fn test_clear_suspicious_activity_marker_removes_quarantine_and_runtime_cooldown() {
        let mut config = Config::default();
        config.suspicious_activity_cooldown_enabled = true;
        config.suspicious_activity_cooldown_ms = 60_000;
        config.suspicious_activity_auto_disable_threshold = 10;

        let manager =
            MultiTokenManager::new(config, vec![available_credential(0)], None, None, false)
                .unwrap();

        manager.report_suspicious_activity_limited(1, Some("suspicious activity"));
        assert!(manager.clear_suspicious_activity_for_credential(1).unwrap());
        assert!(!manager.clear_suspicious_activity_for_credential(1).unwrap());

        let snapshot = manager.snapshot();
        let entry = snapshot.entries.iter().find(|entry| entry.id == 1).unwrap();
        assert_eq!(entry.suspicious_activity_count, 0);
        assert_eq!(entry.suspicious_activity_recovery_success_count, 0);
        assert_eq!(entry.suspicious_activity_quarantine_remaining_ms, None);
        assert_eq!(entry.cooldown_remaining_ms, None);
    }

    #[test]
    fn test_suspicious_activity_auto_clear_after_success_threshold() {
        let mut config = Config::default();
        config.suspicious_activity_cooldown_ms = 0;
        config.suspicious_activity_auto_disable_threshold = 10;
        config.suspicious_activity_auto_clear_enabled = true;
        config.suspicious_activity_auto_clear_success_threshold = 2;
        config.suspicious_activity_auto_clear_after_ms = 0;

        let manager =
            MultiTokenManager::new(config, vec![available_credential(0)], None, None, false)
                .unwrap();

        manager.report_suspicious_activity_limited(1, Some("suspicious activity"));
        manager.report_success(1);
        let snapshot = manager.snapshot();
        let entry = snapshot.entries.iter().find(|entry| entry.id == 1).unwrap();
        assert_eq!(entry.suspicious_activity_count, 1);
        assert_eq!(entry.suspicious_activity_recovery_success_count, 1);

        manager.report_success(1);
        let snapshot = manager.snapshot();
        let entry = snapshot.entries.iter().find(|entry| entry.id == 1).unwrap();
        assert_eq!(entry.suspicious_activity_count, 0);
        assert_eq!(entry.suspicious_activity_recovery_success_count, 0);
        assert!(entry.suspicious_activity_last_seen_at.is_none());
    }

    #[test]
    fn test_suspicious_activity_auto_clear_after_quiet_window() {
        let mut config = Config::default();
        config.suspicious_activity_auto_clear_enabled = true;
        config.suspicious_activity_auto_clear_success_threshold = 0;
        config.suspicious_activity_auto_clear_after_ms = 1_000;

        let mut tainted = available_credential(0);
        tainted.suspicious_activity_count = 1;
        tainted.suspicious_activity_first_seen_at =
            Some((Utc::now() - Duration::days(8)).to_rfc3339());
        tainted.suspicious_activity_last_seen_at =
            Some((Utc::now() - Duration::days(8)).to_rfc3339());
        tainted.suspicious_activity_recovery_success_count = 1;

        let manager = MultiTokenManager::new(config, vec![tainted], None, None, false).unwrap();

        let snapshot = manager.snapshot();
        let entry = snapshot.entries.iter().find(|entry| entry.id == 1).unwrap();
        assert_eq!(entry.suspicious_activity_count, 0);
        assert_eq!(entry.suspicious_activity_recovery_success_count, 0);
        assert!(entry.suspicious_activity_last_seen_at.is_none());
    }

    #[test]
    fn test_set_rate_limit_config_updates_only_requested_fields_and_preserves_cooldown() {
        let mut config = Config::default();
        config.rate_limit_cooldown_enabled = true;
        config.rate_limit_cooldown_ms = 4_000;

        let mut cred = available_credential(0);
        cred.rate_limit_bucket_capacity = Some(5.0);
        cred.rate_limit_refill_per_second = Some(0.8);

        let manager = MultiTokenManager::new(config, vec![cred], None, None, false).unwrap();

        manager.report_rate_limited(1);
        manager
            .set_rate_limit_config(1, None, Some(Some(7.0)), None)
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
            session_affinity_enabled: true,
            queue_max_size: 8,
            queue_max_wait_ms: 1500,
            rate_limit_cooldown_ms: 4500,
            rate_limit_cooldown_enabled: false,
            suspicious_activity_cooldown_ms: 1_800_000,
            suspicious_activity_cooldown_enabled: true,
            suspicious_activity_prefer_clean_credentials: true,
            suspicious_activity_auto_disable_enabled: true,
            suspicious_activity_auto_disable_threshold: 3,
            suspicious_activity_auto_disable_window_ms: 86_400_000,
            suspicious_activity_auto_clear_enabled: true,
            suspicious_activity_auto_clear_success_threshold: 10,
            suspicious_activity_auto_clear_after_ms: 604_800_000,
            model_cooldown_enabled: true,
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
            stream_dispatch_lease_release_enabled: false,
            stream_pre_sse_failover: StreamPreSseFailoverConfig::default(),
            non_stream_body_read_timeout: NonStreamBodyReadTimeoutConfig::default(),
            kiro_request_body_guard: KiroRequestBodyGuardConfig {
                max_bytes: 31 * 1024 * 1024,
                ..KiroRequestBodyGuardConfig::default()
            },
            thinking_signature_validation_mode: ThinkingSignatureValidationMode::WarnOnly,
            response_thinking_signature_compat_enabled: true,
            proxy_pool: ProxyPoolConfig::default(),
            account_type_policies: BTreeMap::new(),
            account_type_dispatch_policies: BTreeMap::new(),
            credential_groups: vec![crate::model::config::CredentialGroupConfig {
                name: "stable".to_string(),
                display_name: Some("Stable".to_string()),
                description: None,
                enabled: true,
            }],
        };

        assert!(manager.apply_dispatch_config_from_state(&persisted));

        let snapshot = manager.load_balancing_config_snapshot();
        assert_eq!(snapshot.mode, "balanced");
        assert!(snapshot.session_affinity_enabled);
        assert_eq!(snapshot.queue_max_size, 8);
        assert_eq!(snapshot.queue_max_wait_ms, 1500);
        assert_eq!(snapshot.rate_limit_cooldown_ms, 4500);
        assert!(!snapshot.rate_limit_cooldown_enabled);
        assert_eq!(snapshot.suspicious_activity_cooldown_ms, 1_800_000);
        assert!(snapshot.suspicious_activity_cooldown_enabled);
        assert!(snapshot.suspicious_activity_prefer_clean_credentials);
        assert!(snapshot.suspicious_activity_auto_disable_enabled);
        assert_eq!(snapshot.suspicious_activity_auto_disable_threshold, 3);
        assert_eq!(
            snapshot.suspicious_activity_auto_disable_window_ms,
            86_400_000
        );
        assert!(snapshot.suspicious_activity_auto_clear_enabled);
        assert_eq!(
            snapshot.suspicious_activity_auto_clear_success_threshold,
            10
        );
        assert_eq!(
            snapshot.suspicious_activity_auto_clear_after_ms,
            604_800_000
        );
        assert!(snapshot.model_cooldown_enabled);
        assert_eq!(snapshot.default_max_concurrency, Some(3));
        assert_eq!(snapshot.rate_limit_bucket_capacity, 4.0);
        assert_eq!(snapshot.rate_limit_refill_per_second, 1.2);
        assert_eq!(snapshot.rate_limit_refill_min_per_second, 0.3);
        assert_eq!(snapshot.rate_limit_refill_recovery_step_per_success, 0.15);
        assert_eq!(snapshot.rate_limit_refill_backoff_factor, 0.6);
        assert_eq!(snapshot.request_weighting.max_weight, 4.0);
        assert!(
            manager
                .credential_group_catalog_snapshot()
                .iter()
                .any(|group| group.name == "stable")
        );
        assert_eq!(snapshot.request_weighting.tools_bonus, 1.0);
        assert!(!snapshot.stream_dispatch_lease_release_enabled);
        assert!(snapshot.stream_pre_sse_failover.enabled);
        assert_eq!(snapshot.kiro_request_body_guard.max_bytes, 31 * 1024 * 1024);
        assert_eq!(
            snapshot.thinking_signature_validation_mode,
            ThinkingSignatureValidationMode::WarnOnly
        );
        assert!(snapshot.response_thinking_signature_compat_enabled);

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
            entry.token_usage_count = 2;
            entry.pending_token_usage_count_delta = 2;
            entry.input_tokens = 50;
            entry.pending_input_tokens_delta = 50;
            entry.output_tokens = 20;
            entry.pending_output_tokens_delta = 20;
            entry.last_used_at = Some("2026-04-15T02:00:00Z".to_string());
        }

        let store = StateStore::file(None, Some(credentials_path.clone()));
        let mut persisted = HashMap::new();
        persisted.insert(
            "1".to_string(),
            StatsEntryRecord {
                success_count: 5,
                last_used_at: Some("2026-04-15T01:00:00Z".to_string()),
                token_usage_count: 3,
                input_tokens: 120,
                output_tokens: 45,
            },
        );
        store.save_stats(&persisted).unwrap();

        assert!(manager.reload_stats_from_state().unwrap());

        let snapshot = manager.snapshot();
        let entry = snapshot.entries.iter().find(|entry| entry.id == 1).unwrap();
        assert_eq!(entry.success_count, 7);
        assert_eq!(entry.token_usage_count, 5);
        assert_eq!(entry.input_tokens, 170);
        assert_eq!(entry.output_tokens, 65);
        assert_eq!(entry.total_tokens, 235);
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
                None,
                Some(1.0),
                Some(1.2),
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
            r#"{"loadBalancingMode":"priority","queueMaxSize":0,"queueMaxWaitMs":0,"rateLimitCooldownMs":2000,"rateLimitCooldownEnabled":false,"suspiciousActivityCooldownMs":7200000,"suspiciousActivityCooldownEnabled":true,"defaultMaxConcurrency":2}"#,
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
                Some(false),
                Some(1_800_000),
                Some(true),
                Some(true),
                Some(true),
                Some(2),
                Some(3_600_000),
                Some(true),
                Some(5),
                Some(7_200_000),
                Some(true),
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
                Some(false),
                Some(StreamPreSseFailoverConfig::default()),
                Some(NonStreamBodyReadTimeoutConfig {
                    timeout_ms: 510_000,
                    retry_on_timeout: true,
                    ..NonStreamBodyReadTimeoutConfig::default()
                }),
                Some(KiroRequestBodyGuardConfig {
                    max_bytes: 31 * 1024 * 1024,
                    ..KiroRequestBodyGuardConfig::default()
                }),
                Some(true),
                Some(ThinkingSignatureValidationMode::StripInvalid),
                Some(true),
                None,
            )
            .unwrap();

        let persisted = Config::load(&config_path).unwrap();
        assert_eq!(persisted.load_balancing_mode, "balanced");
        assert!(persisted.session_affinity_enabled);
        assert_eq!(persisted.queue_max_size, 8);
        assert_eq!(persisted.queue_max_wait_ms, 1500);
        assert_eq!(persisted.rate_limit_cooldown_ms, 4500);
        assert!(!persisted.rate_limit_cooldown_enabled);
        assert_eq!(persisted.suspicious_activity_cooldown_ms, 1_800_000);
        assert!(persisted.suspicious_activity_cooldown_enabled);
        assert!(persisted.suspicious_activity_prefer_clean_credentials);
        assert!(persisted.suspicious_activity_auto_disable_enabled);
        assert_eq!(persisted.suspicious_activity_auto_disable_threshold, 2);
        assert_eq!(
            persisted.suspicious_activity_auto_disable_window_ms,
            3_600_000
        );
        assert!(persisted.suspicious_activity_auto_clear_enabled);
        assert_eq!(
            persisted.suspicious_activity_auto_clear_success_threshold,
            5
        );
        assert_eq!(persisted.suspicious_activity_auto_clear_after_ms, 7_200_000);
        assert!(persisted.model_cooldown_enabled);
        assert_eq!(persisted.default_max_concurrency, Some(3));
        assert_eq!(persisted.rate_limit_bucket_capacity, 4.0);
        assert_eq!(persisted.rate_limit_refill_per_second, 1.2);
        assert_eq!(persisted.rate_limit_refill_min_per_second, 0.3);
        assert_eq!(persisted.rate_limit_refill_recovery_step_per_success, 0.15);
        assert_eq!(persisted.rate_limit_refill_backoff_factor, 0.6);
        assert_eq!(persisted.request_weighting.max_weight, 4.0);
        assert_eq!(persisted.request_weighting.tools_bonus, 1.0);
        assert!(!persisted.stream_dispatch_lease_release_enabled);
        assert_eq!(
            persisted.thinking_signature_validation_mode,
            ThinkingSignatureValidationMode::StripInvalid
        );
        assert_eq!(persisted.non_stream_body_read_timeout.timeout_ms, 510_000);
        assert!(persisted.non_stream_body_read_timeout.retry_on_timeout);
        assert_eq!(
            persisted.kiro_request_body_guard.max_bytes,
            31 * 1024 * 1024
        );
        assert!(persisted.response_thinking_signature_compat_enabled);
        assert_eq!(
            manager.thinking_signature_validation_mode(),
            ThinkingSignatureValidationMode::StripInvalid
        );
        assert!(manager.response_thinking_signature_compat_enabled());
        assert_eq!(manager.get_load_balancing_mode(), "balanced");

        std::fs::remove_file(&config_path).unwrap();
    }

    #[test]
    fn test_report_rate_limited_is_skipped_when_rate_limit_cooldown_disabled() {
        let mut config = Config::default();
        config.rate_limit_bucket_capacity = 4.0;
        config.rate_limit_refill_per_second = 1.5;
        let manager =
            MultiTokenManager::new(config, vec![available_credential(0)], None, None, false)
                .unwrap();

        manager.report_rate_limited(1);

        let snapshot = manager.snapshot();
        let entry = snapshot.entries.iter().find(|entry| entry.id == 1).unwrap();
        assert_eq!(entry.cooldown_remaining_ms, None);
        assert_eq!(entry.rate_limit_hit_streak, 0);
        assert_eq!(entry.rate_limit_refill_per_second, Some(1.5));
    }

    #[test]
    fn test_credential_rate_limit_cooldown_override_can_enable_when_global_disabled() {
        let mut config = Config::default();
        config.rate_limit_cooldown_enabled = false;
        config.rate_limit_cooldown_ms = 4_000;
        config.rate_limit_bucket_capacity = 4.0;
        config.rate_limit_refill_per_second = 2.0;
        config.rate_limit_refill_min_per_second = 1.0;
        config.rate_limit_refill_backoff_factor = 0.5;

        let mut credential = available_credential(0);
        credential.rate_limit_cooldown_enabled = Some(true);

        let manager = MultiTokenManager::new(config, vec![credential], None, None, false).unwrap();
        manager.report_rate_limited(1);

        let snapshot = manager.snapshot();
        let entry = snapshot.entries.iter().find(|entry| entry.id == 1).unwrap();
        assert!(entry.rate_limit_cooldown_enabled);
        assert_eq!(entry.rate_limit_cooldown_enabled_override, Some(true));
        assert_eq!(
            entry.rate_limit_cooldown_enabled_source.as_deref(),
            Some("credential")
        );
        assert!(entry.cooldown_remaining_ms.unwrap_or_default() > 0);
        assert_eq!(entry.rate_limit_hit_streak, 1);
        assert!(entry.rate_limit_refill_per_second.unwrap_or_default() < 2.0);
    }

    #[test]
    fn test_credential_rate_limit_cooldown_override_can_disable_when_global_enabled() {
        let mut config = Config::default();
        config.rate_limit_cooldown_enabled = true;
        config.rate_limit_cooldown_ms = 4_000;
        config.rate_limit_bucket_capacity = 4.0;
        config.rate_limit_refill_per_second = 2.0;

        let mut credential = available_credential(0);
        credential.rate_limit_cooldown_enabled = Some(false);

        let manager = MultiTokenManager::new(config, vec![credential], None, None, false).unwrap();
        manager.report_rate_limited(1);

        let snapshot = manager.snapshot();
        let entry = snapshot.entries.iter().find(|entry| entry.id == 1).unwrap();
        assert!(!entry.rate_limit_cooldown_enabled);
        assert_eq!(entry.rate_limit_cooldown_enabled_override, Some(false));
        assert_eq!(entry.cooldown_remaining_ms, None);
        assert_eq!(entry.rate_limit_hit_streak, 0);
        assert_eq!(entry.rate_limit_refill_per_second, Some(2.0));
    }

    #[test]
    fn test_disabling_rate_limit_cooldown_resets_runtime_state() {
        let mut config = Config::default();
        config.rate_limit_cooldown_enabled = true;
        config.rate_limit_cooldown_ms = 4_000;
        config.rate_limit_bucket_capacity = 4.0;
        config.rate_limit_refill_per_second = 2.0;
        config.rate_limit_refill_min_per_second = 1.0;
        config.rate_limit_refill_recovery_step_per_success = 0.25;
        config.rate_limit_refill_backoff_factor = 0.5;

        let manager =
            MultiTokenManager::new(config, vec![available_credential(0)], None, None, false)
                .unwrap();
        manager.report_rate_limited(1);

        manager
            .set_load_balancing_config(
                None,
                None,
                None,
                None,
                Some(false),
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
                None,
                None,
                None,
                None,
                None,
            )
            .unwrap();

        let snapshot = manager.snapshot();
        let entry = snapshot.entries.iter().find(|entry| entry.id == 1).unwrap();
        assert_eq!(entry.cooldown_remaining_ms, None);
        assert_eq!(entry.rate_limit_hit_streak, 0);
        assert_eq!(
            entry.rate_limit_refill_per_second,
            entry.rate_limit_refill_base_per_second
        );
    }

    #[test]
    fn test_set_rate_limit_cooldown_override_false_clears_existing_penalty() {
        let mut config = Config::default();
        config.rate_limit_cooldown_enabled = true;
        config.rate_limit_cooldown_ms = 4_000;
        config.rate_limit_bucket_capacity = 4.0;
        config.rate_limit_refill_per_second = 2.0;
        config.rate_limit_refill_min_per_second = 1.0;
        config.rate_limit_refill_backoff_factor = 0.5;

        let manager =
            MultiTokenManager::new(config, vec![available_credential(0)], None, None, false)
                .unwrap();
        manager.report_rate_limited(1);
        manager
            .set_rate_limit_config(1, Some(Some(false)), None, None)
            .unwrap();

        let snapshot = manager.snapshot();
        let entry = snapshot.entries.iter().find(|entry| entry.id == 1).unwrap();
        assert!(!entry.rate_limit_cooldown_enabled);
        assert_eq!(entry.rate_limit_cooldown_enabled_override, Some(false));
        assert_eq!(entry.cooldown_remaining_ms, None);
        assert_eq!(entry.rate_limit_hit_streak, 0);
        assert_eq!(
            entry.rate_limit_refill_per_second,
            entry.rate_limit_refill_base_per_second
        );
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

    #[tokio::test]
    async fn test_background_refresh_switches_request_to_fresh_alternate() {
        let mut primary = available_credential(0);
        primary.expires_at = Some((Utc::now() + Duration::minutes(2)).to_rfc3339());
        primary.refresh_token = Some("r".repeat(150));
        let secondary = available_credential(1);

        let manager = std::sync::Arc::new(
            MultiTokenManager::new(
                Config::default(),
                vec![primary, secondary],
                Some(ProxyConfig::new("http://127.0.0.1:9")),
                None,
                false,
            )
            .unwrap(),
        );
        let refresh_lock = manager.refresh_lock_for(1).unwrap();
        let _guard = refresh_lock.lock().await;

        let excluded = HashSet::new();
        let ctx = manager
            .acquire_context_with_background_refresh(None, 1.0, &excluded, None, None)
            .await
            .unwrap();

        assert_eq!(ctx.id, 2);
        let entries = manager.entries.lock();
        let primary_entry = entries.iter().find(|entry| entry.id == 1).unwrap();
        assert!(primary_entry.background_refresh_in_progress);
        assert!(primary_entry.background_refresh_cooldown_until.is_some());
    }

    #[tokio::test]
    async fn test_background_refresh_not_scheduled_without_fresh_alternate() {
        let mut primary = available_credential(0);
        primary.expires_at = Some((Utc::now() + Duration::minutes(2)).to_rfc3339());
        let mut secondary = available_credential(1);
        secondary.expires_at = Some((Utc::now() + Duration::minutes(3)).to_rfc3339());

        let manager = std::sync::Arc::new(
            MultiTokenManager::new(
                Config::default(),
                vec![primary, secondary],
                None,
                None,
                false,
            )
            .unwrap(),
        );
        let excluded = HashSet::new();

        let request_exclusions =
            manager.background_refresh_exclusions_for_request(None, 1.0, &excluded, None);

        assert!(request_exclusions.is_empty());
        let entries = manager.entries.lock();
        assert!(
            entries
                .iter()
                .all(|entry| !entry.background_refresh_in_progress)
        );
        assert!(
            entries
                .iter()
                .all(|entry| entry.background_refresh_cooldown_until.is_none())
        );
    }

    #[tokio::test]
    async fn test_background_refresh_excludes_all_stale_but_schedules_only_one() {
        let mut stale_a = available_credential(0);
        stale_a.expires_at = Some((Utc::now() + Duration::minutes(2)).to_rfc3339());
        stale_a.refresh_token = Some("a".repeat(150));
        let mut stale_b = available_credential(1);
        stale_b.expires_at = Some((Utc::now() + Duration::minutes(2)).to_rfc3339());
        stale_b.refresh_token = Some("b".repeat(150));
        let fresh = available_credential(2);

        let manager = std::sync::Arc::new(
            MultiTokenManager::new(
                Config::default(),
                vec![stale_a, stale_b, fresh],
                Some(ProxyConfig::new("http://127.0.0.1:9")),
                None,
                false,
            )
            .unwrap(),
        );
        let refresh_lock = manager.refresh_lock_for(1).unwrap();
        let _guard = refresh_lock.lock().await;
        let excluded = HashSet::new();

        let request_exclusions =
            manager.background_refresh_exclusions_for_request(None, 1.0, &excluded, None);

        assert_eq!(request_exclusions.len(), 2);
        assert!(request_exclusions.contains(&1));
        assert!(request_exclusions.contains(&2));
        let entries = manager.entries.lock();
        assert_eq!(
            entries
                .iter()
                .filter(|entry| entry.background_refresh_in_progress)
                .count(),
            1
        );
        assert!(
            entries
                .iter()
                .find(|entry| entry.id == 1)
                .unwrap()
                .background_refresh_in_progress
        );
    }

    #[tokio::test]
    async fn test_background_refresh_transient_failure_does_not_increment_disable_counter() {
        let mut stale = available_credential(0);
        stale.expires_at = Some((Utc::now() + Duration::minutes(2)).to_rfc3339());
        stale.refresh_token = Some("r".repeat(150));

        let manager = std::sync::Arc::new(
            MultiTokenManager::new(
                Config::default(),
                vec![stale],
                Some(ProxyConfig::new("http://127.0.0.1:9")),
                None,
                false,
            )
            .unwrap(),
        );

        Arc::clone(&manager).run_background_token_refresh(1).await;

        let snapshot = manager.snapshot();
        let entry = snapshot.entries.iter().find(|entry| entry.id == 1).unwrap();
        assert!(!entry.disabled);
        assert_eq!(entry.refresh_failure_count, 0);
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

    #[test]
    fn test_report_quota_exhausted_persists_disabled_metadata() {
        let credentials_path = temp_credentials_path("quota-disabled-metadata");
        let mut cred = available_credential(0);
        cred.id = Some(1);
        cred.machine_id = Some("machine-1".to_string());
        write_credentials_file(&credentials_path, &[cred.clone()]);

        let manager = MultiTokenManager::new(
            Config::default(),
            vec![cred],
            None,
            Some(credentials_path.clone()),
            true,
        )
        .unwrap();

        let error_summary =
            "status=402 reason=MONTHLY_REQUEST_COUNT message=\"You have reached the limit.\"";
        assert!(!manager.report_quota_exhausted_with_error(1, Some(error_summary)));

        let reloaded = CredentialsConfig::load(&credentials_path)
            .unwrap()
            .into_sorted_credentials();
        let persisted = reloaded.first().unwrap();
        assert!(persisted.disabled);
        assert_eq!(
            persisted.disabled_reason.as_deref(),
            Some(DisabledReason::QuotaExceeded.as_str())
        );
        assert_eq!(persisted.last_error_status, Some(402));
        assert_eq!(persisted.last_error_summary.as_deref(), Some(error_summary));
        assert!(persisted.disabled_at.is_some());
    }

    #[test]
    fn test_report_auth_failure_persists_disabled_metadata() {
        let credentials_path = temp_credentials_path("auth-disabled-metadata");
        let mut cred = available_credential(0);
        cred.id = Some(1);
        cred.machine_id = Some("machine-1".to_string());
        write_credentials_file(&credentials_path, &[cred.clone()]);

        let manager = MultiTokenManager::new(
            Config::default(),
            vec![cred],
            None,
            Some(credentials_path.clone()),
            true,
        )
        .unwrap();

        let error_summary = "status=403 message=\"Your User ID temporarily is suspended.\"";
        assert!(!manager.report_auth_or_permission_failure(
            1,
            DisabledReason::AccountSuspended,
            403,
            error_summary
        ));

        let reloaded = CredentialsConfig::load(&credentials_path)
            .unwrap()
            .into_sorted_credentials();
        let persisted = reloaded.first().unwrap();
        assert!(persisted.disabled);
        assert_eq!(
            persisted.disabled_reason.as_deref(),
            Some(DisabledReason::AccountSuspended.as_str())
        );
        assert_eq!(persisted.last_error_status, Some(403));
        assert_eq!(persisted.last_error_summary.as_deref(), Some(error_summary));
        assert!(persisted.disabled_at.is_some());
    }

    #[test]
    fn test_management_profile_unauthorized_retry_only_for_enterprise() {
        let enterprise = KiroCredentials {
            provider: Some("Enterprise".to_string()),
            start_url: Some("https://example.awsapps.com/start".to_string()),
            profile_arn: Some("arn:aws:codewhisperer:us-east-1:123:profile/old".to_string()),
            ..Default::default()
        };
        let social = KiroCredentials {
            provider: Some("Google".to_string()),
            auth_method: Some("social".to_string()),
            ..Default::default()
        };
        let err = anyhow::anyhow!(
            "权限不足，无法获取使用额度: status=403 body_len=69 message=\"User is not authorized to make this call.\""
        );

        assert!(
            MultiTokenManager::should_retry_management_call_after_profile_rediscovery(
                &enterprise,
                &err
            )
        );
        assert!(
            !MultiTokenManager::should_retry_management_call_after_profile_rediscovery(
                &social, &err
            )
        );
    }

    #[test]
    fn test_management_profile_unauthorized_retry_requires_forbidden_api_error() {
        let enterprise = KiroCredentials {
            provider: Some("Enterprise".to_string()),
            start_url: Some("https://example.awsapps.com/start".to_string()),
            profile_arn: Some("arn:aws:codewhisperer:us-east-1:123:profile/old".to_string()),
            ..Default::default()
        };
        let forbidden = anyhow::Error::new(KiroManagementApiError {
            api: "setUserPreference",
            status_code: 403,
            message: "User is not authorized to make this call.".to_string(),
        });
        let unauthorized = anyhow::Error::new(KiroManagementApiError {
            api: "setUserPreference",
            status_code: 401,
            message: "User is not authorized to make this call.".to_string(),
        });

        assert!(
            MultiTokenManager::should_retry_management_call_after_profile_rediscovery(
                &enterprise,
                &forbidden
            )
        );
        assert!(
            !MultiTokenManager::should_retry_management_call_after_profile_rediscovery(
                &enterprise,
                &unauthorized
            )
        );
    }

    #[test]
    fn test_management_profile_unauthorized_retry_allows_saved_profile_arn() {
        let enterprise = KiroCredentials {
            provider: Some("Enterprise".to_string()),
            start_url: Some("https://example.awsapps.com/start".to_string()),
            profile_arn: Some("arn:aws:codewhisperer:us-east-1:123:profile/stale".to_string()),
            ..Default::default()
        };
        let forbidden = anyhow::Error::new(KiroManagementApiError {
            api: "getUsageLimits",
            status_code: 403,
            message: "User is not authorized to make this call.".to_string(),
        });

        assert!(
            MultiTokenManager::should_retry_management_call_after_profile_rediscovery(
                &enterprise,
                &forbidden
            ),
            "a saved Enterprise profileArn can be stale and must not suppress rediscovery"
        );
    }

    #[test]
    fn test_enterprise_import_model_access_denied_error_detection() {
        assert!(is_enterprise_model_access_denied_error(
            "权限不足，无法获取可用模型列表: status=403 message=\"Your account is not authorized to make this call.\""
        ));
        assert!(is_enterprise_model_access_denied_error(
            "status=403 reason=FEATURE_NOT_SUPPORTED message=\"FEATURE_NOT_SUPPORTED\""
        ));
        assert!(!is_enterprise_model_access_denied_error(
            "请求过于频繁，已被限流: status=429"
        ));
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

        // 凭据.region 参与 api_region 回退链，兼容 KAM/Enterprise 导入
        let api_region = credentials.effective_api_region(&config);
        let api_host = format!("q.{}.amazonaws.com", api_region);

        assert_eq!(api_host, "q.eu-west-1.amazonaws.com");
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
    fn test_profile_discovery_region_ignores_existing_profile_arn_region() {
        let mut config = Config::default();
        config.region = "us-west-2".to_string();

        let credentials = KiroCredentials {
            provider: Some("Enterprise".to_string()),
            api_region: Some("us-east-1".to_string()),
            profile_arn: Some(
                "arn:aws:codewhisperer:eu-west-1:123456789012:profile/stale".to_string(),
            ),
            ..Default::default()
        };

        assert_eq!(
            credentials.effective_api_region(&config),
            "eu-west-1",
            "normal requests should still follow the stored profile ARN region"
        );
        assert_eq!(
            configured_api_region_for_profile_discovery(&credentials, &config),
            "us-east-1",
            "profile discovery must use configured API region, not a stale profile ARN"
        );
    }

    #[test]
    fn test_profile_discovery_region_falls_back_to_credential_region() {
        let mut config = Config::default();
        config.region = "us-west-2".to_string();

        let credentials = KiroCredentials {
            provider: Some("Enterprise".to_string()),
            region: Some("ap-southeast-1".to_string()),
            ..Default::default()
        };

        assert_eq!(
            configured_api_region_for_profile_discovery(&credentials, &config),
            "ap-southeast-1"
        );
    }

    #[test]
    fn test_available_profile_pagination_aggregates_before_selection() {
        let page1: ListAvailableProfilesResponse = serde_json::from_str(
            r#"{
                "profiles": [
                    {
                        "arn": "arn:aws:codewhisperer:eu-west-1:123:profile/OTHER",
                        "profileName": "OtherProfile-eu-west-1"
                    }
                ],
                "nextToken": " page-2 "
            }"#,
        )
        .unwrap();
        let page2: ListAvailableProfilesResponse = serde_json::from_str(
            r#"{
                "profiles": [
                    {
                        "arn": "arn:aws:codewhisperer:us-east-1:123:profile/KIRO",
                        "profileName": "KiroProfile-us-east-1"
                    }
                ]
            }"#,
        )
        .unwrap();

        let mut aggregated = ListAvailableProfilesResponse {
            profiles: Vec::new(),
            next_token: None,
        };
        let next_token = append_available_profiles_page(&mut aggregated, page1);
        assert_eq!(next_token.as_deref(), Some("page-2"));

        let next_token = append_available_profiles_page(&mut aggregated, page2);
        assert_eq!(next_token, None);
        assert_eq!(
            aggregated.selected_profile_arn("us-east-1").as_deref(),
            Some("arn:aws:codewhisperer:us-east-1:123:profile/KIRO")
        );
    }

    #[test]
    fn test_enterprise_management_endpoint_ignores_configured_kiro_management() {
        let mut config = Config::default();
        config.management_endpoint = Some("https://management.us-east-1.kiro.dev/".to_string());
        let credentials = KiroCredentials {
            provider: Some("Enterprise".to_string()),
            api_region: Some("us-east-1".to_string()),
            ..Default::default()
        };

        assert_eq!(
            effective_management_endpoint_base_for_credentials(
                &credentials,
                &config,
                credentials.effective_api_region(&config)
            ),
            "https://q.us-east-1.amazonaws.com"
        );
    }

    #[test]
    fn test_builder_id_management_endpoint_uses_configured_kiro_management() {
        let mut config = Config::default();
        config.management_endpoint = Some("https://management.us-east-1.kiro.dev/".to_string());
        let credentials = KiroCredentials {
            provider: Some("BuilderId".to_string()),
            api_region: Some("us-east-1".to_string()),
            ..Default::default()
        };

        assert_eq!(
            effective_management_endpoint_base_for_credentials(
                &credentials,
                &config,
                credentials.effective_api_region(&config)
            ),
            "https://management.us-east-1.kiro.dev"
        );
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
