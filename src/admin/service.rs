//! Admin API 业务逻辑服务

use std::collections::{BTreeMap, BTreeSet, HashMap, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use std::net::IpAddr;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration as StdDuration;

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::{DateTime, Duration, Utc};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::watch;
use uuid::Uuid;

use crate::common::auth::{
    DEFAULT_CREDENTIAL_GROUP, effective_credential_groups, normalize_credential_groups,
};
use crate::common::logging::summarize_upstream_error;
use crate::http_client::{ProxyConfig, build_client, build_client_no_redirect};
use crate::kiro::model::credentials::KiroCredentials;
use crate::kiro::token_manager::{CredentialEntrySnapshot, MultiTokenManager};
use crate::model::account_type_preset::{
    built_in_account_type_presets, infer_standard_account_type_id_from_subscription,
};
use crate::model::config::{Config, CredentialGroupConfig};
use crate::model::model_catalog::built_in_model_catalog;
use crate::state::{CachedBalanceRecord, RuntimeCoordinationStatus, StateChangeKind};

use super::error::AdminServiceError;
use super::types::{
    AddCredentialRequest, AddCredentialResponse, AdminStateEvent, BalanceResponse,
    CachedBalanceResponse, CredentialGroupConfigItem, CredentialGroupUsageItem,
    CredentialGroupsConfigResponse, CredentialProfilesResponse, CredentialStatusItem,
    CredentialsDeltaRequest, CredentialsDeltaResponse, CredentialsStatusResponse,
    ExternalIdpLoginFlow, ExternalIdpLoginPhase, ExternalIdpLoginStartResponse,
    ExternalIdpLoginStatus, ExternalIdpLoginStatusResponse, ExternalIdpOidcDiscoverySummary,
    ExternalIdpProbeRequest, ExternalIdpProbeResponse, ExternalIdpProbeStatus,
    IdcDeviceLoginStartResponse, IdcDeviceLoginStatus, IdcDeviceLoginStatusResponse,
    LoadBalancingModeResponse, ModelCapabilitiesConfigResponse, ModelCatalogItemResponse,
    ModelCatalogResponse, ProxyPoolConfigResponse, ProxyPoolEntryResponse,
    SetCredentialGroupsConfigRequest, SetCredentialGroupsRequest, SetCredentialModelPolicyRequest,
    SetCredentialProfileRequest, SetCredentialProxyRequest, SetCredentialSourceRequest,
    SetLoadBalancingModeRequest, SetModelCapabilitiesConfigRequest,
    StandardAccountTypePresetResponse, StartExternalIdpLoginRequest, StartIdcDeviceLoginRequest,
    SubmitExternalIdpCallbackRequest,
};

/// 余额缓存过期时间（秒），5 分钟
const BALANCE_CACHE_TTL_SECS: i64 = 300;
const IDC_DEVICE_GRANT_TYPE: &str = "urn:ietf:params:oauth:grant-type:device_code";
const IDC_REFRESH_GRANT_TYPE: &str = "refresh_token";
const BUILDER_ID_START_URL: &str = "https://view.awsapps.com/start";
const IDC_LOGIN_SESSION_RETENTION_SECS: i64 = 15 * 60;
const IDC_DEVICE_LOGIN_CLIENT_NAME: &str = "Kiro IDE";
const EXTERNAL_IDP_LOGIN_SESSION_SECS: i64 = 10 * 60;
const EXTERNAL_IDP_LOGIN_POLL_INTERVAL_SECS: u64 = 3;
const KIRO_AUTH_PORTAL_URL: &str = "https://app.kiro.dev";
const KIRO_IDE_EXTERNAL_IDP_REDIRECT_URI: &str = "kiro://kiro.oauth/callback";
const KIRO_WEB_PORTAL_ENDPOINT: &str =
    "https://app.kiro.dev/service/KiroWebPortalService/operation/GetLoginMetadata";
const UPSTREAM_ERROR_EXCERPT_CHARS: usize = 240;
const ADMIN_EVENTS_SAMPLE_INTERVAL: StdDuration = StdDuration::from_secs(2);
const JS_SAFE_U64_MASK: u64 = (1_u64 << 53) - 1;
const IDC_GRANT_SCOPES: &[&str] = &[
    "codewhisperer:completions",
    "codewhisperer:analysis",
    "codewhisperer:conversations",
    "codewhisperer:transformations",
    "codewhisperer:taskassist",
];

/// Admin 服务
///
/// 封装所有 Admin API 的业务逻辑
pub struct AdminService {
    token_manager: Arc<MultiTokenManager>,
    balance_cache: Mutex<HashMap<u64, CachedBalanceRecord>>,
    last_balance_cache_revision: Mutex<u64>,
    events_tx: watch::Sender<AdminStateEvent>,
    events_watcher_started: AtomicBool,
    idc_device_login_sessions: Mutex<HashMap<String, IdcDeviceLoginSession>>,
    external_idp_login_sessions: Mutex<HashMap<String, ExternalIdpLoginSession>>,
}

#[derive(Debug, Clone)]
struct IdcDeviceLoginSession {
    session_id: String,
    status: IdcDeviceLoginStatus,
    provider: String,
    start_url: String,
    region: String,
    client_id: String,
    client_secret: String,
    device_code: String,
    user_code: String,
    verification_uri: String,
    verification_uri_complete: Option<String>,
    interval_seconds: u64,
    next_poll_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    request: StartIdcDeviceLoginRequest,
    message: Option<String>,
    credential_result: Option<AddCredentialResponse>,
    polling: bool,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct ExternalIdpLoginSession {
    session_id: String,
    status: ExternalIdpLoginStatus,
    phase: ExternalIdpLoginPhase,
    flow: ExternalIdpLoginFlow,
    provider: String,
    auth_url: Option<String>,
    callback_url: Option<String>,
    idp_redirect_uri: Option<String>,
    expires_at: DateTime<Utc>,
    request: StartExternalIdpLoginRequest,
    portal_state: Option<String>,
    portal_code_verifier: Option<String>,
    idp_state: Option<String>,
    idp_code_verifier: Option<String>,
    issuer_url: Option<String>,
    client_id: Option<String>,
    scopes: Option<String>,
    audience: Option<String>,
    login_hint: Option<String>,
    token_endpoint: Option<String>,
    device_authorization_endpoint: Option<String>,
    device_code: Option<String>,
    user_code: Option<String>,
    verification_uri: Option<String>,
    verification_uri_complete: Option<String>,
    interval_seconds: u64,
    next_poll_at: DateTime<Utc>,
    polling: bool,
    idp_callback_consumed: bool,
    message: Option<String>,
    credential_result: Option<AddCredentialResponse>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AwsClientRegistrationResponse {
    client_id: String,
    client_secret: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AwsStartDeviceAuthorizationResponse {
    device_code: String,
    user_code: String,
    verification_uri: String,
    #[serde(default)]
    verification_uri_complete: Option<String>,
    expires_in: i64,
    interval: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AwsCreateTokenResponse {
    refresh_token: String,
}

#[derive(Debug, Deserialize)]
struct AwsOidcErrorResponse {
    error: Option<String>,
    error_description: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct KiroLoginMetadataPayload {
    #[serde(default)]
    found: bool,
    #[serde(default)]
    issuer_url: Option<String>,
    #[serde(default)]
    client_id: Option<String>,
    #[serde(default)]
    scopes: Option<Vec<String>>,
    #[serde(default)]
    audience: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct KiroGetLoginMetadataRequest {
    domain_name: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
struct OidcDiscoveryDocument {
    issuer: Option<String>,
    authorization_endpoint: Option<String>,
    token_endpoint: Option<String>,
    device_authorization_endpoint: Option<String>,
    code_challenge_methods_supported: Option<Vec<String>>,
    grant_types_supported: Option<Vec<String>>,
    response_types_supported: Option<Vec<String>>,
    scopes_supported: Option<Vec<String>>,
    token_endpoint_auth_methods_supported: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct ExternalIdpAuthorizationCodeTokenResponse {
    refresh_token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ExternalIdpDeviceAuthorizationResponse {
    device_code: String,
    user_code: String,
    verification_uri: String,
    #[serde(default)]
    verification_uri_complete: Option<String>,
    expires_in: i64,
    interval: Option<u64>,
    #[serde(default)]
    message: Option<String>,
}

#[derive(Debug)]
enum DeviceTokenPollResult {
    Pending,
    SlowDown,
    Expired(String),
    Failed(String),
    Completed(AwsCreateTokenResponse),
}

#[derive(Debug)]
enum ExternalIdpDeviceTokenPollResult {
    Pending,
    SlowDown,
    Expired(String),
    Failed(String),
    Completed(ExternalIdpAuthorizationCodeTokenResponse),
}

fn normalize_optional_string(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn normalize_idc_device_provider(provider: &str) -> Result<String, AdminServiceError> {
    let provider = provider.trim();
    if provider.eq_ignore_ascii_case("builderid")
        || provider.eq_ignore_ascii_case("builder-id")
        || provider.eq_ignore_ascii_case("builder id")
    {
        return Ok("BuilderId".to_string());
    }
    if provider.eq_ignore_ascii_case("enterprise") {
        return Ok("Enterprise".to_string());
    }
    Err(AdminServiceError::InvalidCredential(
        "在线登录当前仅支持 BuilderId 或 Enterprise".to_string(),
    ))
}

fn resolve_idc_device_start_url(
    provider: &str,
    start_url: Option<&str>,
) -> Result<String, AdminServiceError> {
    if provider.eq_ignore_ascii_case("BuilderId") {
        return Ok(start_url.unwrap_or(BUILDER_ID_START_URL).trim().to_string());
    }

    let Some(start_url) = start_url.map(str::trim).filter(|value| !value.is_empty()) else {
        return Err(AdminServiceError::InvalidCredential(
            "Enterprise 在线登录必须提供 IAM Identity Center Start URL".to_string(),
        ));
    };
    let parsed = url::Url::parse(start_url)
        .map_err(|err| AdminServiceError::InvalidCredential(format!("Start URL 无效: {err}")))?;
    if parsed.scheme() != "https" {
        return Err(AdminServiceError::InvalidCredential(
            "Start URL 必须使用 https".to_string(),
        ));
    }
    Ok(start_url.to_string())
}

fn normalize_domain_name(domain: &str) -> Result<String, AdminServiceError> {
    let domain = domain.trim().trim_start_matches('@').to_ascii_lowercase();
    if domain.is_empty()
        || domain.contains(char::is_whitespace)
        || domain.contains('/')
        || domain.contains(':')
        || domain.starts_with('.')
        || domain.ends_with('.')
        || !domain.contains('.')
    {
        return Err(AdminServiceError::InvalidCredential(
            "External IdP 探测需要有效的工作邮箱或域名".to_string(),
        ));
    }
    Ok(domain)
}

fn domain_from_work_email(email: &str) -> Result<String, AdminServiceError> {
    let email = email.trim();
    let Some((local, domain)) = email.rsplit_once('@') else {
        return Err(AdminServiceError::InvalidCredential(
            "工作邮箱格式无效".to_string(),
        ));
    };
    if local.trim().is_empty() {
        return Err(AdminServiceError::InvalidCredential(
            "工作邮箱格式无效".to_string(),
        ));
    }
    normalize_domain_name(domain)
}

fn resolve_external_idp_probe_domain(
    req: &ExternalIdpProbeRequest,
) -> Result<String, AdminServiceError> {
    if let Some(email) = req
        .work_email
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return domain_from_work_email(email);
    }
    if let Some(domain) = req
        .domain_name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return normalize_domain_name(domain);
    }
    if req
        .issuer_url
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_some()
    {
        return Ok("direct-issuer".to_string());
    }
    Err(AdminServiceError::InvalidCredential(
        "External IdP 探测需要 workEmail、domainName 或 issuerUrl".to_string(),
    ))
}

fn normalize_scope_list_from_str(value: &str) -> Vec<String> {
    let mut result = Vec::new();
    for scope in value.split_whitespace().map(str::trim) {
        if !scope.is_empty() && !result.iter().any(|existing| existing == scope) {
            result.push(scope.to_string());
        }
    }
    result
}

fn normalize_scope_list(values: Option<Vec<String>>) -> Vec<String> {
    let mut result = Vec::new();
    for value in values.unwrap_or_default() {
        for scope in normalize_scope_list_from_str(&value) {
            if !result.iter().any(|existing| existing == &scope) {
                result.push(scope);
            }
        }
    }
    result
}

fn normalize_optional_url(
    value: Option<&str>,
    label: &str,
) -> Result<Option<String>, AdminServiceError> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    let mut parsed = parse_external_idp_url(value, label)?;
    parsed.set_query(None);
    parsed.set_fragment(None);
    let mut normalized = parsed.to_string();
    while normalized.ends_with('/') {
        normalized.pop();
    }
    Ok(Some(normalized))
}

fn parse_external_idp_url(value: &str, label: &str) -> Result<url::Url, AdminServiceError> {
    let parsed = url::Url::parse(value)
        .map_err(|err| AdminServiceError::InvalidCredential(format!("{label} 无效: {err}")))?;
    if parsed.scheme() != "https" {
        return Err(AdminServiceError::InvalidCredential(format!(
            "{label} 必须使用 https"
        )));
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(AdminServiceError::InvalidCredential(format!(
            "{label} 不能包含用户名或密码"
        )));
    }
    if parsed.fragment().is_some() {
        return Err(AdminServiceError::InvalidCredential(format!(
            "{label} 不能包含 fragment"
        )));
    }
    let host = parsed
        .host_str()
        .ok_or_else(|| AdminServiceError::InvalidCredential(format!("{label} 必须包含主机名")))?;
    let host = host.trim().to_ascii_lowercase();
    if host == "localhost" || host.ends_with(".localhost") {
        return Err(AdminServiceError::InvalidCredential(format!(
            "{label} 不能指向 localhost"
        )));
    }
    if host.trim_matches(['[', ']']).parse::<IpAddr>().is_ok() {
        return Err(AdminServiceError::InvalidCredential(format!(
            "{label} 不能使用 IP literal 主机"
        )));
    }
    Ok(parsed)
}

fn normalize_external_idp_endpoint_url(
    value: Option<&str>,
    label: &str,
) -> Result<Option<String>, AdminServiceError> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    Ok(Some(parse_external_idp_url(value, label)?.to_string()))
}

fn oidc_discovery_url(issuer_url: &str) -> Result<String, AdminServiceError> {
    let issuer_url = normalize_optional_url(Some(issuer_url), "Issuer URL")?
        .ok_or_else(|| AdminServiceError::InvalidCredential("Issuer URL 不能为空".to_string()))?;
    let mut parsed = url::Url::parse(&issuer_url)
        .map_err(|err| AdminServiceError::InvalidCredential(format!("Issuer URL 无效: {err}")))?;
    let mut path = parsed.path().trim_end_matches('/').to_string();
    path.push_str("/.well-known/openid-configuration");
    parsed.set_path(&path);
    parsed.set_query(None);
    parsed.set_fragment(None);
    Ok(parsed.to_string())
}

#[cfg(test)]
fn parse_kiro_login_metadata_payload(
    body: &str,
) -> Result<KiroLoginMetadataPayload, AdminServiceError> {
    let value: serde_json::Value = serde_json::from_str(body).map_err(|err| {
        AdminServiceError::UpstreamError(format!("解析 GetLoginMetadata 响应失败: {err}"))
    })?;
    parse_kiro_login_metadata_payload_value(value)
}

fn parse_kiro_login_metadata_payload_value(
    value: serde_json::Value,
) -> Result<KiroLoginMetadataPayload, AdminServiceError> {
    let payload = value.get("Output").unwrap_or(&value).clone();
    if let Some(error_type) = payload
        .get("__type")
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
    {
        return Err(AdminServiceError::UpstreamError(format!(
            "GetLoginMetadata 返回错误: {error_type}"
        )));
    }
    serde_json::from_value(payload).map_err(|err| {
        AdminServiceError::UpstreamError(format!("解析 GetLoginMetadata 响应失败: {err}"))
    })
}

fn parse_kiro_login_metadata_payload_cbor(
    body: &[u8],
) -> Result<KiroLoginMetadataPayload, AdminServiceError> {
    let value: serde_json::Value = serde_cbor::from_slice(body).map_err(|err| {
        AdminServiceError::UpstreamError(format!("解析 GetLoginMetadata CBOR 响应失败: {err}"))
    })?;
    parse_kiro_login_metadata_payload_value(value)
}

fn kiro_login_metadata_error_message(body: &[u8]) -> String {
    if let Ok(value) = serde_cbor::from_slice::<serde_json::Value>(body) {
        if let Some(message) = value
            .get("message")
            .or_else(|| value.get("Message"))
            .and_then(|value| value.as_str())
            .filter(|value| !value.is_empty())
        {
            return message.to_string();
        }
        if let Ok(text) = serde_json::to_string(&value) {
            return text;
        }
    }
    String::from_utf8_lossy(body).trim().to_string()
}

fn string_list_contains(values: &[String], expected: &str) -> bool {
    values
        .iter()
        .any(|value| value.trim().eq_ignore_ascii_case(expected))
}

fn external_idp_public_token_auth_supported(summary: &ExternalIdpOidcDiscoverySummary) -> bool {
    string_list_contains(&summary.token_endpoint_auth_methods_supported, "none")
}

fn external_idp_device_code_secret_requirement_message(
    summary: &ExternalIdpOidcDiscoverySummary,
) -> Option<String> {
    if external_idp_public_token_auth_supported(summary) {
        return None;
    }

    let methods = if summary.token_endpoint_auth_methods_supported.is_empty() {
        "未声明".to_string()
    } else {
        summary.token_endpoint_auth_methods_supported.join(", ")
    };
    Some(format!(
        "OIDC discovery 的 token_endpoint_auth_methods_supported 为 {methods}，未包含 none；该 Azure/OIDC client 很可能需要 client_secret 或 client_assertion。当前 kiro.rs 没有 Kiro/Azure 应用密钥，无法用 External IdP device-code 完成 token exchange。"
    ))
}

fn external_idp_secret_required_error_message(description: &str) -> Option<String> {
    let lower = description.to_ascii_lowercase();
    if lower.contains("aadsts7000218")
        || (lower.contains("client_assertion") && lower.contains("client_secret"))
    {
        return Some(
            "ExternalIdP token exchange 收到 Azure 机密客户端要求：需要 client_secret 或 client_assertion。当前 kiro.rs 没有 Kiro/Azure 应用密钥，无法完成这条 token exchange。".to_string(),
        );
    }
    None
}

fn aws_oidc_error_message(body: &str) -> String {
    if let Ok(parsed) = serde_json::from_str::<AwsOidcErrorResponse>(body) {
        if let Some(description) = parsed.error_description.filter(|value| !value.is_empty()) {
            return description;
        }
        if let Some(error) = parsed.error.filter(|value| !value.is_empty()) {
            return error;
        }
    }
    if body.trim().is_empty() {
        "空响应".to_string()
    } else {
        body.trim().chars().take(500).collect()
    }
}

fn idc_device_login_status_response(
    session: &IdcDeviceLoginSession,
) -> IdcDeviceLoginStatusResponse {
    let credential = session.credential_result.as_ref();
    IdcDeviceLoginStatusResponse {
        session_id: session.session_id.clone(),
        status: session.status,
        provider: session.provider.clone(),
        start_url: session.start_url.clone(),
        region: session.region.clone(),
        user_code: (session.status == IdcDeviceLoginStatus::Pending)
            .then(|| session.user_code.clone()),
        verification_uri: (session.status == IdcDeviceLoginStatus::Pending)
            .then(|| session.verification_uri.clone()),
        verification_uri_complete: (session.status == IdcDeviceLoginStatus::Pending)
            .then(|| session.verification_uri_complete.clone())
            .flatten(),
        expires_at: (session.status == IdcDeviceLoginStatus::Pending).then_some(session.expires_at),
        interval_seconds: session.interval_seconds,
        message: session.message.clone(),
        credential_id: credential.map(|value| value.credential_id),
        email: credential.and_then(|value| value.email.clone()),
        user_id: credential.and_then(|value| value.user_id.clone()),
        subscription_title: credential.and_then(|value| value.subscription_title.clone()),
        subscription_type: credential.and_then(|value| value.subscription_type.clone()),
        auth_account_type: credential.and_then(|value| value.auth_account_type.clone()),
        resolved_account_type: credential.and_then(|value| value.resolved_account_type.clone()),
    }
}

fn external_idp_login_status_response(
    session: &ExternalIdpLoginSession,
) -> ExternalIdpLoginStatusResponse {
    let credential = session.credential_result.as_ref();
    ExternalIdpLoginStatusResponse {
        session_id: session.session_id.clone(),
        status: session.status,
        phase: session.phase,
        flow: session.flow,
        provider: session.provider.clone(),
        auth_url: (session.status == ExternalIdpLoginStatus::Pending)
            .then(|| session.auth_url.clone())
            .flatten(),
        callback_url: external_idp_display_callback_url(session),
        expires_at: (session.status == ExternalIdpLoginStatus::Pending)
            .then_some(session.expires_at),
        interval_seconds: session.interval_seconds,
        issuer_url: session.issuer_url.clone(),
        client_id: session.client_id.clone(),
        scopes: session.scopes.clone(),
        audience: session.audience.clone(),
        user_code: (session.status == ExternalIdpLoginStatus::Pending)
            .then(|| session.user_code.clone())
            .flatten(),
        verification_uri: (session.status == ExternalIdpLoginStatus::Pending)
            .then(|| session.verification_uri.clone())
            .flatten(),
        verification_uri_complete: (session.status == ExternalIdpLoginStatus::Pending)
            .then(|| session.verification_uri_complete.clone())
            .flatten(),
        message: session.message.clone(),
        credential_id: credential.map(|value| value.credential_id),
        email: credential.and_then(|value| value.email.clone()),
        user_id: credential.and_then(|value| value.user_id.clone()),
        subscription_title: credential.and_then(|value| value.subscription_title.clone()),
        subscription_type: credential.and_then(|value| value.subscription_type.clone()),
        auth_account_type: credential.and_then(|value| value.auth_account_type.clone()),
        resolved_account_type: credential.and_then(|value| value.resolved_account_type.clone()),
    }
}

fn external_idp_display_callback_url(session: &ExternalIdpLoginSession) -> String {
    session
        .idp_redirect_uri
        .clone()
        .or_else(|| session.callback_url.clone())
        .unwrap_or_default()
}

fn parse_external_idp_callback_params(value: &str) -> HashMap<String, String> {
    let value = value.trim();
    if value.is_empty() {
        return HashMap::new();
    }

    if let Ok(parsed) = url::Url::parse(value) {
        let mut params: HashMap<String, String> = parsed.query_pairs().into_owned().collect();
        if let Some(fragment) = parsed.fragment().filter(|fragment| !fragment.is_empty()) {
            for (key, value) in url::form_urlencoded::parse(fragment.as_bytes()) {
                params.entry(key.into_owned()).or_insert(value.into_owned());
            }
        }
        return params;
    }

    let query = value
        .strip_prefix('?')
        .or_else(|| value.split_once('?').map(|(_, query)| query))
        .unwrap_or(value);
    url::form_urlencoded::parse(query.as_bytes())
        .into_owned()
        .collect()
}

fn external_idp_submit_callback_params(
    req: &SubmitExternalIdpCallbackRequest,
) -> Result<HashMap<String, String>, AdminServiceError> {
    let mut params = req
        .callback_url
        .as_deref()
        .map(parse_external_idp_callback_params)
        .unwrap_or_default();

    if let Some(code) = normalize_optional_string(req.code.as_deref()) {
        params.insert("code".to_string(), code);
    }
    if let Some(state) = normalize_optional_string(req.state.as_deref()) {
        params.insert("state".to_string(), state);
    }

    let has_code = params
        .get("code")
        .map(String::as_str)
        .map(str::trim)
        .is_some_and(|value| !value.is_empty());
    let has_error = params
        .get("error")
        .map(String::as_str)
        .map(str::trim)
        .is_some_and(|value| !value.is_empty());
    if !has_code && !has_error {
        return Err(AdminServiceError::InvalidCredential(
            "需要粘贴 External IdP 回调 URL 或授权码".to_string(),
        ));
    }

    Ok(params)
}

fn external_idp_portal_descriptor_error(params: &HashMap<String, String>) -> Option<String> {
    let login_option = params
        .get("login_option")
        .map(String::as_str)
        .map(str::trim)
        .unwrap_or_default();
    let has_issuer_url = params
        .get("issuer_url")
        .map(String::as_str)
        .map(str::trim)
        .is_some_and(|value| !value.is_empty());

    if !login_option.is_empty() && !login_option.eq_ignore_ascii_case("external_idp") {
        Some(format!(
            "当前流程只支持 external_idp，收到 login_option={login_option}"
        ))
    } else if login_option.is_empty() && !has_issuer_url {
        Some("Kiro portal 回调缺少 login_option 或 issuer_url".to_string())
    } else {
        None
    }
}

fn random_urlsafe_bytes(len: usize) -> Result<String, AdminServiceError> {
    let mut bytes = vec![0_u8; len];
    getrandom::getrandom(&mut bytes).map_err(|err| {
        AdminServiceError::InternalError(format!("生成 OAuth 随机参数失败: {err}"))
    })?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

fn random_oauth_state() -> Result<String, AdminServiceError> {
    random_urlsafe_bytes(32)
}

fn random_pkce_code_verifier() -> Result<String, AdminServiceError> {
    random_urlsafe_bytes(32)
}

fn pkce_s256_challenge(code_verifier: &str) -> String {
    let digest = Sha256::digest(code_verifier.as_bytes());
    URL_SAFE_NO_PAD.encode(digest)
}

fn normalize_scope_string_with_offline_access(value: &str) -> Result<String, AdminServiceError> {
    let mut scopes = normalize_scope_list_from_str(value);
    if scopes.is_empty() {
        return Err(AdminServiceError::InvalidCredential(
            "ExternalIdP 登录必须提供 scopes".to_string(),
        ));
    }
    if !scopes
        .iter()
        .any(|scope| scope.eq_ignore_ascii_case("offline_access"))
    {
        scopes.push("offline_access".to_string());
    }
    Ok(scopes.join(" "))
}

fn resolve_external_idp_callback_url(
    callback_base_url: Option<&str>,
) -> Result<String, AdminServiceError> {
    let Some(callback_base_url) = callback_base_url
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Err(AdminServiceError::InvalidCredential(
            "ExternalIdP 登录需要 callbackBaseUrl".to_string(),
        ));
    };

    let parsed = url::Url::parse(callback_base_url).map_err(|err| {
        AdminServiceError::InvalidCredential(format!("callbackBaseUrl 无效: {err}"))
    })?;
    if parsed.scheme() != "http" && parsed.scheme() != "https" {
        return Err(AdminServiceError::InvalidCredential(
            "callbackBaseUrl 必须使用 http 或 https".to_string(),
        ));
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(AdminServiceError::InvalidCredential(
            "callbackBaseUrl 不能包含用户名或密码".to_string(),
        ));
    }

    let origin = parsed.origin().ascii_serialization();
    Ok(format!(
        "{}/api/admin/auth/external-idp/callback",
        origin.trim_end_matches('/')
    ))
}

fn build_kiro_portal_auth_url(
    state: &str,
    code_challenge: &str,
    redirect_uri: &str,
) -> Result<String, AdminServiceError> {
    let mut url = url::Url::parse(KIRO_AUTH_PORTAL_URL)
        .map_err(|err| AdminServiceError::InternalError(err.to_string()))?;
    url.set_path("signin");
    url.query_pairs_mut()
        .append_pair("state", state)
        .append_pair("code_challenge", code_challenge)
        .append_pair("code_challenge_method", "S256")
        .append_pair("redirect_uri", redirect_uri)
        .append_pair("redirect_from", "KiroIDE");
    Ok(url.to_string())
}

fn build_external_idp_authorization_url(
    authorization_endpoint: &str,
    client_id: &str,
    redirect_uri: &str,
    scopes: &str,
    state: &str,
    code_challenge: &str,
    login_hint: Option<&str>,
    audience: Option<&str>,
) -> Result<String, AdminServiceError> {
    let mut url = url::Url::parse(authorization_endpoint).map_err(|err| {
        AdminServiceError::InvalidCredential(format!("authorization_endpoint 无效: {err}"))
    })?;
    {
        let mut pairs = url.query_pairs_mut();
        pairs
            .append_pair("client_id", client_id)
            .append_pair("redirect_uri", redirect_uri)
            .append_pair("response_type", "code")
            .append_pair("scope", scopes)
            .append_pair("code_challenge", code_challenge)
            .append_pair("code_challenge_method", "S256")
            .append_pair("response_mode", "query")
            .append_pair("state", state);
        if let Some(login_hint) = login_hint.map(str::trim).filter(|value| !value.is_empty()) {
            pairs.append_pair("login_hint", login_hint);
        }
        if let Some(audience) = audience.map(str::trim).filter(|value| !value.is_empty()) {
            pairs.append_pair("audience", audience);
        }
    }
    Ok(url.to_string())
}

fn build_external_idp_authorization_code_token_form(
    client_id: &str,
    code: &str,
    redirect_uri: &str,
    code_verifier: &str,
    scopes: Option<&str>,
) -> Vec<(&'static str, String)> {
    let mut form = vec![
        ("grant_type", "authorization_code".to_string()),
        ("code", code.to_string()),
        ("redirect_uri", redirect_uri.to_string()),
        ("client_id", client_id.to_string()),
        ("code_verifier", code_verifier.to_string()),
    ];
    if let Some(scopes) = scopes.map(str::trim).filter(|value| !value.is_empty()) {
        form.push(("scope", scopes.to_string()));
    }
    form
}

#[derive(Debug)]
pub enum ExternalIdpCallbackAction {
    Redirect(String),
    Html {
        success: bool,
        title: String,
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminWriteRoute {
    Local,
    Forward(String),
}

impl AdminService {
    pub fn new(token_manager: Arc<MultiTokenManager>) -> Self {
        let state_store = token_manager.state_store();
        let balance_cache = match Self::load_pruned_balance_cache(&state_store) {
            Ok(cache) => cache,
            Err(e) => {
                tracing::warn!("解析余额缓存失败，将忽略: {}", e);
                HashMap::new()
            }
        };
        let balance_cache_revision = match state_store.state_change_revisions() {
            Ok(revisions) => revisions.balance_cache,
            Err(err) => {
                tracing::warn!("读取余额缓存修订号失败，将从 0 开始追踪: {}", err);
                0
            }
        };
        let initial_event =
            Self::build_state_event_for_manager(&token_manager, 0).unwrap_or_else(|err| {
                tracing::warn!("初始化 Admin 实时状态失败，将使用空快照: {}", err);
                AdminStateEvent {
                    sequence: 0,
                    credentials_revision: 0,
                    dispatch_revision: 0,
                    balance_cache_revision,
                    credentials_fingerprint: 0,
                    dispatch_fingerprint: 0,
                    total: 0,
                    available: 0,
                    dispatchable: 0,
                    in_flight: 0,
                    waiting_requests: 0,
                    rate_limited: 0,
                    abnormal: 0,
                    current_id: 0,
                    generated_at: Utc::now(),
                }
            });
        let (events_tx, _) = watch::channel(initial_event);

        Self {
            token_manager,
            balance_cache: Mutex::new(balance_cache),
            last_balance_cache_revision: Mutex::new(balance_cache_revision),
            events_tx,
            events_watcher_started: AtomicBool::new(false),
            idc_device_login_sessions: Mutex::new(HashMap::new()),
            external_idp_login_sessions: Mutex::new(HashMap::new()),
        }
    }

    pub fn subscribe_state_events(self: &Arc<Self>) -> watch::Receiver<AdminStateEvent> {
        self.ensure_events_watcher_started();
        self.events_tx.subscribe()
    }

    fn ensure_events_watcher_started(self: &Arc<Self>) {
        if self
            .events_watcher_started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }

        let service = Arc::clone(self);
        tokio::spawn(async move {
            service.run_events_watcher().await;
        });
    }

    async fn run_events_watcher(self: Arc<Self>) {
        let mut sequence = self.events_tx.borrow().sequence;
        let mut interval = tokio::time::interval(ADMIN_EVENTS_SAMPLE_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;

            if self.events_tx.receiver_count() == 0 {
                self.events_watcher_started.store(false, Ordering::SeqCst);
                if self.events_tx.receiver_count() == 0 {
                    break;
                }
                if self
                    .events_watcher_started
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                    .is_err()
                {
                    break;
                }
            }

            match self.build_state_event(sequence) {
                Ok(mut event) => {
                    let current = self.events_tx.borrow().clone();
                    if Self::state_event_observed_fields_changed(&current, &event) {
                        sequence = sequence.saturating_add(1);
                        event.sequence = sequence;
                        if self.events_tx.send(event).is_err() {
                            tracing::debug!("Admin 实时状态广播暂无订阅者");
                        }
                    }
                }
                Err(err) => {
                    tracing::warn!("生成 Admin 实时状态失败: {}", err);
                }
            }
        }
    }

    fn state_event_observed_fields_changed(
        current: &AdminStateEvent,
        next: &AdminStateEvent,
    ) -> bool {
        current.credentials_revision != next.credentials_revision
            || current.dispatch_revision != next.dispatch_revision
            || current.balance_cache_revision != next.balance_cache_revision
            || current.credentials_fingerprint != next.credentials_fingerprint
            || current.dispatch_fingerprint != next.dispatch_fingerprint
            || current.total != next.total
            || current.available != next.available
            || current.dispatchable != next.dispatchable
            || current.in_flight != next.in_flight
            || current.waiting_requests != next.waiting_requests
            || current.rate_limited != next.rate_limited
            || current.abnormal != next.abnormal
            || current.current_id != next.current_id
    }

    fn build_state_event(&self, sequence: u64) -> Result<AdminStateEvent, AdminServiceError> {
        self.sync_runtime_state_for_read()?;
        Self::build_state_event_for_manager(&self.token_manager, sequence)
    }

    fn build_state_event_for_manager(
        token_manager: &MultiTokenManager,
        sequence: u64,
    ) -> Result<AdminStateEvent, AdminServiceError> {
        let revisions = token_manager
            .state_store()
            .state_change_revisions()
            .map_err(|err| AdminServiceError::InternalError(err.to_string()))?;
        let snapshot = token_manager.snapshot();
        let dispatch = token_manager.load_balancing_config_snapshot();
        let credentials_fingerprint = Self::credentials_event_fingerprint(&snapshot);
        let dispatch_fingerprint = Self::dispatch_event_fingerprint(&dispatch);
        let in_flight = snapshot
            .entries
            .iter()
            .map(|entry| entry.active_requests)
            .sum();
        let rate_limited = snapshot
            .entries
            .iter()
            .filter(|entry| {
                entry.cooldown_remaining_ms.unwrap_or_default() > 0
                    || entry.next_ready_in_ms.unwrap_or_default() > 0
                    || entry.rate_limit_hit_streak > 0
                    || entry
                        .rate_limit_bucket_tokens
                        .zip(entry.rate_limit_bucket_capacity)
                        .is_some_and(|(tokens, capacity)| capacity > 0.0 && tokens < 1.0)
            })
            .count();
        let abnormal = snapshot
            .entries
            .iter()
            .filter(|entry| {
                entry.disabled
                    || entry.failure_count > 0
                    || entry.refresh_failure_count > 0
                    || entry.last_error_status.is_some()
                    || entry.suspicious_activity_count > 0
            })
            .count();

        Ok(AdminStateEvent {
            sequence,
            credentials_revision: revisions.credentials,
            dispatch_revision: revisions.dispatch_config,
            balance_cache_revision: revisions.balance_cache,
            credentials_fingerprint,
            dispatch_fingerprint,
            total: snapshot.total,
            available: snapshot.available,
            dispatchable: snapshot.dispatchable,
            in_flight,
            waiting_requests: dispatch.waiting_requests,
            rate_limited,
            abnormal,
            current_id: snapshot.current_id,
            generated_at: Utc::now(),
        })
    }

    fn credentials_event_fingerprint(
        snapshot: &crate::kiro::token_manager::ManagerSnapshot,
    ) -> u64 {
        let mut hasher = DefaultHasher::new();
        snapshot.total.hash(&mut hasher);
        snapshot.available.hash(&mut hasher);

        for entry in &snapshot.entries {
            entry.id.hash(&mut hasher);
            entry.priority.hash(&mut hasher);
            entry.disabled.hash(&mut hasher);
            entry.failure_count.hash(&mut hasher);
            entry.refresh_failure_count.hash(&mut hasher);
            entry.max_concurrency.hash(&mut hasher);
            entry.max_concurrency_override.hash(&mut hasher);
            entry.max_concurrency_source.hash(&mut hasher);
            entry.profile_arn.hash(&mut hasher);
            entry.subscription_title.hash(&mut hasher);
            entry.subscription_type.hash(&mut hasher);
            entry.auth_account_type.hash(&mut hasher);
            entry.account_type.hash(&mut hasher);
            entry.resolved_account_type.hash(&mut hasher);
            entry.source_supplier_id.hash(&mut hasher);
            entry.source_supplier_name.hash(&mut hasher);
            entry.source_batch.hash(&mut hasher);
            entry.credential_groups.hash(&mut hasher);
            entry.allowed_models.hash(&mut hasher);
            entry.blocked_models.hash(&mut hasher);
            entry.available_model_ids.hash(&mut hasher);
            entry.available_models_cached_at.hash(&mut hasher);
            entry.has_proxy.hash(&mut hasher);
            entry.proxy_id.hash(&mut hasher);
            entry.disabled_reason.hash(&mut hasher);
            entry.disabled_at.hash(&mut hasher);
            entry.last_error_status.hash(&mut hasher);
            entry.last_error_summary.hash(&mut hasher);
            entry.suspicious_activity_count.hash(&mut hasher);
            entry.suspicious_activity_last_seen_at.hash(&mut hasher);
            entry.suspicious_activity_quarantine_until.hash(&mut hasher);
            entry
                .suspicious_activity_recovery_success_count
                .hash(&mut hasher);
            entry.rate_limit_cooldown_enabled.hash(&mut hasher);
            entry.rate_limit_cooldown_enabled_override.hash(&mut hasher);
            entry.rate_limit_cooldown_enabled_source.hash(&mut hasher);
            entry
                .rate_limit_bucket_capacity_override
                .map(f64::to_bits)
                .hash(&mut hasher);
            entry.rate_limit_bucket_capacity_source.hash(&mut hasher);
            entry
                .rate_limit_refill_per_second_override
                .map(f64::to_bits)
                .hash(&mut hasher);
            entry.rate_limit_refill_per_second_source.hash(&mut hasher);
        }

        hasher.finish() & JS_SAFE_U64_MASK
    }

    fn dispatch_event_fingerprint(
        dispatch: &crate::kiro::token_manager::LoadBalancingConfigSnapshot,
    ) -> u64 {
        let mut hasher = DefaultHasher::new();
        dispatch.mode.hash(&mut hasher);
        dispatch.session_affinity_enabled.hash(&mut hasher);
        dispatch.queue_max_size.hash(&mut hasher);
        dispatch.queue_max_wait_ms.hash(&mut hasher);
        dispatch.rate_limit_cooldown_ms.hash(&mut hasher);
        dispatch.rate_limit_cooldown_enabled.hash(&mut hasher);
        dispatch.suspicious_activity_cooldown_ms.hash(&mut hasher);
        dispatch
            .suspicious_activity_cooldown_enabled
            .hash(&mut hasher);
        dispatch
            .suspicious_activity_prefer_clean_credentials
            .hash(&mut hasher);
        dispatch
            .suspicious_activity_auto_disable_enabled
            .hash(&mut hasher);
        dispatch
            .suspicious_activity_auto_disable_threshold
            .hash(&mut hasher);
        dispatch
            .suspicious_activity_auto_disable_window_ms
            .hash(&mut hasher);
        dispatch
            .suspicious_activity_auto_clear_enabled
            .hash(&mut hasher);
        dispatch
            .suspicious_activity_auto_clear_success_threshold
            .hash(&mut hasher);
        dispatch
            .suspicious_activity_auto_clear_after_ms
            .hash(&mut hasher);
        dispatch.model_cooldown_enabled.hash(&mut hasher);
        dispatch.default_max_concurrency.hash(&mut hasher);
        dispatch
            .rate_limit_bucket_capacity
            .to_bits()
            .hash(&mut hasher);
        dispatch
            .rate_limit_refill_per_second
            .to_bits()
            .hash(&mut hasher);
        dispatch
            .rate_limit_refill_min_per_second
            .to_bits()
            .hash(&mut hasher);
        dispatch
            .rate_limit_refill_recovery_step_per_success
            .to_bits()
            .hash(&mut hasher);
        dispatch
            .rate_limit_refill_backoff_factor
            .to_bits()
            .hash(&mut hasher);
        dispatch
            .stream_dispatch_lease_release_enabled
            .hash(&mut hasher);
        dispatch
            .response_thinking_signature_compat_enabled
            .hash(&mut hasher);
        dispatch.proxy_pool.enabled.hash(&mut hasher);
        dispatch.proxy_pool.require_proxy.hash(&mut hasher);
        dispatch.proxy_pool.assignment_strategy.hash(&mut hasher);
        dispatch.proxy_pool.failover.enabled.hash(&mut hasher);
        dispatch
            .proxy_pool
            .failover
            .failure_threshold
            .hash(&mut hasher);
        dispatch.proxy_pool.failover.cooldown_secs.hash(&mut hasher);
        dispatch.proxy_pool.failover.probe_url.hash(&mut hasher);
        for proxy in &dispatch.proxy_pool.proxies {
            proxy.id.hash(&mut hasher);
            proxy.weight.hash(&mut hasher);
            proxy.enabled.hash(&mut hasher);
            proxy.expected_egress_ip.hash(&mut hasher);
        }
        hasher.finish() & JS_SAFE_U64_MASK
    }

    fn normalize_and_validate_credential_groups(
        &self,
        groups: &[String],
    ) -> Result<Vec<String>, AdminServiceError> {
        let normalized = normalize_credential_groups(groups);
        if normalized.is_empty() {
            return Ok(normalized);
        }

        let enabled_groups = self
            .token_manager
            .credential_group_catalog_snapshot()
            .into_iter()
            .filter(|group| group.enabled)
            .map(|group| group.name)
            .collect::<BTreeSet<_>>();
        let unknown = normalized
            .iter()
            .filter(|group| !enabled_groups.contains(*group))
            .cloned()
            .collect::<Vec<_>>();
        if !unknown.is_empty() {
            return Err(AdminServiceError::InvalidCredential(format!(
                "credentialGroups 包含未登记或未启用的凭据分组: {}",
                unknown.join(", ")
            )));
        }

        Ok(normalized)
    }

    fn scoped_api_key_group_counts(&self) -> BTreeMap<String, usize> {
        let mut counts = BTreeMap::new();
        for api_key in &self.token_manager.config().api_keys {
            for group in normalize_credential_groups(&api_key.allowed_credential_groups) {
                *counts.entry(group).or_insert(0) += 1;
            }
        }
        counts
    }

    fn credential_group_counts(&self) -> BTreeMap<String, usize> {
        let mut counts = BTreeMap::new();
        for entry in self.token_manager.snapshot().entries {
            for group in effective_credential_groups(&entry.credential_groups) {
                *counts.entry(group).or_insert(0) += 1;
            }
        }
        counts
    }

    fn validate_catalog_keeps_existing_references(
        &self,
        groups: &[CredentialGroupConfig],
    ) -> Result<(), AdminServiceError> {
        let known = groups
            .iter()
            .map(|group| group.name.clone())
            .collect::<BTreeSet<_>>();

        let missing_api_key_groups = self
            .scoped_api_key_group_counts()
            .into_keys()
            .filter(|group| !known.contains(group))
            .collect::<Vec<_>>();
        if !missing_api_key_groups.is_empty() {
            return Err(AdminServiceError::InvalidCredential(format!(
                "不能删除 API Key 仍在引用的凭据分组: {}",
                missing_api_key_groups.join(", ")
            )));
        }

        let missing_credential_groups = self
            .credential_group_counts()
            .into_keys()
            .filter(|group| !known.contains(group))
            .collect::<Vec<_>>();
        if !missing_credential_groups.is_empty() {
            return Err(AdminServiceError::InvalidCredential(format!(
                "不能删除现有凭据仍在引用的分组，请先批量修复凭据分组: {}",
                missing_credential_groups.join(", ")
            )));
        }

        Ok(())
    }

    /// 获取所有凭据状态
    pub fn get_all_credentials(&self) -> Result<CredentialsStatusResponse, AdminServiceError> {
        let (response, _) = self.build_credentials_status_response()?;
        Ok(response)
    }

    /// 获取客户端缓存之后的凭据列表增量。
    pub fn get_credentials_delta(
        &self,
        request: CredentialsDeltaRequest,
    ) -> Result<CredentialsDeltaResponse, AdminServiceError> {
        let (response, _) = self.build_credentials_status_response()?;
        let known = request
            .known_credentials
            .into_iter()
            .map(|item| (item.id, item.fingerprint))
            .collect::<HashMap<_, _>>();
        let current_ids = response
            .credentials
            .iter()
            .map(|credential| credential.id)
            .collect::<BTreeSet<_>>();

        let mut upserts = Vec::new();
        let mut deleted_ids = Vec::new();

        for credential in response.credentials {
            if known.get(&credential.id).copied() != Some(credential.fingerprint) {
                upserts.push(credential);
            }
        }

        for id in known.keys() {
            if !current_ids.contains(id) {
                deleted_ids.push(*id);
            }
        }
        deleted_ids.sort_unstable();

        let unchanged = request.since_revision == response.credentials_revision
            && request.balance_cache_revision == response.balance_cache_revision
            && request.credentials_fingerprint == response.credentials_fingerprint
            && upserts.is_empty()
            && deleted_ids.is_empty();

        Ok(CredentialsDeltaResponse {
            reset_required: false,
            reason: if unchanged {
                Some("notModified".to_string())
            } else {
                None
            },
            revision: response.credentials_revision,
            balance_revision: response.balance_cache_revision,
            fingerprint: response.credentials_fingerprint,
            total: response.total,
            available: response.available,
            dispatchable: response.dispatchable,
            current_id: response.current_id,
            upserts,
            deleted_ids,
            generated_at: Utc::now(),
        })
    }

    fn build_credentials_status_response(
        &self,
    ) -> Result<(CredentialsStatusResponse, AdminStateEvent), AdminServiceError> {
        self.sync_runtime_state_for_read()?;
        self.sync_balance_cache_if_changed()
            .map_err(|e| AdminServiceError::InternalError(e.to_string()))?;
        let event = self.build_state_event(self.events_tx.borrow().sequence)?;
        let snapshot = self.token_manager.snapshot();
        let current_id = snapshot.current_id;
        let total = snapshot.total;
        let available = snapshot.available;
        let dispatchable = snapshot.dispatchable;
        let balance_cache = self.balance_cache.lock().clone();

        let mut credentials: Vec<CredentialStatusItem> = snapshot
            .entries
            .into_iter()
            .map(|entry| {
                Self::credential_status_item_from_snapshot(entry, current_id, &balance_cache)
            })
            .collect();

        // 按优先级排序（数字越小优先级越高）
        credentials.sort_by_key(|c| c.priority);

        Ok((
            CredentialsStatusResponse {
                total,
                available,
                dispatchable,
                current_id,
                credentials_revision: event.credentials_revision,
                balance_cache_revision: event.balance_cache_revision,
                credentials_fingerprint: event.credentials_fingerprint,
                credentials,
            },
            event,
        ))
    }

    fn credential_status_item_from_snapshot(
        entry: CredentialEntrySnapshot,
        current_id: u64,
        balance_cache: &HashMap<u64, CachedBalanceRecord>,
    ) -> CredentialStatusItem {
        let fingerprint = Self::credential_item_fingerprint(&entry, balance_cache);
        let standard_account_type = infer_standard_account_type_id_from_subscription(
            entry.subscription_title.as_deref(),
            entry.subscription_type.as_deref(),
        )
        .map(|value| value.to_string());
        let profile_arn = entry.profile_arn.clone();
        let cached_balance = balance_cache
            .get(&entry.id)
            .filter(|cached| Self::is_balance_cache_fresh(cached))
            .filter(|cached| cached.profile_arn.as_deref() == profile_arn.as_deref())
            .map(|cached| CachedBalanceResponse {
                cached_at: cached.cached_at,
                balance: cached.data.clone(),
            });

        CredentialStatusItem {
            id: entry.id,
            fingerprint,
            priority: entry.priority,
            disabled: entry.disabled,
            failure_count: entry.failure_count,
            is_current: entry.id == current_id,
            expires_at: entry.expires_at,
            auth_method: entry.auth_method,
            provider: entry.provider,
            has_profile_arn: entry.has_profile_arn,
            profile_arn,
            refresh_token_hash: entry.refresh_token_hash,
            email: entry.email,
            user_id: entry.user_id,
            subscription_title: entry.subscription_title,
            subscription_type: entry.subscription_type,
            auth_account_type: entry.auth_account_type,
            account_type: entry.account_type,
            source_supplier_id: entry.source_supplier_id,
            source_supplier_name: entry.source_supplier_name,
            source_batch: entry.source_batch,
            credential_groups: entry.credential_groups,
            resolved_account_type: entry.resolved_account_type,
            account_type_source: entry.account_type_source,
            standard_account_type,
            allowed_models: entry.allowed_models,
            blocked_models: entry.blocked_models,
            runtime_model_restrictions: entry.runtime_model_restrictions,
            available_model_ids: entry.available_model_ids,
            available_models_cached_at: entry.available_models_cached_at,
            imported_at: entry.imported_at,
            success_count: entry.success_count,
            token_usage_count: entry.token_usage_count,
            input_tokens: entry.input_tokens,
            output_tokens: entry.output_tokens,
            total_tokens: entry.total_tokens,
            last_used_at: entry.last_used_at.clone(),
            in_flight: entry.active_requests,
            max_concurrency: entry.max_concurrency,
            max_concurrency_override: entry.max_concurrency_override,
            max_concurrency_source: entry.max_concurrency_source,
            has_proxy: entry.has_proxy,
            proxy_url: entry.proxy_url,
            proxy_id: entry.proxy_id,
            refresh_failure_count: entry.refresh_failure_count,
            disabled_reason: entry.disabled_reason,
            disabled_at: entry.disabled_at,
            last_error_status: entry.last_error_status,
            last_error_summary: entry.last_error_summary,
            suspicious_activity_count: entry.suspicious_activity_count,
            suspicious_activity_first_seen_at: entry.suspicious_activity_first_seen_at,
            suspicious_activity_last_seen_at: entry.suspicious_activity_last_seen_at,
            suspicious_activity_quarantine_until: entry.suspicious_activity_quarantine_until,
            suspicious_activity_recovery_success_count: entry
                .suspicious_activity_recovery_success_count,
            suspicious_activity_quarantine_remaining_ms: entry
                .suspicious_activity_quarantine_remaining_ms,
            rate_limit_cooldown_enabled: entry.rate_limit_cooldown_enabled,
            rate_limit_cooldown_enabled_override: entry.rate_limit_cooldown_enabled_override,
            rate_limit_cooldown_enabled_source: entry.rate_limit_cooldown_enabled_source,
            cooldown_remaining_ms: entry.cooldown_remaining_ms,
            rate_limit_bucket_tokens: entry.rate_limit_bucket_tokens,
            rate_limit_bucket_capacity: entry.rate_limit_bucket_capacity,
            rate_limit_bucket_capacity_override: entry.rate_limit_bucket_capacity_override,
            rate_limit_bucket_capacity_source: entry.rate_limit_bucket_capacity_source,
            rate_limit_refill_per_second: entry.rate_limit_refill_per_second,
            rate_limit_refill_per_second_override: entry.rate_limit_refill_per_second_override,
            rate_limit_refill_per_second_source: entry.rate_limit_refill_per_second_source,
            rate_limit_refill_base_per_second: entry.rate_limit_refill_base_per_second,
            rate_limit_hit_streak: entry.rate_limit_hit_streak,
            next_ready_in_ms: entry.next_ready_in_ms,
            cached_balance,
        }
    }

    fn credential_item_fingerprint(
        entry: &CredentialEntrySnapshot,
        balance_cache: &HashMap<u64, CachedBalanceRecord>,
    ) -> u64 {
        let mut hasher = DefaultHasher::new();
        entry.id.hash(&mut hasher);
        entry.priority.hash(&mut hasher);
        entry.disabled.hash(&mut hasher);
        entry.failure_count.hash(&mut hasher);
        entry.auth_method.hash(&mut hasher);
        entry.provider.hash(&mut hasher);
        entry.has_profile_arn.hash(&mut hasher);
        entry.profile_arn.hash(&mut hasher);
        entry.expires_at.hash(&mut hasher);
        entry.refresh_token_hash.hash(&mut hasher);
        entry.email.hash(&mut hasher);
        entry.user_id.hash(&mut hasher);
        entry.subscription_title.hash(&mut hasher);
        entry.subscription_type.hash(&mut hasher);
        entry.auth_account_type.hash(&mut hasher);
        entry.account_type.hash(&mut hasher);
        entry.source_supplier_id.hash(&mut hasher);
        entry.source_supplier_name.hash(&mut hasher);
        entry.source_batch.hash(&mut hasher);
        entry.credential_groups.hash(&mut hasher);
        entry.resolved_account_type.hash(&mut hasher);
        entry.account_type_source.hash(&mut hasher);
        entry.allowed_models.hash(&mut hasher);
        entry.blocked_models.hash(&mut hasher);
        entry.runtime_model_restrictions.hash(&mut hasher);
        entry.available_model_ids.hash(&mut hasher);
        entry.available_models_cached_at.hash(&mut hasher);
        entry.imported_at.hash(&mut hasher);
        entry.max_concurrency.hash(&mut hasher);
        entry.max_concurrency_override.hash(&mut hasher);
        entry.max_concurrency_source.hash(&mut hasher);
        entry.has_proxy.hash(&mut hasher);
        entry.proxy_url.hash(&mut hasher);
        entry.proxy_id.hash(&mut hasher);
        entry.refresh_failure_count.hash(&mut hasher);
        entry.disabled_reason.hash(&mut hasher);
        entry.disabled_at.hash(&mut hasher);
        entry.last_error_status.hash(&mut hasher);
        entry.last_error_summary.hash(&mut hasher);
        entry.suspicious_activity_count.hash(&mut hasher);
        entry.suspicious_activity_first_seen_at.hash(&mut hasher);
        entry.suspicious_activity_last_seen_at.hash(&mut hasher);
        entry.suspicious_activity_quarantine_until.hash(&mut hasher);
        entry
            .suspicious_activity_recovery_success_count
            .hash(&mut hasher);
        entry.rate_limit_cooldown_enabled.hash(&mut hasher);
        entry.rate_limit_cooldown_enabled_override.hash(&mut hasher);
        entry.rate_limit_cooldown_enabled_source.hash(&mut hasher);
        entry
            .rate_limit_bucket_capacity_override
            .map(f64::to_bits)
            .hash(&mut hasher);
        entry.rate_limit_bucket_capacity_source.hash(&mut hasher);
        entry
            .rate_limit_refill_per_second_override
            .map(f64::to_bits)
            .hash(&mut hasher);
        entry.rate_limit_refill_per_second_source.hash(&mut hasher);

        if let Some(cached) = balance_cache
            .get(&entry.id)
            .filter(|cached| Self::is_balance_cache_fresh(cached))
            .filter(|cached| cached.profile_arn.as_deref() == entry.profile_arn.as_deref())
        {
            cached.cached_at.to_bits().hash(&mut hasher);
            cached.profile_arn.hash(&mut hasher);
            Self::hash_balance_response(&cached.data, &mut hasher);
        }

        hasher.finish() & JS_SAFE_U64_MASK
    }

    fn hash_balance_response(balance: &BalanceResponse, hasher: &mut DefaultHasher) {
        balance.id.hash(hasher);
        balance.profile_arn.hash(hasher);
        balance.subscription_title.hash(hasher);
        balance.subscription_type.hash(hasher);
        balance.current_usage.to_bits().hash(hasher);
        balance.usage_limit.to_bits().hash(hasher);
        balance.effective_usage_limit.to_bits().hash(hasher);
        balance.remaining.to_bits().hash(hasher);
        balance.usage_percentage.to_bits().hash(hasher);
        balance.next_reset_at.map(f64::to_bits).hash(hasher);
        balance.overage_capability.hash(hasher);
        balance.overage_status.hash(hasher);
        balance.overage_enabled.hash(hasher);
        balance.overage_cap.to_bits().hash(hasher);
        balance.current_overages.to_bits().hash(hasher);
        balance.overage_charges.to_bits().hash(hasher);
        balance.overage_rate.map(f64::to_bits).hash(hasher);
        balance.currency.hash(hasher);
        balance.unit.hash(hasher);
    }

    /// 探测 Kiro external IdP 组织发现和 OIDC 能力
    pub async fn probe_external_idp(
        &self,
        mut req: ExternalIdpProbeRequest,
    ) -> Result<ExternalIdpProbeResponse, AdminServiceError> {
        req.work_email = normalize_optional_string(req.work_email.as_deref());
        req.domain_name = normalize_optional_string(req.domain_name.as_deref());
        req.issuer_url = normalize_optional_string(req.issuer_url.as_deref());
        req.client_id = normalize_optional_string(req.client_id.as_deref());
        req.audience = normalize_optional_string(req.audience.as_deref());
        req.proxy_url = normalize_optional_string(req.proxy_url.as_deref());
        req.proxy_username = normalize_optional_string(req.proxy_username.as_deref());
        req.proxy_password = normalize_optional_string(req.proxy_password.as_deref());
        req.proxy_id = normalize_optional_string(req.proxy_id.as_deref());

        let domain_name = resolve_external_idp_probe_domain(&req)?;
        let config = self.token_manager.config();
        let proxy = self.login_proxy_for_external_idp_probe(&req)?;
        let client = build_client_no_redirect(proxy.as_ref(), 60, config.tls_backend)
            .map_err(|err| AdminServiceError::InternalError(err.to_string()))?;

        let mut kiro_metadata_status = ExternalIdpProbeStatus::Skipped;
        let mut oidc_discovery_status = ExternalIdpProbeStatus::Skipped;
        let mut found = false;
        let mut issuer_url = normalize_optional_url(req.issuer_url.as_deref(), "Issuer URL")?;
        let mut client_id = req.client_id.clone();
        let mut scopes = req
            .scopes
            .as_deref()
            .map(normalize_scope_list_from_str)
            .unwrap_or_default();
        let mut audience = req.audience.clone();
        let mut oidc = None;
        let mut recommendations = Vec::new();
        let mut message = None;

        if domain_name != "direct-issuer" {
            match self.fetch_kiro_login_metadata(&client, &domain_name).await {
                Ok(metadata) => {
                    found = metadata.found;
                    if metadata.found {
                        kiro_metadata_status = ExternalIdpProbeStatus::Ok;
                        if issuer_url.is_none() {
                            issuer_url = normalize_optional_url(
                                metadata.issuer_url.as_deref(),
                                "Issuer URL",
                            )?;
                        }
                        if client_id.is_none() {
                            client_id = normalize_optional_string(metadata.client_id.as_deref());
                        }
                        if scopes.is_empty() {
                            scopes = normalize_scope_list(metadata.scopes);
                        }
                        if audience.is_none() {
                            audience = normalize_optional_string(metadata.audience.as_deref());
                        }
                    } else {
                        kiro_metadata_status = ExternalIdpProbeStatus::NotFound;
                        message = Some("Kiro 未发现该域名的组织登录 metadata".to_string());
                        recommendations
                            .push("确认该工作邮箱域名已在 Kiro 组织登录中配置".to_string());
                    }
                }
                Err(err) => {
                    kiro_metadata_status = ExternalIdpProbeStatus::Failed;
                    message = Some(err.to_string());
                    recommendations.push(
                        "需要用 Kiro IDE 或浏览器开发者工具抓取 GetLoginMetadata 的实际请求路径/headers"
                            .to_string(),
                    );
                }
            }
        } else {
            recommendations.push("已跳过 Kiro 组织发现，仅测试手动提供的 OIDC issuer".to_string());
        }

        if let Some(issuer) = issuer_url.as_deref() {
            if req.probe_oidc {
                match self.fetch_oidc_discovery(&client, issuer).await {
                    Ok(summary) => {
                        oidc_discovery_status = ExternalIdpProbeStatus::Ok;
                        oidc = Some(summary);
                    }
                    Err(err) => {
                        oidc_discovery_status = ExternalIdpProbeStatus::Failed;
                        if message.is_none() {
                            message = Some(err.to_string());
                        }
                        recommendations.push(
                            "OIDC discovery 失败时需要确认 issuerUrl 是否可公网访问且指向 .well-known/openid-configuration 的上级 issuer".to_string(),
                        );
                    }
                }
            } else {
                recommendations.push("已按请求跳过 OIDC discovery".to_string());
            }
        } else {
            recommendations
                .push("缺少 issuerUrl，无法测试 PKCE/token/device-code 能力".to_string());
        }

        if client_id.is_none() {
            recommendations.push("缺少 clientId，后续无法构造 PKCE authorization URL".to_string());
        }
        if scopes.is_empty() {
            recommendations
                .push("缺少 scopes，后续需要从 Kiro metadata 或手动配置补齐".to_string());
        }

        let authorization_code_supported = oidc.as_ref().is_some_and(|summary| {
            string_list_contains(&summary.response_types_supported, "code")
                || summary.response_types_supported.iter().any(|value| {
                    value
                        .split_whitespace()
                        .any(|part| part.eq_ignore_ascii_case("code"))
                })
        });
        let pkce_s256_supported = oidc.as_ref().is_some_and(|summary| {
            summary.authorization_endpoint.is_some()
                && summary.token_endpoint.is_some()
                && string_list_contains(&summary.code_challenge_methods_supported, "S256")
        });
        let device_code_supported = oidc
            .as_ref()
            .is_some_and(|summary| summary.device_authorization_endpoint.is_some());
        let refresh_without_client_secret_likely_supported = oidc
            .as_ref()
            .is_some_and(external_idp_public_token_auth_supported);

        if oidc_discovery_status == ExternalIdpProbeStatus::Ok {
            if !authorization_code_supported {
                recommendations.push(
                    "OIDC metadata 未明确支持 authorization_code/code response，PKCE 登录需实测"
                        .to_string(),
                );
            }
            if !pkce_s256_supported {
                recommendations.push("OIDC metadata 未明确支持 PKCE S256".to_string());
            }
            if !device_code_supported {
                recommendations
                    .push("未发现 device_authorization_endpoint，External IdP device-code 不可作为默认方案".to_string());
            }
            if !refresh_without_client_secret_likely_supported {
                recommendations.push(
                    "token_endpoint_auth_methods_supported 未包含 none；External IdP device-code token exchange 很可能需要 Kiro/Azure 应用密钥。IDE-style PKCE 使用已注册的 kiro://kiro.oauth/callback，仍可继续实测".to_string(),
                );
            }
            if pkce_s256_supported && client_id.is_some() && !scopes.is_empty() {
                recommendations
                    .push("建议使用 Kiro PKCE 手动回调测试；密码只在组织 IdP 页面输入，授权后粘贴 kiro:// 回调 URL 或授权码".to_string());
            }
        }

        Ok(ExternalIdpProbeResponse {
            domain_name,
            work_email: req.work_email,
            kiro_metadata_status,
            oidc_discovery_status,
            found,
            issuer_url,
            client_id,
            scopes,
            audience,
            oidc,
            pkce_s256_supported,
            device_code_supported,
            authorization_code_supported,
            refresh_without_client_secret_likely_supported,
            recommendations,
            message,
        })
    }

    /// 启动 External IdP 在线登录。auto 只在 public token exchange 明确可用时使用 device-code。
    pub async fn start_external_idp_login(
        &self,
        mut req: StartExternalIdpLoginRequest,
    ) -> Result<ExternalIdpLoginStartResponse, AdminServiceError> {
        self.ensure_runtime_write_leader()?;
        self.prune_external_idp_login_sessions();

        req.work_email = normalize_optional_string(req.work_email.as_deref());
        req.domain_name = normalize_optional_string(req.domain_name.as_deref());
        req.issuer_url = normalize_optional_string(req.issuer_url.as_deref());
        req.client_id = normalize_optional_string(req.client_id.as_deref());
        req.scopes = req
            .scopes
            .as_deref()
            .map(normalize_scope_string_with_offline_access)
            .transpose()?;
        req.audience = normalize_optional_string(req.audience.as_deref());
        req.login_hint =
            normalize_optional_string(req.login_hint.as_deref()).or_else(|| req.work_email.clone());
        req.callback_base_url = normalize_optional_string(req.callback_base_url.as_deref());
        req.profile_arn = normalize_optional_string(req.profile_arn.as_deref());
        req.auth_region = normalize_optional_string(req.auth_region.as_deref());
        req.api_region = normalize_optional_string(req.api_region.as_deref());
        req.machine_id = normalize_optional_string(req.machine_id.as_deref());
        req.account_type = normalize_optional_string(req.account_type.as_deref());
        req.source_supplier_id = normalize_optional_string(req.source_supplier_id.as_deref());
        req.source_supplier_name = normalize_optional_string(req.source_supplier_name.as_deref());
        req.source_batch = normalize_optional_string(req.source_batch.as_deref());
        if let Some(groups) = req.credential_groups.as_ref() {
            req.credential_groups = Some(self.normalize_and_validate_credential_groups(groups)?);
        }
        req.proxy_url = normalize_optional_string(req.proxy_url.as_deref());
        req.proxy_username = normalize_optional_string(req.proxy_username.as_deref());
        req.proxy_password = normalize_optional_string(req.proxy_password.as_deref());
        req.proxy_id = normalize_optional_string(req.proxy_id.as_deref());

        let discovery_domain = if let Some(email) = req.work_email.as_deref() {
            Some(domain_from_work_email(email)?)
        } else if let Some(domain) = req.domain_name.as_deref() {
            Some(normalize_domain_name(domain)?)
        } else {
            None
        };

        let config = self.token_manager.config();
        let proxy = self.login_proxy_for_external_idp_login(&req)?;
        let client = build_client_no_redirect(proxy.as_ref(), 60, config.tls_backend)
            .map_err(|err| AdminServiceError::InternalError(err.to_string()))?;

        let now = Utc::now();
        let expires_at = now + Duration::seconds(EXTERNAL_IDP_LOGIN_SESSION_SECS);
        let session_id = Uuid::new_v4().to_string();
        let direct_idp =
            req.issuer_url.is_some() && req.client_id.is_some() && req.scopes.is_some();
        let requested_flow = req.flow;
        let needs_portal_callback = !direct_idp && discovery_domain.is_none();
        let needs_https_idp_callback = requested_flow == ExternalIdpLoginFlow::Pkce;
        let callback_url = if needs_https_idp_callback || needs_portal_callback {
            Some(resolve_external_idp_callback_url(
                req.callback_base_url.as_deref(),
            )?)
        } else {
            req.callback_base_url
                .as_deref()
                .map(|value| resolve_external_idp_callback_url(Some(value)))
                .transpose()?
        };

        let mut session = ExternalIdpLoginSession {
            session_id: session_id.clone(),
            status: ExternalIdpLoginStatus::Pending,
            phase: if direct_idp {
                ExternalIdpLoginPhase::IdpAuthorization
            } else {
                ExternalIdpLoginPhase::PortalDiscovery
            },
            flow: requested_flow,
            provider: "ExternalIdp".to_string(),
            auth_url: None,
            callback_url,
            idp_redirect_uri: None,
            expires_at,
            request: req.clone(),
            portal_state: None,
            portal_code_verifier: None,
            idp_state: None,
            idp_code_verifier: None,
            issuer_url: req.issuer_url.clone(),
            client_id: req.client_id.clone(),
            scopes: req.scopes.clone(),
            audience: req.audience.clone(),
            login_hint: req.login_hint.clone(),
            token_endpoint: None,
            device_authorization_endpoint: None,
            device_code: None,
            user_code: None,
            verification_uri: None,
            verification_uri_complete: None,
            interval_seconds: EXTERNAL_IDP_LOGIN_POLL_INTERVAL_SECS,
            next_poll_at: now,
            polling: false,
            idp_callback_consumed: false,
            message: None,
            credential_result: None,
            updated_at: now,
        };

        if direct_idp {
            self.prepare_external_idp_login(&client, &mut session)
                .await?;
        } else if let Some(domain_name) = discovery_domain.as_deref() {
            let metadata = self.fetch_kiro_login_metadata(&client, domain_name).await?;
            if !metadata.found {
                return Err(AdminServiceError::InvalidCredential(
                    "Kiro 未发现该工作邮箱域名的组织登录 metadata".to_string(),
                ));
            }
            if session.issuer_url.is_none() {
                session.issuer_url =
                    normalize_optional_url(metadata.issuer_url.as_deref(), "Issuer URL")?;
            }
            if session.client_id.is_none() {
                session.client_id = normalize_optional_string(metadata.client_id.as_deref());
            }
            if session.scopes.is_none() {
                let scopes = normalize_scope_list(metadata.scopes);
                if !scopes.is_empty() {
                    session.scopes = Some(normalize_scope_string_with_offline_access(
                        &scopes.join(" "),
                    )?);
                }
            }
            if session.audience.is_none() {
                session.audience = normalize_optional_string(metadata.audience.as_deref());
            }
            self.prepare_external_idp_login(&client, &mut session)
                .await?;
        } else {
            let callback_url = session.callback_url.as_deref().ok_or_else(|| {
                AdminServiceError::InvalidCredential(
                    "ExternalIdP portal discovery 需要 callbackBaseUrl".to_string(),
                )
            })?;
            let portal_state = random_oauth_state()?;
            let portal_code_verifier = random_pkce_code_verifier()?;
            let code_challenge = pkce_s256_challenge(&portal_code_verifier);
            let auth_url =
                build_kiro_portal_auth_url(&portal_state, &code_challenge, callback_url)?;
            session.portal_state = Some(portal_state);
            session.portal_code_verifier = Some(portal_code_verifier);
            session.auth_url = Some(auth_url);
            session.message = Some("等待 Kiro portal 返回组织 External IdP metadata".to_string());
        }

        let auth_url = session.auth_url.clone().ok_or_else(|| {
            AdminServiceError::InternalError("ExternalIdP 登录 URL 未生成".to_string())
        })?;
        let response = ExternalIdpLoginStartResponse {
            session_id: session.session_id.clone(),
            status: session.status,
            phase: session.phase,
            flow: session.flow,
            provider: session.provider.clone(),
            auth_url,
            callback_url: external_idp_display_callback_url(&session),
            expires_at: session.expires_at,
            interval_seconds: session.interval_seconds,
            issuer_url: session.issuer_url.clone(),
            client_id: session.client_id.clone(),
            scopes: session.scopes.clone(),
            audience: session.audience.clone(),
            user_code: session.user_code.clone(),
            verification_uri: session.verification_uri.clone(),
            verification_uri_complete: session.verification_uri_complete.clone(),
            message: session.message.clone(),
        };

        self.external_idp_login_sessions
            .lock()
            .insert(session_id, session);

        Ok(response)
    }

    /// 查询并推进 External IdP 在线登录状态
    pub async fn get_external_idp_login_status(
        &self,
        session_id: &str,
    ) -> Result<ExternalIdpLoginStatusResponse, AdminServiceError> {
        self.ensure_runtime_write_leader()?;
        self.prune_external_idp_login_sessions();

        let poll_session = {
            let mut sessions = self.external_idp_login_sessions.lock();
            let session = sessions.get_mut(session_id).ok_or_else(|| {
                AdminServiceError::InvalidCredential(
                    "ExternalIdP 登录会话不存在或已过期".to_string(),
                )
            })?;

            if session.status != ExternalIdpLoginStatus::Pending {
                return Ok(external_idp_login_status_response(session));
            }

            let now = Utc::now();
            if now >= session.expires_at {
                session.status = ExternalIdpLoginStatus::Expired;
                session.message = Some("ExternalIdP 登录会话已过期，请重新开始登录".to_string());
                session.updated_at = now;
                return Ok(external_idp_login_status_response(session));
            }

            if session.phase != ExternalIdpLoginPhase::DeviceAuthorization {
                return Ok(external_idp_login_status_response(session));
            }

            if session.polling || now < session.next_poll_at {
                return Ok(external_idp_login_status_response(session));
            }

            session.polling = true;
            session.updated_at = now;
            session.clone()
        };

        let config = self.token_manager.config();
        let proxy = self.login_proxy_for_external_idp_login(&poll_session.request)?;
        let client = build_client_no_redirect(proxy.as_ref(), 60, config.tls_backend)
            .map_err(|err| AdminServiceError::InternalError(err.to_string()))?;
        let poll_result = self
            .poll_external_idp_device_token(&client, &poll_session)
            .await;

        match poll_result {
            Ok(ExternalIdpDeviceTokenPollResult::Pending) => {
                let mut sessions = self.external_idp_login_sessions.lock();
                let session = sessions.get_mut(session_id).ok_or_else(|| {
                    AdminServiceError::InvalidCredential(
                        "ExternalIdP 登录会话不存在或已过期".to_string(),
                    )
                })?;
                session.polling = false;
                session.next_poll_at =
                    Utc::now() + Duration::seconds(session.interval_seconds as i64);
                session.message = Some("等待用户完成 External IdP 设备授权".to_string());
                session.updated_at = Utc::now();
                Ok(external_idp_login_status_response(session))
            }
            Ok(ExternalIdpDeviceTokenPollResult::SlowDown) => {
                let mut sessions = self.external_idp_login_sessions.lock();
                let session = sessions.get_mut(session_id).ok_or_else(|| {
                    AdminServiceError::InvalidCredential(
                        "ExternalIdP 登录会话不存在或已过期".to_string(),
                    )
                })?;
                session.polling = false;
                session.interval_seconds = session.interval_seconds.saturating_add(5).max(1);
                session.next_poll_at =
                    Utc::now() + Duration::seconds(session.interval_seconds as i64);
                session.message = Some("External IdP 要求降低轮询频率，继续等待授权".to_string());
                session.updated_at = Utc::now();
                Ok(external_idp_login_status_response(session))
            }
            Ok(ExternalIdpDeviceTokenPollResult::Expired(message)) => {
                let mut sessions = self.external_idp_login_sessions.lock();
                let session = sessions.get_mut(session_id).ok_or_else(|| {
                    AdminServiceError::InvalidCredential(
                        "ExternalIdP 登录会话不存在或已过期".to_string(),
                    )
                })?;
                session.polling = false;
                session.status = ExternalIdpLoginStatus::Expired;
                session.message = Some(message);
                session.updated_at = Utc::now();
                Ok(external_idp_login_status_response(session))
            }
            Ok(ExternalIdpDeviceTokenPollResult::Failed(message)) => {
                let mut sessions = self.external_idp_login_sessions.lock();
                let session = sessions.get_mut(session_id).ok_or_else(|| {
                    AdminServiceError::InvalidCredential(
                        "ExternalIdP 登录会话不存在或已过期".to_string(),
                    )
                })?;
                session.polling = false;
                session.status = ExternalIdpLoginStatus::Failed;
                session.message = Some(message);
                session.updated_at = Utc::now();
                Ok(external_idp_login_status_response(session))
            }
            Ok(ExternalIdpDeviceTokenPollResult::Completed(token)) => {
                let add_req = self.build_external_idp_add_credential_request(&poll_session, token);
                match self.add_credential(add_req).await {
                    Ok(add_response) => {
                        let mut sessions = self.external_idp_login_sessions.lock();
                        let session = sessions.get_mut(session_id).ok_or_else(|| {
                            AdminServiceError::InvalidCredential(
                                "ExternalIdP 登录会话不存在或已过期".to_string(),
                            )
                        })?;
                        session.polling = false;
                        session.status = ExternalIdpLoginStatus::Completed;
                        session.phase = ExternalIdpLoginPhase::Completed;
                        session.message = Some(add_response.message.clone());
                        session.credential_result = Some(add_response);
                        session.updated_at = Utc::now();
                        Ok(external_idp_login_status_response(session))
                    }
                    Err(err) => {
                        let mut sessions = self.external_idp_login_sessions.lock();
                        if let Some(session) = sessions.get_mut(session_id) {
                            session.polling = false;
                            session.status = ExternalIdpLoginStatus::Failed;
                            session.message = Some(err.to_string());
                            session.updated_at = Utc::now();
                            return Ok(external_idp_login_status_response(session));
                        }
                        Err(err)
                    }
                }
            }
            Err(err) => {
                let mut sessions = self.external_idp_login_sessions.lock();
                if let Some(session) = sessions.get_mut(session_id) {
                    session.polling = false;
                    session.next_poll_at =
                        Utc::now() + Duration::seconds(session.interval_seconds as i64);
                    session.message = Some(err.to_string());
                    session.updated_at = Utc::now();
                    return Ok(external_idp_login_status_response(session));
                }
                Err(err)
            }
        }
    }

    /// 取消 External IdP 浏览器登录
    pub fn cancel_external_idp_login(
        &self,
        session_id: &str,
    ) -> Result<ExternalIdpLoginStatusResponse, AdminServiceError> {
        self.ensure_runtime_write_leader()?;
        let mut sessions = self.external_idp_login_sessions.lock();
        let session = sessions.get_mut(session_id).ok_or_else(|| {
            AdminServiceError::InvalidCredential("ExternalIdP 登录会话不存在或已过期".to_string())
        })?;
        if session.status == ExternalIdpLoginStatus::Pending {
            session.status = ExternalIdpLoginStatus::Cancelled;
            session.message = Some("ExternalIdP 登录已取消".to_string());
            session.polling = false;
            session.updated_at = Utc::now();
        }
        Ok(external_idp_login_status_response(session))
    }

    /// 手动提交 External IdP 自定义 scheme 回调 URL 或授权码
    pub async fn submit_external_idp_callback(
        &self,
        session_id: &str,
        payload: SubmitExternalIdpCallbackRequest,
    ) -> Result<ExternalIdpLoginStatusResponse, AdminServiceError> {
        self.ensure_runtime_write_leader()?;
        self.prune_external_idp_login_sessions();

        let params = external_idp_submit_callback_params(&payload)?;
        if let Some(error) = params
            .get("error")
            .map(String::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            let description = params
                .get("error_description")
                .map(String::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or(error);
            let mut sessions = self.external_idp_login_sessions.lock();
            let session = sessions.get_mut(session_id).ok_or_else(|| {
                AdminServiceError::InvalidCredential(
                    "ExternalIdP 登录会话不存在或已过期".to_string(),
                )
            })?;
            session.status = ExternalIdpLoginStatus::Failed;
            session.message = Some(description.to_string());
            session.updated_at = Utc::now();
            return Ok(external_idp_login_status_response(session));
        }

        let code = params
            .get("code")
            .map(String::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| AdminServiceError::InvalidCredential("OAuth 回调缺少 code".to_string()))?
            .to_string();
        let submitted_state = params
            .get("state")
            .map(String::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty());

        let exchange_session = {
            let mut sessions = self.external_idp_login_sessions.lock();
            let current = sessions.get_mut(session_id).ok_or_else(|| {
                AdminServiceError::InvalidCredential(
                    "ExternalIdP 登录会话不存在或已过期".to_string(),
                )
            })?;
            if current.status != ExternalIdpLoginStatus::Pending {
                return Ok(external_idp_login_status_response(current));
            }
            if Utc::now() >= current.expires_at {
                current.status = ExternalIdpLoginStatus::Expired;
                current.message = Some("ExternalIdP 登录会话已过期，请重新开始登录".to_string());
                current.updated_at = Utc::now();
                return Ok(external_idp_login_status_response(current));
            }
            if current.phase != ExternalIdpLoginPhase::IdpAuthorization {
                current.message = Some("当前 External IdP 会话尚未进入 IdP 授权阶段".to_string());
                current.updated_at = Utc::now();
                return Err(AdminServiceError::InvalidCredential(
                    "当前 External IdP 会话尚未进入 IdP 授权阶段".to_string(),
                ));
            }
            if let Some(submitted_state) = submitted_state {
                if current.idp_state.as_deref() != Some(submitted_state) {
                    current.message = Some(
                        "提交的 OAuth state 与当前登录会话不匹配，请重新复制回调 URL".to_string(),
                    );
                    current.updated_at = Utc::now();
                    return Err(AdminServiceError::InvalidCredential(
                        "提交的 OAuth state 与当前登录会话不匹配".to_string(),
                    ));
                }
            }
            if current.idp_callback_consumed {
                return Ok(external_idp_login_status_response(current));
            }

            current.idp_callback_consumed = true;
            current.message = Some("已收到授权码，正在交换 token".to_string());
            current.updated_at = Utc::now();
            current.clone()
        };

        let result: Result<AddCredentialResponse, AdminServiceError> = async {
            let config = self.token_manager.config();
            let proxy = self.login_proxy_for_external_idp_login(&exchange_session.request)?;
            let client = build_client_no_redirect(proxy.as_ref(), 60, config.tls_backend)
                .map_err(|err| AdminServiceError::InternalError(err.to_string()))?;
            let token = self
                .exchange_external_idp_authorization_code(&client, &exchange_session, &code)
                .await?;
            let add_req = self.build_external_idp_add_credential_request(&exchange_session, token);
            self.add_credential(add_req).await
        }
        .await;

        let mut sessions = self.external_idp_login_sessions.lock();
        let session = sessions.get_mut(session_id).ok_or_else(|| {
            AdminServiceError::InvalidCredential("ExternalIdP 登录会话不存在或已过期".to_string())
        })?;
        match result {
            Ok(add_response) => {
                session.status = ExternalIdpLoginStatus::Completed;
                session.phase = ExternalIdpLoginPhase::Completed;
                session.message = Some(add_response.message.clone());
                session.credential_result = Some(add_response);
                session.updated_at = Utc::now();
            }
            Err(err) => {
                session.status = ExternalIdpLoginStatus::Failed;
                session.message = Some(err.to_string());
                session.updated_at = Utc::now();
            }
        }
        Ok(external_idp_login_status_response(session))
    }

    /// 通过 OAuth state 自动定位 External IdP 登录会话并提交自定义 scheme 回调。
    pub async fn submit_external_idp_callback_by_state(
        &self,
        payload: SubmitExternalIdpCallbackRequest,
    ) -> Result<ExternalIdpLoginStatusResponse, AdminServiceError> {
        self.ensure_runtime_write_leader()?;
        self.prune_external_idp_login_sessions();

        let params = external_idp_submit_callback_params(&payload)?;
        let state = params
            .get("state")
            .map(String::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                AdminServiceError::InvalidCredential(
                    "公共 External IdP 回调提交需要完整回调 URL 或 state".to_string(),
                )
            })?
            .to_string();

        let session = self.clone_external_idp_session_by_state(&state)?;
        self.submit_external_idp_callback(&session.session_id, payload)
            .await
    }

    /// 处理 Kiro portal 或 External IdP 的浏览器回调
    pub async fn handle_external_idp_callback(
        &self,
        params: HashMap<String, String>,
    ) -> Result<ExternalIdpCallbackAction, AdminServiceError> {
        self.ensure_runtime_write_leader()?;
        self.prune_external_idp_login_sessions();

        let state = params
            .get("state")
            .map(String::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                AdminServiceError::InvalidCredential("OAuth 回调缺少 state".to_string())
            })?
            .to_string();

        if let Some(error) = params
            .get("error")
            .map(String::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            let description = params
                .get("error_description")
                .map(String::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or(error);
            self.fail_external_idp_session_by_state(&state, description);
            return Ok(ExternalIdpCallbackAction::Html {
                success: false,
                title: "External IdP 登录失败".to_string(),
                message: description.to_string(),
            });
        }

        let session = self.clone_external_idp_session_by_state(&state)?;
        if session.portal_state.as_deref() == Some(state.as_str())
            && session.phase == ExternalIdpLoginPhase::PortalDiscovery
        {
            self.handle_external_idp_portal_callback(session, params)
                .await
        } else if session.idp_state.as_deref() == Some(state.as_str())
            && session.phase == ExternalIdpLoginPhase::IdpAuthorization
        {
            self.handle_external_idp_authorization_callback(session, params)
                .await
        } else {
            Err(AdminServiceError::InvalidCredential(
                "OAuth 回调 state 与登录阶段不匹配".to_string(),
            ))
        }
    }

    /// 启动 AWS IdC device-code 在线登录
    pub async fn start_idc_device_login(
        &self,
        mut req: StartIdcDeviceLoginRequest,
    ) -> Result<IdcDeviceLoginStartResponse, AdminServiceError> {
        self.ensure_runtime_write_leader()?;
        self.prune_idc_device_login_sessions();

        let provider = normalize_idc_device_provider(&req.provider)?;
        req.provider = provider.clone();
        req.start_url = normalize_optional_string(req.start_url.as_deref());
        req.region = normalize_optional_string(req.region.as_deref());
        req.auth_region = normalize_optional_string(req.auth_region.as_deref());
        req.api_region = normalize_optional_string(req.api_region.as_deref());
        req.profile_arn = normalize_optional_string(req.profile_arn.as_deref());
        req.machine_id = normalize_optional_string(req.machine_id.as_deref());
        req.account_type = normalize_optional_string(req.account_type.as_deref());
        req.source_supplier_id = normalize_optional_string(req.source_supplier_id.as_deref());
        req.source_supplier_name = normalize_optional_string(req.source_supplier_name.as_deref());
        req.source_batch = normalize_optional_string(req.source_batch.as_deref());
        if let Some(groups) = req.credential_groups.as_ref() {
            req.credential_groups = Some(self.normalize_and_validate_credential_groups(groups)?);
        }
        req.proxy_url = normalize_optional_string(req.proxy_url.as_deref());
        req.proxy_username = normalize_optional_string(req.proxy_username.as_deref());
        req.proxy_password = normalize_optional_string(req.proxy_password.as_deref());
        req.proxy_id = normalize_optional_string(req.proxy_id.as_deref());

        let start_url = resolve_idc_device_start_url(&provider, req.start_url.as_deref())?;
        req.start_url = Some(start_url.clone());

        let config = self.token_manager.config();
        let region = req
            .auth_region
            .as_deref()
            .or(req.region.as_deref())
            .unwrap_or_else(|| config.effective_auth_region())
            .trim()
            .to_string();
        if region.is_empty() {
            return Err(AdminServiceError::InvalidCredential(
                "登录 region 不能为空".to_string(),
            ));
        }

        let proxy = self.login_proxy_for_request(&req)?;
        let client = build_client(proxy.as_ref(), 60, config.tls_backend)
            .map_err(|err| AdminServiceError::InternalError(err.to_string()))?;

        let client_registration = self
            .register_idc_device_client(&client, &region, &start_url)
            .await?;
        let device_authorization = self
            .start_idc_device_authorization(
                &client,
                &region,
                &client_registration.client_id,
                &client_registration.client_secret,
                &start_url,
            )
            .await?;

        let now = Utc::now();
        let interval_seconds = device_authorization.interval.unwrap_or(5).max(1);
        let expires_at = now + Duration::seconds(device_authorization.expires_in.max(1));
        let session_id = Uuid::new_v4().to_string();
        let session = IdcDeviceLoginSession {
            session_id: session_id.clone(),
            status: IdcDeviceLoginStatus::Pending,
            provider: provider.clone(),
            start_url: start_url.clone(),
            region: region.clone(),
            client_id: client_registration.client_id,
            client_secret: client_registration.client_secret,
            device_code: device_authorization.device_code,
            user_code: device_authorization.user_code.clone(),
            verification_uri: device_authorization.verification_uri.clone(),
            verification_uri_complete: device_authorization.verification_uri_complete.clone(),
            interval_seconds,
            next_poll_at: now,
            expires_at,
            request: req,
            message: Some("等待用户完成授权".to_string()),
            credential_result: None,
            polling: false,
            updated_at: now,
        };

        self.idc_device_login_sessions
            .lock()
            .insert(session_id.clone(), session);

        Ok(IdcDeviceLoginStartResponse {
            session_id,
            status: IdcDeviceLoginStatus::Pending,
            provider,
            start_url,
            region,
            user_code: device_authorization.user_code,
            verification_uri: device_authorization.verification_uri,
            verification_uri_complete: device_authorization.verification_uri_complete,
            expires_at,
            interval_seconds,
            message: Some("等待用户完成授权".to_string()),
        })
    }

    /// 查询并推进 AWS IdC device-code 在线登录状态
    pub async fn get_idc_device_login_status(
        &self,
        session_id: &str,
    ) -> Result<IdcDeviceLoginStatusResponse, AdminServiceError> {
        self.ensure_runtime_write_leader()?;
        self.prune_idc_device_login_sessions();

        let poll_session = {
            let mut sessions = self.idc_device_login_sessions.lock();
            let session = sessions.get_mut(session_id).ok_or_else(|| {
                AdminServiceError::InvalidCredential("登录会话不存在或已过期".to_string())
            })?;

            if session.status != IdcDeviceLoginStatus::Pending {
                return Ok(idc_device_login_status_response(session));
            }

            let now = Utc::now();
            if now >= session.expires_at {
                session.status = IdcDeviceLoginStatus::Expired;
                session.message = Some("授权码已过期，请重新开始登录".to_string());
                session.updated_at = now;
                return Ok(idc_device_login_status_response(session));
            }

            if session.polling || now < session.next_poll_at {
                return Ok(idc_device_login_status_response(session));
            }

            session.polling = true;
            session.updated_at = now;
            session.clone()
        };

        let config = self.token_manager.config();
        let proxy = self.login_proxy_for_request(&poll_session.request)?;
        let client = build_client(proxy.as_ref(), 60, config.tls_backend)
            .map_err(|err| AdminServiceError::InternalError(err.to_string()))?;
        let poll_result = self.poll_idc_device_token(&client, &poll_session).await;

        match poll_result {
            Ok(DeviceTokenPollResult::Pending) => {
                let mut sessions = self.idc_device_login_sessions.lock();
                let session = sessions.get_mut(session_id).ok_or_else(|| {
                    AdminServiceError::InvalidCredential("登录会话不存在或已过期".to_string())
                })?;
                session.polling = false;
                session.next_poll_at =
                    Utc::now() + Duration::seconds(session.interval_seconds as i64);
                session.message = Some("等待用户完成授权".to_string());
                session.updated_at = Utc::now();
                Ok(idc_device_login_status_response(session))
            }
            Ok(DeviceTokenPollResult::SlowDown) => {
                let mut sessions = self.idc_device_login_sessions.lock();
                let session = sessions.get_mut(session_id).ok_or_else(|| {
                    AdminServiceError::InvalidCredential("登录会话不存在或已过期".to_string())
                })?;
                session.polling = false;
                session.interval_seconds = session.interval_seconds.saturating_add(5).max(1);
                session.next_poll_at =
                    Utc::now() + Duration::seconds(session.interval_seconds as i64);
                session.message = Some("AWS 要求降低轮询频率，继续等待授权".to_string());
                session.updated_at = Utc::now();
                Ok(idc_device_login_status_response(session))
            }
            Ok(DeviceTokenPollResult::Expired(message)) => {
                let mut sessions = self.idc_device_login_sessions.lock();
                let session = sessions.get_mut(session_id).ok_or_else(|| {
                    AdminServiceError::InvalidCredential("登录会话不存在或已过期".to_string())
                })?;
                session.polling = false;
                session.status = IdcDeviceLoginStatus::Expired;
                session.message = Some(message);
                session.updated_at = Utc::now();
                Ok(idc_device_login_status_response(session))
            }
            Ok(DeviceTokenPollResult::Failed(message)) => {
                let mut sessions = self.idc_device_login_sessions.lock();
                let session = sessions.get_mut(session_id).ok_or_else(|| {
                    AdminServiceError::InvalidCredential("登录会话不存在或已过期".to_string())
                })?;
                session.polling = false;
                session.status = IdcDeviceLoginStatus::Failed;
                session.message = Some(message);
                session.updated_at = Utc::now();
                Ok(idc_device_login_status_response(session))
            }
            Ok(DeviceTokenPollResult::Completed(token)) => {
                let add_req = self.build_idc_device_add_credential_request(&poll_session, token);
                match self.add_credential(add_req).await {
                    Ok(add_response) => {
                        let mut sessions = self.idc_device_login_sessions.lock();
                        let session = sessions.get_mut(session_id).ok_or_else(|| {
                            AdminServiceError::InvalidCredential(
                                "登录会话不存在或已过期".to_string(),
                            )
                        })?;
                        session.polling = false;
                        session.status = IdcDeviceLoginStatus::Completed;
                        session.message = Some(add_response.message.clone());
                        session.credential_result = Some(add_response);
                        session.updated_at = Utc::now();
                        Ok(idc_device_login_status_response(session))
                    }
                    Err(err) => {
                        let mut sessions = self.idc_device_login_sessions.lock();
                        if let Some(session) = sessions.get_mut(session_id) {
                            session.polling = false;
                            session.status = IdcDeviceLoginStatus::Failed;
                            session.message = Some(err.to_string());
                            session.updated_at = Utc::now();
                            return Ok(idc_device_login_status_response(session));
                        }
                        Err(err)
                    }
                }
            }
            Err(err) => {
                let mut sessions = self.idc_device_login_sessions.lock();
                if let Some(session) = sessions.get_mut(session_id) {
                    session.polling = false;
                    session.next_poll_at =
                        Utc::now() + Duration::seconds(session.interval_seconds as i64);
                    session.message = Some(err.to_string());
                    session.updated_at = Utc::now();
                    return Ok(idc_device_login_status_response(session));
                }
                Err(err)
            }
        }
    }

    /// 取消 AWS IdC device-code 在线登录
    pub fn cancel_idc_device_login(
        &self,
        session_id: &str,
    ) -> Result<IdcDeviceLoginStatusResponse, AdminServiceError> {
        self.ensure_runtime_write_leader()?;
        let mut sessions = self.idc_device_login_sessions.lock();
        let session = sessions.get_mut(session_id).ok_or_else(|| {
            AdminServiceError::InvalidCredential("登录会话不存在或已过期".to_string())
        })?;
        if session.status == IdcDeviceLoginStatus::Pending {
            session.status = IdcDeviceLoginStatus::Cancelled;
            session.message = Some("登录已取消".to_string());
            session.polling = false;
            session.updated_at = Utc::now();
        }
        Ok(idc_device_login_status_response(session))
    }

    fn prune_idc_device_login_sessions(&self) {
        let now = Utc::now();
        self.idc_device_login_sessions.lock().retain(|_, session| {
            let stale_terminal = session.status != IdcDeviceLoginStatus::Pending
                && now - session.updated_at > Duration::seconds(IDC_LOGIN_SESSION_RETENTION_SECS);
            let stale_pending = session.status == IdcDeviceLoginStatus::Pending
                && now - session.expires_at > Duration::seconds(IDC_LOGIN_SESSION_RETENTION_SECS);
            !(stale_terminal || stale_pending)
        });
    }

    fn prune_external_idp_login_sessions(&self) {
        let now = Utc::now();
        self.external_idp_login_sessions
            .lock()
            .retain(|_, session| {
                let stale_terminal = session.status != ExternalIdpLoginStatus::Pending
                    && now - session.updated_at
                        > Duration::seconds(IDC_LOGIN_SESSION_RETENTION_SECS);
                let stale_pending = session.status == ExternalIdpLoginStatus::Pending
                    && now - session.expires_at
                        > Duration::seconds(IDC_LOGIN_SESSION_RETENTION_SECS);
                !(stale_terminal || stale_pending)
            });
    }

    fn clone_external_idp_session_by_state(
        &self,
        state: &str,
    ) -> Result<ExternalIdpLoginSession, AdminServiceError> {
        let sessions = self.external_idp_login_sessions.lock();
        sessions
            .values()
            .find(|session| {
                session.portal_state.as_deref() == Some(state)
                    || session.idp_state.as_deref() == Some(state)
            })
            .cloned()
            .ok_or_else(|| {
                AdminServiceError::InvalidCredential(
                    "ExternalIdP 登录会话不存在、已过期或 state 无效".to_string(),
                )
            })
    }

    fn fail_external_idp_session_by_state(&self, state: &str, message: &str) {
        let mut sessions = self.external_idp_login_sessions.lock();
        if let Some(session) = sessions.values_mut().find(|session| {
            session.portal_state.as_deref() == Some(state)
                || session.idp_state.as_deref() == Some(state)
        }) {
            session.status = ExternalIdpLoginStatus::Failed;
            session.message = Some(message.to_string());
            session.updated_at = Utc::now();
        }
    }

    fn fail_external_idp_session_by_id(&self, session_id: &str, message: &str) {
        let mut sessions = self.external_idp_login_sessions.lock();
        if let Some(session) = sessions.get_mut(session_id) {
            session.status = ExternalIdpLoginStatus::Failed;
            session.message = Some(message.to_string());
            session.updated_at = Utc::now();
        }
    }

    fn external_idp_session_inputs(
        session: &ExternalIdpLoginSession,
    ) -> Result<(String, String, String), AdminServiceError> {
        let issuer_url = normalize_optional_url(session.issuer_url.as_deref(), "Issuer URL")?
            .ok_or_else(|| {
                AdminServiceError::InvalidCredential("ExternalIdP 登录缺少 issuerUrl".to_string())
            })?;
        let client_id =
            normalize_optional_string(session.client_id.as_deref()).ok_or_else(|| {
                AdminServiceError::InvalidCredential("ExternalIdP 登录缺少 clientId".to_string())
            })?;
        let scopes = normalize_scope_string_with_offline_access(
            session.scopes.as_deref().unwrap_or_default(),
        )?;

        Ok((issuer_url, client_id, scopes))
    }

    async fn prepare_external_idp_login(
        &self,
        client: &reqwest::Client,
        session: &mut ExternalIdpLoginSession,
    ) -> Result<(), AdminServiceError> {
        let (issuer_url, _, _) = Self::external_idp_session_inputs(session)?;
        let discovery = self.fetch_oidc_discovery(client, &issuer_url).await?;

        if !matches!(
            session.flow,
            ExternalIdpLoginFlow::Pkce | ExternalIdpLoginFlow::KiroPkce
        ) {
            if discovery.device_authorization_endpoint.is_some() {
                if let Some(message) =
                    external_idp_device_code_secret_requirement_message(&discovery)
                {
                    return Err(AdminServiceError::InvalidCredential(message));
                }
                return self
                    .prepare_external_idp_device_authorization(client, session, discovery)
                    .await;
            }
            if session.flow == ExternalIdpLoginFlow::DeviceCode {
                return Err(AdminServiceError::InvalidCredential(
                    "OIDC discovery 未返回 device_authorization_endpoint，无法使用 ExternalIdP device-code 登录"
                        .to_string(),
                ));
            }
        }

        self.prepare_external_idp_authorization(session, discovery)
    }

    fn prepare_external_idp_authorization(
        &self,
        session: &mut ExternalIdpLoginSession,
        discovery: ExternalIdpOidcDiscoverySummary,
    ) -> Result<(), AdminServiceError> {
        let (issuer_url, client_id, scopes) = Self::external_idp_session_inputs(session)?;
        let authorization_endpoint = discovery.authorization_endpoint.clone().ok_or_else(|| {
            AdminServiceError::InvalidCredential(
                "OIDC discovery 未返回 authorization_endpoint".to_string(),
            )
        })?;
        let token_endpoint = discovery.token_endpoint.clone().ok_or_else(|| {
            AdminServiceError::InvalidCredential("OIDC discovery 未返回 token_endpoint".to_string())
        })?;
        let idp_redirect_uri = if session.flow == ExternalIdpLoginFlow::KiroPkce {
            KIRO_IDE_EXTERNAL_IDP_REDIRECT_URI.to_string()
        } else {
            session
                .callback_url
                .as_deref()
                .ok_or_else(|| {
                    AdminServiceError::InvalidCredential(
                        "ExternalIdP PKCE 登录需要 callbackBaseUrl".to_string(),
                    )
                })?
                .to_string()
        };

        let state = random_oauth_state()?;
        let code_verifier = random_pkce_code_verifier()?;
        let code_challenge = pkce_s256_challenge(&code_verifier);
        let auth_url = build_external_idp_authorization_url(
            &authorization_endpoint,
            &client_id,
            &idp_redirect_uri,
            &scopes,
            &state,
            &code_challenge,
            session.login_hint.as_deref(),
            session.audience.as_deref(),
        )?;

        if session.flow != ExternalIdpLoginFlow::KiroPkce {
            session.flow = ExternalIdpLoginFlow::Pkce;
        }
        session.phase = ExternalIdpLoginPhase::IdpAuthorization;
        session.auth_url = Some(auth_url);
        session.idp_redirect_uri = Some(idp_redirect_uri);
        session.idp_state = Some(state);
        session.idp_code_verifier = Some(code_verifier);
        session.issuer_url = Some(issuer_url);
        session.client_id = Some(client_id);
        session.scopes = Some(scopes);
        session.token_endpoint = Some(token_endpoint);
        session.device_authorization_endpoint = discovery.device_authorization_endpoint;
        session.interval_seconds = EXTERNAL_IDP_LOGIN_POLL_INTERVAL_SECS;
        session.message = Some(if session.flow == ExternalIdpLoginFlow::KiroPkce {
            "等待用户完成组织 IdP 登录，并粘贴 kiro:// 回调 URL 或授权码".to_string()
        } else {
            "等待用户在组织 IdP 页面完成登录".to_string()
        });
        session.updated_at = Utc::now();
        Ok(())
    }

    async fn prepare_external_idp_device_authorization(
        &self,
        client: &reqwest::Client,
        session: &mut ExternalIdpLoginSession,
        discovery: ExternalIdpOidcDiscoverySummary,
    ) -> Result<(), AdminServiceError> {
        let (issuer_url, client_id, scopes) = Self::external_idp_session_inputs(session)?;
        let token_endpoint = discovery.token_endpoint.clone().ok_or_else(|| {
            AdminServiceError::InvalidCredential("OIDC discovery 未返回 token_endpoint".to_string())
        })?;
        let device_authorization_endpoint = discovery
            .device_authorization_endpoint
            .clone()
            .ok_or_else(|| {
                AdminServiceError::InvalidCredential(
                    "OIDC discovery 未返回 device_authorization_endpoint".to_string(),
                )
            })?;

        let device_authorization = self
            .start_external_idp_device_authorization(
                client,
                &device_authorization_endpoint,
                &client_id,
                &scopes,
                session.audience.as_deref(),
            )
            .await?;

        let now = Utc::now();
        let interval_seconds = device_authorization.interval.unwrap_or(5).max(1);
        let expires_at = now + Duration::seconds(device_authorization.expires_in.max(1));
        let auth_url = device_authorization
            .verification_uri_complete
            .clone()
            .unwrap_or_else(|| device_authorization.verification_uri.clone());

        session.flow = ExternalIdpLoginFlow::DeviceCode;
        session.phase = ExternalIdpLoginPhase::DeviceAuthorization;
        session.auth_url = Some(auth_url);
        session.issuer_url = Some(issuer_url);
        session.client_id = Some(client_id);
        session.scopes = Some(scopes);
        session.token_endpoint = Some(token_endpoint);
        session.device_authorization_endpoint = Some(device_authorization_endpoint);
        session.device_code = Some(device_authorization.device_code);
        session.user_code = Some(device_authorization.user_code);
        session.verification_uri = Some(device_authorization.verification_uri);
        session.verification_uri_complete = device_authorization.verification_uri_complete;
        session.interval_seconds = interval_seconds;
        session.next_poll_at = now;
        session.expires_at = expires_at;
        session.message = device_authorization
            .message
            .filter(|value| !value.trim().is_empty())
            .or_else(|| Some("等待用户在组织 IdP 验证页面输入代码".to_string()));
        session.updated_at = now;
        Ok(())
    }

    async fn handle_external_idp_portal_callback(
        &self,
        mut session: ExternalIdpLoginSession,
        params: HashMap<String, String>,
    ) -> Result<ExternalIdpCallbackAction, AdminServiceError> {
        if Utc::now() >= session.expires_at {
            self.fail_external_idp_session_by_id(&session.session_id, "ExternalIdP 登录会话已过期");
            return Ok(ExternalIdpCallbackAction::Html {
                success: false,
                title: "External IdP 登录已过期".to_string(),
                message: "请回到管理页面重新开始登录".to_string(),
            });
        }

        if let Some(message) = external_idp_portal_descriptor_error(&params) {
            self.fail_external_idp_session_by_id(&session.session_id, &message);
            return Ok(ExternalIdpCallbackAction::Html {
                success: false,
                title: "External IdP 登录失败".to_string(),
                message,
            });
        }

        session.issuer_url =
            normalize_optional_url(params.get("issuer_url").map(String::as_str), "Issuer URL")?
                .or(session.issuer_url);
        session.client_id = normalize_optional_string(params.get("client_id").map(String::as_str))
            .or(session.client_id);
        session.scopes = params
            .get("scopes")
            .map(String::as_str)
            .map(normalize_scope_string_with_offline_access)
            .transpose()?
            .or(session.scopes);
        session.login_hint =
            normalize_optional_string(params.get("login_hint").map(String::as_str))
                .or(session.login_hint);
        session.audience = normalize_optional_string(params.get("audience").map(String::as_str))
            .or(session.audience);

        let config = self.token_manager.config();
        let proxy = self.login_proxy_for_external_idp_login(&session.request)?;
        let client = build_client_no_redirect(proxy.as_ref(), 60, config.tls_backend)
            .map_err(|err| AdminServiceError::InternalError(err.to_string()))?;
        self.prepare_external_idp_login(&client, &mut session)
            .await?;

        let auth_url = session.auth_url.clone().ok_or_else(|| {
            AdminServiceError::InternalError("ExternalIdP 登录 URL 未生成".to_string())
        })?;

        let mut sessions = self.external_idp_login_sessions.lock();
        let current = sessions.get_mut(&session.session_id).ok_or_else(|| {
            AdminServiceError::InvalidCredential("ExternalIdP 登录会话不存在或已过期".to_string())
        })?;
        if current.status != ExternalIdpLoginStatus::Pending {
            return Ok(ExternalIdpCallbackAction::Html {
                success: false,
                title: "External IdP 登录已结束".to_string(),
                message: current
                    .message
                    .clone()
                    .unwrap_or_else(|| "当前登录会话已结束".to_string()),
            });
        }
        *current = session;

        Ok(ExternalIdpCallbackAction::Redirect(auth_url))
    }

    async fn handle_external_idp_authorization_callback(
        &self,
        session: ExternalIdpLoginSession,
        params: HashMap<String, String>,
    ) -> Result<ExternalIdpCallbackAction, AdminServiceError> {
        let code = params
            .get("code")
            .map(String::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| AdminServiceError::InvalidCredential("OAuth 回调缺少 code".to_string()))?
            .to_string();

        let exchange_session = {
            let mut sessions = self.external_idp_login_sessions.lock();
            let current = sessions.get_mut(&session.session_id).ok_or_else(|| {
                AdminServiceError::InvalidCredential(
                    "ExternalIdP 登录会话不存在或已过期".to_string(),
                )
            })?;
            if current.status != ExternalIdpLoginStatus::Pending {
                return Ok(ExternalIdpCallbackAction::Html {
                    success: current.status == ExternalIdpLoginStatus::Completed,
                    title: "External IdP 登录已结束".to_string(),
                    message: current
                        .message
                        .clone()
                        .unwrap_or_else(|| "当前登录会话已结束".to_string()),
                });
            }
            if Utc::now() >= current.expires_at {
                current.status = ExternalIdpLoginStatus::Expired;
                current.message = Some("ExternalIdP 登录会话已过期，请重新开始登录".to_string());
                current.updated_at = Utc::now();
                return Ok(ExternalIdpCallbackAction::Html {
                    success: false,
                    title: "External IdP 登录已过期".to_string(),
                    message: "请回到管理页面重新开始登录".to_string(),
                });
            }
            if current.idp_callback_consumed {
                return Ok(ExternalIdpCallbackAction::Html {
                    success: false,
                    title: "External IdP 回调已处理".to_string(),
                    message: "请回到管理页面查看登录状态".to_string(),
                });
            }
            current.idp_callback_consumed = true;
            current.message = Some("已收到授权码，正在交换 token".to_string());
            current.updated_at = Utc::now();
            current.clone()
        };

        let result: Result<AddCredentialResponse, AdminServiceError> = async {
            let config = self.token_manager.config();
            let proxy = self.login_proxy_for_external_idp_login(&exchange_session.request)?;
            let client = build_client_no_redirect(proxy.as_ref(), 60, config.tls_backend)
                .map_err(|err| AdminServiceError::InternalError(err.to_string()))?;
            let token = self
                .exchange_external_idp_authorization_code(&client, &exchange_session, &code)
                .await?;
            let add_req = self.build_external_idp_add_credential_request(&exchange_session, token);
            self.add_credential(add_req).await
        }
        .await;

        match result {
            Ok(add_response) => {
                let message = add_response.message.clone();
                let mut sessions = self.external_idp_login_sessions.lock();
                if let Some(current) = sessions.get_mut(&session.session_id) {
                    current.status = ExternalIdpLoginStatus::Completed;
                    current.phase = ExternalIdpLoginPhase::Completed;
                    current.message = Some(message.clone());
                    current.credential_result = Some(add_response);
                    current.updated_at = Utc::now();
                }
                Ok(ExternalIdpCallbackAction::Html {
                    success: true,
                    title: "External IdP 登录完成".to_string(),
                    message,
                })
            }
            Err(err) => {
                let message = err.to_string();
                self.fail_external_idp_session_by_id(&session.session_id, &message);
                Ok(ExternalIdpCallbackAction::Html {
                    success: false,
                    title: "External IdP 登录失败".to_string(),
                    message,
                })
            }
        }
    }

    fn login_proxy_for_request(
        &self,
        req: &StartIdcDeviceLoginRequest,
    ) -> Result<Option<ProxyConfig>, AdminServiceError> {
        self.login_proxy_for_values(
            req.proxy_url.clone(),
            req.proxy_username.clone(),
            req.proxy_password.clone(),
            req.proxy_id.clone(),
        )
    }

    fn login_proxy_for_external_idp_probe(
        &self,
        req: &ExternalIdpProbeRequest,
    ) -> Result<Option<ProxyConfig>, AdminServiceError> {
        self.login_proxy_for_values(
            req.proxy_url.clone(),
            req.proxy_username.clone(),
            req.proxy_password.clone(),
            req.proxy_id.clone(),
        )
    }

    fn login_proxy_for_external_idp_login(
        &self,
        req: &StartExternalIdpLoginRequest,
    ) -> Result<Option<ProxyConfig>, AdminServiceError> {
        self.login_proxy_for_values(
            req.proxy_url.clone(),
            req.proxy_username.clone(),
            req.proxy_password.clone(),
            req.proxy_id.clone(),
        )
    }

    fn login_proxy_for_values(
        &self,
        proxy_url: Option<String>,
        proxy_username: Option<String>,
        proxy_password: Option<String>,
        proxy_id: Option<String>,
    ) -> Result<Option<ProxyConfig>, AdminServiceError> {
        let credentials = KiroCredentials {
            proxy_url,
            proxy_username,
            proxy_password,
            proxy_id,
            ..Default::default()
        };
        self.token_manager
            .effective_proxy_for_credentials(&credentials)
            .map_err(|err| AdminServiceError::InvalidCredential(err.to_string()))
    }

    async fn fetch_kiro_login_metadata(
        &self,
        client: &reqwest::Client,
        domain_name: &str,
    ) -> Result<KiroLoginMetadataPayload, AdminServiceError> {
        let body = serde_cbor::to_vec(&KiroGetLoginMetadataRequest {
            domain_name: domain_name.to_string(),
        })
        .map_err(|err| {
            AdminServiceError::InternalError(format!("构造 GetLoginMetadata CBOR 请求失败: {err}"))
        })?;

        let response = client
            .post(KIRO_WEB_PORTAL_ENDPOINT)
            .header("content-type", "application/cbor")
            .header("accept", "application/cbor")
            .header("smithy-protocol", "rpc-v2-cbor")
            .body(body)
            .send()
            .await
            .map_err(|err| {
                AdminServiceError::UpstreamError(format!("GetLoginMetadata 请求失败: {err}"))
            })?;

        let status = response.status();
        let bytes = response.bytes().await.unwrap_or_default();
        if !status.is_success() {
            let body_text = kiro_login_metadata_error_message(&bytes);
            let message =
                summarize_upstream_error(status.as_u16(), &body_text, UPSTREAM_ERROR_EXCERPT_CHARS);
            return Err(AdminServiceError::UpstreamError(format!(
                "GetLoginMetadata 失败 ({status}): {message}"
            )));
        }

        parse_kiro_login_metadata_payload_cbor(&bytes)
    }

    async fn fetch_oidc_discovery(
        &self,
        client: &reqwest::Client,
        issuer_url: &str,
    ) -> Result<ExternalIdpOidcDiscoverySummary, AdminServiceError> {
        let discovery_url = oidc_discovery_url(issuer_url)?;
        let response = client
            .get(discovery_url)
            .header("accept", "application/json")
            .send()
            .await
            .map_err(|err| {
                AdminServiceError::UpstreamError(format!("OIDC discovery 请求失败: {err}"))
            })?;

        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        if !status.is_success() {
            let message =
                summarize_upstream_error(status.as_u16(), &text, UPSTREAM_ERROR_EXCERPT_CHARS);
            return Err(AdminServiceError::UpstreamError(format!(
                "OIDC discovery 失败 ({status}): {message}"
            )));
        }

        let document: OidcDiscoveryDocument = serde_json::from_str(&text).map_err(|err| {
            AdminServiceError::UpstreamError(format!("解析 OIDC discovery 响应失败: {err}"))
        })?;

        Ok(ExternalIdpOidcDiscoverySummary {
            issuer: normalize_optional_url(document.issuer.as_deref(), "issuer")?,
            authorization_endpoint: normalize_external_idp_endpoint_url(
                document.authorization_endpoint.as_deref(),
                "authorization_endpoint",
            )?,
            token_endpoint: normalize_external_idp_endpoint_url(
                document.token_endpoint.as_deref(),
                "token_endpoint",
            )?,
            device_authorization_endpoint: normalize_external_idp_endpoint_url(
                document.device_authorization_endpoint.as_deref(),
                "device_authorization_endpoint",
            )?,
            code_challenge_methods_supported: document
                .code_challenge_methods_supported
                .unwrap_or_default(),
            grant_types_supported: document.grant_types_supported.unwrap_or_default(),
            response_types_supported: document.response_types_supported.unwrap_or_default(),
            scopes_supported: document.scopes_supported.unwrap_or_default(),
            token_endpoint_auth_methods_supported: document
                .token_endpoint_auth_methods_supported
                .unwrap_or_default(),
        })
    }

    async fn exchange_external_idp_authorization_code(
        &self,
        client: &reqwest::Client,
        session: &ExternalIdpLoginSession,
        code: &str,
    ) -> Result<ExternalIdpAuthorizationCodeTokenResponse, AdminServiceError> {
        let token_endpoint = session
            .token_endpoint
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                AdminServiceError::InvalidCredential(
                    "ExternalIdP token exchange 缺少 tokenEndpoint".to_string(),
                )
            })?;
        let client_id = session
            .client_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                AdminServiceError::InvalidCredential(
                    "ExternalIdP token exchange 缺少 clientId".to_string(),
                )
            })?;
        let code_verifier = session
            .idp_code_verifier
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                AdminServiceError::InvalidCredential(
                    "ExternalIdP token exchange 缺少 PKCE verifier".to_string(),
                )
            })?;
        let redirect_uri = session
            .idp_redirect_uri
            .as_deref()
            .or(session.callback_url.as_deref())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                AdminServiceError::InvalidCredential(
                    "ExternalIdP token exchange 缺少 redirectUri".to_string(),
                )
            })?;

        let form = build_external_idp_authorization_code_token_form(
            client_id,
            code,
            redirect_uri,
            code_verifier,
            session.scopes.as_deref(),
        );

        let response = client
            .post(token_endpoint)
            .header("accept", "application/json")
            .form(&form)
            .send()
            .await
            .map_err(|err| {
                AdminServiceError::UpstreamError(format!(
                    "ExternalIdP token exchange 请求失败: {err}"
                ))
            })?;

        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        if !status.is_success() {
            let message =
                summarize_upstream_error(status.as_u16(), &text, UPSTREAM_ERROR_EXCERPT_CHARS);
            return Err(AdminServiceError::UpstreamError(format!(
                "ExternalIdP token exchange 失败 ({status}): {message}"
            )));
        }

        let token: ExternalIdpAuthorizationCodeTokenResponse = serde_json::from_str(&text)
            .map_err(|err| {
                AdminServiceError::UpstreamError(format!(
                    "解析 ExternalIdP token exchange 响应失败: {err}"
                ))
            })?;
        if token
            .refresh_token
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .is_none()
        {
            return Err(AdminServiceError::InvalidCredential(
                "ExternalIdP 未返回 refresh_token；需要 IdP 允许 offline_access/refresh token"
                    .to_string(),
            ));
        }

        Ok(token)
    }

    async fn start_external_idp_device_authorization(
        &self,
        client: &reqwest::Client,
        device_authorization_endpoint: &str,
        client_id: &str,
        scopes: &str,
        audience: Option<&str>,
    ) -> Result<ExternalIdpDeviceAuthorizationResponse, AdminServiceError> {
        let mut form = vec![
            ("client_id", client_id.to_string()),
            ("scope", scopes.to_string()),
        ];
        if let Some(audience) = audience.map(str::trim).filter(|value| !value.is_empty()) {
            form.push(("audience", audience.to_string()));
        }

        let response = client
            .post(device_authorization_endpoint)
            .header("accept", "application/json")
            .form(&form)
            .send()
            .await
            .map_err(|err| {
                AdminServiceError::UpstreamError(format!(
                    "ExternalIdP device authorization 请求失败: {err}"
                ))
            })?;

        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        if !status.is_success() {
            let message =
                summarize_upstream_error(status.as_u16(), &text, UPSTREAM_ERROR_EXCERPT_CHARS);
            let full = format!("ExternalIdP device authorization 失败 ({status}): {message}");
            if status.as_u16() == 400 {
                return Err(AdminServiceError::InvalidCredential(full));
            }
            return Err(AdminServiceError::UpstreamError(full));
        }

        let authorization: ExternalIdpDeviceAuthorizationResponse = serde_json::from_str(&text)
            .map_err(|err| {
                AdminServiceError::UpstreamError(format!(
                    "解析 ExternalIdP device authorization 响应失败: {err}"
                ))
            })?;
        if authorization.device_code.trim().is_empty()
            || authorization.user_code.trim().is_empty()
            || authorization.verification_uri.trim().is_empty()
        {
            return Err(AdminServiceError::UpstreamError(
                "ExternalIdP device authorization 响应缺少 device_code/user_code/verification_uri"
                    .to_string(),
            ));
        }
        Ok(authorization)
    }

    async fn poll_external_idp_device_token(
        &self,
        client: &reqwest::Client,
        session: &ExternalIdpLoginSession,
    ) -> Result<ExternalIdpDeviceTokenPollResult, AdminServiceError> {
        let token_endpoint = session
            .token_endpoint
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                AdminServiceError::InvalidCredential(
                    "ExternalIdP device token 缺少 tokenEndpoint".to_string(),
                )
            })?;
        let client_id = session
            .client_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                AdminServiceError::InvalidCredential(
                    "ExternalIdP device token 缺少 clientId".to_string(),
                )
            })?;
        let device_code = session
            .device_code
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                AdminServiceError::InvalidCredential(
                    "ExternalIdP device token 缺少 deviceCode".to_string(),
                )
            })?;

        let form = [
            ("grant_type", IDC_DEVICE_GRANT_TYPE),
            ("client_id", client_id),
            ("device_code", device_code),
        ];

        let response = client
            .post(token_endpoint)
            .header("accept", "application/json")
            .form(&form)
            .send()
            .await
            .map_err(|err| {
                AdminServiceError::UpstreamError(format!(
                    "ExternalIdP device token 请求失败: {err}"
                ))
            })?;

        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        if status.is_success() {
            let token: ExternalIdpAuthorizationCodeTokenResponse = serde_json::from_str(&text)
                .map_err(|err| {
                    AdminServiceError::UpstreamError(format!(
                        "解析 ExternalIdP device token 响应失败: {err}"
                    ))
                })?;
            if token
                .refresh_token
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .is_none()
            {
                return Ok(ExternalIdpDeviceTokenPollResult::Failed(
                    "ExternalIdP device token 未返回 refresh_token；需要 IdP 允许 offline_access/refresh token"
                        .to_string(),
                ));
            }
            return Ok(ExternalIdpDeviceTokenPollResult::Completed(token));
        }

        let parsed_error = serde_json::from_str::<AwsOidcErrorResponse>(&text).ok();
        let code = parsed_error
            .as_ref()
            .and_then(|value| value.error.as_deref())
            .unwrap_or_default();
        let description = parsed_error
            .as_ref()
            .and_then(|value| value.error_description.as_deref())
            .unwrap_or_else(|| text.as_str());

        match code {
            "authorization_pending" => Ok(ExternalIdpDeviceTokenPollResult::Pending),
            "slow_down" => Ok(ExternalIdpDeviceTokenPollResult::SlowDown),
            "expired_token" => Ok(ExternalIdpDeviceTokenPollResult::Expired(
                "External IdP 授权码已过期，请重新开始登录".to_string(),
            )),
            "access_denied" | "authorization_declined" => {
                Ok(ExternalIdpDeviceTokenPollResult::Failed(
                    "用户拒绝了 External IdP 授权请求".to_string(),
                ))
            }
            _ => Ok(ExternalIdpDeviceTokenPollResult::Failed(
                external_idp_secret_required_error_message(description).unwrap_or_else(|| {
                    format!("ExternalIdP device token 失败 ({status}): {description}")
                }),
            )),
        }
    }

    async fn register_idc_device_client(
        &self,
        client: &reqwest::Client,
        region: &str,
        start_url: &str,
    ) -> Result<AwsClientRegistrationResponse, AdminServiceError> {
        let url = format!("https://oidc.{region}.amazonaws.com/client/register");
        let scopes: Vec<_> = IDC_GRANT_SCOPES
            .iter()
            .map(|scope| (*scope).to_string())
            .collect();
        let body = serde_json::json!({
            "clientName": IDC_DEVICE_LOGIN_CLIENT_NAME,
            "clientType": "public",
            "scopes": scopes,
            "grantTypes": [IDC_DEVICE_GRANT_TYPE, IDC_REFRESH_GRANT_TYPE],
            "issuerUrl": start_url
        });
        self.post_aws_oidc_json(client, region, &url, body, "RegisterClient")
            .await
    }

    async fn start_idc_device_authorization(
        &self,
        client: &reqwest::Client,
        region: &str,
        client_id: &str,
        client_secret: &str,
        start_url: &str,
    ) -> Result<AwsStartDeviceAuthorizationResponse, AdminServiceError> {
        let url = format!("https://oidc.{region}.amazonaws.com/device_authorization");
        let body = serde_json::json!({
            "clientId": client_id,
            "clientSecret": client_secret,
            "startUrl": start_url
        });
        self.post_aws_oidc_json(client, region, &url, body, "StartDeviceAuthorization")
            .await
    }

    async fn poll_idc_device_token(
        &self,
        client: &reqwest::Client,
        session: &IdcDeviceLoginSession,
    ) -> Result<DeviceTokenPollResult, AdminServiceError> {
        let url = format!("https://oidc.{}.amazonaws.com/token", session.region);
        let body = serde_json::json!({
            "clientId": session.client_id,
            "clientSecret": session.client_secret,
            "grantType": IDC_DEVICE_GRANT_TYPE,
            "deviceCode": session.device_code
        });

        let response = client
            .post(&url)
            .header("content-type", "application/json")
            .header("x-amz-user-agent", "aws-sdk-js/3.980.0 KiroIDE")
            .header("user-agent", self.aws_sso_user_agent())
            .header("host", format!("oidc.{}.amazonaws.com", session.region))
            .header("amz-sdk-invocation-id", Uuid::new_v4().to_string())
            .header("amz-sdk-request", "attempt=1; max=4")
            .json(&body)
            .send()
            .await
            .map_err(|err| {
                AdminServiceError::UpstreamError(format!("CreateToken 请求失败: {err}"))
            })?;

        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        if status.is_success() {
            let token = serde_json::from_str::<AwsCreateTokenResponse>(&text).map_err(|err| {
                AdminServiceError::UpstreamError(format!("解析 CreateToken 响应失败: {err}"))
            })?;
            return Ok(DeviceTokenPollResult::Completed(token));
        }

        let parsed_error = serde_json::from_str::<AwsOidcErrorResponse>(&text).ok();
        let code = parsed_error
            .as_ref()
            .and_then(|value| value.error.as_deref())
            .unwrap_or_default();
        let description = parsed_error
            .as_ref()
            .and_then(|value| value.error_description.as_deref())
            .unwrap_or_else(|| text.as_str());

        match code {
            "authorization_pending" => Ok(DeviceTokenPollResult::Pending),
            "slow_down" => Ok(DeviceTokenPollResult::SlowDown),
            "expired_token" => Ok(DeviceTokenPollResult::Expired(
                "授权码已过期，请重新开始登录".to_string(),
            )),
            "access_denied" => Ok(DeviceTokenPollResult::Failed(
                "用户拒绝了授权请求".to_string(),
            )),
            _ => Ok(DeviceTokenPollResult::Failed(format!(
                "CreateToken 失败 ({status}): {description}"
            ))),
        }
    }

    async fn post_aws_oidc_json<T>(
        &self,
        client: &reqwest::Client,
        region: &str,
        url: &str,
        body: serde_json::Value,
        api: &'static str,
    ) -> Result<T, AdminServiceError>
    where
        T: for<'de> Deserialize<'de>,
    {
        let response = client
            .post(url)
            .header("content-type", "application/json")
            .header("x-amz-user-agent", "aws-sdk-js/3.980.0 KiroIDE")
            .header("user-agent", self.aws_sso_user_agent())
            .header("host", format!("oidc.{region}.amazonaws.com"))
            .header("amz-sdk-invocation-id", Uuid::new_v4().to_string())
            .header("amz-sdk-request", "attempt=1; max=4")
            .json(&body)
            .send()
            .await
            .map_err(|err| AdminServiceError::UpstreamError(format!("{api} 请求失败: {err}")))?;

        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        if !status.is_success() {
            let message = aws_oidc_error_message(&text);
            let full = format!("{api} 失败 ({status}): {message}");
            if status.as_u16() == 400 {
                return Err(AdminServiceError::InvalidCredential(full));
            }
            return Err(AdminServiceError::UpstreamError(full));
        }

        serde_json::from_str(&text)
            .map_err(|err| AdminServiceError::UpstreamError(format!("解析 {api} 响应失败: {err}")))
    }

    fn aws_sso_user_agent(&self) -> String {
        let config = self.token_manager.config();
        format!(
            "aws-sdk-js/3.980.0 ua/2.1 os/{} lang/js md/nodejs#{} api/sso-oidc#3.980.0 m/E KiroIDE",
            config.system_version, config.node_version
        )
    }

    fn build_idc_device_add_credential_request(
        &self,
        session: &IdcDeviceLoginSession,
        token: AwsCreateTokenResponse,
    ) -> AddCredentialRequest {
        let req = &session.request;
        AddCredentialRequest {
            refresh_token: token.refresh_token,
            auth_method: "idc".to_string(),
            // BuilderId device-code tokens should behave like the existing KAM/IDE
            // idc imports, which do not persist provider=BuilderId. Persisting it
            // makes quota lookups inject Kiro's fixed BuilderId profileArn and can
            // report a paid account as KIRO FREE.
            provider: if session.provider.eq_ignore_ascii_case("Enterprise") {
                Some(session.provider.clone())
            } else {
                None
            },
            profile_arn: req.profile_arn.clone(),
            client_id: Some(session.client_id.clone()),
            client_secret: Some(session.client_secret.clone()),
            token_endpoint: None,
            issuer_url: None,
            scopes: None,
            audience: None,
            priority: req.priority,
            max_concurrency: req.max_concurrency,
            rate_limit_cooldown_enabled: None,
            rate_limit_bucket_capacity: None,
            rate_limit_refill_per_second: None,
            // The device-login region is the AWS SSO OIDC/Auth region. Persist it
            // as authRegion so it does not become the API region fallback for
            // Enterprise ListAvailableModels/runtime calls.
            region: None,
            auth_region: req
                .auth_region
                .clone()
                .or_else(|| req.region.clone())
                .or_else(|| Some(session.region.clone())),
            api_region: req.api_region.clone(),
            machine_id: req.machine_id.clone(),
            start_url: if session.provider.eq_ignore_ascii_case("Enterprise") {
                Some(session.start_url.clone())
            } else {
                None
            },
            email: None,
            user_id: None,
            account_type: req.account_type.clone(),
            source_supplier_id: req.source_supplier_id.clone(),
            source_supplier_name: req.source_supplier_name.clone(),
            source_batch: req.source_batch.clone(),
            credential_groups: req.credential_groups.clone(),
            allowed_models: None,
            blocked_models: None,
            available_model_ids: None,
            proxy_url: req.proxy_url.clone(),
            proxy_username: req.proxy_username.clone(),
            proxy_password: req.proxy_password.clone(),
            proxy_id: req.proxy_id.clone(),
        }
    }

    fn build_external_idp_add_credential_request(
        &self,
        session: &ExternalIdpLoginSession,
        token: ExternalIdpAuthorizationCodeTokenResponse,
    ) -> AddCredentialRequest {
        let req = &session.request;
        AddCredentialRequest {
            refresh_token: token.refresh_token.unwrap_or_default(),
            auth_method: "external_idp".to_string(),
            provider: Some("ExternalIdp".to_string()),
            profile_arn: req.profile_arn.clone(),
            client_id: session.client_id.clone(),
            client_secret: None,
            token_endpoint: session.token_endpoint.clone(),
            issuer_url: session.issuer_url.clone(),
            scopes: session.scopes.clone(),
            audience: session.audience.clone(),
            priority: req.priority,
            max_concurrency: req.max_concurrency,
            rate_limit_cooldown_enabled: None,
            rate_limit_bucket_capacity: None,
            rate_limit_refill_per_second: None,
            region: None,
            auth_region: req.auth_region.clone(),
            api_region: req.api_region.clone(),
            machine_id: req.machine_id.clone(),
            start_url: None,
            email: req
                .work_email
                .clone()
                .or_else(|| session.login_hint.clone()),
            user_id: None,
            account_type: req.account_type.clone(),
            source_supplier_id: req.source_supplier_id.clone(),
            source_supplier_name: req.source_supplier_name.clone(),
            source_batch: req.source_batch.clone(),
            credential_groups: req.credential_groups.clone(),
            allowed_models: None,
            blocked_models: None,
            available_model_ids: None,
            proxy_url: req.proxy_url.clone(),
            proxy_username: req.proxy_username.clone(),
            proxy_password: req.proxy_password.clone(),
            proxy_id: req.proxy_id.clone(),
        }
    }

    /// 设置凭据禁用状态
    pub fn set_disabled(&self, id: u64, disabled: bool) -> Result<(), AdminServiceError> {
        self.ensure_runtime_write_leader()?;

        // 先获取当前凭据 ID，用于判断是否需要切换
        let snapshot = self.token_manager.snapshot();
        let current_id = snapshot.current_id;

        self.token_manager
            .set_disabled(id, disabled)
            .map_err(|e| self.classify_error(e, id))?;

        // 只有禁用的是当前凭据时才尝试切换到下一个
        if disabled && id == current_id {
            let _ = self.token_manager.switch_to_next();
        }
        Ok(())
    }

    /// 设置凭据优先级
    pub fn set_priority(&self, id: u64, priority: u32) -> Result<(), AdminServiceError> {
        self.ensure_runtime_write_leader()?;

        self.token_manager
            .set_priority(id, priority)
            .map_err(|e| self.classify_error(e, id))
    }

    /// 设置凭据并发上限
    pub fn set_max_concurrency(
        &self,
        id: u64,
        max_concurrency: Option<u32>,
    ) -> Result<(), AdminServiceError> {
        self.ensure_runtime_write_leader()?;

        self.token_manager
            .set_max_concurrency(id, max_concurrency)
            .map_err(|e| self.classify_error(e, id))
    }

    /// 设置凭据级 token bucket 配置
    pub fn set_rate_limit_config(
        &self,
        id: u64,
        rate_limit_cooldown_enabled: Option<Option<bool>>,
        rate_limit_bucket_capacity: Option<Option<f64>>,
        rate_limit_refill_per_second: Option<Option<f64>>,
    ) -> Result<(), AdminServiceError> {
        self.ensure_runtime_write_leader()?;

        self.token_manager
            .set_rate_limit_config(
                id,
                rate_limit_cooldown_enabled,
                rate_limit_bucket_capacity,
                rate_limit_refill_per_second,
            )
            .map_err(|e| self.classify_error(e, id))
    }

    pub fn set_model_policy(
        &self,
        id: u64,
        req: SetCredentialModelPolicyRequest,
    ) -> Result<(), AdminServiceError> {
        self.ensure_runtime_write_leader()?;

        self.token_manager
            .set_credential_model_policy(
                id,
                req.account_type,
                req.allowed_models,
                req.blocked_models,
                req.clear_runtime_model_restrictions,
            )
            .map_err(|e| self.classify_error(e, id))
    }

    pub fn set_source(
        &self,
        id: u64,
        req: SetCredentialSourceRequest,
    ) -> Result<(), AdminServiceError> {
        self.ensure_runtime_write_leader()?;

        self.token_manager
            .set_credential_source(
                id,
                req.source_supplier_id,
                req.source_supplier_name,
                req.source_batch,
            )
            .map_err(|e| self.classify_error(e, id))
    }

    pub fn set_groups(
        &self,
        id: u64,
        req: SetCredentialGroupsRequest,
    ) -> Result<(), AdminServiceError> {
        self.ensure_runtime_write_leader()?;
        let credential_groups =
            self.normalize_and_validate_credential_groups(&req.credential_groups)?;

        self.token_manager
            .set_credential_groups(id, credential_groups)
            .map_err(|e| self.classify_error(e, id))
    }

    pub fn set_proxy(
        &self,
        id: u64,
        req: SetCredentialProxyRequest,
    ) -> Result<(), AdminServiceError> {
        self.ensure_runtime_write_leader()?;

        self.token_manager
            .set_credential_proxy(
                id,
                req.mode,
                req.proxy_id,
                req.proxy_url,
                req.proxy_username,
                req.proxy_password,
            )
            .map_err(|e| self.classify_error(e, id))
    }

    pub async fn get_profiles(
        &self,
        id: u64,
    ) -> Result<CredentialProfilesResponse, AdminServiceError> {
        self.sync_runtime_state_for_read()?;

        let (profiles, selected_profile_arn) = self
            .token_manager
            .list_available_profiles_for(id)
            .await
            .map_err(|e| self.classify_balance_error(e, id))?;

        Ok(CredentialProfilesResponse {
            id,
            selected_profile_arn,
            profiles,
        })
    }

    pub fn set_profile(
        &self,
        id: u64,
        req: SetCredentialProfileRequest,
    ) -> Result<(), AdminServiceError> {
        self.ensure_runtime_write_leader()?;

        self.token_manager
            .set_profile_arn(id, req.profile_arn)
            .map_err(|e| self.classify_error(e, id))?;
        self.invalidate_balance_cache(id);

        Ok(())
    }

    pub fn clear_runtime_model_restrictions(&self, id: u64) -> Result<bool, AdminServiceError> {
        self.ensure_runtime_write_leader()?;

        self.token_manager
            .clear_runtime_model_restrictions_for_credential(id)
            .map_err(|e| self.classify_error(e, id))
    }

    pub fn clear_suspicious_activity(&self, id: u64) -> Result<bool, AdminServiceError> {
        self.ensure_runtime_write_leader()?;

        self.token_manager
            .clear_suspicious_activity_for_credential(id)
            .map_err(|e| self.classify_error(e, id))
    }

    /// 重置失败计数并重新启用
    pub fn reset_and_enable(&self, id: u64) -> Result<(), AdminServiceError> {
        self.ensure_runtime_write_leader()?;

        self.token_manager
            .reset_and_enable(id)
            .map_err(|e| self.classify_error(e, id))
    }

    /// 获取凭据余额（带缓存）
    pub async fn get_balance(&self, id: u64) -> Result<BalanceResponse, AdminServiceError> {
        self.sync_runtime_state_for_read()?;
        self.sync_balance_cache_if_changed()
            .map_err(|e| AdminServiceError::InternalError(e.to_string()))?;
        let profile_arn = self
            .token_manager
            .effective_profile_arn_for(id)
            .map_err(|e| self.classify_error(e, id))?;

        if let Some(cached) = self.cached_balance(id, profile_arn.as_deref()) {
            tracing::debug!("凭据 #{} 余额命中本地缓存", id);
            return Ok(cached);
        }

        if let Err(e) = self.sync_balance_cache_from_state() {
            tracing::warn!("同步共享余额缓存失败，将直接回源查询: {}", e);
        } else if let Some(cached) = self.cached_balance(id, profile_arn.as_deref()) {
            tracing::debug!("凭据 #{} 余额命中共享缓存", id);
            return Ok(cached);
        }

        // 缓存未命中或已过期，从上游获取
        let balance = self.fetch_balance(id).await?;

        // 更新缓存
        {
            let mut cache = self.balance_cache.lock();
            cache.insert(
                id,
                CachedBalanceRecord {
                    cached_at: Utc::now().timestamp() as f64,
                    profile_arn: balance.profile_arn.clone(),
                    data: balance.clone(),
                },
            );
        }
        self.save_balance_cache();

        Ok(balance)
    }

    /// 从上游获取余额（无缓存）
    async fn fetch_balance(&self, id: u64) -> Result<BalanceResponse, AdminServiceError> {
        let usage = self
            .token_manager
            .get_usage_limits_for(id)
            .await
            .map_err(|e| self.classify_balance_error(e, id))?;

        let profile_arn = self
            .token_manager
            .effective_profile_arn_for(id)
            .map_err(|e| self.classify_error(e, id))?;

        Ok(BalanceResponse::from_usage(id, profile_arn, &usage))
    }

    /// 设置凭据超额使用开关
    pub async fn set_overage_status(
        &self,
        id: u64,
        enabled: bool,
    ) -> Result<BalanceResponse, AdminServiceError> {
        self.ensure_runtime_write_leader()?;

        let usage = self
            .token_manager
            .set_overage_status_for(id, enabled)
            .await
            .map_err(|e| self.classify_balance_error(e, id))?;
        let profile_arn = self
            .token_manager
            .effective_profile_arn_for(id)
            .map_err(|e| self.classify_error(e, id))?;
        let balance = BalanceResponse::from_usage(id, profile_arn, &usage);

        self.cache_balance(id, balance.clone());

        Ok(balance)
    }

    /// 添加新凭据
    pub async fn add_credential(
        &self,
        req: AddCredentialRequest,
    ) -> Result<AddCredentialResponse, AdminServiceError> {
        self.ensure_runtime_write_leader()?;
        let normalized_credential_groups = req
            .credential_groups
            .as_ref()
            .map(|groups| self.normalize_and_validate_credential_groups(groups))
            .transpose()?
            .unwrap_or_default();

        // 构建凭据对象
        let email = req.email.clone();
        let is_enterprise_provider = req
            .provider
            .as_deref()
            .is_some_and(|provider| provider.trim().eq_ignore_ascii_case("enterprise"));
        let is_external_idp = req.auth_method.trim().eq_ignore_ascii_case("external_idp")
            || req.auth_method.trim().eq_ignore_ascii_case("external-idp")
            || req.provider.as_deref().is_some_and(|provider| {
                let provider = provider.trim();
                provider.eq_ignore_ascii_case("externalidp")
                    || provider.eq_ignore_ascii_case("external-idp")
                    || provider.eq_ignore_ascii_case("external_idp")
                    || provider.eq_ignore_ascii_case("external idp")
            })
            || req
                .issuer_url
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .is_some();
        if is_enterprise_provider {
            if req
                .client_id
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .is_none()
                || req
                    .client_secret
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .is_none()
            {
                return Err(AdminServiceError::InvalidCredential(
                    "Enterprise 账号必须提供 clientId/clientSecret".to_string(),
                ));
            }
            if req
                .start_url
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .is_none()
            {
                return Err(AdminServiceError::InvalidCredential(
                    "Enterprise 账号必须提供 startUrl".to_string(),
                ));
            }
            if req
                .region
                .as_deref()
                .or(req.auth_region.as_deref())
                .or(req.api_region.as_deref())
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .is_none()
            {
                return Err(AdminServiceError::InvalidCredential(
                    "Enterprise 账号必须提供 region/authRegion/apiRegion 之一".to_string(),
                ));
            }
        }
        if is_external_idp {
            if req
                .client_id
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .is_none()
            {
                return Err(AdminServiceError::InvalidCredential(
                    "ExternalIdP 账号必须提供 clientId".to_string(),
                ));
            }
            if req
                .issuer_url
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .is_none()
            {
                return Err(AdminServiceError::InvalidCredential(
                    "ExternalIdP 账号必须提供 issuerUrl".to_string(),
                ));
            }
        }
        let new_cred = KiroCredentials {
            id: None,
            access_token: None,
            refresh_token: Some(req.refresh_token),
            profile_arn: req.profile_arn,
            expires_at: None,
            auth_method: Some(if is_external_idp {
                "external_idp".to_string()
            } else if is_enterprise_provider {
                "idc".to_string()
            } else {
                req.auth_method
            }),
            provider: if is_external_idp && req.provider.is_none() {
                Some("ExternalIdp".to_string())
            } else {
                req.provider
            },
            client_id: req.client_id,
            client_secret: req.client_secret,
            token_endpoint: req.token_endpoint,
            issuer_url: req.issuer_url,
            scopes: req.scopes,
            audience: req.audience,
            start_url: req.start_url,
            priority: req.priority,
            max_concurrency: req.max_concurrency,
            rate_limit_cooldown_enabled: req.rate_limit_cooldown_enabled,
            rate_limit_bucket_capacity: req.rate_limit_bucket_capacity,
            rate_limit_refill_per_second: req.rate_limit_refill_per_second,
            region: req.region,
            auth_region: req.auth_region,
            api_region: req.api_region,
            machine_id: req.machine_id,
            email: req.email,
            user_id: req.user_id,
            subscription_title: None, // 将在首次获取使用额度时自动更新
            subscription_type: None,
            account_type: req.account_type,
            source_supplier_id: req.source_supplier_id,
            source_supplier_name: req.source_supplier_name,
            source_batch: req.source_batch,
            credential_groups: normalized_credential_groups,
            allowed_models: req.allowed_models.unwrap_or_default(),
            blocked_models: req.blocked_models.unwrap_or_default(),
            runtime_model_restrictions: Vec::new(),
            available_model_ids: req.available_model_ids.unwrap_or_default(),
            available_models_cached_at: None,
            imported_at: None,
            proxy_url: req.proxy_url,
            proxy_username: req.proxy_username,
            proxy_password: req.proxy_password,
            proxy_id: req.proxy_id,
            disabled: false, // 新添加的凭据默认启用
            disabled_reason: None,
            disabled_at: None,
            last_error_status: None,
            last_error_summary: None,
            suspicious_activity_count: 0,
            suspicious_activity_first_seen_at: None,
            suspicious_activity_last_seen_at: None,
            suspicious_activity_quarantine_until: None,
            suspicious_activity_recovery_success_count: 0,
        };

        // 调用 token_manager 添加凭据
        let credential_id = self
            .token_manager
            .add_credential(new_cred)
            .await
            .map_err(|e| self.classify_add_error(e))?;

        // 主动获取订阅信息，避免首次请求时 Free 账号绕过 Opus 模型过滤
        let usage_info = match self.token_manager.get_usage_limits_for(credential_id).await {
            Ok(usage) => Some(usage),
            Err(e) => {
                tracing::warn!("添加凭据后获取订阅等级失败（不影响凭据添加）: {}", e);
                None
            }
        };

        let credential_snapshot = self
            .token_manager
            .snapshot()
            .entries
            .into_iter()
            .find(|entry| entry.id == credential_id);

        Ok(AddCredentialResponse {
            success: true,
            message: format!("凭据添加成功，ID: {}", credential_id),
            credential_id,
            email: usage_info
                .as_ref()
                .and_then(|usage| usage.email().map(str::to_string))
                .or_else(|| {
                    credential_snapshot
                        .as_ref()
                        .and_then(|entry| entry.email.clone())
                })
                .or(email),
            user_id: usage_info
                .as_ref()
                .and_then(|usage| usage.user_id().map(str::to_string))
                .or_else(|| {
                    credential_snapshot
                        .as_ref()
                        .and_then(|entry| entry.user_id.clone())
                }),
            provider: credential_snapshot
                .as_ref()
                .and_then(|entry| entry.provider.clone()),
            subscription_title: usage_info
                .as_ref()
                .and_then(|usage| usage.subscription_title().map(str::to_string))
                .or_else(|| {
                    credential_snapshot
                        .as_ref()
                        .and_then(|entry| entry.subscription_title.clone())
                }),
            subscription_type: usage_info
                .as_ref()
                .and_then(|usage| usage.subscription_type().map(str::to_string))
                .or_else(|| {
                    credential_snapshot
                        .as_ref()
                        .and_then(|entry| entry.subscription_type.clone())
                }),
            auth_account_type: credential_snapshot
                .as_ref()
                .and_then(|entry| entry.auth_account_type.clone()),
            resolved_account_type: credential_snapshot
                .and_then(|entry| entry.resolved_account_type),
        })
    }

    /// 删除凭据
    pub fn delete_credential(&self, id: u64) -> Result<(), AdminServiceError> {
        self.ensure_runtime_write_leader()?;

        self.token_manager
            .delete_credential(id)
            .map_err(|e| self.classify_delete_error(e, id))?;

        // 清理已删除凭据的余额缓存
        {
            let mut cache = self.balance_cache.lock();
            cache.remove(&id);
        }
        self.save_balance_cache();

        Ok(())
    }

    /// 获取负载均衡模式
    pub fn get_load_balancing_mode(&self) -> Result<LoadBalancingModeResponse, AdminServiceError> {
        self.sync_runtime_state_for_read()?;
        let snapshot = self.token_manager.load_balancing_config_snapshot();
        let mut assigned_by_proxy_id: HashMap<String, usize> = HashMap::new();
        for entry in self.token_manager.snapshot().entries {
            if entry.disabled {
                continue;
            }
            if let Some(proxy_id) = entry
                .proxy_id
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                *assigned_by_proxy_id
                    .entry(proxy_id.to_string())
                    .or_insert(0) += 1;
            }
        }
        let proxy_pool = ProxyPoolConfigResponse {
            enabled: snapshot.proxy_pool.enabled,
            require_proxy: snapshot.proxy_pool.require_proxy,
            assignment_strategy: snapshot.proxy_pool.assignment_strategy,
            proxies: snapshot
                .proxy_pool
                .proxies
                .into_iter()
                .map(|proxy| {
                    let assigned_credentials = assigned_by_proxy_id
                        .get(proxy.id.trim())
                        .copied()
                        .unwrap_or(0);
                    ProxyPoolEntryResponse {
                        id: proxy.id,
                        url: proxy.url,
                        username: proxy.username,
                        password: proxy.password,
                        weight: proxy.weight,
                        enabled: proxy.enabled,
                        expected_egress_ip: proxy.expected_egress_ip,
                        assigned_credentials,
                    }
                })
                .collect(),
            failover: snapshot.proxy_pool.failover,
        };
        Ok(LoadBalancingModeResponse {
            mode: snapshot.mode,
            session_affinity_enabled: snapshot.session_affinity_enabled,
            queue_max_size: snapshot.queue_max_size,
            queue_max_wait_ms: snapshot.queue_max_wait_ms,
            rate_limit_cooldown_ms: snapshot.rate_limit_cooldown_ms,
            rate_limit_cooldown_enabled: snapshot.rate_limit_cooldown_enabled,
            suspicious_activity_cooldown_ms: snapshot.suspicious_activity_cooldown_ms,
            suspicious_activity_cooldown_enabled: snapshot.suspicious_activity_cooldown_enabled,
            suspicious_activity_prefer_clean_credentials: snapshot
                .suspicious_activity_prefer_clean_credentials,
            suspicious_activity_auto_disable_enabled: snapshot
                .suspicious_activity_auto_disable_enabled,
            suspicious_activity_auto_disable_threshold: snapshot
                .suspicious_activity_auto_disable_threshold,
            suspicious_activity_auto_disable_window_ms: snapshot
                .suspicious_activity_auto_disable_window_ms,
            suspicious_activity_auto_clear_enabled: snapshot.suspicious_activity_auto_clear_enabled,
            suspicious_activity_auto_clear_success_threshold: snapshot
                .suspicious_activity_auto_clear_success_threshold,
            suspicious_activity_auto_clear_after_ms: snapshot
                .suspicious_activity_auto_clear_after_ms,
            model_cooldown_enabled: snapshot.model_cooldown_enabled,
            default_max_concurrency: snapshot.default_max_concurrency,
            rate_limit_bucket_capacity: snapshot.rate_limit_bucket_capacity,
            rate_limit_refill_per_second: snapshot.rate_limit_refill_per_second,
            rate_limit_refill_min_per_second: snapshot.rate_limit_refill_min_per_second,
            rate_limit_refill_recovery_step_per_success: snapshot
                .rate_limit_refill_recovery_step_per_success,
            rate_limit_refill_backoff_factor: snapshot.rate_limit_refill_backoff_factor,
            request_weighting: snapshot.request_weighting,
            stream_dispatch_lease_release_enabled: snapshot.stream_dispatch_lease_release_enabled,
            stream_pre_sse_failover: snapshot.stream_pre_sse_failover,
            non_stream_body_read_timeout: snapshot.non_stream_body_read_timeout,
            kiro_request_body_guard: snapshot.kiro_request_body_guard,
            thinking_signature_validation_mode: snapshot.thinking_signature_validation_mode,
            response_thinking_signature_compat_enabled: snapshot
                .response_thinking_signature_compat_enabled,
            proxy_pool: proxy_pool,
            waiting_requests: snapshot.waiting_requests,
        })
    }

    pub fn get_model_capabilities_config(
        &self,
    ) -> Result<ModelCapabilitiesConfigResponse, AdminServiceError> {
        self.sync_runtime_state_for_read()?;
        Ok(ModelCapabilitiesConfigResponse {
            account_type_policies: self.token_manager.account_type_policies_snapshot(),
            account_type_dispatch_policies: self
                .token_manager
                .account_type_dispatch_policies_snapshot(),
            standard_account_type_presets: built_in_account_type_presets()
                .iter()
                .map(|preset| StandardAccountTypePresetResponse {
                    id: preset.id.to_string(),
                    display_name: preset.display_name.to_string(),
                    description: preset.description.to_string(),
                    subscription_title_examples: preset
                        .subscription_title_examples
                        .iter()
                        .map(|value| (*value).to_string())
                        .collect(),
                    recommended_policy: preset.recommended_policy(),
                    recommended_dispatch_policy: preset.recommended_dispatch_policy(),
                })
                .collect(),
        })
    }

    pub fn get_credential_groups_config(
        &self,
    ) -> Result<CredentialGroupsConfigResponse, AdminServiceError> {
        self.sync_runtime_state_for_read()?;
        let groups = self.token_manager.credential_group_catalog_snapshot();
        let known = groups
            .iter()
            .map(|group| (group.name.clone(), group.enabled))
            .collect::<BTreeMap<_, _>>();
        let credential_counts = self.credential_group_counts();
        let api_key_counts = self.scoped_api_key_group_counts();
        let mut usage_names = BTreeSet::new();
        usage_names.extend(known.keys().cloned());
        usage_names.extend(credential_counts.keys().cloned());
        usage_names.extend(api_key_counts.keys().cloned());
        usage_names.insert(DEFAULT_CREDENTIAL_GROUP.to_string());

        let usage = usage_names
            .into_iter()
            .map(|name| CredentialGroupUsageItem {
                credential_count: credential_counts.get(&name).copied().unwrap_or(0),
                api_key_count: api_key_counts.get(&name).copied().unwrap_or(0),
                enabled: known.get(&name).copied().unwrap_or(false),
                known: known.contains_key(&name),
                name,
            })
            .collect::<Vec<_>>();
        let unknown_credential_groups = usage
            .iter()
            .filter(|item| !item.known && item.credential_count > 0)
            .map(|item| item.name.clone())
            .collect::<Vec<_>>();
        let legacy_full_access_key = self
            .token_manager
            .config()
            .api_key
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty());

        Ok(CredentialGroupsConfigResponse {
            groups: groups
                .into_iter()
                .map(CredentialGroupConfigItem::from)
                .collect(),
            usage,
            legacy_full_access_key,
            unknown_credential_groups,
        })
    }

    pub fn get_model_catalog(&self) -> ModelCatalogResponse {
        ModelCatalogResponse {
            models: built_in_model_catalog()
                .iter()
                .map(|item| ModelCatalogItemResponse {
                    api_id: item.api_id.to_string(),
                    policy_id: item.policy_id.to_string(),
                    display_name: item.display_name.to_string(),
                })
                .collect(),
        }
    }

    pub fn set_credential_groups_config(
        &self,
        req: SetCredentialGroupsConfigRequest,
    ) -> Result<CredentialGroupsConfigResponse, AdminServiceError> {
        self.ensure_runtime_write_leader()?;

        let groups = Config::normalize_and_validate_credential_group_catalog(
            req.groups
                .into_iter()
                .map(CredentialGroupConfig::from)
                .collect(),
        )
        .map_err(|err| AdminServiceError::InvalidCredential(err.to_string()))?;
        self.validate_catalog_keeps_existing_references(&groups)?;

        let previous_groups = self.token_manager.credential_group_catalog_snapshot();
        self.token_manager
            .set_credential_group_catalog_config(groups.clone())
            .map_err(|err| AdminServiceError::InternalError(err.to_string()))?;

        if let Some(config_path) = self.token_manager.config().config_path() {
            let mut config = Config::load(config_path).map_err(|err| {
                let _ = self
                    .token_manager
                    .set_credential_group_catalog_config(previous_groups.clone());
                AdminServiceError::InternalError(err.to_string())
            })?;
            config.credential_groups = groups.clone();
            if let Err(err) = config.save() {
                let _ = self
                    .token_manager
                    .set_credential_group_catalog_config(previous_groups);
                return Err(AdminServiceError::InternalError(err.to_string()));
            }
        } else {
            tracing::warn!("配置文件路径未知，凭据分组目录仅通过运行时状态持久化");
        }

        self.get_credential_groups_config()
    }

    /// 设置负载均衡模式
    pub fn set_load_balancing_mode(
        &self,
        req: SetLoadBalancingModeRequest,
    ) -> Result<LoadBalancingModeResponse, AdminServiceError> {
        self.ensure_runtime_write_leader()?;

        if req.mode.is_none()
            && req.session_affinity_enabled.is_none()
            && req.queue_max_size.is_none()
            && req.queue_max_wait_ms.is_none()
            && req.rate_limit_cooldown_ms.is_none()
            && req.rate_limit_cooldown_enabled.is_none()
            && req.suspicious_activity_cooldown_ms.is_none()
            && req.suspicious_activity_cooldown_enabled.is_none()
            && req.suspicious_activity_prefer_clean_credentials.is_none()
            && req.suspicious_activity_auto_disable_enabled.is_none()
            && req.suspicious_activity_auto_disable_threshold.is_none()
            && req.suspicious_activity_auto_disable_window_ms.is_none()
            && req.suspicious_activity_auto_clear_enabled.is_none()
            && req
                .suspicious_activity_auto_clear_success_threshold
                .is_none()
            && req.suspicious_activity_auto_clear_after_ms.is_none()
            && req.model_cooldown_enabled.is_none()
            && req.default_max_concurrency.is_none()
            && req.rate_limit_bucket_capacity.is_none()
            && req.rate_limit_refill_per_second.is_none()
            && req.rate_limit_refill_min_per_second.is_none()
            && req.rate_limit_refill_recovery_step_per_success.is_none()
            && req.rate_limit_refill_backoff_factor.is_none()
            && req.request_weighting.is_none()
            && req.stream_dispatch_lease_release_enabled.is_none()
            && req.stream_pre_sse_failover.is_none()
            && req.non_stream_body_read_timeout.is_none()
            && req.kiro_request_body_guard.is_none()
            && req.thinking_signature_validation_mode.is_none()
            && req.response_thinking_signature_compat_enabled.is_none()
            && req.proxy_pool.is_none()
        {
            return self.get_load_balancing_mode();
        }
        if let Some(mode) = &req.mode {
            if mode != "priority" && mode != "balanced" {
                return Err(AdminServiceError::InvalidCredential(
                    "mode 必须是 'priority' 或 'balanced'".to_string(),
                ));
            }
        }

        self.token_manager
            .set_load_balancing_config(
                req.mode.clone(),
                req.queue_max_size,
                req.queue_max_wait_ms,
                req.rate_limit_cooldown_ms,
                req.rate_limit_cooldown_enabled,
                req.suspicious_activity_cooldown_ms,
                req.suspicious_activity_cooldown_enabled,
                req.suspicious_activity_prefer_clean_credentials,
                req.suspicious_activity_auto_disable_enabled,
                req.suspicious_activity_auto_disable_threshold,
                req.suspicious_activity_auto_disable_window_ms,
                req.suspicious_activity_auto_clear_enabled,
                req.suspicious_activity_auto_clear_success_threshold,
                req.suspicious_activity_auto_clear_after_ms,
                req.model_cooldown_enabled,
                req.default_max_concurrency,
                req.rate_limit_bucket_capacity,
                req.rate_limit_refill_per_second,
                req.rate_limit_refill_min_per_second,
                req.rate_limit_refill_recovery_step_per_success,
                req.rate_limit_refill_backoff_factor,
                req.request_weighting,
                req.stream_dispatch_lease_release_enabled,
                req.stream_pre_sse_failover,
                req.non_stream_body_read_timeout,
                req.kiro_request_body_guard,
                req.session_affinity_enabled,
                req.thinking_signature_validation_mode,
                req.response_thinking_signature_compat_enabled,
                req.proxy_pool,
            )
            .map_err(|e| AdminServiceError::InternalError(e.to_string()))?;

        self.get_load_balancing_mode()
    }

    pub fn set_model_capabilities_config(
        &self,
        req: SetModelCapabilitiesConfigRequest,
    ) -> Result<ModelCapabilitiesConfigResponse, AdminServiceError> {
        self.ensure_runtime_write_leader()?;

        if req.account_type_policies.is_none() && req.account_type_dispatch_policies.is_none() {
            return self.get_model_capabilities_config();
        }

        self.token_manager
            .set_account_type_strategy_config(
                req.account_type_policies,
                req.account_type_dispatch_policies,
            )
            .map_err(|e| AdminServiceError::InternalError(e.to_string()))?;

        self.get_model_capabilities_config()
    }

    /// 强制刷新指定凭据的 Token
    pub async fn force_refresh_token(&self, id: u64) -> Result<(), AdminServiceError> {
        self.ensure_runtime_write_leader()?;

        self.token_manager
            .force_refresh_token_for(id)
            .await
            .map_err(|e| self.classify_balance_error(e, id))
    }

    // ============ 余额缓存持久化 ============

    pub fn sync_balance_cache_from_state(&self) -> anyhow::Result<()> {
        let state_store = self.token_manager.state_store();
        let shared_cache = Self::load_pruned_balance_cache(&state_store)?;
        let mut local_cache = self.balance_cache.lock();
        local_cache.retain(|_, entry| Self::is_balance_cache_fresh(entry));
        for (id, shared_entry) in shared_cache {
            let should_replace = local_cache
                .get(&id)
                .map(|local_entry| local_entry.cached_at < shared_entry.cached_at)
                .unwrap_or(true);
            if should_replace {
                local_cache.insert(id, shared_entry);
            }
        }
        if let Ok(revisions) = state_store.state_change_revisions() {
            *self.last_balance_cache_revision.lock() = revisions.balance_cache;
        }
        Ok(())
    }

    fn load_pruned_balance_cache(
        state_store: &crate::state::StateStore,
    ) -> anyhow::Result<HashMap<u64, CachedBalanceRecord>> {
        let mut cache = state_store.load_balance_cache()?;
        for entry in cache.values_mut() {
            entry.data.normalize_cached_compat();
            if entry.data.profile_arn.is_none() {
                entry.data.profile_arn = entry.profile_arn.clone();
            }
            if entry.profile_arn.is_none() {
                entry.profile_arn = entry.data.profile_arn.clone();
            }
        }
        let original_len = cache.len();
        let pruned = Self::prune_expired_balance_cache(cache);
        if pruned.len() != original_len {
            state_store.save_balance_cache(&pruned)?;
            if let Err(err) = state_store.bump_state_change_revision(StateChangeKind::BalanceCache)
            {
                tracing::warn!("更新余额缓存修订号失败: {}", err);
            }
        }
        Ok(pruned)
    }

    fn cached_balance(&self, id: u64, profile_arn: Option<&str>) -> Option<BalanceResponse> {
        let cache = self.balance_cache.lock();
        cache
            .get(&id)
            .filter(|cached| Self::is_balance_cache_fresh(cached))
            .filter(|cached| cached.profile_arn.as_deref() == profile_arn)
            .map(|cached| cached.data.clone())
    }

    fn cache_balance(&self, id: u64, balance: BalanceResponse) {
        {
            let mut cache = self.balance_cache.lock();
            cache.insert(
                id,
                CachedBalanceRecord {
                    cached_at: Utc::now().timestamp() as f64,
                    profile_arn: balance.profile_arn.clone(),
                    data: balance,
                },
            );
        }
        self.save_balance_cache();
    }

    fn invalidate_balance_cache(&self, id: u64) {
        let removed = {
            let mut cache = self.balance_cache.lock();
            cache.remove(&id).is_some()
        };
        if removed {
            self.save_balance_cache();
        }
    }

    fn is_balance_cache_fresh(cached: &CachedBalanceRecord) -> bool {
        let now = Utc::now().timestamp() as f64;
        (now - cached.cached_at) < BALANCE_CACHE_TTL_SECS as f64
    }

    fn prune_expired_balance_cache(
        cache: HashMap<u64, CachedBalanceRecord>,
    ) -> HashMap<u64, CachedBalanceRecord> {
        cache
            .into_iter()
            .filter_map(|(id, entry)| {
                // 丢弃超过 TTL 的条目
                if Self::is_balance_cache_fresh(&entry) {
                    Some((id, entry))
                } else {
                    None
                }
            })
            .collect()
    }

    fn save_balance_cache(&self) {
        let cache = self.balance_cache.lock().clone();
        let state_store = self.token_manager.state_store();
        if let Err(e) = state_store.save_balance_cache(&cache) {
            tracing::warn!("保存余额缓存失败: {}", e);
            return;
        }

        match state_store.bump_state_change_revision(StateChangeKind::BalanceCache) {
            Ok(revision) => {
                if revision > 0 {
                    *self.last_balance_cache_revision.lock() = revision;
                }
            }
            Err(err) => tracing::warn!("更新余额缓存修订号失败: {}", err),
        }
    }

    fn sync_runtime_state_for_read(&self) -> Result<(), AdminServiceError> {
        self.token_manager
            .sync_external_state_if_changed()
            .map_err(|e| AdminServiceError::InternalError(e.to_string()))?;
        Ok(())
    }

    fn sync_balance_cache_if_changed(&self) -> anyhow::Result<bool> {
        let state_store = self.token_manager.state_store();
        let revisions = state_store.state_change_revisions()?;
        let mut last_revision = self.last_balance_cache_revision.lock();
        if revisions.balance_cache <= *last_revision {
            return Ok(false);
        }
        drop(last_revision);

        self.sync_balance_cache_from_state()?;

        last_revision = self.last_balance_cache_revision.lock();
        *last_revision = revisions.balance_cache;
        Ok(true)
    }

    pub fn resolve_write_route(&self) -> Result<AdminWriteRoute, AdminServiceError> {
        let Some(status) = self.runtime_write_status()? else {
            return Ok(AdminWriteRoute::Local);
        };

        if status.is_leader {
            return Ok(AdminWriteRoute::Local);
        }

        if let Some(leader_http_base_url) = status.leader_http_base_url.clone() {
            return Ok(AdminWriteRoute::Forward(leader_http_base_url));
        }

        Err(AdminServiceError::NotLeader {
            instance_id: status.instance_id,
            leader_id: status.leader_id,
        })
    }

    fn ensure_runtime_write_leader(&self) -> Result<(), AdminServiceError> {
        let Some(status) = self.runtime_write_status()? else {
            return Ok(());
        };

        if status.is_leader {
            return Ok(());
        }

        Err(AdminServiceError::NotLeader {
            instance_id: status.instance_id,
            leader_id: status.leader_id,
        })
    }

    fn runtime_write_status(&self) -> Result<Option<RuntimeCoordinationStatus>, AdminServiceError> {
        let state_store = self.token_manager.state_store();
        let Some(mut status) = state_store
            .runtime_coordination_status()
            .map_err(|e| AdminServiceError::InternalError(e.to_string()))?
        else {
            return Ok(None);
        };

        if !status.is_leader
            && (status.leader_id.is_none() || status.leader_http_base_url.is_none())
        {
            if let Some(updated_status) = state_store
                .runtime_coordination_tick()
                .map_err(|e| AdminServiceError::InternalError(e.to_string()))?
            {
                status = updated_status;
            }
        }

        Ok(Some(status))
    }

    // ============ 错误分类 ============

    /// 分类简单操作错误（set_disabled, set_priority, reset_and_enable）
    fn classify_error(&self, e: anyhow::Error, id: u64) -> AdminServiceError {
        let msg = e.to_string();
        if msg.contains("不存在") {
            AdminServiceError::NotFound { id }
        } else {
            AdminServiceError::InternalError(msg)
        }
    }

    /// 分类余额查询错误（可能涉及上游 API 调用）
    fn classify_balance_error(&self, e: anyhow::Error, id: u64) -> AdminServiceError {
        if let Some(coordination_err) =
            e.downcast_ref::<crate::kiro::token_manager::RuntimeRefreshLeaderRequiredError>()
        {
            return AdminServiceError::NotLeader {
                instance_id: coordination_err.instance_id.clone(),
                leader_id: coordination_err.leader_id.clone(),
            };
        }

        let msg = e.to_string();

        // 1. 凭据不存在
        if msg.contains("不存在") {
            return AdminServiceError::NotFound { id };
        }

        if msg.contains("不支持超额") {
            return AdminServiceError::InvalidCredential(msg);
        }

        // 2. 上游服务错误特征：HTTP 响应错误或网络错误
        let is_upstream_error =
            // HTTP 响应错误（来自 refresh_*_token 的错误消息）
            msg.contains("凭证已过期或无效") ||
            msg.contains("权限不足") ||
            msg.contains("已被限流") ||
            msg.contains("服务器错误") ||
            msg.contains("Token 刷新失败") ||
            msg.contains("暂时不可用") ||
            // 网络错误（reqwest 错误）
            msg.contains("error trying to connect") ||
            msg.contains("connection") ||
            msg.contains("timeout") ||
            msg.contains("timed out");

        if is_upstream_error {
            AdminServiceError::UpstreamError(msg)
        } else {
            // 3. 默认归类为内部错误（本地验证失败、配置错误等）
            // 包括：缺少 refreshToken、refreshToken 已被截断、无法生成 machineId 等
            AdminServiceError::InternalError(msg)
        }
    }

    /// 分类添加凭据错误
    fn classify_add_error(&self, e: anyhow::Error) -> AdminServiceError {
        let msg = e.to_string();

        // 凭据验证失败（refreshToken 无效、格式错误等）
        let is_invalid_credential = msg.contains("缺少 refreshToken")
            || msg.contains("refreshToken 为空")
            || msg.contains("refreshToken 已被截断")
            || msg.contains("凭据已存在")
            || msg.contains("refreshToken 重复")
            || msg.contains("凭证已过期或无效")
            || msg.contains("权限不足")
            || msg.contains("已被限流");

        if is_invalid_credential {
            AdminServiceError::InvalidCredential(msg)
        } else if msg.contains("error trying to connect")
            || msg.contains("connection")
            || msg.contains("timeout")
        {
            AdminServiceError::UpstreamError(msg)
        } else {
            AdminServiceError::InternalError(msg)
        }
    }

    /// 分类删除凭据错误
    fn classify_delete_error(&self, e: anyhow::Error, id: u64) -> AdminServiceError {
        let msg = e.to_string();
        if msg.contains("不存在") {
            AdminServiceError::NotFound { id }
        } else if msg.contains("只能删除已禁用的凭据") || msg.contains("请先禁用凭据")
        {
            AdminServiceError::InvalidCredential(msg)
        } else {
            AdminServiceError::InternalError(msg)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    use crate::admin::types::BalanceResponse;
    use crate::admin::types::KnownCredentialFingerprint;
    use crate::kiro::model::credentials::KiroCredentials;
    use crate::model::config::{Config, ProxyPoolConfig, ProxyPoolEntry};

    fn temp_credentials_path(test_name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "kiro-admin-service-{test_name}-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("credentials.json")
    }

    fn available_credential() -> KiroCredentials {
        let mut credentials = KiroCredentials::default();
        credentials.id = Some(1);
        credentials.machine_id = Some("machine-1".to_string());
        credentials.access_token = Some("token-1".to_string());
        credentials.expires_at = Some((Utc::now() + Duration::hours(1)).to_rfc3339());
        credentials
    }

    #[test]
    fn credentials_event_fingerprint_ignores_hot_runtime_fields() {
        let manager = MultiTokenManager::new(
            Config::default(),
            vec![available_credential()],
            None,
            None,
            false,
        )
        .unwrap();
        let mut snapshot = manager.snapshot();
        let baseline = AdminService::credentials_event_fingerprint(&snapshot);

        snapshot.dispatchable = snapshot.dispatchable.saturating_add(1);
        snapshot.current_id = snapshot.current_id.saturating_add(1);
        let entry = snapshot.entries.first_mut().unwrap();
        entry.success_count = 42;
        entry.token_usage_count = 7;
        entry.input_tokens = 12_000;
        entry.output_tokens = 4_000;
        entry.total_tokens = 16_000;
        entry.last_used_at = Some("2026-06-27T00:00:00Z".to_string());
        entry.active_requests = 3;
        entry.cooldown_remaining_ms = Some(2_000);
        entry.rate_limit_bucket_tokens = Some(0.25);
        entry.rate_limit_hit_streak = 2;
        entry.next_ready_in_ms = Some(1_000);

        assert_eq!(
            baseline,
            AdminService::credentials_event_fingerprint(&snapshot)
        );
    }

    #[test]
    fn credentials_event_fingerprint_tracks_structural_fields() {
        let manager = MultiTokenManager::new(
            Config::default(),
            vec![available_credential()],
            None,
            None,
            false,
        )
        .unwrap();
        let snapshot = manager.snapshot();
        let baseline = AdminService::credentials_event_fingerprint(&snapshot);

        let mut priority_changed = snapshot.clone();
        priority_changed.entries.first_mut().unwrap().priority = 10;
        assert_ne!(
            baseline,
            AdminService::credentials_event_fingerprint(&priority_changed)
        );

        let mut disabled_changed = snapshot;
        disabled_changed.entries.first_mut().unwrap().disabled = true;
        disabled_changed.available = disabled_changed.available.saturating_sub(1);
        assert_ne!(
            baseline,
            AdminService::credentials_event_fingerprint(&disabled_changed)
        );
    }

    #[test]
    fn credentials_delta_returns_only_changed_and_deleted_credentials() {
        let mut first = available_credential();
        first.id = Some(1);
        let mut second = available_credential();
        second.id = Some(2);
        second.machine_id = Some("machine-2".to_string());
        second.priority = 5;

        let manager = Arc::new(
            MultiTokenManager::new(Config::default(), vec![first, second], None, None, false)
                .unwrap(),
        );
        let service = AdminService::new(manager);
        let full = service.get_all_credentials().unwrap();
        assert_eq!(full.credentials.len(), 2);

        let unchanged = service
            .get_credentials_delta(CredentialsDeltaRequest {
                since_revision: full.credentials_revision,
                balance_cache_revision: full.balance_cache_revision,
                credentials_fingerprint: full.credentials_fingerprint,
                known_credentials: full
                    .credentials
                    .iter()
                    .map(|credential| KnownCredentialFingerprint {
                        id: credential.id,
                        fingerprint: credential.fingerprint,
                    })
                    .collect(),
            })
            .unwrap();
        assert!(unchanged.upserts.is_empty());
        assert!(unchanged.deleted_ids.is_empty());

        let first_id = full.credentials[0].id;
        let second_id = full.credentials[1].id;
        let changed = service
            .get_credentials_delta(CredentialsDeltaRequest {
                since_revision: full.credentials_revision,
                balance_cache_revision: full.balance_cache_revision,
                credentials_fingerprint: full.credentials_fingerprint,
                known_credentials: vec![
                    KnownCredentialFingerprint {
                        id: first_id,
                        fingerprint: full.credentials[0].fingerprint,
                    },
                    KnownCredentialFingerprint {
                        id: second_id,
                        fingerprint: full.credentials[1].fingerprint.saturating_add(1),
                    },
                    KnownCredentialFingerprint {
                        id: 999,
                        fingerprint: 1,
                    },
                ],
            })
            .unwrap();

        assert_eq!(changed.upserts.len(), 1);
        assert_eq!(changed.upserts[0].id, second_id);
        assert_eq!(changed.deleted_ids, vec![999]);
    }

    #[test]
    fn normalize_idc_device_provider_accepts_builder_aliases() {
        assert_eq!(
            normalize_idc_device_provider("builder-id").unwrap(),
            "BuilderId"
        );
        assert_eq!(
            normalize_idc_device_provider("Builder ID").unwrap(),
            "BuilderId"
        );
        assert_eq!(
            normalize_idc_device_provider("enterprise").unwrap(),
            "Enterprise"
        );
        assert!(normalize_idc_device_provider("Google").is_err());
    }

    #[test]
    fn resolve_idc_device_start_url_requires_enterprise_url() {
        assert_eq!(
            resolve_idc_device_start_url("BuilderId", None).unwrap(),
            BUILDER_ID_START_URL
        );
        assert!(resolve_idc_device_start_url("Enterprise", None).is_err());
        assert!(
            resolve_idc_device_start_url("Enterprise", Some("http://example.com/start")).is_err()
        );
        assert_eq!(
            resolve_idc_device_start_url(
                "Enterprise",
                Some("https://d-1234567890.awsapps.com/start")
            )
            .unwrap(),
            "https://d-1234567890.awsapps.com/start"
        );
    }

    fn idc_device_login_session_for_test(provider: &str) -> IdcDeviceLoginSession {
        let now = Utc::now();
        let start_url = if provider.eq_ignore_ascii_case("Enterprise") {
            "https://example.awsapps.com/start"
        } else {
            BUILDER_ID_START_URL
        };

        IdcDeviceLoginSession {
            session_id: "session-1".to_string(),
            status: IdcDeviceLoginStatus::Pending,
            provider: provider.to_string(),
            start_url: start_url.to_string(),
            region: "us-east-1".to_string(),
            client_id: "client-1".to_string(),
            client_secret: "secret-1".to_string(),
            device_code: "device-code".to_string(),
            user_code: "user-code".to_string(),
            verification_uri: "https://device.example.com".to_string(),
            verification_uri_complete: None,
            interval_seconds: 5,
            next_poll_at: now,
            expires_at: now + Duration::minutes(10),
            request: StartIdcDeviceLoginRequest {
                provider: provider.to_string(),
                start_url: Some(start_url.to_string()),
                region: None,
                auth_region: None,
                api_region: None,
                profile_arn: None,
                priority: 0,
                max_concurrency: None,
                machine_id: None,
                account_type: None,
                credential_groups: None,
                source_supplier_id: None,
                source_supplier_name: None,
                source_batch: None,
                proxy_url: None,
                proxy_username: None,
                proxy_password: None,
                proxy_id: None,
            },
            message: None,
            credential_result: None,
            polling: false,
            updated_at: now,
        }
    }

    #[test]
    fn builder_id_device_login_add_request_does_not_persist_builder_provider() {
        let manager =
            Arc::new(MultiTokenManager::new(Config::default(), vec![], None, None, false).unwrap());
        let service = AdminService::new(manager);
        let session = idc_device_login_session_for_test("BuilderId");

        let req = service.build_idc_device_add_credential_request(
            &session,
            AwsCreateTokenResponse {
                refresh_token: "refresh-token".to_string(),
            },
        );

        assert_eq!(req.auth_method, "idc");
        assert_eq!(req.provider, None);
        assert_eq!(req.start_url, None);
    }

    #[test]
    fn enterprise_device_login_add_request_persists_provider_and_start_url() {
        let manager =
            Arc::new(MultiTokenManager::new(Config::default(), vec![], None, None, false).unwrap());
        let service = AdminService::new(manager);
        let session = idc_device_login_session_for_test("Enterprise");

        let req = service.build_idc_device_add_credential_request(
            &session,
            AwsCreateTokenResponse {
                refresh_token: "refresh-token".to_string(),
            },
        );

        assert_eq!(req.auth_method, "idc");
        assert_eq!(req.provider.as_deref(), Some("Enterprise"));
        assert_eq!(
            req.start_url.as_deref(),
            Some("https://example.awsapps.com/start")
        );
    }

    #[test]
    fn enterprise_device_login_add_request_keeps_auth_region_out_of_api_region_fallback() {
        let manager =
            Arc::new(MultiTokenManager::new(Config::default(), vec![], None, None, false).unwrap());
        let service = AdminService::new(manager);
        let mut session = idc_device_login_session_for_test("Enterprise");
        session.region = "us-east-2".to_string();
        session.request.region = Some("us-east-2".to_string());

        let req = service.build_idc_device_add_credential_request(
            &session,
            AwsCreateTokenResponse {
                refresh_token: "refresh-token".to_string(),
            },
        );

        assert_eq!(req.region, None);
        assert_eq!(req.auth_region.as_deref(), Some("us-east-2"));
        assert_eq!(req.api_region, None);
    }

    #[test]
    fn external_idp_probe_domain_accepts_work_email_or_domain() {
        assert_eq!(
            domain_from_work_email("User@Example.COM").unwrap(),
            "example.com"
        );
        assert_eq!(
            normalize_domain_name("@Example.COM").unwrap(),
            "example.com"
        );
        assert!(domain_from_work_email("missing-at").is_err());
        assert!(normalize_domain_name("https://example.com").is_err());
    }

    #[test]
    fn external_idp_url_validation_rejects_unsafe_targets() {
        assert_eq!(
            normalize_optional_url(
                Some("https://login.example.com/tenant/v2.0/?ignored=1"),
                "Issuer URL"
            )
            .unwrap()
            .as_deref(),
            Some("https://login.example.com/tenant/v2.0")
        );
        assert_eq!(
            oidc_discovery_url("https://login.example.com/tenant/v2.0/?ignored=1").unwrap(),
            "https://login.example.com/tenant/v2.0/.well-known/openid-configuration"
        );
        assert!(normalize_optional_url(Some("http://login.example.com"), "Issuer URL").is_err());
        assert!(
            normalize_optional_url(Some("https://user@login.example.com"), "Issuer URL").is_err()
        );
        assert!(normalize_optional_url(Some("https://127.0.0.1/tenant"), "Issuer URL").is_err());
        assert!(normalize_optional_url(Some("https://localhost/tenant"), "Issuer URL").is_err());
        assert!(
            normalize_external_idp_endpoint_url(Some("https://[::1]/token"), "token_endpoint")
                .is_err()
        );
    }

    #[test]
    fn external_idp_scopes_append_offline_access_once() {
        assert_eq!(
            normalize_scope_string_with_offline_access("openid profile").unwrap(),
            "openid profile offline_access"
        );
        assert_eq!(
            normalize_scope_string_with_offline_access("openid offline_access profile").unwrap(),
            "openid offline_access profile"
        );
        assert!(normalize_scope_string_with_offline_access("   ").is_err());
    }

    #[test]
    fn external_idp_callback_url_uses_origin() {
        assert_eq!(
            resolve_external_idp_callback_url(Some("https://example.com/admin?x=1")).unwrap(),
            "https://example.com/api/admin/auth/external-idp/callback"
        );
        assert_eq!(
            resolve_external_idp_callback_url(Some("http://127.0.0.1:3000")).unwrap(),
            "http://127.0.0.1:3000/api/admin/auth/external-idp/callback"
        );
        assert!(resolve_external_idp_callback_url(Some("ftp://example.com")).is_err());
        assert!(resolve_external_idp_callback_url(Some("https://user@example.com")).is_err());
    }

    #[test]
    fn external_idp_portal_descriptor_accepts_issuer_without_login_option() {
        let mut params = HashMap::new();
        params.insert(
            "issuer_url".to_string(),
            "https://login.example.com/tenant/v2.0".to_string(),
        );
        assert!(external_idp_portal_descriptor_error(&params).is_none());

        params.insert("login_option".to_string(), "external_idp".to_string());
        assert!(external_idp_portal_descriptor_error(&params).is_none());

        params.insert("login_option".to_string(), "google".to_string());
        let message = external_idp_portal_descriptor_error(&params).unwrap();
        assert!(message.contains("login_option=google"));
    }

    #[test]
    fn external_idp_login_flow_defaults_to_auto_and_accepts_device_code() {
        let default_req: StartExternalIdpLoginRequest = serde_json::from_str("{}").unwrap();
        assert_eq!(default_req.flow, ExternalIdpLoginFlow::Auto);

        let device_req: StartExternalIdpLoginRequest =
            serde_json::from_str(r#"{"flow":"device-code"}"#).unwrap();
        assert_eq!(device_req.flow, ExternalIdpLoginFlow::DeviceCode);

        let kiro_pkce_req: StartExternalIdpLoginRequest =
            serde_json::from_str(r#"{"flow":"kiro-pkce"}"#).unwrap();
        assert_eq!(kiro_pkce_req.flow, ExternalIdpLoginFlow::KiroPkce);
    }

    #[test]
    fn external_idp_authorization_url_contains_pkce_parameters() {
        let url = build_external_idp_authorization_url(
            "https://login.example.com/oauth2/v1/authorize?existing=1",
            "client-1",
            "https://proxy.example.com/api/admin/auth/external-idp/callback",
            "openid profile offline_access",
            "state-1",
            "challenge-1",
            Some("user@example.com"),
            Some("aud-1"),
        )
        .unwrap();
        let parsed = url::Url::parse(&url).unwrap();
        let pairs: HashMap<_, _> = parsed.query_pairs().into_owned().collect();

        assert_eq!(pairs.get("existing").map(String::as_str), Some("1"));
        assert_eq!(pairs.get("client_id").map(String::as_str), Some("client-1"));
        assert_eq!(pairs.get("response_type").map(String::as_str), Some("code"));
        assert_eq!(
            pairs.get("scope").map(String::as_str),
            Some("openid profile offline_access")
        );
        assert_eq!(
            pairs.get("code_challenge_method").map(String::as_str),
            Some("S256")
        );
        assert_eq!(
            pairs.get("login_hint").map(String::as_str),
            Some("user@example.com")
        );
        assert_eq!(pairs.get("audience").map(String::as_str), Some("aud-1"));
    }

    #[test]
    fn external_idp_token_exchange_form_includes_scope_when_present() {
        let form = build_external_idp_authorization_code_token_form(
            "client-1",
            "code-1",
            KIRO_IDE_EXTERNAL_IDP_REDIRECT_URI,
            "verifier-1",
            Some("openid profile offline_access"),
        );
        let pairs: HashMap<_, _> = form.into_iter().collect();

        assert_eq!(pairs.get("client_id").map(String::as_str), Some("client-1"));
        assert_eq!(
            pairs.get("grant_type").map(String::as_str),
            Some("authorization_code")
        );
        assert_eq!(
            pairs.get("scope").map(String::as_str),
            Some("openid profile offline_access")
        );

        let form = build_external_idp_authorization_code_token_form(
            "client-1",
            "code-1",
            KIRO_IDE_EXTERNAL_IDP_REDIRECT_URI,
            "verifier-1",
            Some("   "),
        );
        let pairs: HashMap<_, _> = form.into_iter().collect();
        assert!(!pairs.contains_key("scope"));
    }

    #[test]
    fn external_idp_authorization_url_accepts_kiro_ide_redirect_uri() {
        let url = build_external_idp_authorization_url(
            "https://login.example.com/oauth2/v2.0/authorize",
            "client-1",
            KIRO_IDE_EXTERNAL_IDP_REDIRECT_URI,
            "openid offline_access",
            "state-1",
            "challenge-1",
            None,
            None,
        )
        .unwrap();
        let parsed = url::Url::parse(&url).unwrap();
        let pairs: HashMap<_, _> = parsed.query_pairs().into_owned().collect();

        assert_eq!(
            pairs.get("redirect_uri").map(String::as_str),
            Some(KIRO_IDE_EXTERNAL_IDP_REDIRECT_URI)
        );
        assert_eq!(
            pairs.get("code_challenge_method").map(String::as_str),
            Some("S256")
        );
    }

    #[test]
    fn external_idp_submit_callback_params_accept_url_query_or_code() {
        let params = external_idp_submit_callback_params(&SubmitExternalIdpCallbackRequest {
            callback_url: Some("kiro://kiro.oauth/callback?code=code-1&state=state-1".to_string()),
            code: None,
            state: None,
        })
        .unwrap();
        assert_eq!(params.get("code").map(String::as_str), Some("code-1"));
        assert_eq!(params.get("state").map(String::as_str), Some("state-1"));

        let params = external_idp_submit_callback_params(&SubmitExternalIdpCallbackRequest {
            callback_url: Some("?code=code-2&state=state-2".to_string()),
            code: None,
            state: None,
        })
        .unwrap();
        assert_eq!(params.get("code").map(String::as_str), Some("code-2"));
        assert_eq!(params.get("state").map(String::as_str), Some("state-2"));

        let params = external_idp_submit_callback_params(&SubmitExternalIdpCallbackRequest {
            callback_url: None,
            code: Some(" code-3 ".to_string()),
            state: None,
        })
        .unwrap();
        assert_eq!(params.get("code").map(String::as_str), Some("code-3"));

        assert!(
            external_idp_submit_callback_params(&SubmitExternalIdpCallbackRequest {
                callback_url: None,
                code: None,
                state: None,
            })
            .is_err()
        );
    }

    #[test]
    fn parse_kiro_login_metadata_accepts_direct_and_wrapped_payloads() {
        let direct = parse_kiro_login_metadata_payload(
            r#"{"found":true,"issuerUrl":"https://login.example.com","clientId":"client","scopes":["openid","email profile"],"audience":"aud"}"#,
        )
        .unwrap();
        assert!(direct.found);
        assert_eq!(
            direct.issuer_url.as_deref(),
            Some("https://login.example.com")
        );
        assert_eq!(direct.client_id.as_deref(), Some("client"));
        assert_eq!(direct.scopes.unwrap().len(), 2);

        let wrapped =
            parse_kiro_login_metadata_payload(r#"{"Output":{"found":false},"Version":"1.0"}"#)
                .unwrap();
        assert!(!wrapped.found);

        assert!(
            parse_kiro_login_metadata_payload(
                r#"{"Output":{"__type":"com.amazon.coral.service#UnknownOperationException"}}"#,
            )
            .is_err()
        );
    }

    #[test]
    fn parse_kiro_login_metadata_accepts_cbor_payload() {
        let body = serde_cbor::to_vec(&serde_json::json!({
            "found": true,
            "issuerUrl": "https://login.example.com",
            "clientId": "client",
            "scopes": ["openid", "profile"],
        }))
        .unwrap();
        let payload = parse_kiro_login_metadata_payload_cbor(&body).unwrap();

        assert!(payload.found);
        assert_eq!(payload.client_id.as_deref(), Some("client"));
        assert_eq!(payload.scopes.unwrap(), vec!["openid", "profile"]);

        let indefinite =
            hex::decode("bf65666f756e64f56673636f7065739f666f70656e69646770726f66696c65ffff")
                .unwrap();
        let payload = parse_kiro_login_metadata_payload_cbor(&indefinite).unwrap();

        assert!(payload.found);
        assert_eq!(payload.scopes.unwrap(), vec!["openid", "profile"]);
    }

    #[test]
    fn oidc_discovery_document_parses_standard_snake_case_fields() {
        let document: OidcDiscoveryDocument = serde_json::from_str(
            r#"{
                "issuer": "https://login.example.com/tenant/v2.0",
                "authorization_endpoint": "https://login.example.com/tenant/oauth2/v2.0/authorize",
                "token_endpoint": "https://login.example.com/tenant/oauth2/v2.0/token",
                "response_types_supported": ["code"],
                "code_challenge_methods_supported": ["S256"],
                "token_endpoint_auth_methods_supported": ["none"]
            }"#,
        )
        .unwrap();

        assert_eq!(
            document.authorization_endpoint.as_deref(),
            Some("https://login.example.com/tenant/oauth2/v2.0/authorize")
        );
        assert_eq!(
            document.token_endpoint.as_deref(),
            Some("https://login.example.com/tenant/oauth2/v2.0/token")
        );
        assert_eq!(
            document.response_types_supported.unwrap(),
            vec!["code".to_string()]
        );
        assert_eq!(
            document.code_challenge_methods_supported.unwrap(),
            vec!["S256".to_string()]
        );
    }

    #[test]
    fn external_idp_device_code_requires_public_token_auth() {
        let mut summary = ExternalIdpOidcDiscoverySummary {
            issuer: Some("https://login.example.com/tenant/v2.0".to_string()),
            authorization_endpoint: Some("https://login.example.com/authorize".to_string()),
            token_endpoint: Some("https://login.example.com/token".to_string()),
            device_authorization_endpoint: Some("https://login.example.com/devicecode".to_string()),
            code_challenge_methods_supported: vec!["S256".to_string()],
            grant_types_supported: Vec::new(),
            response_types_supported: vec!["code".to_string()],
            scopes_supported: Vec::new(),
            token_endpoint_auth_methods_supported: vec![
                "client_secret_post".to_string(),
                "private_key_jwt".to_string(),
            ],
        };

        assert!(!external_idp_public_token_auth_supported(&summary));
        let message = external_idp_device_code_secret_requirement_message(&summary).unwrap();
        assert!(message.contains("client_secret"));
        assert!(message.contains("client_assertion"));

        summary.token_endpoint_auth_methods_supported = vec!["none".to_string()];
        assert!(external_idp_public_token_auth_supported(&summary));
        assert!(external_idp_device_code_secret_requirement_message(&summary).is_none());
    }

    #[test]
    fn external_idp_device_token_secret_required_error_is_classified() {
        let message = external_idp_secret_required_error_message(
            "AADSTS7000218: The request body must contain the following parameter: 'client_assertion' or 'client_secret'.",
        )
        .unwrap();

        assert!(message.contains("client_secret"));
        assert!(message.contains("token exchange"));
    }

    #[test]
    fn idc_device_login_status_response_hides_codes_after_completion() {
        let now = Utc::now();
        let session = IdcDeviceLoginSession {
            session_id: "session-1".to_string(),
            status: IdcDeviceLoginStatus::Completed,
            provider: "BuilderId".to_string(),
            start_url: BUILDER_ID_START_URL.to_string(),
            region: "us-east-1".to_string(),
            client_id: "client".to_string(),
            client_secret: "secret".to_string(),
            device_code: "device".to_string(),
            user_code: "USER-CODE".to_string(),
            verification_uri: "https://device.sso.aws.amazon.com/".to_string(),
            verification_uri_complete: Some(
                "https://device.sso.aws.amazon.com/?user_code=USER-CODE".to_string(),
            ),
            interval_seconds: 5,
            next_poll_at: now,
            expires_at: now + Duration::minutes(10),
            request: StartIdcDeviceLoginRequest {
                provider: "BuilderId".to_string(),
                start_url: None,
                region: None,
                auth_region: None,
                api_region: None,
                profile_arn: None,
                priority: 0,
                max_concurrency: None,
                machine_id: None,
                account_type: None,
                credential_groups: None,
                source_supplier_id: None,
                source_supplier_name: None,
                source_batch: None,
                proxy_url: None,
                proxy_username: None,
                proxy_password: None,
                proxy_id: None,
            },
            message: Some("ok".to_string()),
            credential_result: Some(AddCredentialResponse {
                success: true,
                message: "added".to_string(),
                credential_id: 9,
                email: Some("user@example.com".to_string()),
                user_id: None,
                provider: Some("BuilderId".to_string()),
                subscription_title: None,
                subscription_type: None,
                auth_account_type: Some("builder-id".to_string()),
                resolved_account_type: None,
            }),
            polling: false,
            updated_at: now,
        };

        let response = idc_device_login_status_response(&session);

        assert_eq!(response.status, IdcDeviceLoginStatus::Completed);
        assert_eq!(response.credential_id, Some(9));
        assert_eq!(response.email.as_deref(), Some("user@example.com"));
        assert_eq!(response.user_code, None);
        assert_eq!(response.verification_uri, None);
        assert_eq!(response.verification_uri_complete, None);
        assert_eq!(response.expires_at, None);
    }

    #[test]
    fn external_idp_device_login_status_response_hides_codes_after_completion() {
        let now = Utc::now();
        let mut session = ExternalIdpLoginSession {
            session_id: "session-1".to_string(),
            status: ExternalIdpLoginStatus::Pending,
            phase: ExternalIdpLoginPhase::DeviceAuthorization,
            flow: ExternalIdpLoginFlow::DeviceCode,
            provider: "ExternalIdp".to_string(),
            auth_url: Some("https://microsoft.com/devicelogin".to_string()),
            callback_url: None,
            idp_redirect_uri: None,
            expires_at: now + Duration::minutes(10),
            request: serde_json::from_str("{}").unwrap(),
            portal_state: None,
            portal_code_verifier: None,
            idp_state: None,
            idp_code_verifier: None,
            issuer_url: Some("https://login.example.com/tenant/v2.0".to_string()),
            client_id: Some("client".to_string()),
            scopes: Some("openid profile offline_access".to_string()),
            audience: None,
            login_hint: Some("user@example.com".to_string()),
            token_endpoint: Some("https://login.example.com/token".to_string()),
            device_authorization_endpoint: Some("https://login.example.com/devicecode".to_string()),
            device_code: Some("device".to_string()),
            user_code: Some("USER-CODE".to_string()),
            verification_uri: Some("https://microsoft.com/devicelogin".to_string()),
            verification_uri_complete: None,
            interval_seconds: 5,
            next_poll_at: now,
            polling: false,
            idp_callback_consumed: false,
            message: Some("pending".to_string()),
            credential_result: None,
            updated_at: now,
        };

        let pending = external_idp_login_status_response(&session);
        assert_eq!(pending.flow, ExternalIdpLoginFlow::DeviceCode);
        assert_eq!(pending.user_code.as_deref(), Some("USER-CODE"));
        assert_eq!(
            pending.verification_uri.as_deref(),
            Some("https://microsoft.com/devicelogin")
        );

        session.status = ExternalIdpLoginStatus::Completed;
        session.phase = ExternalIdpLoginPhase::Completed;
        session.credential_result = Some(AddCredentialResponse {
            success: true,
            message: "added".to_string(),
            credential_id: 9,
            email: Some("user@example.com".to_string()),
            user_id: None,
            provider: Some("ExternalIdp".to_string()),
            subscription_title: None,
            subscription_type: None,
            auth_account_type: Some("enterprise".to_string()),
            resolved_account_type: None,
        });

        let completed = external_idp_login_status_response(&session);
        assert_eq!(completed.status, ExternalIdpLoginStatus::Completed);
        assert_eq!(completed.credential_id, Some(9));
        assert_eq!(completed.user_code, None);
        assert_eq!(completed.verification_uri, None);
        assert_eq!(completed.expires_at, None);
    }

    #[test]
    fn get_load_balancing_mode_reports_proxy_assignment_counts() {
        let mut config = Config::default();
        config.proxy_pool = ProxyPoolConfig {
            enabled: true,
            proxies: vec![
                ProxyPoolEntry {
                    id: "node-a".to_string(),
                    url: "http://node-a.local:3128".to_string(),
                    username: None,
                    password: None,
                    weight: 1,
                    enabled: true,
                    expected_egress_ip: None,
                },
                ProxyPoolEntry {
                    id: "node-b".to_string(),
                    url: "http://node-b.local:3128".to_string(),
                    username: None,
                    password: None,
                    weight: 1,
                    enabled: true,
                    expected_egress_ip: None,
                },
            ],
            ..ProxyPoolConfig::default()
        };

        let mut first = available_credential();
        first.proxy_id = Some("node-a".to_string());
        let mut second = available_credential();
        second.id = Some(2);
        second.machine_id = Some("machine-2".to_string());
        second.proxy_id = Some("node-a".to_string());
        let mut third = available_credential();
        third.id = Some(3);
        third.machine_id = Some("machine-3".to_string());
        third.proxy_id = Some("node-b".to_string());
        let mut disabled = available_credential();
        disabled.id = Some(4);
        disabled.machine_id = Some("machine-4".to_string());
        disabled.proxy_id = Some("node-a".to_string());
        disabled.disabled = true;

        let manager = Arc::new(
            MultiTokenManager::new(
                config,
                vec![first, second, third, disabled],
                None,
                None,
                false,
            )
            .unwrap(),
        );
        let service = AdminService::new(manager);

        let response = service.get_load_balancing_mode().unwrap();
        let counts: HashMap<_, _> = response
            .proxy_pool
            .proxies
            .into_iter()
            .map(|entry| (entry.id, entry.assigned_credentials))
            .collect();

        assert_eq!(counts.get("node-a"), Some(&2));
        assert_eq!(counts.get("node-b"), Some(&1));
    }

    #[tokio::test]
    async fn test_get_balance_syncs_shared_cache_before_fetching_upstream() {
        let credentials_path = temp_credentials_path("shared-balance-cache");
        let manager = Arc::new(
            MultiTokenManager::new(
                Config::default(),
                vec![available_credential()],
                None,
                Some(credentials_path.clone()),
                false,
            )
            .unwrap(),
        );
        let service = AdminService::new(manager.clone());

        let shared_balance = BalanceResponse {
            id: 1,
            profile_arn: None,
            subscription_title: Some("KIRO PRO+".to_string()),
            subscription_type: Some("Q_DEVELOPER_STANDALONE_PRO_PLUS".to_string()),
            current_usage: 12.5,
            usage_limit: 100.0,
            effective_usage_limit: 100.0,
            remaining: 87.5,
            usage_percentage: 12.5,
            next_reset_at: Some(1_744_739_200.0),
            overage_capability: Some("OVERAGE_CAPABLE".to_string()),
            overage_status: Some("DISABLED".to_string()),
            overage_enabled: Some(false),
            overage_cap: 0.0,
            current_overages: 0.0,
            overage_charges: 0.0,
            overage_rate: None,
            currency: None,
            unit: None,
        };
        let mut shared_cache = HashMap::new();
        shared_cache.insert(
            1,
            CachedBalanceRecord {
                cached_at: Utc::now().timestamp() as f64,
                profile_arn: None,
                data: shared_balance.clone(),
            },
        );
        manager
            .state_store()
            .save_balance_cache(&shared_cache)
            .unwrap();

        let balance = service.get_balance(1).await.unwrap();
        assert_eq!(balance.id, shared_balance.id);
        assert_eq!(
            balance.subscription_title.as_deref(),
            shared_balance.subscription_title.as_deref()
        );
        assert_eq!(balance.current_usage, shared_balance.current_usage);
        assert_eq!(balance.usage_limit, shared_balance.usage_limit);
        assert_eq!(balance.remaining, shared_balance.remaining);
        assert_eq!(balance.usage_percentage, shared_balance.usage_percentage);
        assert_eq!(balance.next_reset_at, shared_balance.next_reset_at);
        assert!(service.balance_cache.lock().contains_key(&1));

        let balance_cache_path = credentials_path
            .parent()
            .unwrap()
            .join("kiro_balance_cache.json");
        std::fs::remove_file(balance_cache_path).unwrap();
    }
}
