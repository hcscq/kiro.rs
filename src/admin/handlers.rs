//! Admin API HTTP 处理器

use std::collections::HashMap;

use axum::{
    Json,
    extract::{Path, Query, State},
    response::{Html, IntoResponse, Redirect, Response},
};

use super::{
    middleware::AdminState,
    service::ExternalIdpCallbackAction,
    types::{
        AddCredentialRequest, ExternalIdpProbeRequest, SetCredentialGroupsRequest,
        SetCredentialModelPolicyRequest, SetCredentialProfileRequest, SetCredentialProxyRequest,
        SetCredentialRateLimitConfigRequest, SetCredentialSourceRequest, SetDisabledRequest,
        SetLoadBalancingModeRequest, SetMaxConcurrencyRequest, SetModelCapabilitiesConfigRequest,
        SetOverageStatusRequest, SetPriorityRequest, StartExternalIdpLoginRequest,
        StartIdcDeviceLoginRequest, SubmitExternalIdpCallbackRequest, SuccessResponse,
    },
};

fn html_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn external_idp_callback_html(success: bool, title: &str, message: &str) -> Html<String> {
    let color = if success { "#166534" } else { "#991b1b" };
    let title = html_escape(title);
    let message = html_escape(message);
    Html(format!(
        r#"<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{title}</title>
  <style>
    body {{ font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 0; min-height: 100vh; display: grid; place-items: center; background: #f8fafc; color: #0f172a; }}
    main {{ width: min(520px, calc(100vw - 32px)); border: 1px solid #e2e8f0; border-radius: 8px; background: white; padding: 24px; box-shadow: 0 10px 30px rgba(15, 23, 42, 0.08); }}
    h1 {{ margin: 0 0 12px; font-size: 20px; color: {color}; }}
    p {{ margin: 0; line-height: 1.6; }}
  </style>
</head>
<body>
  <main>
    <h1>{title}</h1>
    <p>{message}</p>
  </main>
  <script>setTimeout(() => window.close(), 1200)</script>
</body>
</html>"#
    ))
}

/// GET /api/admin/credentials
/// 获取所有凭据状态
pub async fn get_all_credentials(State(state): State<AdminState>) -> impl IntoResponse {
    match state.service.get_all_credentials() {
        Ok(response) => Json(response).into_response(),
        Err(e) => (e.status_code(), Json(e.into_response())).into_response(),
    }
}

/// POST /api/admin/credentials/:id/disabled
/// 设置凭据禁用状态
pub async fn set_credential_disabled(
    State(state): State<AdminState>,
    Path(id): Path<u64>,
    Json(payload): Json<SetDisabledRequest>,
) -> impl IntoResponse {
    match state.service.set_disabled(id, payload.disabled) {
        Ok(_) => {
            let action = if payload.disabled { "禁用" } else { "启用" };
            Json(SuccessResponse::new(format!("凭据 #{} 已{}", id, action))).into_response()
        }
        Err(e) => (e.status_code(), Json(e.into_response())).into_response(),
    }
}

/// POST /api/admin/credentials/:id/priority
/// 设置凭据优先级
pub async fn set_credential_priority(
    State(state): State<AdminState>,
    Path(id): Path<u64>,
    Json(payload): Json<SetPriorityRequest>,
) -> impl IntoResponse {
    match state.service.set_priority(id, payload.priority) {
        Ok(_) => Json(SuccessResponse::new(format!(
            "凭据 #{} 优先级已设置为 {}",
            id, payload.priority
        )))
        .into_response(),
        Err(e) => (e.status_code(), Json(e.into_response())).into_response(),
    }
}

/// POST /api/admin/credentials/:id/max-concurrency
/// 设置凭据并发上限
pub async fn set_credential_max_concurrency(
    State(state): State<AdminState>,
    Path(id): Path<u64>,
    Json(payload): Json<SetMaxConcurrencyRequest>,
) -> impl IntoResponse {
    match state
        .service
        .set_max_concurrency(id, payload.max_concurrency)
    {
        Ok(_) => Json(SuccessResponse::new(format!(
            "凭据 #{} 并发上限已设置为 {}",
            id,
            payload
                .max_concurrency
                .filter(|limit| *limit > 0)
                .map(|limit| limit.to_string())
                .unwrap_or_else(|| "不限".to_string())
        )))
        .into_response(),
        Err(e) => (e.status_code(), Json(e.into_response())).into_response(),
    }
}

/// POST /api/admin/credentials/:id/rate-limit-config
/// 设置凭据级 token bucket 配置
pub async fn set_credential_rate_limit_config(
    State(state): State<AdminState>,
    Path(id): Path<u64>,
    Json(payload): Json<SetCredentialRateLimitConfigRequest>,
) -> impl IntoResponse {
    match state.service.set_rate_limit_config(
        id,
        payload.rate_limit_cooldown_enabled,
        payload.rate_limit_bucket_capacity,
        payload.rate_limit_refill_per_second,
    ) {
        Ok(_) => Json(SuccessResponse::new(format!("凭据 #{} 限速配置已更新", id))).into_response(),
        Err(e) => (e.status_code(), Json(e.into_response())).into_response(),
    }
}

/// POST /api/admin/credentials/:id/model-policy
/// 设置凭据级模型策略
pub async fn set_credential_model_policy(
    State(state): State<AdminState>,
    Path(id): Path<u64>,
    Json(payload): Json<SetCredentialModelPolicyRequest>,
) -> impl IntoResponse {
    match state.service.set_model_policy(id, payload) {
        Ok(_) => Json(SuccessResponse::new(format!("凭据 #{} 模型策略已更新", id))).into_response(),
        Err(e) => (e.status_code(), Json(e.into_response())).into_response(),
    }
}

/// POST /api/admin/credentials/:id/source
/// 设置凭据来源标记
pub async fn set_credential_source(
    State(state): State<AdminState>,
    Path(id): Path<u64>,
    Json(payload): Json<SetCredentialSourceRequest>,
) -> impl IntoResponse {
    match state.service.set_source(id, payload) {
        Ok(_) => Json(SuccessResponse::new(format!("凭据 #{} 来源标记已更新", id))).into_response(),
        Err(e) => (e.status_code(), Json(e.into_response())).into_response(),
    }
}

/// POST /api/admin/credentials/:id/groups
/// 设置凭据分组标记
pub async fn set_credential_groups(
    State(state): State<AdminState>,
    Path(id): Path<u64>,
    Json(payload): Json<SetCredentialGroupsRequest>,
) -> impl IntoResponse {
    match state.service.set_groups(id, payload) {
        Ok(_) => Json(SuccessResponse::new(format!("凭据 #{} 分组标记已更新", id))).into_response(),
        Err(e) => (e.status_code(), Json(e.into_response())).into_response(),
    }
}

/// POST /api/admin/credentials/:id/proxy
/// 设置凭据代理绑定
pub async fn set_credential_proxy(
    State(state): State<AdminState>,
    Path(id): Path<u64>,
    Json(payload): Json<SetCredentialProxyRequest>,
) -> impl IntoResponse {
    match state.service.set_proxy(id, payload) {
        Ok(_) => Json(SuccessResponse::new(format!("凭据 #{} 代理配置已更新", id))).into_response(),
        Err(e) => (e.status_code(), Json(e.into_response())).into_response(),
    }
}

/// GET /api/admin/credentials/:id/profiles
/// 获取指定凭据可用的 Profile
pub async fn get_credential_profiles(
    State(state): State<AdminState>,
    Path(id): Path<u64>,
) -> impl IntoResponse {
    match state.service.get_profiles(id).await {
        Ok(response) => Json(response).into_response(),
        Err(e) => (e.status_code(), Json(e.into_response())).into_response(),
    }
}

/// POST /api/admin/credentials/:id/profile
/// 设置指定凭据当前使用的 Profile
pub async fn set_credential_profile(
    State(state): State<AdminState>,
    Path(id): Path<u64>,
    Json(payload): Json<SetCredentialProfileRequest>,
) -> impl IntoResponse {
    match state.service.set_profile(id, payload) {
        Ok(_) => Json(SuccessResponse::new(format!("凭据 #{} profile 已更新", id))).into_response(),
        Err(e) => (e.status_code(), Json(e.into_response())).into_response(),
    }
}

/// POST /api/admin/credentials/:id/overage
/// 设置凭据超额使用开关
pub async fn set_credential_overage_status(
    State(state): State<AdminState>,
    Path(id): Path<u64>,
    Json(payload): Json<SetOverageStatusRequest>,
) -> impl IntoResponse {
    match state.service.set_overage_status(id, payload.enabled).await {
        Ok(response) => Json(response).into_response(),
        Err(e) => (e.status_code(), Json(e.into_response())).into_response(),
    }
}

/// POST /api/admin/credentials/:id/runtime-model-restrictions/clear
/// 清除凭据运行时模型限制
pub async fn clear_credential_runtime_model_restrictions(
    State(state): State<AdminState>,
    Path(id): Path<u64>,
) -> impl IntoResponse {
    match state.service.clear_runtime_model_restrictions(id) {
        Ok(true) => {
            Json(SuccessResponse::new(format!("凭据 #{} 模型冷却已清除", id))).into_response()
        }
        Ok(false) => Json(SuccessResponse::new(format!(
            "凭据 #{} 当前没有模型冷却",
            id
        )))
        .into_response(),
        Err(e) => (e.status_code(), Json(e.into_response())).into_response(),
    }
}

/// POST /api/admin/credentials/:id/suspicious-activity/clear
/// 清除凭据 suspicious activity 标记与隔离
pub async fn clear_credential_suspicious_activity(
    State(state): State<AdminState>,
    Path(id): Path<u64>,
) -> impl IntoResponse {
    match state.service.clear_suspicious_activity(id) {
        Ok(true) => Json(SuccessResponse::new(format!(
            "凭据 #{} suspicious activity 已清除",
            id
        )))
        .into_response(),
        Ok(false) => Json(SuccessResponse::new(format!(
            "凭据 #{} 当前没有 suspicious activity 标记",
            id
        )))
        .into_response(),
        Err(e) => (e.status_code(), Json(e.into_response())).into_response(),
    }
}

/// POST /api/admin/credentials/:id/reset
/// 重置失败计数并重新启用
pub async fn reset_failure_count(
    State(state): State<AdminState>,
    Path(id): Path<u64>,
) -> impl IntoResponse {
    match state.service.reset_and_enable(id) {
        Ok(_) => Json(SuccessResponse::new(format!(
            "凭据 #{} 失败计数已重置并重新启用",
            id
        )))
        .into_response(),
        Err(e) => (e.status_code(), Json(e.into_response())).into_response(),
    }
}

/// GET /api/admin/credentials/:id/balance
/// 获取指定凭据的余额
pub async fn get_credential_balance(
    State(state): State<AdminState>,
    Path(id): Path<u64>,
) -> impl IntoResponse {
    match state.service.get_balance(id).await {
        Ok(response) => Json(response).into_response(),
        Err(e) => (e.status_code(), Json(e.into_response())).into_response(),
    }
}

/// POST /api/admin/credentials
/// 添加新凭据
pub async fn add_credential(
    State(state): State<AdminState>,
    Json(payload): Json<AddCredentialRequest>,
) -> impl IntoResponse {
    match state.service.add_credential(payload).await {
        Ok(response) => Json(response).into_response(),
        Err(e) => (e.status_code(), Json(e.into_response())).into_response(),
    }
}

/// POST /api/admin/auth/idc-device/start
/// 启动 BuilderId / Enterprise device-code 在线登录
pub async fn start_idc_device_login(
    State(state): State<AdminState>,
    Json(payload): Json<StartIdcDeviceLoginRequest>,
) -> impl IntoResponse {
    match state.service.start_idc_device_login(payload).await {
        Ok(response) => Json(response).into_response(),
        Err(e) => (e.status_code(), Json(e.into_response())).into_response(),
    }
}

/// POST /api/admin/auth/idc-device/:session_id/status
/// 查询并推进 device-code 在线登录状态
pub async fn get_idc_device_login_status(
    State(state): State<AdminState>,
    Path(session_id): Path<String>,
) -> impl IntoResponse {
    match state.service.get_idc_device_login_status(&session_id).await {
        Ok(response) => Json(response).into_response(),
        Err(e) => (e.status_code(), Json(e.into_response())).into_response(),
    }
}

/// POST /api/admin/auth/idc-device/:session_id/cancel
/// 取消 device-code 在线登录
pub async fn cancel_idc_device_login(
    State(state): State<AdminState>,
    Path(session_id): Path<String>,
) -> impl IntoResponse {
    match state.service.cancel_idc_device_login(&session_id) {
        Ok(response) => Json(response).into_response(),
        Err(e) => (e.status_code(), Json(e.into_response())).into_response(),
    }
}

/// POST /api/admin/auth/external-idp/probe
/// 探测 Kiro external IdP 组织发现和 OIDC 能力
pub async fn probe_external_idp(
    State(state): State<AdminState>,
    Json(payload): Json<ExternalIdpProbeRequest>,
) -> impl IntoResponse {
    match state.service.probe_external_idp(payload).await {
        Ok(response) => Json(response).into_response(),
        Err(e) => (e.status_code(), Json(e.into_response())).into_response(),
    }
}

/// POST /api/admin/auth/external-idp/start
/// 启动 External IdP 浏览器 PKCE 登录
pub async fn start_external_idp_login(
    State(state): State<AdminState>,
    Json(payload): Json<StartExternalIdpLoginRequest>,
) -> impl IntoResponse {
    match state.service.start_external_idp_login(payload).await {
        Ok(response) => Json(response).into_response(),
        Err(e) => (e.status_code(), Json(e.into_response())).into_response(),
    }
}

/// POST /api/admin/auth/external-idp/:session_id/status
/// 查询 External IdP 在线登录状态
pub async fn get_external_idp_login_status(
    State(state): State<AdminState>,
    Path(session_id): Path<String>,
) -> impl IntoResponse {
    match state
        .service
        .get_external_idp_login_status(&session_id)
        .await
    {
        Ok(response) => Json(response).into_response(),
        Err(e) => (e.status_code(), Json(e.into_response())).into_response(),
    }
}

/// POST /api/admin/auth/external-idp/:session_id/cancel
/// 取消 External IdP 浏览器登录
pub async fn cancel_external_idp_login(
    State(state): State<AdminState>,
    Path(session_id): Path<String>,
) -> impl IntoResponse {
    match state.service.cancel_external_idp_login(&session_id) {
        Ok(response) => Json(response).into_response(),
        Err(e) => (e.status_code(), Json(e.into_response())).into_response(),
    }
}

/// POST /api/admin/auth/external-idp/:session_id/callback
/// 手动提交 External IdP 自定义 scheme 回调 URL 或授权码
pub async fn submit_external_idp_callback(
    State(state): State<AdminState>,
    Path(session_id): Path<String>,
    Json(payload): Json<SubmitExternalIdpCallbackRequest>,
) -> impl IntoResponse {
    match state
        .service
        .submit_external_idp_callback(&session_id, payload)
        .await
    {
        Ok(response) => Json(response).into_response(),
        Err(e) => (e.status_code(), Json(e.into_response())).into_response(),
    }
}

/// POST /api/admin/auth/external-idp/callback
/// 按 OAuth state 自动定位会话并提交 External IdP 自定义 scheme 回调
pub async fn submit_external_idp_callback_by_state(
    State(state): State<AdminState>,
    Json(payload): Json<SubmitExternalIdpCallbackRequest>,
) -> impl IntoResponse {
    match state
        .service
        .submit_external_idp_callback_by_state(payload)
        .await
    {
        Ok(response) => Json(response).into_response(),
        Err(e) => (e.status_code(), Json(e.into_response())).into_response(),
    }
}

/// GET /api/admin/auth/external-idp/callback
/// 接收 Kiro portal / External IdP 浏览器回调
pub async fn handle_external_idp_callback(
    State(state): State<AdminState>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    match state.service.handle_external_idp_callback(params).await {
        Ok(ExternalIdpCallbackAction::Redirect(url)) => Redirect::temporary(&url).into_response(),
        Ok(ExternalIdpCallbackAction::Html {
            success,
            title,
            message,
        }) => external_idp_callback_html(success, &title, &message).into_response(),
        Err(e) => external_idp_callback_html(false, "External IdP 登录失败", &e.to_string())
            .into_response(),
    }
}

/// DELETE /api/admin/credentials/:id
/// 删除凭据
pub async fn delete_credential(
    State(state): State<AdminState>,
    Path(id): Path<u64>,
) -> impl IntoResponse {
    match state.service.delete_credential(id) {
        Ok(_) => Json(SuccessResponse::new(format!("凭据 #{} 已删除", id))).into_response(),
        Err(e) => (e.status_code(), Json(e.into_response())).into_response(),
    }
}

/// POST /api/admin/credentials/:id/refresh
/// 强制刷新凭据 Token
pub async fn force_refresh_token(
    State(state): State<AdminState>,
    Path(id): Path<u64>,
) -> impl IntoResponse {
    match state.service.force_refresh_token(id).await {
        Ok(_) => Json(SuccessResponse::new(format!(
            "凭据 #{} Token 已强制刷新",
            id
        )))
        .into_response(),
        Err(e) => (e.status_code(), Json(e.into_response())).into_response(),
    }
}

/// GET /api/admin/config/load-balancing
/// 获取负载均衡与等待队列配置
pub async fn get_load_balancing_mode(State(state): State<AdminState>) -> impl IntoResponse {
    match state.service.get_load_balancing_mode() {
        Ok(response) => Json(response).into_response(),
        Err(e) => (e.status_code(), Json(e.into_response())).into_response(),
    }
}

/// GET /api/admin/config/model-capabilities
/// 获取账号类型模型策略配置
pub async fn get_model_capabilities_config(State(state): State<AdminState>) -> impl IntoResponse {
    match state.service.get_model_capabilities_config() {
        Ok(response) => Json(response).into_response(),
        Err(e) => (e.status_code(), Json(e.into_response())).into_response(),
    }
}

/// GET /api/admin/config/model-catalog
/// 获取内置模型目录
pub async fn get_model_catalog(State(state): State<AdminState>) -> impl IntoResponse {
    Json(state.service.get_model_catalog()).into_response()
}

/// PUT /api/admin/config/load-balancing
/// 设置负载均衡与等待队列配置
pub async fn set_load_balancing_mode(
    State(state): State<AdminState>,
    Json(payload): Json<SetLoadBalancingModeRequest>,
) -> impl IntoResponse {
    match state.service.set_load_balancing_mode(payload) {
        Ok(response) => Json(response).into_response(),
        Err(e) => (e.status_code(), Json(e.into_response())).into_response(),
    }
}

/// PUT /api/admin/config/model-capabilities
/// 设置账号类型模型策略配置
pub async fn set_model_capabilities_config(
    State(state): State<AdminState>,
    Json(payload): Json<SetModelCapabilitiesConfigRequest>,
) -> impl IntoResponse {
    match state.service.set_model_capabilities_config(payload) {
        Ok(response) => Json(response).into_response(),
        Err(e) => (e.status_code(), Json(e.into_response())).into_response(),
    }
}
