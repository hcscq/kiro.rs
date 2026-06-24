//! Admin API 路由配置

use axum::{
    Router, middleware,
    routing::{delete, get, post},
};

use super::{
    handlers::{
        add_credential, cancel_external_idp_login, cancel_idc_device_login,
        clear_credential_runtime_model_restrictions, clear_credential_suspicious_activity,
        delete_credential, force_refresh_token, get_all_credentials, get_credential_balance,
        get_credential_groups_config, get_credential_profiles, get_external_idp_login_status,
        get_idc_device_login_status, get_load_balancing_mode, get_model_capabilities_config,
        get_model_catalog, handle_external_idp_callback, probe_external_idp, reset_failure_count,
        set_credential_disabled, set_credential_groups, set_credential_groups_config,
        set_credential_max_concurrency, set_credential_model_policy, set_credential_overage_status,
        set_credential_priority, set_credential_profile, set_credential_proxy,
        set_credential_rate_limit_config, set_credential_source, set_load_balancing_mode,
        set_model_capabilities_config, start_external_idp_login, start_idc_device_login,
        submit_external_idp_callback, submit_external_idp_callback_by_state,
    },
    middleware::{AdminState, admin_auth_middleware, admin_write_routing_middleware},
};

/// 创建 Admin API 路由
///
/// # 端点
/// - `GET /credentials` - 获取所有凭据状态
/// - `POST /credentials` - 添加新凭据
/// - `DELETE /credentials/:id` - 删除凭据
/// - `POST /credentials/:id/disabled` - 设置凭据禁用状态
/// - `POST /credentials/:id/priority` - 设置凭据优先级
/// - `POST /credentials/:id/max-concurrency` - 设置凭据并发上限
/// - `POST /credentials/:id/rate-limit-config` - 设置凭据级 token bucket 参数
/// - `POST /credentials/:id/model-policy` - 设置凭据级模型策略
/// - `POST /credentials/:id/source` - 设置凭据来源标记
/// - `GET /credentials/:id/profiles` - 获取可用 Profile 列表
/// - `POST /credentials/:id/profile` - 设置凭据当前 Profile
/// - `POST /credentials/:id/overage` - 设置凭据超额使用开关
/// - `POST /credentials/:id/runtime-model-restrictions/clear` - 清除运行时模型限制
/// - `POST /credentials/:id/suspicious-activity/clear` - 清除 suspicious activity 标记与隔离
/// - `POST /credentials/:id/reset` - 重置失败计数
/// - `POST /credentials/:id/refresh` - 强制刷新 Token
/// - `GET /credentials/:id/balance` - 获取凭据余额
/// - `GET /config/load-balancing` - 获取负载均衡与等待队列配置
/// - `PUT /config/load-balancing` - 设置负载均衡与等待队列配置
/// - `GET /config/credential-groups` - 获取凭据分组目录
/// - `PUT /config/credential-groups` - 设置凭据分组目录
/// - `GET /config/model-capabilities` - 获取账号类型模型策略
/// - `GET /config/model-catalog` - 获取内置模型目录
/// - `PUT /config/model-capabilities` - 设置账号类型模型策略
/// - `POST /auth/external-idp/probe` - 探测 External IdP discovery/PKCE/device-code 兼容性
/// - `POST /auth/external-idp/start` - 启动 External IdP 浏览器 PKCE 登录
/// - `POST /auth/external-idp/:session_id/callback` - 手动提交 External IdP 自定义 scheme 回调
/// - `GET /auth/external-idp/callback` - External IdP 浏览器回调
/// - `POST /auth/external-idp/callback` - 按 state 提交自定义 scheme 回调
///
/// # 认证
/// 需要 Admin API Key 认证，支持：
/// - `x-api-key` header
/// - `Authorization: Bearer <token>` header
pub fn create_admin_router(state: AdminState) -> Router {
    Router::new()
        .route(
            "/credentials",
            get(get_all_credentials).post(add_credential),
        )
        .route("/auth/idc-device/start", post(start_idc_device_login))
        .route(
            "/auth/idc-device/{session_id}/status",
            post(get_idc_device_login_status),
        )
        .route(
            "/auth/idc-device/{session_id}/cancel",
            post(cancel_idc_device_login),
        )
        .route("/auth/external-idp/probe", post(probe_external_idp))
        .route("/auth/external-idp/start", post(start_external_idp_login))
        .route(
            "/auth/external-idp/{session_id}/status",
            post(get_external_idp_login_status),
        )
        .route(
            "/auth/external-idp/{session_id}/cancel",
            post(cancel_external_idp_login),
        )
        .route(
            "/auth/external-idp/{session_id}/callback",
            post(submit_external_idp_callback),
        )
        .route(
            "/auth/external-idp/callback",
            get(handle_external_idp_callback).post(submit_external_idp_callback_by_state),
        )
        .route("/credentials/{id}", delete(delete_credential))
        .route("/credentials/{id}/disabled", post(set_credential_disabled))
        .route("/credentials/{id}/priority", post(set_credential_priority))
        .route(
            "/credentials/{id}/max-concurrency",
            post(set_credential_max_concurrency),
        )
        .route(
            "/credentials/{id}/rate-limit-config",
            post(set_credential_rate_limit_config),
        )
        .route(
            "/credentials/{id}/model-policy",
            post(set_credential_model_policy),
        )
        .route("/credentials/{id}/source", post(set_credential_source))
        .route("/credentials/{id}/groups", post(set_credential_groups))
        .route("/credentials/{id}/proxy", post(set_credential_proxy))
        .route("/credentials/{id}/profiles", get(get_credential_profiles))
        .route("/credentials/{id}/profile", post(set_credential_profile))
        .route(
            "/credentials/{id}/overage",
            post(set_credential_overage_status),
        )
        .route(
            "/credentials/{id}/runtime-model-restrictions/clear",
            post(clear_credential_runtime_model_restrictions),
        )
        .route(
            "/credentials/{id}/suspicious-activity/clear",
            post(clear_credential_suspicious_activity),
        )
        .route("/credentials/{id}/reset", post(reset_failure_count))
        .route("/credentials/{id}/refresh", post(force_refresh_token))
        .route("/credentials/{id}/balance", get(get_credential_balance))
        .route(
            "/config/load-balancing",
            get(get_load_balancing_mode).put(set_load_balancing_mode),
        )
        .route(
            "/config/credential-groups",
            get(get_credential_groups_config).put(set_credential_groups_config),
        )
        .route(
            "/config/model-capabilities",
            get(get_model_capabilities_config).put(set_model_capabilities_config),
        )
        .route("/config/model-catalog", get(get_model_catalog))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            admin_write_routing_middleware,
        ))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            admin_auth_middleware,
        ))
        .with_state(state)
}
