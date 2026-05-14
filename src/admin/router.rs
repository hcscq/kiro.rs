//! Admin API 路由配置

use axum::{
    Router, middleware,
    routing::{delete, get, post},
};

use super::{
    handlers::{
        add_credential, clear_credential_runtime_model_restrictions, delete_credential,
        force_refresh_token, get_all_credentials, get_credential_balance, get_load_balancing_mode,
        get_model_capabilities_config, get_model_catalog, reset_failure_count,
        set_credential_disabled, set_credential_max_concurrency, set_credential_model_policy,
        set_credential_priority, set_credential_rate_limit_config, set_load_balancing_mode,
        set_model_capabilities_config,
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
/// - `POST /credentials/:id/runtime-model-restrictions/clear` - 清除运行时模型限制
/// - `POST /credentials/:id/reset` - 重置失败计数
/// - `POST /credentials/:id/refresh` - 强制刷新 Token
/// - `GET /credentials/:id/balance` - 获取凭据余额
/// - `GET /config/load-balancing` - 获取负载均衡与等待队列配置
/// - `PUT /config/load-balancing` - 设置负载均衡与等待队列配置
/// - `GET /config/model-capabilities` - 获取账号类型模型策略
/// - `GET /config/model-catalog` - 获取内置模型目录
/// - `PUT /config/model-capabilities` - 设置账号类型模型策略
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
        .route(
            "/credentials/{id}/runtime-model-restrictions/clear",
            post(clear_credential_runtime_model_restrictions),
        )
        .route("/credentials/{id}/reset", post(reset_failure_count))
        .route("/credentials/{id}/refresh", post(force_refresh_token))
        .route("/credentials/{id}/balance", get(get_credential_balance))
        .route(
            "/config/load-balancing",
            get(get_load_balancing_mode).put(set_load_balancing_mode),
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
