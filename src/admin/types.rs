//! Admin API 类型定义

use std::collections::BTreeMap;

use crate::model::config::RequestWeightingConfig;
use crate::model::model_policy::{
    AccountTypeDispatchPolicy, ModelSupportPolicy, RuntimeModelRestriction,
};
use serde::{Deserialize, Deserializer, Serialize};

fn deserialize_optional_nullable<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer).map(Some)
}

// ============ 凭据状态 ============

/// 所有凭据状态响应
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CredentialsStatusResponse {
    /// 凭据总数
    pub total: usize,
    /// 可用凭据数量（未禁用）
    pub available: usize,
    /// 当前可立即调度的凭据数量
    pub dispatchable: usize,
    /// 当前活跃凭据 ID
    pub current_id: u64,
    /// 各凭据状态列表
    pub credentials: Vec<CredentialStatusItem>,
}

/// 单个凭据的状态信息
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CredentialStatusItem {
    /// 凭据唯一 ID
    pub id: u64,
    /// 优先级（数字越小优先级越高）
    pub priority: u32,
    /// 是否被禁用
    pub disabled: bool,
    /// 连续失败次数
    pub failure_count: u32,
    /// 是否为当前活跃凭据
    pub is_current: bool,
    /// Token 过期时间（RFC3339 格式）
    pub expires_at: Option<String>,
    /// 认证方式
    pub auth_method: Option<String>,
    /// 是否有 Profile ARN
    pub has_profile_arn: bool,
    /// refreshToken 的 SHA-256 哈希（用于前端重复检测）
    pub refresh_token_hash: Option<String>,
    /// 用户邮箱（用于前端显示）
    pub email: Option<String>,
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
    /// 当前命中的账号类型（显式账号类型或标准档位推断）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolved_account_type: Option<String>,
    /// 当前账号类型来源
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account_type_source: Option<String>,
    /// 由订阅标题识别出的标准账号类型
    #[serde(skip_serializing_if = "Option::is_none")]
    pub standard_account_type: Option<String>,
    /// 账号级额外允许模型
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub allowed_models: Vec<String>,
    /// 账号级额外禁用模型
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub blocked_models: Vec<String>,
    /// 运行时探测到的临时模型限制
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub runtime_model_restrictions: Vec<RuntimeModelRestriction>,
    /// 导入时间（RFC3339 格式）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub imported_at: Option<String>,
    /// API 调用成功次数
    pub success_count: u64,
    /// 最后一次 API 调用时间（RFC3339 格式）
    pub last_used_at: Option<String>,
    /// 当前运行中的请求数
    pub in_flight: usize,
    /// 当前生效的单账号并发上限（空表示不限制）
    pub max_concurrency: Option<u32>,
    /// 凭据级显式并发覆盖
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_concurrency_override: Option<u32>,
    /// 当前并发上限来源
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_concurrency_source: Option<String>,
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
    /// 禁用时间
    #[serde(skip_serializing_if = "Option::is_none")]
    pub disabled_at: Option<String>,
    /// 最近一次异常状态码
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error_status: Option<u16>,
    /// 最近一次异常摘要
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error_summary: Option<String>,
    /// 429 冷却剩余时间（毫秒）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cooldown_remaining_ms: Option<u64>,
    /// 当前 bucket token 数
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limit_bucket_tokens: Option<f64>,
    /// 当前 bucket 容量
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limit_bucket_capacity: Option<f64>,
    /// 凭据级 bucket 容量覆盖
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limit_bucket_capacity_override: Option<f64>,
    /// 当前 bucket 容量来源
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limit_bucket_capacity_source: Option<String>,
    /// 当前生效回填速率（token/s）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limit_refill_per_second: Option<f64>,
    /// 凭据级回填速率覆盖
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limit_refill_per_second_override: Option<f64>,
    /// 当前回填速率来源
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

// ============ 操作请求 ============

/// 启用/禁用凭据请求
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetDisabledRequest {
    /// 是否禁用
    pub disabled: bool,
}

/// 修改优先级请求
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetPriorityRequest {
    /// 新优先级值
    pub priority: u32,
}

/// 修改并发上限请求
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetMaxConcurrencyRequest {
    /// 并发上限，null 或 0 表示不限制
    pub max_concurrency: Option<u32>,
}

/// 修改凭据级 token bucket 配置
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetCredentialRateLimitConfigRequest {
    /// 凭据级 bucket 容量覆盖
    /// 字段缺失表示不修改；null 表示跟随全局；0 表示仅对该账号禁用 bucket
    #[serde(default, deserialize_with = "deserialize_optional_nullable")]
    pub rate_limit_bucket_capacity: Option<Option<f64>>,
    /// 凭据级回填速率覆盖（token/s）
    /// 字段缺失表示不修改；null 表示跟随全局；0 表示仅对该账号禁用 bucket
    #[serde(default, deserialize_with = "deserialize_optional_nullable")]
    pub rate_limit_refill_per_second: Option<Option<f64>>,
}

/// 修改凭据模型策略请求
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetCredentialModelPolicyRequest {
    /// 字段缺失表示不修改；null 表示清空账号类型
    #[serde(default, deserialize_with = "deserialize_optional_nullable")]
    pub account_type: Option<Option<String>>,
    /// 字段缺失表示不修改；null 表示清空允许列表
    #[serde(default, deserialize_with = "deserialize_optional_nullable")]
    pub allowed_models: Option<Option<Vec<String>>>,
    /// 字段缺失表示不修改；null 表示清空拒绝列表
    #[serde(default, deserialize_with = "deserialize_optional_nullable")]
    pub blocked_models: Option<Option<Vec<String>>>,
    /// 是否清空运行时探测到的临时限制
    #[serde(default)]
    pub clear_runtime_model_restrictions: bool,
}

/// 添加凭据请求
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AddCredentialRequest {
    /// 刷新令牌（必填）
    pub refresh_token: String,

    /// 认证方式（可选，默认 social）
    #[serde(default = "default_auth_method")]
    pub auth_method: String,

    /// OIDC Client ID（IdC 认证需要）
    pub client_id: Option<String>,

    /// OIDC Client Secret（IdC 认证需要）
    pub client_secret: Option<String>,

    /// 优先级（可选，默认 0）
    #[serde(default)]
    pub priority: u32,

    /// 单账号并发上限（可选）
    pub max_concurrency: Option<u32>,

    /// 凭据级 token bucket 容量覆盖（可选）
    pub rate_limit_bucket_capacity: Option<f64>,

    /// 凭据级 token bucket 回填速率覆盖（token/s，可选）
    pub rate_limit_refill_per_second: Option<f64>,

    /// 凭据级 Region 配置（用于 OIDC token 刷新）
    /// 未配置时回退到 config.json 的全局 region
    pub region: Option<String>,

    /// 凭据级 Auth Region（用于 Token 刷新）
    pub auth_region: Option<String>,

    /// 凭据级 API Region（用于 API 请求）
    pub api_region: Option<String>,

    /// 凭据级 Machine ID（可选，64 位字符串）
    /// 未配置时回退到 config.json 的 machineId
    pub machine_id: Option<String>,

    /// AWS IAM Identity Center Start URL（企业 IdC 账号可选）
    pub start_url: Option<String>,

    /// 用户邮箱（可选，用于前端显示）
    pub email: Option<String>,

    /// 账号类型（可选）
    pub account_type: Option<String>,

    /// 账号级额外允许模型
    pub allowed_models: Option<Vec<String>>,

    /// 账号级额外禁用模型
    pub blocked_models: Option<Vec<String>>,

    /// 凭据级代理 URL（可选，特殊值 "direct" 表示不使用代理）
    pub proxy_url: Option<String>,

    /// 凭据级代理认证用户名（可选）
    pub proxy_username: Option<String>,

    /// 凭据级代理认证密码（可选）
    pub proxy_password: Option<String>,
}

fn default_auth_method() -> String {
    "social".to_string()
}

/// 添加凭据成功响应
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AddCredentialResponse {
    pub success: bool,
    pub message: String,
    /// 新添加的凭据 ID
    pub credential_id: u64,
    /// 用户邮箱（如果获取成功）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subscription_title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subscription_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_account_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolved_account_type: Option<String>,
}

// ============ 余额查询 ============

/// 余额查询响应
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BalanceResponse {
    /// 凭据 ID
    pub id: u64,
    /// 订阅类型
    pub subscription_title: Option<String>,
    /// 订阅内部类型
    pub subscription_type: Option<String>,
    /// 当前使用量
    pub current_usage: f64,
    /// 使用限额
    pub usage_limit: f64,
    /// 剩余额度
    pub remaining: f64,
    /// 使用百分比
    pub usage_percentage: f64,
    /// 下次重置时间（Unix 时间戳）
    pub next_reset_at: Option<f64>,
}

// ============ 负载均衡配置 ============

/// 负载均衡配置响应
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LoadBalancingModeResponse {
    /// 当前模式（"priority" 或 "balanced"）
    pub mode: String,
    /// 是否启用会话到凭据的软亲和调度
    pub session_affinity_enabled: bool,
    /// 最大排队数量（0 表示禁用等待队列）
    pub queue_max_size: usize,
    /// 最大等待时间（毫秒，0 表示禁用等待队列）
    pub queue_max_wait_ms: u64,
    /// 单账号触发 429 后的冷却时间（毫秒，0 表示禁用 429 冷却）
    pub rate_limit_cooldown_ms: u64,
    /// 是否启用上游 429 后的本地冷却与 bucket 退避
    pub rate_limit_cooldown_enabled: bool,
    /// 是否启用“模型不支持”后的运行时模型冷却
    pub model_cooldown_enabled: bool,
    /// 全局默认单账号并发上限（null 表示不限制）
    pub default_max_concurrency: Option<u32>,
    /// 单账号 token bucket 容量（<= 0 表示禁用）
    pub rate_limit_bucket_capacity: f64,
    /// 单账号 token bucket 基础回填速率（token/s，<= 0 表示禁用）
    pub rate_limit_refill_per_second: f64,
    /// 429 退避后允许降到的最小回填速率（token/s）
    pub rate_limit_refill_min_per_second: f64,
    /// 每次成功请求恢复的回填速率增量（token/s）
    pub rate_limit_refill_recovery_step_per_success: f64,
    /// 遭遇 429 时的回填速率衰减系数（0.05-1）
    pub rate_limit_refill_backoff_factor: f64,
    /// 轻/重请求的本地令牌消耗权重规则
    pub request_weighting: RequestWeightingConfig,
    /// 当前正在排队的请求数
    pub waiting_requests: usize,
}

/// 设置负载均衡配置请求
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetLoadBalancingModeRequest {
    /// 模式（"priority" 或 "balanced"）
    pub mode: Option<String>,
    /// 是否启用会话到凭据的软亲和调度
    pub session_affinity_enabled: Option<bool>,
    /// 最大排队数量（0 表示禁用等待队列）
    pub queue_max_size: Option<usize>,
    /// 最大等待时间（毫秒，0 表示禁用等待队列）
    pub queue_max_wait_ms: Option<u64>,
    /// 单账号触发 429 后的冷却时间（毫秒，0 表示禁用 429 冷却）
    pub rate_limit_cooldown_ms: Option<u64>,
    /// 是否启用上游 429 后的本地冷却与 bucket 退避
    pub rate_limit_cooldown_enabled: Option<bool>,
    /// 是否启用“模型不支持”后的运行时模型冷却
    pub model_cooldown_enabled: Option<bool>,
    /// 全局默认单账号并发上限（0 表示不限制；字段缺失表示不修改）
    pub default_max_concurrency: Option<u32>,
    /// 单账号 token bucket 容量（<= 0 表示禁用）
    pub rate_limit_bucket_capacity: Option<f64>,
    /// 单账号 token bucket 基础回填速率（token/s，<= 0 表示禁用）
    pub rate_limit_refill_per_second: Option<f64>,
    /// 429 退避后允许降到的最小回填速率（token/s）
    pub rate_limit_refill_min_per_second: Option<f64>,
    /// 每次成功请求恢复的回填速率增量（token/s）
    pub rate_limit_refill_recovery_step_per_success: Option<f64>,
    /// 遭遇 429 时的回填速率衰减系数（0.05-1）
    pub rate_limit_refill_backoff_factor: Option<f64>,
    /// 轻/重请求的本地令牌消耗权重规则
    pub request_weighting: Option<RequestWeightingConfig>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StandardAccountTypePresetResponse {
    pub id: String,
    pub display_name: String,
    pub description: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub subscription_title_examples: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recommended_policy: Option<ModelSupportPolicy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recommended_dispatch_policy: Option<AccountTypeDispatchPolicy>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelCapabilitiesConfigResponse {
    pub account_type_policies: BTreeMap<String, ModelSupportPolicy>,
    pub account_type_dispatch_policies: BTreeMap<String, AccountTypeDispatchPolicy>,
    pub standard_account_type_presets: Vec<StandardAccountTypePresetResponse>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelCatalogItemResponse {
    pub api_id: String,
    pub policy_id: String,
    pub display_name: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelCatalogResponse {
    pub models: Vec<ModelCatalogItemResponse>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetModelCapabilitiesConfigRequest {
    pub account_type_policies: Option<BTreeMap<String, ModelSupportPolicy>>,
    pub account_type_dispatch_policies: Option<BTreeMap<String, AccountTypeDispatchPolicy>>,
}

// ============ 通用响应 ============

/// 操作成功响应
#[derive(Debug, Serialize)]
pub struct SuccessResponse {
    pub success: bool,
    pub message: String,
}

impl SuccessResponse {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            success: true,
            message: message.into(),
        }
    }
}

/// 错误响应
#[derive(Debug, Serialize)]
pub struct AdminErrorResponse {
    pub error: AdminError,
}

#[derive(Debug, Serialize)]
pub struct AdminError {
    #[serde(rename = "type")]
    pub error_type: String,
    pub message: String,
}

impl AdminErrorResponse {
    pub fn new(error_type: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            error: AdminError {
                error_type: error_type.into(),
                message: message.into(),
            },
        }
    }

    pub fn invalid_request(message: impl Into<String>) -> Self {
        Self::new("invalid_request", message)
    }

    pub fn authentication_error() -> Self {
        Self::new("authentication_error", "Invalid or missing admin API key")
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new("not_found", message)
    }

    pub fn api_error(message: impl Into<String>) -> Self {
        Self::new("api_error", message)
    }

    pub fn internal_error(message: impl Into<String>) -> Self {
        Self::new("internal_error", message)
    }
}

#[cfg(test)]
mod tests {
    use super::SetCredentialRateLimitConfigRequest;

    #[test]
    fn set_credential_rate_limit_config_request_distinguishes_missing_null_and_values() {
        let missing: SetCredentialRateLimitConfigRequest = serde_json::from_str("{}").unwrap();
        assert_eq!(missing.rate_limit_bucket_capacity, None);
        assert_eq!(missing.rate_limit_refill_per_second, None);

        let nulls: SetCredentialRateLimitConfigRequest = serde_json::from_str(
            r#"{
                "rateLimitBucketCapacity": null,
                "rateLimitRefillPerSecond": null
            }"#,
        )
        .unwrap();
        assert_eq!(nulls.rate_limit_bucket_capacity, Some(None));
        assert_eq!(nulls.rate_limit_refill_per_second, Some(None));

        let values: SetCredentialRateLimitConfigRequest = serde_json::from_str(
            r#"{
                "rateLimitBucketCapacity": 0,
                "rateLimitRefillPerSecond": 1.5
            }"#,
        )
        .unwrap();
        assert_eq!(values.rate_limit_bucket_capacity, Some(Some(0.0)));
        assert_eq!(values.rate_limit_refill_per_second, Some(Some(1.5)));
    }
}
