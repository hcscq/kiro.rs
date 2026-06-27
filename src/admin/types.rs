//! Admin API 类型定义

use std::collections::BTreeMap;

use crate::kiro::model::available_profiles::AvailableProfile;
use crate::kiro::model::usage_limits::UsageLimitsResponse;
use crate::model::config::{
    CredentialGroupConfig, KiroRequestBodyGuardConfig, NonStreamBodyReadTimeoutConfig,
    ProxyPoolConfig, ProxyPoolFailoverConfig, RequestWeightingConfig, StreamPreSseFailoverConfig,
    ThinkingSignatureValidationMode,
};
use crate::model::model_policy::{
    AccountTypeDispatchPolicy, ModelSupportPolicy, RuntimeModelRestriction,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};

fn deserialize_optional_nullable<'de, D, T>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer).map(Some)
}

fn normalize_scope_string(value: &str) -> Option<String> {
    let joined = value
        .split_whitespace()
        .filter(|part| !part.trim().is_empty())
        .collect::<Vec<_>>()
        .join(" ");
    (!joined.is_empty()).then_some(joined)
}

fn deserialize_optional_scope_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    let Some(value) = value else {
        return Ok(None);
    };

    match value {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::String(value) => Ok(normalize_scope_string(&value)),
        serde_json::Value::Array(values) => {
            let mut scopes = Vec::new();
            for value in values {
                match value {
                    serde_json::Value::String(value) => {
                        scopes.extend(
                            value
                                .split_whitespace()
                                .filter(|scope| !scope.trim().is_empty())
                                .map(str::to_string),
                        );
                    }
                    serde_json::Value::Null => {}
                    other => {
                        return Err(serde::de::Error::custom(format!(
                            "scopes entries must be strings, got {other}"
                        )));
                    }
                }
            }
            let joined = scopes.join(" ");
            Ok((!joined.is_empty()).then_some(joined))
        }
        other => Err(serde::de::Error::custom(format!(
            "scopes must be a string or string array, got {other}"
        ))),
    }
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
    /// 凭据状态修订号（共享后端启用时来自 Redis）
    pub credentials_revision: u64,
    /// 余额缓存修订号
    pub balance_cache_revision: u64,
    /// 凭据列表摘要指纹
    pub credentials_fingerprint: u64,
    /// 各凭据状态列表
    pub credentials: Vec<CredentialStatusItem>,
}

/// 凭据列表增量请求。客户端提交当前缓存中的每个凭据 ID 与指纹。
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CredentialsDeltaRequest {
    /// 客户端已知的凭据状态修订号
    #[serde(default)]
    pub since_revision: u64,
    /// 客户端已知的余额缓存修订号
    #[serde(default)]
    pub balance_cache_revision: u64,
    /// 客户端已知的全局凭据摘要指纹
    #[serde(default)]
    pub credentials_fingerprint: u64,
    /// 客户端已缓存的凭据项指纹
    #[serde(default)]
    pub known_credentials: Vec<KnownCredentialFingerprint>,
}

/// 客户端已缓存的单个凭据指纹。
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KnownCredentialFingerprint {
    pub id: u64,
    pub fingerprint: u64,
}

/// 凭据列表增量响应。
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CredentialsDeltaResponse {
    /// 当前是否要求客户端退回全量刷新
    pub reset_required: bool,
    /// 退回全量刷新原因
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// 最新凭据状态修订号
    pub revision: u64,
    /// 最新余额缓存修订号
    pub balance_revision: u64,
    /// 最新凭据列表摘要指纹
    pub fingerprint: u64,
    /// 当前凭据总数
    pub total: usize,
    /// 当前可用凭据数量
    pub available: usize,
    /// 当前可立即调度的凭据数量
    pub dispatchable: usize,
    /// 当前活跃凭据 ID
    pub current_id: u64,
    /// 新增或已变化的凭据项
    pub upserts: Vec<CredentialStatusItem>,
    /// 已删除的凭据 ID
    pub deleted_ids: Vec<u64>,
    /// 响应生成时间
    pub generated_at: DateTime<Utc>,
}

/// 凭据运行态增量请求。客户端提交当前缓存中的每个凭据 ID 与运行态指纹。
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CredentialsRuntimeDeltaRequest {
    /// 客户端已缓存的凭据运行态指纹
    #[serde(default)]
    pub known_runtime: Vec<KnownCredentialRuntimeFingerprint>,
}

/// 客户端已缓存的单个凭据运行态指纹。
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KnownCredentialRuntimeFingerprint {
    pub id: u64,
    pub runtime_fingerprint: u64,
}

/// 凭据运行态增量响应。
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CredentialsRuntimeDeltaResponse {
    /// 新增或已变化的凭据运行态
    pub updates: Vec<CredentialRuntimeStatusItem>,
    /// 客户端缓存中已不存在的凭据 ID
    pub deleted_ids: Vec<u64>,
    /// 响应生成时间
    pub generated_at: DateTime<Utc>,
}

/// 单个凭据的热运行态字段。
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CredentialRuntimeStatusItem {
    pub id: u64,
    pub runtime_fingerprint: u64,
    pub success_count: u64,
    pub token_usage_count: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_tokens: u64,
    pub last_used_at: Option<String>,
    pub in_flight: usize,
    pub cooldown_remaining_ms: Option<u64>,
    pub suspicious_activity_quarantine_remaining_ms: Option<u64>,
    pub rate_limit_bucket_tokens: Option<f64>,
    pub rate_limit_bucket_capacity: Option<f64>,
    pub rate_limit_refill_per_second: Option<f64>,
    pub rate_limit_hit_streak: u32,
    pub next_ready_in_ms: Option<u64>,
}

/// 单个凭据的状态信息
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CredentialStatusItem {
    /// 凭据唯一 ID
    pub id: u64,
    /// 单项结构/配置/异常/余额缓存指纹，用于增量更新比较
    pub fingerprint: u64,
    /// 单项热运行态指纹，用于局部更新高频字段
    pub runtime_fingerprint: u64,
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
    /// 登录 Provider（Google / Github / BuilderId / Enterprise）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    /// 是否有当前生效的 Profile ARN
    pub has_profile_arn: bool,
    /// 当前生效的 Profile ARN（显式保存、发现得到或账号类型默认值）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_arn: Option<String>,
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
    pub credential_groups: Vec<String>,
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
    /// 完整响应累计计费输入 tokens
    pub input_tokens: u64,
    /// 完整响应累计输出 tokens
    pub output_tokens: u64,
    /// 完整响应累计总 tokens
    pub total_tokens: u64,
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
    /// 当前 429 冷却与 bucket 退避开关来源
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limit_cooldown_enabled_source: Option<String>,
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
    /// 最近一次缓存的额度数据（不会在列表接口触发上游查询）
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cached_balance: Option<CachedBalanceResponse>,
}

/// 凭据列表中附带的额度缓存快照
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CachedBalanceResponse {
    /// 缓存写入时间（Unix 秒）
    pub cached_at: f64,
    /// 缓存的额度数据
    pub balance: BalanceResponse,
}

/// Admin 实时状态事件，仅包含轻量摘要和修订号。
#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AdminStateEvent {
    /// 服务端事件序号，每次摘要变化时递增
    pub sequence: u64,
    /// 凭据状态修订号（共享后端启用时来自 Redis）
    pub credentials_revision: u64,
    /// 调度配置修订号（共享后端启用时来自 Redis）
    pub dispatch_revision: u64,
    /// 余额缓存修订号（共享后端启用时来自 Redis）
    pub balance_cache_revision: u64,
    /// 凭据摘要指纹，本地状态后端没有共享修订号时用于变更检测
    pub credentials_fingerprint: u64,
    /// 调度配置摘要指纹，本地状态后端没有共享修订号时用于变更检测
    pub dispatch_fingerprint: u64,
    /// 凭据热运行态摘要指纹，用于触发轻量运行态增量刷新
    pub runtime_fingerprint: u64,
    /// 凭据总数
    pub total: usize,
    /// 可用凭据数量（未禁用）
    pub available: usize,
    /// 当前可立即调度的凭据数量
    pub dispatchable: usize,
    /// 当前运行中的请求总数
    pub in_flight: usize,
    /// 等待队列中的请求数
    pub waiting_requests: usize,
    /// 当前有 429 冷却、bucket 等待或 suspicious activity 隔离的凭据数量
    pub rate_limited: usize,
    /// 当前禁用、失败、刷新失败或有最近错误的凭据数量
    pub abnormal: usize,
    /// 当前选中的凭据 ID
    pub current_id: u64,
    /// 事件生成时间
    pub generated_at: DateTime<Utc>,
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
    /// 凭据级 429 冷却与 bucket 退避开关覆盖
    /// 字段缺失表示不修改；null 表示跟随全局；true/false 表示凭据级强制开启/关闭
    #[serde(default, deserialize_with = "deserialize_optional_nullable")]
    pub rate_limit_cooldown_enabled: Option<Option<bool>>,
    /// 凭据级 bucket 容量覆盖
    /// 字段缺失表示不修改；null 表示跟随全局；0 表示仅对该账号禁用 bucket
    #[serde(default, deserialize_with = "deserialize_optional_nullable")]
    pub rate_limit_bucket_capacity: Option<Option<f64>>,
    /// 凭据级回填速率覆盖（token/s）
    /// 字段缺失表示不修改；null 表示跟随全局；0 表示仅对该账号禁用 bucket
    #[serde(default, deserialize_with = "deserialize_optional_nullable")]
    pub rate_limit_refill_per_second: Option<Option<f64>>,
}

/// 修改凭据代理绑定请求
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetCredentialProxyRequest {
    /// 代理模式：auto / pool / custom / direct / global
    pub mode: String,
    /// pool 模式使用的代理池节点 ID
    #[serde(default)]
    pub proxy_id: Option<String>,
    /// custom 模式使用的显式代理 URL
    #[serde(default)]
    pub proxy_url: Option<String>,
    /// custom 模式使用的代理用户名
    #[serde(default)]
    pub proxy_username: Option<String>,
    /// custom 模式使用的代理密码
    #[serde(default)]
    pub proxy_password: Option<String>,
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

/// 修改凭据来源标记请求
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetCredentialSourceRequest {
    /// 字段缺失表示不修改；null 或空字符串表示清空供应商 ID
    #[serde(default, deserialize_with = "deserialize_optional_nullable")]
    pub source_supplier_id: Option<Option<String>>,
    /// 字段缺失表示不修改；null 或空字符串表示清空供应商名称
    #[serde(default, deserialize_with = "deserialize_optional_nullable")]
    pub source_supplier_name: Option<Option<String>>,
    /// 字段缺失表示不修改；null 或空字符串表示清空批次
    #[serde(default, deserialize_with = "deserialize_optional_nullable")]
    pub source_batch: Option<Option<String>>,
}

/// 修改凭据分组请求
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetCredentialGroupsRequest {
    /// 完整替换凭据分组；空数组表示清空，运行时按 default 参与旧凭据兼容匹配
    #[serde(default)]
    pub credential_groups: Vec<String>,
}

/// 凭据分组目录项
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CredentialGroupConfigItem {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

impl From<CredentialGroupConfig> for CredentialGroupConfigItem {
    fn from(value: CredentialGroupConfig) -> Self {
        Self {
            name: value.name,
            display_name: value.display_name,
            description: value.description,
            enabled: value.enabled,
        }
    }
}

impl From<CredentialGroupConfigItem> for CredentialGroupConfig {
    fn from(value: CredentialGroupConfigItem) -> Self {
        Self {
            name: value.name,
            display_name: value.display_name,
            description: value.description,
            enabled: value.enabled,
        }
    }
}

fn default_true() -> bool {
    true
}

/// 凭据分组使用情况
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CredentialGroupUsageItem {
    pub name: String,
    pub credential_count: usize,
    pub api_key_count: usize,
    pub known: bool,
    pub enabled: bool,
}

/// 凭据分组目录响应
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CredentialGroupsConfigResponse {
    pub groups: Vec<CredentialGroupConfigItem>,
    pub usage: Vec<CredentialGroupUsageItem>,
    pub legacy_full_access_key: bool,
    pub unknown_credential_groups: Vec<String>,
}

/// 设置凭据分组目录请求
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetCredentialGroupsConfigRequest {
    pub groups: Vec<CredentialGroupConfigItem>,
}

/// 设置凭据超额使用开关请求
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetOverageStatusRequest {
    /// 是否开启超额使用
    pub enabled: bool,
}

/// 设置凭据 Profile ARN 请求
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetCredentialProfileRequest {
    /// 要选择的 Profile ARN
    pub profile_arn: String,
}

/// 凭据可用 Profile 列表响应
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CredentialProfilesResponse {
    /// 凭据 ID
    pub id: u64,
    /// 当前显式选择/保存的 Profile ARN
    pub selected_profile_arn: Option<String>,
    /// 上游返回的可用 Profile 列表
    pub profiles: Vec<AvailableProfile>,
}

/// 添加凭据请求
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AddCredentialRequest {
    /// 刷新令牌（必填）
    pub refresh_token: String,

    /// 认证方式（可选，默认 social；支持 social / idc / external_idp）
    #[serde(default = "default_auth_method")]
    pub auth_method: String,

    /// 登录 Provider（Google / Github / BuilderId / Enterprise）
    pub provider: Option<String>,

    /// Profile ARN（可选；BuilderID 缺省时后端会使用默认 BuilderID profile，Enterprise 会优先自动发现可用 profile）
    pub profile_arn: Option<String>,

    /// OIDC Client ID（IdC 认证需要）
    pub client_id: Option<String>,

    /// OIDC Client Secret（IdC 认证需要）
    pub client_secret: Option<String>,

    /// 外部 IdP Token Endpoint（external_idp 可选；缺省时通过 issuerUrl discovery）
    pub token_endpoint: Option<String>,

    /// 外部 IdP Issuer URL（external_idp 需要）
    pub issuer_url: Option<String>,

    /// 外部 IdP scopes（空格分隔）
    #[serde(default, deserialize_with = "deserialize_optional_scope_string")]
    pub scopes: Option<String>,

    /// 外部 IdP audience（可选）
    pub audience: Option<String>,

    /// 优先级（可选，默认 0）
    #[serde(default)]
    pub priority: u32,

    /// 单账号并发上限（可选）
    pub max_concurrency: Option<u32>,

    /// 凭据级 429 冷却与 bucket 退避开关覆盖（可选）
    pub rate_limit_cooldown_enabled: Option<bool>,

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

    /// 用户 ID（企业账号可能没有 email）
    pub user_id: Option<String>,

    /// 账号类型（可选）
    pub account_type: Option<String>,

    /// 账号来源供应商 ID（可选，用于兼容 KAM 导入）
    pub source_supplier_id: Option<String>,

    /// 账号来源供应商名称（可选）
    pub source_supplier_name: Option<String>,

    /// 账号来源批次（可选）
    pub source_batch: Option<String>,

    /// 凭据分组标记（可选）
    pub credential_groups: Option<Vec<String>>,

    /// 账号级额外允许模型
    pub allowed_models: Option<Vec<String>>,

    /// 账号级额外禁用模型
    pub blocked_models: Option<Vec<String>>,

    /// 从 KAM 导入的可用模型缓存（可选）
    pub available_model_ids: Option<Vec<String>>,

    /// 凭据级代理 URL（可选，特殊值 "direct" 表示不使用代理）
    pub proxy_url: Option<String>,

    /// 凭据级代理认证用户名（可选）
    pub proxy_username: Option<String>,

    /// 凭据级代理认证密码（可选）
    pub proxy_password: Option<String>,

    /// 代理池 ID（可选；留空时由后端按代理池策略自动分配）
    pub proxy_id: Option<String>,
}

fn default_auth_method() -> String {
    "social".to_string()
}

/// 添加凭据成功响应
#[derive(Debug, Clone, Serialize)]
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
    pub user_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subscription_title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subscription_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_account_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolved_account_type: Option<String>,
}

// ============ 在线登录 ============

/// AWS IdC device-code 登录请求
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartIdcDeviceLoginRequest {
    /// Provider：BuilderId 或 Enterprise
    pub provider: String,

    /// Enterprise 必填；BuilderId 留空时使用 https://view.awsapps.com/start
    #[serde(default)]
    pub start_url: Option<String>,

    /// AWS SSO OIDC Region。留空时使用 config.authRegion/config.region
    #[serde(default)]
    pub region: Option<String>,

    /// 凭据级 Auth Region（用于后续刷新）
    #[serde(default)]
    pub auth_region: Option<String>,

    /// 凭据级 API Region（用于后续 API 请求）
    #[serde(default)]
    pub api_region: Option<String>,

    /// Profile ARN（可选；Enterprise 可留空由后端自动发现）
    #[serde(default)]
    pub profile_arn: Option<String>,

    /// 优先级（可选，默认 0）
    #[serde(default)]
    pub priority: u32,

    /// 单账号并发上限（可选）
    #[serde(default)]
    pub max_concurrency: Option<u32>,

    /// 凭据级 Machine ID（可选）
    #[serde(default)]
    pub machine_id: Option<String>,

    /// 账号类型（可选）
    #[serde(default)]
    pub account_type: Option<String>,

    /// 账号来源供应商 ID（可选）
    #[serde(default)]
    pub source_supplier_id: Option<String>,

    /// 账号来源供应商名称（可选）
    #[serde(default)]
    pub source_supplier_name: Option<String>,

    /// 账号来源批次（可选）
    #[serde(default)]
    pub source_batch: Option<String>,

    /// 凭据分组标记（可选）
    #[serde(default)]
    pub credential_groups: Option<Vec<String>>,

    /// 凭据级代理 URL（可选，特殊值 "direct" 表示不使用代理）
    #[serde(default)]
    pub proxy_url: Option<String>,

    /// 凭据级代理认证用户名（可选）
    #[serde(default)]
    pub proxy_username: Option<String>,

    /// 凭据级代理认证密码（可选）
    #[serde(default)]
    pub proxy_password: Option<String>,

    /// 代理池 ID（可选）
    #[serde(default)]
    pub proxy_id: Option<String>,
}

/// AWS IdC device-code 登录状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum IdcDeviceLoginStatus {
    Pending,
    Completed,
    Failed,
    Expired,
    Cancelled,
}

/// AWS IdC device-code 登录启动响应
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IdcDeviceLoginStartResponse {
    pub session_id: String,
    pub status: IdcDeviceLoginStatus,
    pub provider: String,
    pub start_url: String,
    pub region: String,
    pub user_code: String,
    pub verification_uri: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verification_uri_complete: Option<String>,
    pub expires_at: chrono::DateTime<chrono::Utc>,
    pub interval_seconds: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// AWS IdC device-code 登录状态响应
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IdcDeviceLoginStatusResponse {
    pub session_id: String,
    pub status: IdcDeviceLoginStatus,
    pub provider: String,
    pub start_url: String,
    pub region: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verification_uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verification_uri_complete: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<chrono::DateTime<chrono::Utc>>,
    pub interval_seconds: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credential_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subscription_title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subscription_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_account_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolved_account_type: Option<String>,
}

/// External IdP 兼容性探测请求
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExternalIdpProbeRequest {
    /// 工作邮箱；后端会提取 @ 后的域名做 Kiro 组织发现
    #[serde(default)]
    pub work_email: Option<String>,

    /// 直接指定组织域名；优先级低于 workEmail
    #[serde(default)]
    pub domain_name: Option<String>,

    /// 直接指定 Issuer URL；用于绕过 Kiro metadata 后单独测试 OIDC discovery
    #[serde(default)]
    pub issuer_url: Option<String>,

    /// 直接指定 OIDC Client ID；用于后续 PKCE 流程验证
    #[serde(default)]
    pub client_id: Option<String>,

    /// 直接指定 scopes；未指定时使用 Kiro metadata 返回值
    #[serde(default, deserialize_with = "deserialize_optional_scope_string")]
    pub scopes: Option<String>,

    /// 直接指定 audience；未指定时使用 Kiro metadata 返回值
    #[serde(default)]
    pub audience: Option<String>,

    /// 是否抓取 issuer 的 OIDC discovery 文档，默认 true
    #[serde(default = "default_probe_oidc")]
    pub probe_oidc: bool,

    /// 凭据级代理 URL（可选，特殊值 "direct" 表示不使用代理）
    #[serde(default)]
    pub proxy_url: Option<String>,

    /// 凭据级代理认证用户名（可选）
    #[serde(default)]
    pub proxy_username: Option<String>,

    /// 凭据级代理认证密码（可选）
    #[serde(default)]
    pub proxy_password: Option<String>,

    /// 代理池 ID（可选）
    #[serde(default)]
    pub proxy_id: Option<String>,
}

fn default_probe_oidc() -> bool {
    true
}

/// External IdP 探测阶段状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ExternalIdpProbeStatus {
    Ok,
    NotFound,
    Skipped,
    Failed,
}

/// External IdP OIDC discovery 摘要
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExternalIdpOidcDiscoverySummary {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub issuer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub authorization_endpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token_endpoint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_authorization_endpoint: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub code_challenge_methods_supported: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub grant_types_supported: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub response_types_supported: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scopes_supported: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub token_endpoint_auth_methods_supported: Vec<String>,
}

/// External IdP 兼容性探测响应
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExternalIdpProbeResponse {
    pub domain_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub work_email: Option<String>,
    pub kiro_metadata_status: ExternalIdpProbeStatus,
    pub oidc_discovery_status: ExternalIdpProbeStatus,
    pub found: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub issuer_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scopes: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audience: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oidc: Option<ExternalIdpOidcDiscoverySummary>,
    pub pkce_s256_supported: bool,
    pub device_code_supported: bool,
    pub authorization_code_supported: bool,
    pub refresh_without_client_secret_likely_supported: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recommendations: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// External IdP 浏览器登录请求
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StartExternalIdpLoginRequest {
    /// 工作邮箱；手动 issuer/client/scopes 不完整时会走 Kiro portal 组织发现
    #[serde(default)]
    pub work_email: Option<String>,

    /// 组织域名；用于展示/记录，portal 模式仍由 Kiro portal 让用户确认组织
    #[serde(default)]
    pub domain_name: Option<String>,

    /// 直接指定 Issuer URL；issuer/client/scopes 齐全时跳过 Kiro portal
    #[serde(default)]
    pub issuer_url: Option<String>,

    /// 直接指定 OIDC Client ID
    #[serde(default)]
    pub client_id: Option<String>,

    /// 直接指定 scopes；未包含 offline_access 时后端会自动追加
    #[serde(default, deserialize_with = "deserialize_optional_scope_string")]
    pub scopes: Option<String>,

    /// 可选 OIDC audience
    #[serde(default)]
    pub audience: Option<String>,

    /// 可选 login_hint；留空时使用 workEmail
    #[serde(default)]
    pub login_hint: Option<String>,

    /// 登录方式；auto 只在 discovery 明确支持无密钥 token exchange 时使用 device-code
    #[serde(default)]
    pub flow: ExternalIdpLoginFlow,

    /// 浏览器可访问的 kiro.rs origin，例如 https://example.com
    #[serde(default)]
    pub callback_base_url: Option<String>,

    /// Profile ARN（可选；留空由后端自动发现）
    #[serde(default)]
    pub profile_arn: Option<String>,

    /// 优先级（可选，默认 0）
    #[serde(default)]
    pub priority: u32,

    /// 单账号并发上限（可选）
    #[serde(default)]
    pub max_concurrency: Option<u32>,

    /// 凭据级 Auth Region（通常留空；External IdP 刷新不依赖 AWS OIDC region）
    #[serde(default)]
    pub auth_region: Option<String>,

    /// 凭据级 API Region（用于后续 API 请求）
    #[serde(default)]
    pub api_region: Option<String>,

    /// 凭据级 Machine ID（可选）
    #[serde(default)]
    pub machine_id: Option<String>,

    /// 账号类型（可选）
    #[serde(default)]
    pub account_type: Option<String>,

    /// 账号来源供应商 ID（可选）
    #[serde(default)]
    pub source_supplier_id: Option<String>,

    /// 账号来源供应商名称（可选）
    #[serde(default)]
    pub source_supplier_name: Option<String>,

    /// 账号来源批次（可选）
    #[serde(default)]
    pub source_batch: Option<String>,

    /// 凭据分组标记（可选）
    #[serde(default)]
    pub credential_groups: Option<Vec<String>>,

    /// 凭据级代理 URL（可选，特殊值 "direct" 表示不使用代理）
    #[serde(default)]
    pub proxy_url: Option<String>,

    /// 凭据级代理认证用户名（可选）
    #[serde(default)]
    pub proxy_username: Option<String>,

    /// 凭据级代理认证密码（可选）
    #[serde(default)]
    pub proxy_password: Option<String>,

    /// 代理池 ID（可选）
    #[serde(default)]
    pub proxy_id: Option<String>,
}

/// External IdP 登录方式
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum ExternalIdpLoginFlow {
    #[default]
    Auto,
    DeviceCode,
    Pkce,
    KiroPkce,
}

/// External IdP 浏览器登录状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ExternalIdpLoginStatus {
    Pending,
    Completed,
    Failed,
    Expired,
    Cancelled,
}

/// External IdP 浏览器登录阶段
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ExternalIdpLoginPhase {
    PortalDiscovery,
    DeviceAuthorization,
    IdpAuthorization,
    Completed,
}

/// External IdP 浏览器登录启动响应
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExternalIdpLoginStartResponse {
    pub session_id: String,
    pub status: ExternalIdpLoginStatus,
    pub phase: ExternalIdpLoginPhase,
    pub flow: ExternalIdpLoginFlow,
    pub provider: String,
    pub auth_url: String,
    pub callback_url: String,
    pub expires_at: chrono::DateTime<chrono::Utc>,
    pub interval_seconds: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub issuer_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scopes: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audience: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verification_uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verification_uri_complete: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// External IdP 浏览器登录状态响应
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExternalIdpLoginStatusResponse {
    pub session_id: String,
    pub status: ExternalIdpLoginStatus,
    pub phase: ExternalIdpLoginPhase,
    pub flow: ExternalIdpLoginFlow,
    pub provider: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_url: Option<String>,
    pub callback_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<chrono::DateTime<chrono::Utc>>,
    pub interval_seconds: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub issuer_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scopes: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audience: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verification_uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verification_uri_complete: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credential_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subscription_title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subscription_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_account_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolved_account_type: Option<String>,
}

/// External IdP 手动提交授权回调请求
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubmitExternalIdpCallbackRequest {
    /// 完整回调 URL，例如 kiro://kiro.oauth/callback?code=...&state=...
    #[serde(default)]
    pub callback_url: Option<String>,

    /// 仅授权码；提供完整 callbackUrl 时可留空
    #[serde(default)]
    pub code: Option<String>,

    /// 可选 state；提供完整 callbackUrl 时会从 URL 中读取
    #[serde(default)]
    pub state: Option<String>,
}

// ============ 余额查询 ============

/// 余额查询响应
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BalanceResponse {
    /// 凭据 ID
    pub id: u64,
    /// 查询额度时使用的 Profile ARN
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile_arn: Option<String>,
    /// 订阅类型
    pub subscription_title: Option<String>,
    /// 订阅内部类型
    pub subscription_type: Option<String>,
    /// 当前使用量
    pub current_usage: f64,
    /// 使用限额
    pub usage_limit: f64,
    /// 实际可用限额；超额开启时包含 overageCap
    #[serde(default)]
    pub effective_usage_limit: f64,
    /// 剩余额度
    pub remaining: f64,
    /// 使用百分比
    pub usage_percentage: f64,
    /// 下次重置时间（Unix 时间戳）
    pub next_reset_at: Option<f64>,
    /// 超额使用能力
    pub overage_capability: Option<String>,
    /// 超额使用状态
    pub overage_status: Option<String>,
    /// 超额使用开关
    pub overage_enabled: Option<bool>,
    /// 超额上限
    #[serde(default)]
    pub overage_cap: f64,
    /// 当前超额使用量
    #[serde(default)]
    pub current_overages: f64,
    /// 当前超额费用
    #[serde(default)]
    pub overage_charges: f64,
    /// 超额费率
    pub overage_rate: Option<f64>,
    /// 费用币种
    pub currency: Option<String>,
    /// 计量单位
    pub unit: Option<String>,
}

impl BalanceResponse {
    pub fn from_usage(id: u64, profile_arn: Option<String>, usage: &UsageLimitsResponse) -> Self {
        let current_usage = usage.current_usage();
        let usage_limit = usage.usage_limit();
        let effective_usage_limit = usage.effective_usage_limit();
        let remaining = (effective_usage_limit - current_usage).max(0.0);
        let usage_percentage = if effective_usage_limit > 0.0 {
            (current_usage / effective_usage_limit * 100.0).min(100.0)
        } else {
            0.0
        };

        Self {
            id,
            profile_arn,
            subscription_title: usage.subscription_title().map(|s| s.to_string()),
            subscription_type: usage.subscription_type().map(|s| s.to_string()),
            current_usage,
            usage_limit,
            effective_usage_limit,
            remaining,
            usage_percentage,
            next_reset_at: usage.next_date_reset,
            overage_capability: usage.overage_capability().map(|value| value.to_string()),
            overage_status: usage.overage_status().map(|value| value.to_string()),
            overage_enabled: usage.overage_enabled(),
            overage_cap: usage.overage_cap(),
            current_overages: usage.current_overages(),
            overage_charges: usage.overage_charges(),
            overage_rate: usage.overage_rate(),
            currency: usage.currency().map(|value| value.to_string()),
            unit: usage.unit().map(|value| value.to_string()),
        }
    }

    pub fn normalize_cached_compat(&mut self) {
        if self.effective_usage_limit <= 0.0 {
            self.effective_usage_limit = self.usage_limit;
        }
    }
}

// ============ 负载均衡配置 ============

/// 代理池节点响应
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProxyPoolEntryResponse {
    pub id: String,
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    pub weight: u32,
    pub enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_egress_ip: Option<String>,
    /// 当前绑定到该代理池节点的凭据数量
    pub assigned_credentials: usize,
}

/// 代理池配置响应
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProxyPoolConfigResponse {
    pub enabled: bool,
    pub require_proxy: bool,
    pub assignment_strategy: String,
    pub proxies: Vec<ProxyPoolEntryResponse>,
    pub failover: ProxyPoolFailoverConfig,
}

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
    /// suspicious activity 临时限制后的账号级全局冷却时间（毫秒）
    pub suspicious_activity_cooldown_ms: u64,
    /// 是否启用 suspicious activity 临时限制后的账号级全局冷却
    pub suspicious_activity_cooldown_enabled: bool,
    /// 是否优先调度从未触发 suspicious activity 的账号
    pub suspicious_activity_prefer_clean_credentials: bool,
    /// 是否在 suspicious activity 多次命中后自动禁用账号
    pub suspicious_activity_auto_disable_enabled: bool,
    /// suspicious activity 自动禁用阈值
    pub suspicious_activity_auto_disable_threshold: u32,
    /// suspicious activity 自动禁用统计窗口（毫秒）
    pub suspicious_activity_auto_disable_window_ms: u64,
    /// 是否在账号恢复稳定后自动清除 suspicious activity 标记
    pub suspicious_activity_auto_clear_enabled: bool,
    /// 自动清除 suspicious activity 标记所需的连续成功请求次数
    pub suspicious_activity_auto_clear_success_threshold: u32,
    /// 最近一次 suspicious activity 后经过多久自动清除标记（毫秒）
    pub suspicious_activity_auto_clear_after_ms: u64,
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
    /// 流式请求首内容后是否释放调度 lease，无法探测首内容时在响应建立后释放
    pub stream_dispatch_lease_release_enabled: bool,
    /// 流式请求上游响应头前的自适应故障转移策略
    pub stream_pre_sse_failover: StreamPreSseFailoverConfig,
    /// 非流式请求上游响应体读取超时策略
    pub non_stream_body_read_timeout: NonStreamBodyReadTimeoutConfig,
    /// 最终 Kiro 上游请求体大小保护
    pub kiro_request_body_guard: KiroRequestBodyGuardConfig,
    /// 历史 thinking signature 的本地校验策略
    pub thinking_signature_validation_mode: ThinkingSignatureValidationMode,
    /// 响应侧隐藏 thinking signature 兼容补齐开关
    pub response_thinking_signature_compat_enabled: bool,
    /// 凭据级代理池配置
    pub proxy_pool: ProxyPoolConfigResponse,
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
    /// suspicious activity 临时限制后的账号级全局冷却时间（毫秒）
    pub suspicious_activity_cooldown_ms: Option<u64>,
    /// 是否启用 suspicious activity 临时限制后的账号级全局冷却
    pub suspicious_activity_cooldown_enabled: Option<bool>,
    /// 是否优先调度从未触发 suspicious activity 的账号
    pub suspicious_activity_prefer_clean_credentials: Option<bool>,
    /// 是否在 suspicious activity 多次命中后自动禁用账号
    pub suspicious_activity_auto_disable_enabled: Option<bool>,
    /// suspicious activity 自动禁用阈值
    pub suspicious_activity_auto_disable_threshold: Option<u32>,
    /// suspicious activity 自动禁用统计窗口（毫秒）
    pub suspicious_activity_auto_disable_window_ms: Option<u64>,
    /// 是否在账号恢复稳定后自动清除 suspicious activity 标记
    pub suspicious_activity_auto_clear_enabled: Option<bool>,
    /// 自动清除 suspicious activity 标记所需的连续成功请求次数
    pub suspicious_activity_auto_clear_success_threshold: Option<u32>,
    /// 最近一次 suspicious activity 后经过多久自动清除标记（毫秒）
    pub suspicious_activity_auto_clear_after_ms: Option<u64>,
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
    /// 流式请求首内容后是否释放调度 lease，无法探测首内容时在响应建立后释放
    pub stream_dispatch_lease_release_enabled: Option<bool>,
    /// 流式请求上游响应头前的自适应故障转移策略
    pub stream_pre_sse_failover: Option<StreamPreSseFailoverConfig>,
    /// 非流式请求上游响应体读取超时策略
    pub non_stream_body_read_timeout: Option<NonStreamBodyReadTimeoutConfig>,
    /// 最终 Kiro 上游请求体大小保护
    pub kiro_request_body_guard: Option<KiroRequestBodyGuardConfig>,
    /// 历史 thinking signature 的本地校验策略
    pub thinking_signature_validation_mode: Option<ThinkingSignatureValidationMode>,
    /// 响应侧隐藏 thinking signature 兼容补齐开关
    pub response_thinking_signature_compat_enabled: Option<bool>,
    /// 凭据级代理池配置
    pub proxy_pool: Option<ProxyPoolConfig>,
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
    pub auth_account_type_dispatch_policies: BTreeMap<String, AccountTypeDispatchPolicy>,
    pub auth_account_type_account_type_dispatch_policies:
        BTreeMap<String, BTreeMap<String, AccountTypeDispatchPolicy>>,
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
    pub auth_account_type_dispatch_policies: Option<BTreeMap<String, AccountTypeDispatchPolicy>>,
    pub auth_account_type_account_type_dispatch_policies:
        Option<BTreeMap<String, BTreeMap<String, AccountTypeDispatchPolicy>>>,
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
        assert_eq!(missing.rate_limit_cooldown_enabled, None);
        assert_eq!(missing.rate_limit_bucket_capacity, None);
        assert_eq!(missing.rate_limit_refill_per_second, None);

        let nulls: SetCredentialRateLimitConfigRequest = serde_json::from_str(
            r#"{
                "rateLimitCooldownEnabled": null,
                "rateLimitBucketCapacity": null,
                "rateLimitRefillPerSecond": null
            }"#,
        )
        .unwrap();
        assert_eq!(nulls.rate_limit_cooldown_enabled, Some(None));
        assert_eq!(nulls.rate_limit_bucket_capacity, Some(None));
        assert_eq!(nulls.rate_limit_refill_per_second, Some(None));

        let values: SetCredentialRateLimitConfigRequest = serde_json::from_str(
            r#"{
                "rateLimitCooldownEnabled": false,
                "rateLimitBucketCapacity": 0,
                "rateLimitRefillPerSecond": 1.5
            }"#,
        )
        .unwrap();
        assert_eq!(values.rate_limit_cooldown_enabled, Some(Some(false)));
        assert_eq!(values.rate_limit_bucket_capacity, Some(Some(0.0)));
        assert_eq!(values.rate_limit_refill_per_second, Some(Some(1.5)));
    }
}
