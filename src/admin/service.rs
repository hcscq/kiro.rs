//! Admin API 业务逻辑服务

use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use parking_lot::Mutex;

use crate::kiro::model::credentials::KiroCredentials;
use crate::kiro::token_manager::MultiTokenManager;
use crate::model::account_type_preset::{
    built_in_account_type_presets, infer_standard_account_type_id,
};
use crate::model::model_catalog::built_in_model_catalog;
use crate::state::{CachedBalanceRecord, RuntimeCoordinationStatus, StateChangeKind};

use super::error::AdminServiceError;
use super::types::{
    AddCredentialRequest, AddCredentialResponse, BalanceResponse, CredentialStatusItem,
    CredentialsStatusResponse, LoadBalancingModeResponse, ModelCapabilitiesConfigResponse,
    ModelCatalogItemResponse, ModelCatalogResponse, SetCredentialModelPolicyRequest,
    SetLoadBalancingModeRequest, SetModelCapabilitiesConfigRequest,
    StandardAccountTypePresetResponse,
};

/// 余额缓存过期时间（秒），5 分钟
const BALANCE_CACHE_TTL_SECS: i64 = 300;

/// Admin 服务
///
/// 封装所有 Admin API 的业务逻辑
pub struct AdminService {
    token_manager: Arc<MultiTokenManager>,
    balance_cache: Mutex<HashMap<u64, CachedBalanceRecord>>,
    last_balance_cache_revision: Mutex<u64>,
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

        Self {
            token_manager,
            balance_cache: Mutex::new(balance_cache),
            last_balance_cache_revision: Mutex::new(balance_cache_revision),
        }
    }

    /// 获取所有凭据状态
    pub fn get_all_credentials(&self) -> Result<CredentialsStatusResponse, AdminServiceError> {
        self.sync_runtime_state_for_read()?;
        let snapshot = self.token_manager.snapshot();
        let current_id = snapshot.current_id;
        let total = snapshot.total;
        let available = snapshot.available;
        let dispatchable = snapshot.dispatchable;

        let mut credentials: Vec<CredentialStatusItem> = snapshot
            .entries
            .into_iter()
            .map(|entry| {
                let standard_account_type = entry
                    .subscription_title
                    .as_deref()
                    .and_then(infer_standard_account_type_id)
                    .map(|value| value.to_string());

                CredentialStatusItem {
                    id: entry.id,
                    priority: entry.priority,
                    disabled: entry.disabled,
                    failure_count: entry.failure_count,
                    is_current: entry.id == current_id,
                    expires_at: entry.expires_at,
                    auth_method: entry.auth_method,
                    has_profile_arn: entry.has_profile_arn,
                    refresh_token_hash: entry.refresh_token_hash,
                    email: entry.email,
                    subscription_title: entry.subscription_title,
                    account_type: entry.account_type,
                    resolved_account_type: entry.resolved_account_type,
                    account_type_source: entry.account_type_source,
                    standard_account_type,
                    allowed_models: entry.allowed_models,
                    blocked_models: entry.blocked_models,
                    runtime_model_restrictions: entry.runtime_model_restrictions,
                    imported_at: entry.imported_at,
                    success_count: entry.success_count,
                    last_used_at: entry.last_used_at.clone(),
                    in_flight: entry.active_requests,
                    max_concurrency: entry.max_concurrency,
                    max_concurrency_override: entry.max_concurrency_override,
                    max_concurrency_source: entry.max_concurrency_source,
                    has_proxy: entry.has_proxy,
                    proxy_url: entry.proxy_url,
                    refresh_failure_count: entry.refresh_failure_count,
                    disabled_reason: entry.disabled_reason,
                    disabled_at: entry.disabled_at,
                    last_error_status: entry.last_error_status,
                    last_error_summary: entry.last_error_summary,
                    cooldown_remaining_ms: entry.cooldown_remaining_ms,
                    rate_limit_bucket_tokens: entry.rate_limit_bucket_tokens,
                    rate_limit_bucket_capacity: entry.rate_limit_bucket_capacity,
                    rate_limit_bucket_capacity_override: entry.rate_limit_bucket_capacity_override,
                    rate_limit_bucket_capacity_source: entry.rate_limit_bucket_capacity_source,
                    rate_limit_refill_per_second: entry.rate_limit_refill_per_second,
                    rate_limit_refill_per_second_override: entry
                        .rate_limit_refill_per_second_override,
                    rate_limit_refill_per_second_source: entry.rate_limit_refill_per_second_source,
                    rate_limit_refill_base_per_second: entry.rate_limit_refill_base_per_second,
                    rate_limit_hit_streak: entry.rate_limit_hit_streak,
                    next_ready_in_ms: entry.next_ready_in_ms,
                }
            })
            .collect();

        // 按优先级排序（数字越小优先级越高）
        credentials.sort_by_key(|c| c.priority);

        Ok(CredentialsStatusResponse {
            total,
            available,
            dispatchable,
            current_id,
            credentials,
        })
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
        rate_limit_bucket_capacity: Option<Option<f64>>,
        rate_limit_refill_per_second: Option<Option<f64>>,
    ) -> Result<(), AdminServiceError> {
        self.ensure_runtime_write_leader()?;

        self.token_manager
            .set_rate_limit_config(id, rate_limit_bucket_capacity, rate_limit_refill_per_second)
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

        if let Some(cached) = self.cached_balance(id) {
            tracing::debug!("凭据 #{} 余额命中本地缓存", id);
            return Ok(cached);
        }

        if let Err(e) = self.sync_balance_cache_from_state() {
            tracing::warn!("同步共享余额缓存失败，将直接回源查询: {}", e);
        } else if let Some(cached) = self.cached_balance(id) {
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

        let current_usage = usage.current_usage();
        let usage_limit = usage.usage_limit();
        let remaining = (usage_limit - current_usage).max(0.0);
        let usage_percentage = if usage_limit > 0.0 {
            (current_usage / usage_limit * 100.0).min(100.0)
        } else {
            0.0
        };

        Ok(BalanceResponse {
            id,
            subscription_title: usage.subscription_title().map(|s| s.to_string()),
            current_usage,
            usage_limit,
            remaining,
            usage_percentage,
            next_reset_at: usage.next_date_reset,
        })
    }

    /// 添加新凭据
    pub async fn add_credential(
        &self,
        req: AddCredentialRequest,
    ) -> Result<AddCredentialResponse, AdminServiceError> {
        self.ensure_runtime_write_leader()?;

        // 构建凭据对象
        let email = req.email.clone();
        let new_cred = KiroCredentials {
            id: None,
            access_token: None,
            refresh_token: Some(req.refresh_token),
            profile_arn: None,
            expires_at: None,
            auth_method: Some(req.auth_method),
            client_id: req.client_id,
            client_secret: req.client_secret,
            priority: req.priority,
            max_concurrency: req.max_concurrency,
            rate_limit_bucket_capacity: req.rate_limit_bucket_capacity,
            rate_limit_refill_per_second: req.rate_limit_refill_per_second,
            region: req.region,
            auth_region: req.auth_region,
            api_region: req.api_region,
            machine_id: req.machine_id,
            email: req.email,
            subscription_title: None, // 将在首次获取使用额度时自动更新
            account_type: req.account_type,
            allowed_models: req.allowed_models.unwrap_or_default(),
            blocked_models: req.blocked_models.unwrap_or_default(),
            runtime_model_restrictions: Vec::new(),
            imported_at: None,
            proxy_url: req.proxy_url,
            proxy_username: req.proxy_username,
            proxy_password: req.proxy_password,
            disabled: false, // 新添加的凭据默认启用
            disabled_reason: None,
            disabled_at: None,
            last_error_status: None,
            last_error_summary: None,
        };

        // 调用 token_manager 添加凭据
        let credential_id = self
            .token_manager
            .add_credential(new_cred)
            .await
            .map_err(|e| self.classify_add_error(e))?;

        // 主动获取订阅等级，避免首次请求时 Free 账号绕过 Opus 模型过滤
        if let Err(e) = self.token_manager.get_usage_limits_for(credential_id).await {
            tracing::warn!("添加凭据后获取订阅等级失败（不影响凭据添加）: {}", e);
        }

        Ok(AddCredentialResponse {
            success: true,
            message: format!("凭据添加成功，ID: {}", credential_id),
            credential_id,
            email,
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
        Ok(LoadBalancingModeResponse {
            mode: snapshot.mode,
            queue_max_size: snapshot.queue_max_size,
            queue_max_wait_ms: snapshot.queue_max_wait_ms,
            rate_limit_cooldown_ms: snapshot.rate_limit_cooldown_ms,
            rate_limit_cooldown_enabled: snapshot.rate_limit_cooldown_enabled,
            model_cooldown_enabled: snapshot.model_cooldown_enabled,
            default_max_concurrency: snapshot.default_max_concurrency,
            rate_limit_bucket_capacity: snapshot.rate_limit_bucket_capacity,
            rate_limit_refill_per_second: snapshot.rate_limit_refill_per_second,
            rate_limit_refill_min_per_second: snapshot.rate_limit_refill_min_per_second,
            rate_limit_refill_recovery_step_per_success: snapshot
                .rate_limit_refill_recovery_step_per_success,
            rate_limit_refill_backoff_factor: snapshot.rate_limit_refill_backoff_factor,
            request_weighting: snapshot.request_weighting,
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

    /// 设置负载均衡模式
    pub fn set_load_balancing_mode(
        &self,
        req: SetLoadBalancingModeRequest,
    ) -> Result<LoadBalancingModeResponse, AdminServiceError> {
        self.ensure_runtime_write_leader()?;

        if req.mode.is_none()
            && req.queue_max_size.is_none()
            && req.queue_max_wait_ms.is_none()
            && req.rate_limit_cooldown_ms.is_none()
            && req.rate_limit_cooldown_enabled.is_none()
            && req.model_cooldown_enabled.is_none()
            && req.default_max_concurrency.is_none()
            && req.rate_limit_bucket_capacity.is_none()
            && req.rate_limit_refill_per_second.is_none()
            && req.rate_limit_refill_min_per_second.is_none()
            && req.rate_limit_refill_recovery_step_per_success.is_none()
            && req.rate_limit_refill_backoff_factor.is_none()
            && req.request_weighting.is_none()
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
                req.model_cooldown_enabled,
                req.default_max_concurrency,
                req.rate_limit_bucket_capacity,
                req.rate_limit_refill_per_second,
                req.rate_limit_refill_min_per_second,
                req.rate_limit_refill_recovery_step_per_success,
                req.rate_limit_refill_backoff_factor,
                req.request_weighting,
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
        let cache = state_store.load_balance_cache()?;
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

    fn cached_balance(&self, id: u64) -> Option<BalanceResponse> {
        let cache = self.balance_cache.lock();
        cache
            .get(&id)
            .filter(|cached| Self::is_balance_cache_fresh(cached))
            .map(|cached| cached.data.clone())
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
    use crate::kiro::model::credentials::KiroCredentials;
    use crate::model::config::Config;

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
            subscription_title: Some("KIRO PRO+".to_string()),
            current_usage: 12.5,
            usage_limit: 100.0,
            remaining: 87.5,
            usage_percentage: 12.5,
            next_reset_at: Some(1_744_739_200.0),
        };
        let mut shared_cache = HashMap::new();
        shared_cache.insert(
            1,
            CachedBalanceRecord {
                cached_at: Utc::now().timestamp() as f64,
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
