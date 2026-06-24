use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use crate::common::auth::{
    ApiKeyAuthEntry, CredentialGroupScope, DEFAULT_CREDENTIAL_GROUP, normalize_credential_group,
    normalize_credential_groups,
};

use super::model_policy::{
    AccountTypeDispatchPolicy, ModelSupportPolicy, normalize_account_type_dispatch_policies,
    normalize_account_type_policies,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RequestWeightingConfig {
    #[serde(default = "default_request_weighting_enabled")]
    pub enabled: bool,

    #[serde(default = "default_request_weighting_base_weight")]
    pub base_weight: f64,

    #[serde(default = "default_request_weighting_max_weight")]
    pub max_weight: f64,

    #[serde(default = "default_request_weighting_tools_bonus")]
    pub tools_bonus: f64,

    #[serde(default = "default_request_weighting_large_max_tokens_threshold")]
    pub large_max_tokens_threshold: i32,

    #[serde(default = "default_request_weighting_large_max_tokens_bonus")]
    pub large_max_tokens_bonus: f64,

    #[serde(default = "default_request_weighting_large_input_tokens_threshold")]
    pub large_input_tokens_threshold: i32,

    #[serde(default = "default_request_weighting_large_input_tokens_bonus")]
    pub large_input_tokens_bonus: f64,

    #[serde(default = "default_request_weighting_very_large_input_tokens_threshold")]
    pub very_large_input_tokens_threshold: i32,

    #[serde(default = "default_request_weighting_very_large_input_tokens_bonus")]
    pub very_large_input_tokens_bonus: f64,

    #[serde(default = "default_request_weighting_thinking_bonus")]
    pub thinking_bonus: f64,

    #[serde(default = "default_request_weighting_heavy_thinking_budget_threshold")]
    pub heavy_thinking_budget_threshold: i32,

    #[serde(default = "default_request_weighting_heavy_thinking_budget_bonus")]
    pub heavy_thinking_budget_bonus: f64,
}

impl Default for RequestWeightingConfig {
    fn default() -> Self {
        Self {
            enabled: default_request_weighting_enabled(),
            base_weight: default_request_weighting_base_weight(),
            max_weight: default_request_weighting_max_weight(),
            tools_bonus: default_request_weighting_tools_bonus(),
            large_max_tokens_threshold: default_request_weighting_large_max_tokens_threshold(),
            large_max_tokens_bonus: default_request_weighting_large_max_tokens_bonus(),
            large_input_tokens_threshold: default_request_weighting_large_input_tokens_threshold(),
            large_input_tokens_bonus: default_request_weighting_large_input_tokens_bonus(),
            very_large_input_tokens_threshold:
                default_request_weighting_very_large_input_tokens_threshold(),
            very_large_input_tokens_bonus: default_request_weighting_very_large_input_tokens_bonus(
            ),
            thinking_bonus: default_request_weighting_thinking_bonus(),
            heavy_thinking_budget_threshold:
                default_request_weighting_heavy_thinking_budget_threshold(),
            heavy_thinking_budget_bonus: default_request_weighting_heavy_thinking_budget_bonus(),
        }
    }
}

impl RequestWeightingConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        for (name, value) in [
            ("requestWeighting.baseWeight", self.base_weight),
            ("requestWeighting.maxWeight", self.max_weight),
            ("requestWeighting.toolsBonus", self.tools_bonus),
            (
                "requestWeighting.largeMaxTokensBonus",
                self.large_max_tokens_bonus,
            ),
            (
                "requestWeighting.largeInputTokensBonus",
                self.large_input_tokens_bonus,
            ),
            (
                "requestWeighting.veryLargeInputTokensBonus",
                self.very_large_input_tokens_bonus,
            ),
            ("requestWeighting.thinkingBonus", self.thinking_bonus),
            (
                "requestWeighting.heavyThinkingBudgetBonus",
                self.heavy_thinking_budget_bonus,
            ),
        ] {
            if !value.is_finite() || value < 0.0 {
                anyhow::bail!("{name} 必须是大于等于 0 的有限数字");
            }
        }

        for (name, value) in [
            (
                "requestWeighting.largeMaxTokensThreshold",
                self.large_max_tokens_threshold,
            ),
            (
                "requestWeighting.largeInputTokensThreshold",
                self.large_input_tokens_threshold,
            ),
            (
                "requestWeighting.veryLargeInputTokensThreshold",
                self.very_large_input_tokens_threshold,
            ),
            (
                "requestWeighting.heavyThinkingBudgetThreshold",
                self.heavy_thinking_budget_threshold,
            ),
        ] {
            if value < 0 {
                anyhow::bail!("{name} 必须大于等于 0");
            }
        }

        if self.base_weight <= 0.0 {
            anyhow::bail!("requestWeighting.baseWeight 必须大于 0");
        }
        if self.max_weight < self.base_weight {
            anyhow::bail!("requestWeighting.maxWeight 不能小于 requestWeighting.baseWeight");
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StreamPreSseFailoverConfig {
    #[serde(default = "default_stream_pre_sse_failover_enabled")]
    pub enabled: bool,

    /// 流式请求在收到上游 SSE/响应头之前的总等待预算。
    #[serde(default = "default_stream_pre_sse_total_budget_ms")]
    pub total_budget_ms: u64,

    #[serde(default = "default_stream_pre_sse_small_request_threshold_bytes")]
    pub small_request_threshold_bytes: usize,

    #[serde(default = "default_stream_pre_sse_medium_request_threshold_bytes")]
    pub medium_request_threshold_bytes: usize,

    #[serde(default = "default_stream_pre_sse_large_request_threshold_bytes")]
    pub large_request_threshold_bytes: usize,

    #[serde(default = "default_stream_pre_sse_small_request_timeout_ms")]
    pub small_request_timeout_ms: u64,

    #[serde(default = "default_stream_pre_sse_medium_request_timeout_ms")]
    pub medium_request_timeout_ms: u64,

    #[serde(default = "default_stream_pre_sse_large_request_timeout_ms")]
    pub large_request_timeout_ms: u64,

    /// 0 表示超大请求使用剩余总预算，不做快速故障转移。
    #[serde(default)]
    pub huge_request_timeout_ms: u64,

    /// 真实高阶 Opus 模型的最小单次响应头等待时间。
    #[serde(default = "default_stream_pre_sse_slow_model_min_timeout_ms")]
    pub slow_model_min_timeout_ms: u64,

    #[serde(default = "default_stream_pre_sse_max_fast_failovers")]
    pub max_fast_failovers: usize,

    #[serde(default = "default_stream_pre_sse_min_remaining_ms")]
    pub min_remaining_ms: u64,
}

impl Default for StreamPreSseFailoverConfig {
    fn default() -> Self {
        Self {
            enabled: default_stream_pre_sse_failover_enabled(),
            total_budget_ms: default_stream_pre_sse_total_budget_ms(),
            small_request_threshold_bytes: default_stream_pre_sse_small_request_threshold_bytes(),
            medium_request_threshold_bytes: default_stream_pre_sse_medium_request_threshold_bytes(),
            large_request_threshold_bytes: default_stream_pre_sse_large_request_threshold_bytes(),
            small_request_timeout_ms: default_stream_pre_sse_small_request_timeout_ms(),
            medium_request_timeout_ms: default_stream_pre_sse_medium_request_timeout_ms(),
            large_request_timeout_ms: default_stream_pre_sse_large_request_timeout_ms(),
            huge_request_timeout_ms: 0,
            slow_model_min_timeout_ms: default_stream_pre_sse_slow_model_min_timeout_ms(),
            max_fast_failovers: default_stream_pre_sse_max_fast_failovers(),
            min_remaining_ms: default_stream_pre_sse_min_remaining_ms(),
        }
    }
}

impl StreamPreSseFailoverConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.total_budget_ms == 0 {
            anyhow::bail!("streamPreSseFailover.totalBudgetMs 必须大于 0");
        }

        if self.small_request_threshold_bytes == 0 {
            anyhow::bail!("streamPreSseFailover.smallRequestThresholdBytes 必须大于 0");
        }
        if self.medium_request_threshold_bytes < self.small_request_threshold_bytes {
            anyhow::bail!(
                "streamPreSseFailover.mediumRequestThresholdBytes 不能小于 smallRequestThresholdBytes"
            );
        }
        if self.large_request_threshold_bytes < self.medium_request_threshold_bytes {
            anyhow::bail!(
                "streamPreSseFailover.largeRequestThresholdBytes 不能小于 mediumRequestThresholdBytes"
            );
        }

        for (name, value) in [
            (
                "streamPreSseFailover.smallRequestTimeoutMs",
                self.small_request_timeout_ms,
            ),
            (
                "streamPreSseFailover.mediumRequestTimeoutMs",
                self.medium_request_timeout_ms,
            ),
            (
                "streamPreSseFailover.largeRequestTimeoutMs",
                self.large_request_timeout_ms,
            ),
            (
                "streamPreSseFailover.slowModelMinTimeoutMs",
                self.slow_model_min_timeout_ms,
            ),
            ("streamPreSseFailover.minRemainingMs", self.min_remaining_ms),
        ] {
            if value == 0 {
                anyhow::bail!("{name} 必须大于 0");
            }
        }

        if self.max_fast_failovers > 8 {
            anyhow::bail!("streamPreSseFailover.maxFastFailovers 不能大于 8");
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NonStreamBodyReadTimeoutConfig {
    #[serde(default = "default_non_stream_body_read_timeout_enabled")]
    pub enabled: bool,

    /// 非流式请求收到上游响应头后，读取完整 body 的最长等待时间。
    #[serde(default = "default_non_stream_body_read_timeout_ms")]
    pub timeout_ms: u64,

    /// 非流式上游返回 Amazon EventStream 后，连续无 body chunk 的最长等待时间。
    #[serde(default = "default_non_stream_eventstream_idle_timeout_ms")]
    pub eventstream_idle_timeout_ms: u64,

    /// body 读取超时后是否尝试切换到其他凭据重试。默认关闭，避免大请求被多次长时间占用。
    #[serde(default = "default_non_stream_body_read_timeout_retry_on_timeout")]
    pub retry_on_timeout: bool,

    /// EventStream 已开始但尚未产生可用输出时，卡住后允许一次保守切凭据重试。
    #[serde(default = "default_non_stream_eventstream_safe_retry_on_stall")]
    pub eventstream_safe_retry_on_stall: bool,
}

impl Default for NonStreamBodyReadTimeoutConfig {
    fn default() -> Self {
        Self {
            enabled: default_non_stream_body_read_timeout_enabled(),
            timeout_ms: default_non_stream_body_read_timeout_ms(),
            eventstream_idle_timeout_ms: default_non_stream_eventstream_idle_timeout_ms(),
            retry_on_timeout: default_non_stream_body_read_timeout_retry_on_timeout(),
            eventstream_safe_retry_on_stall: default_non_stream_eventstream_safe_retry_on_stall(),
        }
    }
}

impl NonStreamBodyReadTimeoutConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.enabled && self.timeout_ms == 0 {
            anyhow::bail!("nonStreamBodyReadTimeout.timeoutMs 必须大于 0，或关闭 enabled");
        }
        if self.enabled && self.eventstream_idle_timeout_ms == 0 {
            anyhow::bail!(
                "nonStreamBodyReadTimeout.eventstreamIdleTimeoutMs 必须大于 0，或关闭 enabled"
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct KiroRequestBodyGuardConfig {
    #[serde(default = "default_kiro_request_body_guard_enabled")]
    pub enabled: bool,

    /// 最终发往 Kiro 上游的 JSON body 上限。该值在 profileArn 注入后检查。
    #[serde(default = "default_kiro_request_body_guard_max_bytes")]
    pub max_bytes: usize,
}

impl Default for KiroRequestBodyGuardConfig {
    fn default() -> Self {
        Self {
            enabled: default_kiro_request_body_guard_enabled(),
            max_bytes: default_kiro_request_body_guard_max_bytes(),
        }
    }
}

impl KiroRequestBodyGuardConfig {
    pub fn should_reject(&self, body_bytes: usize) -> bool {
        self.enabled && body_bytes > self.max_bytes
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if !self.enabled {
            return Ok(());
        }

        if self.max_bytes < 1024 * 1024 {
            anyhow::bail!("kiroRequestBodyGuard.maxBytes 必须不小于 1MiB，或关闭 enabled");
        }
        if self.max_bytes > 64 * 1024 * 1024 {
            anyhow::bail!("kiroRequestBodyGuard.maxBytes 不能大于 64MiB");
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ConversionRuntimeConfig {
    #[serde(default = "default_conversion_max_concurrent")]
    pub max_concurrent: usize,

    #[serde(default = "default_conversion_max_queue")]
    pub max_queue: usize,

    #[serde(default = "default_conversion_max_queue_weight")]
    pub max_queue_weight: usize,

    #[serde(default = "default_conversion_queue_wait_ms")]
    pub queue_wait_ms: u64,

    #[serde(default = "default_conversion_max_request_weight")]
    pub max_request_weight: usize,
}

impl Default for ConversionRuntimeConfig {
    fn default() -> Self {
        Self {
            max_concurrent: default_conversion_max_concurrent(),
            max_queue: default_conversion_max_queue(),
            max_queue_weight: default_conversion_max_queue_weight(),
            queue_wait_ms: default_conversion_queue_wait_ms(),
            max_request_weight: default_conversion_max_request_weight(),
        }
    }
}

impl ConversionRuntimeConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.max_concurrent == 0 {
            anyhow::bail!("conversionRuntime.maxConcurrent 必须大于 0");
        }
        if self.max_request_weight == 0 {
            anyhow::bail!("conversionRuntime.maxRequestWeight 必须大于 0");
        }
        if self.max_queue > 0 && self.queue_wait_ms == 0 {
            anyhow::bail!(
                "conversionRuntime.queueWaitMs 必须大于 0，或将 conversionRuntime.maxQueue 设为 0"
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum TlsBackend {
    Rustls,
    NativeTls,
}

impl Default for TlsBackend {
    fn default() -> Self {
        Self::Rustls
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum StateBackendKind {
    File,
    Postgres,
}

impl Default for StateBackendKind {
    fn default() -> Self {
        Self::File
    }
}

/// 历史 thinking signature 的本地校验策略。
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ThinkingSignatureValidationMode {
    /// 校验失败时拒绝请求。
    #[default]
    Strict,
    /// 校验失败只记录告警，继续转发请求。
    #[serde(alias = "warn-only", alias = "warnOnly")]
    WarnOnly,
    /// 完全跳过历史 thinking signature 校验。
    Disabled,
    /// 移除本服务签发但校验失败的 signature 后继续转发。
    #[serde(alias = "strip-invalid", alias = "stripInvalid")]
    StripInvalid,
}

impl ThinkingSignatureValidationMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Strict => "strict",
            Self::WarnOnly => "warn_only",
            Self::Disabled => "disabled",
            Self::StripInvalid => "strip_invalid",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum ServerWebToolsMode {
    /// 最大化复原 Anthropic server-side web tools 行为。
    #[default]
    #[serde(alias = "max-compat", alias = "maxCompat")]
    MaxCompat,
    /// 只使用 Kiro 上游原生能力；当前原生 MCP 仅提供 web_search。
    #[serde(alias = "native-only", alias = "nativeOnly")]
    NativeOnly,
    /// 禁用 Anthropic server-side web tools 兼容层。
    Disabled,
}

impl ServerWebToolsMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MaxCompat => "max_compat",
            Self::NativeOnly => "native_only",
            Self::Disabled => "disabled",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ProxyPoolEntry {
    pub id: String,
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    #[serde(default = "default_proxy_pool_entry_weight")]
    pub weight: u32,
    #[serde(default = "default_proxy_pool_entry_enabled")]
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_egress_ip: Option<String>,
}

fn default_proxy_pool_entry_weight() -> u32 {
    1
}

fn default_proxy_pool_entry_enabled() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ProxyPoolFailoverConfig {
    #[serde(default = "default_proxy_pool_failover_enabled")]
    pub enabled: bool,
    #[serde(default = "default_proxy_pool_failure_threshold")]
    pub failure_threshold: u32,
    #[serde(default = "default_proxy_pool_cooldown_secs")]
    pub cooldown_secs: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub probe_url: Option<String>,
}

impl Default for ProxyPoolFailoverConfig {
    fn default() -> Self {
        Self {
            enabled: default_proxy_pool_failover_enabled(),
            failure_threshold: default_proxy_pool_failure_threshold(),
            cooldown_secs: default_proxy_pool_cooldown_secs(),
            probe_url: None,
        }
    }
}

fn default_proxy_pool_failover_enabled() -> bool {
    true
}

fn default_proxy_pool_failure_threshold() -> u32 {
    3
}

fn default_proxy_pool_cooldown_secs() -> u64 {
    300
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ProxyPoolConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub require_proxy: bool,
    #[serde(default = "default_proxy_pool_assignment_strategy")]
    pub assignment_strategy: String,
    #[serde(default)]
    pub proxies: Vec<ProxyPoolEntry>,
    #[serde(default)]
    pub failover: ProxyPoolFailoverConfig,
}

impl Default for ProxyPoolConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            require_proxy: false,
            assignment_strategy: default_proxy_pool_assignment_strategy(),
            proxies: Vec::new(),
            failover: ProxyPoolFailoverConfig::default(),
        }
    }
}

fn default_proxy_pool_assignment_strategy() -> String {
    "weighted_least_assigned".to_string()
}

impl ProxyPoolConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        let strategy = self.assignment_strategy.trim();
        if strategy != "weighted_least_assigned" && strategy != "hash" {
            anyhow::bail!("proxyPool.assignmentStrategy 必须是 weighted_least_assigned 或 hash");
        }

        let mut ids = std::collections::BTreeSet::new();
        for proxy in &self.proxies {
            let id = proxy.id.trim();
            if id.is_empty() {
                anyhow::bail!("proxyPool.proxies[].id 不能为空");
            }
            if !ids.insert(id.to_string()) {
                anyhow::bail!("proxyPool.proxies[].id 重复: {}", id);
            }
            if proxy.url.trim().is_empty() {
                anyhow::bail!("proxyPool.proxies[{}].url 不能为空", id);
            }
            let parsed_url = url::Url::parse(proxy.url.trim())
                .map_err(|err| anyhow::anyhow!("proxyPool.proxies[{}].url 无效: {}", id, err))?;
            match parsed_url.scheme() {
                "http" | "https" | "socks5" | "socks5h" => {}
                scheme => {
                    anyhow::bail!("proxyPool.proxies[{}].url scheme 不支持: {}", id, scheme);
                }
            }
            if parsed_url.host_str().is_none() {
                anyhow::bail!("proxyPool.proxies[{}].url 缺少 host", id);
            }
            if let Err(err) = reqwest::Proxy::all(proxy.url.trim()) {
                anyhow::bail!("proxyPool.proxies[{}].url 无效: {}", id, err);
            }
            if proxy.weight == 0 {
                anyhow::bail!("proxyPool.proxies[{}].weight 必须大于 0", id);
            }
        }

        if self.require_proxy && !self.enabled {
            anyhow::bail!("proxyPool.requireProxy=true 时必须启用 proxyPool.enabled");
        }

        if self.enabled && self.proxies.iter().all(|proxy| !proxy.enabled) {
            anyhow::bail!("proxyPool.enabled=true 时至少需要一个启用的代理");
        }

        if self.failover.enabled {
            if self.failover.failure_threshold == 0 {
                anyhow::bail!("proxyPool.failover.failureThreshold 必须大于 0");
            }
            if self.failover.cooldown_secs == 0 {
                anyhow::bail!("proxyPool.failover.cooldownSecs 必须大于 0");
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CredentialGroupConfig {
    pub name: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    #[serde(default = "default_true")]
    pub enabled: bool,
}

impl CredentialGroupConfig {
    pub fn default_group() -> Self {
        Self {
            name: DEFAULT_CREDENTIAL_GROUP.to_string(),
            display_name: Some("Default".to_string()),
            description: Some("未显式标记分组的旧凭据会按 default 分组参与匹配".to_string()),
            enabled: true,
        }
    }

    fn normalize(&mut self) -> bool {
        let Some(name) = normalize_credential_group(&self.name) else {
            return false;
        };
        self.name = name;
        self.display_name = normalize_optional_text(self.display_name.as_deref());
        self.description = normalize_optional_text(self.description.as_deref());
        true
    }
}

pub fn normalize_credential_group_catalog(
    groups: &[CredentialGroupConfig],
) -> Vec<CredentialGroupConfig> {
    let mut by_name = BTreeMap::new();
    for group in groups {
        let mut group = group.clone();
        if group.normalize() {
            by_name.entry(group.name.clone()).or_insert(group);
        }
    }

    by_name
        .entry(DEFAULT_CREDENTIAL_GROUP.to_string())
        .or_insert_with(CredentialGroupConfig::default_group)
        .enabled = true;

    by_name.into_values().collect()
}

fn normalize_optional_text(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ApiKeyConfig {
    /// API key 标识，仅用于日志与调度亲和隔离，不会暴露明文 key
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,

    /// 客户端认证使用的 API key
    pub key: String,

    /// 该 API key 可使用的凭据分组
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_credential_groups: Vec<String>,
}

/// KNA 应用配置
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Config {
    #[serde(default = "default_host")]
    pub host: String,

    #[serde(default = "default_port")]
    pub port: u16,

    /// 独立健康检查端口。未配置时使用 port + 1；port=65535 时不额外监听。
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub health_port: Option<u16>,

    #[serde(default = "default_region")]
    pub region: String,

    /// Auth Region（用于 Token 刷新），未配置时回退到 region
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_region: Option<String>,

    /// API Region（用于 API 请求），未配置时回退到 region
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_region: Option<String>,

    /// Runtime API Endpoint（用于推理/流式请求），未配置时回退到 q.<region>.amazonaws.com
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime_endpoint: Option<String>,

    /// Management API Endpoint（用于配置/生命周期/额度查询），未配置时回退到 q.<region>.amazonaws.com
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub management_endpoint: Option<String>,

    #[serde(default = "default_kiro_version")]
    pub kiro_version: String,

    #[serde(default)]
    pub machine_id: Option<String>,

    #[serde(default)]
    pub api_key: Option<String>,

    /// 多 API key 配置。每个 key 可绑定一个或多个凭据分组。
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub api_keys: Vec<ApiKeyConfig>,

    /// 凭据分组目录。只做分组治理，不承载计价或稳定性属性。
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub credential_groups: Vec<CredentialGroupConfig>,

    #[serde(default = "default_system_version")]
    pub system_version: String,

    #[serde(default = "default_node_version")]
    pub node_version: String,

    #[serde(default = "default_tls_backend")]
    pub tls_backend: TlsBackend,

    /// 外部 count_tokens API 地址（可选）
    #[serde(default)]
    pub count_tokens_api_url: Option<String>,

    /// count_tokens API 密钥（可选）
    #[serde(default)]
    pub count_tokens_api_key: Option<String>,

    /// count_tokens API 认证类型（可选，"x-api-key" 或 "bearer"，默认 "x-api-key"）
    #[serde(default = "default_count_tokens_auth_type")]
    pub count_tokens_auth_type: String,

    /// HTTP 代理地址（可选）
    /// 支持格式: http://host:port, https://host:port, socks5://host:port
    #[serde(default)]
    pub proxy_url: Option<String>,

    /// 代理认证用户名（可选）
    #[serde(default)]
    pub proxy_username: Option<String>,

    /// 代理认证密码（可选）
    #[serde(default)]
    pub proxy_password: Option<String>,

    /// 凭据级代理池。启用后，未显式指定代理的新凭据会在导入时绑定池内代理 ID。
    #[serde(default)]
    pub proxy_pool: ProxyPoolConfig,

    /// Admin API 密钥（可选，启用 Admin API 功能）
    #[serde(default)]
    pub admin_api_key: Option<String>,

    /// 状态存储后端：`file` 或 `postgres`
    #[serde(default = "default_state_backend")]
    pub state_backend: StateBackendKind,

    /// PostgreSQL 连接串（state_backend=postgres 时必填）
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_postgres_url: Option<String>,

    /// Redis 连接串（可选）。配置后，短生命周期缓存会优先存入 Redis。
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_redis_url: Option<String>,

    /// 运行时实例标识（可选）。未配置时会在启动时自动推导。
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<String>,

    /// Redis 运行时协调心跳间隔（秒）
    #[serde(default = "default_state_redis_heartbeat_interval_secs")]
    pub state_redis_heartbeat_interval_secs: u64,

    /// Redis Leader 租约 TTL（秒）
    #[serde(default = "default_state_redis_leader_lease_ttl_secs")]
    pub state_redis_leader_lease_ttl_secs: u64,

    /// 数据面热路径检查共享状态修订号的最小间隔（毫秒，0 表示每次请求都检查）
    #[serde(default = "default_state_hot_path_sync_min_interval_ms")]
    pub state_hot_path_sync_min_interval_ms: u64,

    /// 负载均衡模式（"priority" 或 "balanced"）
    #[serde(default = "default_load_balancing_mode")]
    pub load_balancing_mode: String,

    /// 是否启用会话到凭据的软亲和调度
    #[serde(default)]
    pub session_affinity_enabled: bool,

    /// 默认单账号并发上限（可选）
    /// 仅在凭据未单独配置 maxConcurrency 时生效
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_max_concurrency: Option<u32>,

    /// 等待队列最大长度（0 表示禁用等待队列）
    #[serde(default)]
    pub queue_max_size: usize,

    /// 请求在等待队列中的最大等待时间（毫秒，0 表示禁用等待队列）
    #[serde(default)]
    pub queue_max_wait_ms: u64,

    /// 单账号遭遇上游 429 后的冷却时间（毫秒，0 表示禁用 429 冷却）
    #[serde(default = "default_rate_limit_cooldown_ms")]
    pub rate_limit_cooldown_ms: u64,

    /// 是否启用上游 429 后的本地冷却与 bucket 退避
    #[serde(default = "default_rate_limit_cooldown_enabled")]
    pub rate_limit_cooldown_enabled: bool,

    /// 上游 suspicious activity 临时限制后的账号级全局冷却时间（毫秒，0 表示不写入固定冷却）
    #[serde(default = "default_suspicious_activity_cooldown_ms")]
    pub suspicious_activity_cooldown_ms: u64,

    /// 是否启用上游 suspicious activity 临时限制后的账号级全局冷却
    #[serde(default = "default_suspicious_activity_cooldown_enabled")]
    pub suspicious_activity_cooldown_enabled: bool,

    /// 调度时是否优先选择从未触发 suspicious activity 的账号
    #[serde(default = "default_suspicious_activity_prefer_clean_credentials")]
    pub suspicious_activity_prefer_clean_credentials: bool,

    /// 是否在同一窗口内多次触发 suspicious activity 后自动禁用账号
    #[serde(default = "default_suspicious_activity_auto_disable_enabled")]
    pub suspicious_activity_auto_disable_enabled: bool,

    /// suspicious activity 自动禁用阈值（同一窗口内命中次数，0 表示不自动禁用）
    #[serde(default = "default_suspicious_activity_auto_disable_threshold")]
    pub suspicious_activity_auto_disable_threshold: u32,

    /// suspicious activity 自动禁用统计窗口（毫秒，0 表示不重置窗口计数）
    #[serde(default = "default_suspicious_activity_auto_disable_window_ms")]
    pub suspicious_activity_auto_disable_window_ms: u64,

    /// 是否在账号恢复稳定后自动清除 suspicious activity 标记
    #[serde(default = "default_suspicious_activity_auto_clear_enabled")]
    pub suspicious_activity_auto_clear_enabled: bool,

    /// 自动清除 suspicious activity 标记所需的连续成功请求次数（0 表示不按成功次数清除）
    #[serde(default = "default_suspicious_activity_auto_clear_success_threshold")]
    pub suspicious_activity_auto_clear_success_threshold: u32,

    /// 最近一次 suspicious activity 后经过多久自动清除标记（毫秒，0 表示不按时间清除）
    #[serde(default = "default_suspicious_activity_auto_clear_after_ms")]
    pub suspicious_activity_auto_clear_after_ms: u64,

    /// 是否启用“模型不支持”后的运行时模型冷却
    #[serde(default = "default_model_cooldown_enabled")]
    pub model_cooldown_enabled: bool,

    /// 单账号本地 token bucket 的容量（<= 0 表示禁用 token bucket）
    #[serde(default = "default_rate_limit_bucket_capacity")]
    pub rate_limit_bucket_capacity: f64,

    /// 单账号本地 token bucket 的基础回填速率（token/s，<= 0 表示禁用 token bucket）
    #[serde(default = "default_rate_limit_refill_per_second")]
    pub rate_limit_refill_per_second: f64,

    /// 429 自适应退避后允许降到的最小回填速率（token/s）
    #[serde(default = "default_rate_limit_refill_min_per_second")]
    pub rate_limit_refill_min_per_second: f64,

    /// 每次成功请求后恢复的回填速率增量（token/s）
    #[serde(default = "default_rate_limit_refill_recovery_step_per_success")]
    pub rate_limit_refill_recovery_step_per_success: f64,

    /// 遭遇 429 时当前回填速率的衰减系数（0.05-1，越小退避越激进）
    #[serde(default = "default_rate_limit_refill_backoff_factor")]
    pub rate_limit_refill_backoff_factor: f64,

    /// 轻/重请求的本地令牌消耗权重规则
    #[serde(default)]
    pub request_weighting: RequestWeightingConfig,

    /// 流式请求是否在上游已开始产生可转发内容后释放调度 lease。
    /// 无法进行首内容探测时会在上游响应建立后释放，避免长时间 SSE 转发持续占用凭据槽。
    #[serde(default = "default_stream_dispatch_lease_release_enabled")]
    pub stream_dispatch_lease_release_enabled: bool,

    /// 流式请求在收到上游响应头前的自适应故障转移策略。
    #[serde(default)]
    pub stream_pre_sse_failover: StreamPreSseFailoverConfig,

    /// 非流式请求收到上游响应头后读取完整 body 的超时策略。
    #[serde(default)]
    pub non_stream_body_read_timeout: NonStreamBodyReadTimeoutConfig,

    /// 最终 Kiro 上游请求体大小保护。
    #[serde(default, alias = "kiro_request_body_guard")]
    pub kiro_request_body_guard: KiroRequestBodyGuardConfig,

    /// Anthropic -> Kiro 转换与图片处理的本地 blocking 运行池。
    #[serde(default)]
    pub conversion_runtime: ConversionRuntimeConfig,

    /// 历史 thinking signature 校验模式。
    /// 支持 strict、warn_only、disabled、strip_invalid；默认 strict。
    #[serde(default, alias = "thinking_signature_validation_mode")]
    pub thinking_signature_validation_mode: ThinkingSignatureValidationMode,

    /// 响应侧 thinking signature 兼容模式：当 thinking 请求的上游流先返回文本/工具块时，
    /// 补齐隐藏 thinking block 和动态 signature_delta。默认关闭。
    #[serde(default, alias = "response_thinking_signature_compat_enabled")]
    pub response_thinking_signature_compat_enabled: bool,

    /// Anthropic server-side web tools 兼容模式。
    /// max_compat 会在 kiro.rs 本地补齐 web_fetch；native_only 只使用 Kiro 原生 MCP 能力；
    /// disabled 直接拒绝 server web tool 请求，作为快速回滚开关。
    #[serde(default, alias = "server_web_tools_mode")]
    pub server_web_tools_mode: ServerWebToolsMode,

    /// 账号类型默认模型策略
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub account_type_policies: BTreeMap<String, ModelSupportPolicy>,

    /// 账号类型默认调度策略
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub account_type_dispatch_policies: BTreeMap<String, AccountTypeDispatchPolicy>,

    /// 配置文件路径（运行时元数据，不写入 JSON）
    #[serde(skip)]
    config_path: Option<PathBuf>,
}

fn default_host() -> String {
    "127.0.0.1".to_string()
}

fn default_port() -> u16 {
    8080
}

fn default_region() -> String {
    "us-east-1".to_string()
}

fn default_kiro_version() -> String {
    "0.11.107".to_string()
}

fn default_system_version() -> String {
    const SYSTEM_VERSIONS: &[&str] = &["darwin#24.6.0", "win32#10.0.22631"];
    SYSTEM_VERSIONS[fastrand::usize(..SYSTEM_VERSIONS.len())].to_string()
}

fn default_node_version() -> String {
    "22.22.0".to_string()
}

fn default_count_tokens_auth_type() -> String {
    "x-api-key".to_string()
}

fn default_tls_backend() -> TlsBackend {
    TlsBackend::Rustls
}

fn default_load_balancing_mode() -> String {
    "priority".to_string()
}

fn default_state_backend() -> StateBackendKind {
    StateBackendKind::File
}

fn default_rate_limit_cooldown_ms() -> u64 {
    2_000
}

fn default_rate_limit_cooldown_enabled() -> bool {
    false
}

fn default_suspicious_activity_cooldown_ms() -> u64 {
    7_200_000
}

fn default_suspicious_activity_cooldown_enabled() -> bool {
    true
}

fn default_suspicious_activity_prefer_clean_credentials() -> bool {
    true
}

fn default_suspicious_activity_auto_disable_enabled() -> bool {
    true
}

fn default_suspicious_activity_auto_disable_threshold() -> u32 {
    3
}

fn default_suspicious_activity_auto_disable_window_ms() -> u64 {
    86_400_000
}

fn default_suspicious_activity_auto_clear_enabled() -> bool {
    true
}

fn default_suspicious_activity_auto_clear_success_threshold() -> u32 {
    10
}

fn default_suspicious_activity_auto_clear_after_ms() -> u64 {
    604_800_000
}

fn default_model_cooldown_enabled() -> bool {
    true
}

fn default_rate_limit_bucket_capacity() -> f64 {
    6.0
}

fn default_rate_limit_refill_per_second() -> f64 {
    2.0
}

fn default_rate_limit_refill_min_per_second() -> f64 {
    1.0
}

fn default_rate_limit_refill_recovery_step_per_success() -> f64 {
    0.25
}

fn default_rate_limit_refill_backoff_factor() -> f64 {
    0.75
}

fn default_state_redis_heartbeat_interval_secs() -> u64 {
    3
}

fn default_state_redis_leader_lease_ttl_secs() -> u64 {
    9
}

fn default_state_hot_path_sync_min_interval_ms() -> u64 {
    25
}

fn default_request_weighting_enabled() -> bool {
    true
}

fn default_request_weighting_base_weight() -> f64 {
    1.0
}

fn default_request_weighting_max_weight() -> f64 {
    2.5
}

fn default_request_weighting_tools_bonus() -> f64 {
    0.4
}

fn default_request_weighting_large_max_tokens_threshold() -> i32 {
    8_000
}

fn default_request_weighting_large_max_tokens_bonus() -> f64 {
    0.25
}

fn default_request_weighting_large_input_tokens_threshold() -> i32 {
    12_000
}

fn default_request_weighting_large_input_tokens_bonus() -> f64 {
    0.25
}

fn default_request_weighting_very_large_input_tokens_threshold() -> i32 {
    24_000
}

fn default_request_weighting_very_large_input_tokens_bonus() -> f64 {
    0.35
}

fn default_request_weighting_thinking_bonus() -> f64 {
    0.35
}

fn default_request_weighting_heavy_thinking_budget_threshold() -> i32 {
    24_000
}

fn default_request_weighting_heavy_thinking_budget_bonus() -> f64 {
    0.35
}

fn default_stream_dispatch_lease_release_enabled() -> bool {
    true
}

fn default_stream_pre_sse_failover_enabled() -> bool {
    true
}

fn default_stream_pre_sse_total_budget_ms() -> u64 {
    170_000
}

fn default_stream_pre_sse_small_request_threshold_bytes() -> usize {
    128 * 1024
}

fn default_stream_pre_sse_medium_request_threshold_bytes() -> usize {
    1024 * 1024
}

fn default_stream_pre_sse_large_request_threshold_bytes() -> usize {
    5 * 1024 * 1024
}

fn default_stream_pre_sse_small_request_timeout_ms() -> u64 {
    30_000
}

fn default_stream_pre_sse_medium_request_timeout_ms() -> u64 {
    60_000
}

fn default_stream_pre_sse_large_request_timeout_ms() -> u64 {
    120_000
}

fn default_stream_pre_sse_slow_model_min_timeout_ms() -> u64 {
    60_000
}

fn default_stream_pre_sse_max_fast_failovers() -> usize {
    2
}

fn default_stream_pre_sse_min_remaining_ms() -> u64 {
    15_000
}

fn default_non_stream_body_read_timeout_enabled() -> bool {
    true
}

fn default_non_stream_body_read_timeout_ms() -> u64 {
    540_000
}

fn default_non_stream_eventstream_idle_timeout_ms() -> u64 {
    120_000
}

fn default_non_stream_body_read_timeout_retry_on_timeout() -> bool {
    false
}

fn default_non_stream_eventstream_safe_retry_on_stall() -> bool {
    true
}

fn default_kiro_request_body_guard_enabled() -> bool {
    true
}

pub fn default_kiro_request_body_guard_max_bytes() -> usize {
    30 * 1024 * 1024
}

fn default_conversion_max_concurrent() -> usize {
    16
}

fn default_conversion_max_queue() -> usize {
    64
}

fn default_conversion_max_queue_weight() -> usize {
    128
}

fn default_conversion_queue_wait_ms() -> u64 {
    60_000
}

fn default_conversion_max_request_weight() -> usize {
    8
}

fn default_true() -> bool {
    true
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
            health_port: None,
            region: default_region(),
            auth_region: None,
            api_region: None,
            runtime_endpoint: None,
            management_endpoint: None,
            kiro_version: default_kiro_version(),
            machine_id: None,
            api_key: None,
            api_keys: Vec::new(),
            credential_groups: vec![CredentialGroupConfig::default_group()],
            system_version: default_system_version(),
            node_version: default_node_version(),
            tls_backend: default_tls_backend(),
            count_tokens_api_url: None,
            count_tokens_api_key: None,
            count_tokens_auth_type: default_count_tokens_auth_type(),
            proxy_url: None,
            proxy_username: None,
            proxy_password: None,
            proxy_pool: ProxyPoolConfig::default(),
            admin_api_key: None,
            state_backend: default_state_backend(),
            state_postgres_url: None,
            state_redis_url: None,
            instance_id: None,
            state_redis_heartbeat_interval_secs: default_state_redis_heartbeat_interval_secs(),
            state_redis_leader_lease_ttl_secs: default_state_redis_leader_lease_ttl_secs(),
            state_hot_path_sync_min_interval_ms: default_state_hot_path_sync_min_interval_ms(),
            load_balancing_mode: default_load_balancing_mode(),
            session_affinity_enabled: false,
            default_max_concurrency: None,
            queue_max_size: 0,
            queue_max_wait_ms: 0,
            rate_limit_cooldown_ms: default_rate_limit_cooldown_ms(),
            rate_limit_cooldown_enabled: default_rate_limit_cooldown_enabled(),
            suspicious_activity_cooldown_ms: default_suspicious_activity_cooldown_ms(),
            suspicious_activity_cooldown_enabled: default_suspicious_activity_cooldown_enabled(),
            suspicious_activity_prefer_clean_credentials:
                default_suspicious_activity_prefer_clean_credentials(),
            suspicious_activity_auto_disable_enabled:
                default_suspicious_activity_auto_disable_enabled(),
            suspicious_activity_auto_disable_threshold:
                default_suspicious_activity_auto_disable_threshold(),
            suspicious_activity_auto_disable_window_ms:
                default_suspicious_activity_auto_disable_window_ms(),
            suspicious_activity_auto_clear_enabled: default_suspicious_activity_auto_clear_enabled(
            ),
            suspicious_activity_auto_clear_success_threshold:
                default_suspicious_activity_auto_clear_success_threshold(),
            suspicious_activity_auto_clear_after_ms:
                default_suspicious_activity_auto_clear_after_ms(),
            model_cooldown_enabled: default_model_cooldown_enabled(),
            rate_limit_bucket_capacity: default_rate_limit_bucket_capacity(),
            rate_limit_refill_per_second: default_rate_limit_refill_per_second(),
            rate_limit_refill_min_per_second: default_rate_limit_refill_min_per_second(),
            rate_limit_refill_recovery_step_per_success:
                default_rate_limit_refill_recovery_step_per_success(),
            rate_limit_refill_backoff_factor: default_rate_limit_refill_backoff_factor(),
            request_weighting: RequestWeightingConfig::default(),
            stream_dispatch_lease_release_enabled: default_stream_dispatch_lease_release_enabled(),
            stream_pre_sse_failover: StreamPreSseFailoverConfig::default(),
            non_stream_body_read_timeout: NonStreamBodyReadTimeoutConfig::default(),
            kiro_request_body_guard: KiroRequestBodyGuardConfig::default(),
            conversion_runtime: ConversionRuntimeConfig::default(),
            thinking_signature_validation_mode: ThinkingSignatureValidationMode::default(),
            response_thinking_signature_compat_enabled: false,
            server_web_tools_mode: ServerWebToolsMode::default(),
            account_type_policies: BTreeMap::new(),
            account_type_dispatch_policies: BTreeMap::new(),
            config_path: None,
        }
    }
}

impl Config {
    /// 获取默认配置文件路径
    pub fn default_config_path() -> &'static str {
        "config.json"
    }

    /// 获取有效的 Auth Region（用于 Token 刷新）
    /// 优先使用 auth_region，未配置时回退到 region
    pub fn effective_auth_region(&self) -> &str {
        self.auth_region.as_deref().unwrap_or(&self.region)
    }

    /// 获取有效的 API Region（用于 API 请求）
    /// 优先使用 api_region，未配置时回退到 region
    pub fn effective_api_region(&self) -> &str {
        self.api_region.as_deref().unwrap_or(&self.region)
    }

    /// 获取有效的 Runtime API base URL（用于推理/MCP 请求）
    pub fn effective_runtime_endpoint_base(&self, api_region: &str) -> String {
        self.runtime_endpoint
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| value.trim_end_matches('/').to_string())
            .unwrap_or_else(|| Self::q_endpoint_base(api_region))
    }

    /// 获取有效的 Management API base URL（用于管理/额度查询请求）
    pub fn effective_management_endpoint_base(&self, api_region: &str) -> String {
        self.management_endpoint
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(|value| value.trim_end_matches('/').to_string())
            .unwrap_or_else(|| Self::q_endpoint_base(api_region))
    }

    pub fn q_endpoint_base(api_region: &str) -> String {
        format!("https://q.{}.amazonaws.com", api_region)
    }

    /// 从 endpoint base URL 中提取 Host header 使用的域名
    pub fn endpoint_host(endpoint_base: &str) -> String {
        url::Url::parse(endpoint_base)
            .ok()
            .and_then(|parsed| parsed.host_str().map(str::to_string))
            .unwrap_or_else(|| {
                endpoint_base
                    .trim_start_matches("https://")
                    .trim_start_matches("http://")
                    .split('/')
                    .next()
                    .unwrap_or(endpoint_base)
                    .to_string()
            })
    }

    /// 从文件加载配置
    pub fn load<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            // 配置文件不存在，返回默认配置
            let mut config = Self::default();
            config.config_path = Some(path.to_path_buf());
            config.validate()?;
            return Ok(config);
        }

        let content = fs::read_to_string(path)?;
        let mut config: Config = serde_json::from_str(&content)?;
        config.config_path = Some(path.to_path_buf());
        config.normalize();
        config.validate()?;
        Ok(config)
    }

    /// 获取配置文件路径（如果有）
    pub fn config_path(&self) -> Option<&Path> {
        self.config_path.as_deref()
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        if self
            .instance_id
            .as_ref()
            .is_some_and(|value| value.trim().is_empty())
        {
            anyhow::bail!("instanceId 不能为空字符串");
        }

        if self.state_redis_heartbeat_interval_secs == 0 {
            anyhow::bail!("stateRedisHeartbeatIntervalSecs 必须大于 0");
        }

        if self.state_redis_leader_lease_ttl_secs == 0 {
            anyhow::bail!("stateRedisLeaderLeaseTtlSecs 必须大于 0");
        }

        if self.state_redis_heartbeat_interval_secs >= self.state_redis_leader_lease_ttl_secs {
            anyhow::bail!("stateRedisHeartbeatIntervalSecs 必须小于 stateRedisLeaderLeaseTtlSecs");
        }

        if self.health_port.is_some_and(|port| port == 0) {
            anyhow::bail!("healthPort 不能为 0");
        }

        if self.health_port.is_some_and(|port| port == self.port) {
            anyhow::bail!("healthPort 不能与 port 相同");
        }

        self.validate_api_keys()?;
        if self.suspicious_activity_auto_disable_enabled
            && self.suspicious_activity_auto_disable_threshold == 0
        {
            anyhow::bail!(
                "suspiciousActivityAutoDisableThreshold 必须大于 0，或关闭 suspiciousActivityAutoDisableEnabled"
            );
        }

        if self.suspicious_activity_auto_clear_enabled
            && self.suspicious_activity_auto_clear_success_threshold == 0
            && self.suspicious_activity_auto_clear_after_ms == 0
        {
            anyhow::bail!(
                "suspiciousActivityAutoClearSuccessThreshold 和 suspiciousActivityAutoClearAfterMs 不能同时为 0，或关闭 suspiciousActivityAutoClearEnabled"
            );
        }

        self.request_weighting.validate()?;
        self.stream_pre_sse_failover.validate()?;
        self.non_stream_body_read_timeout.validate()?;
        self.kiro_request_body_guard.validate()?;
        self.conversion_runtime.validate()?;
        self.proxy_pool.validate()?;

        Ok(())
    }

    fn normalize(&mut self) {
        normalize_account_type_policies(&mut self.account_type_policies);
        normalize_account_type_dispatch_policies(&mut self.account_type_dispatch_policies);
        let had_explicit_credential_groups = !self.credential_groups.is_empty();
        for api_key in &mut self.api_keys {
            api_key.id = api_key
                .id
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string);
            api_key.key = api_key.key.trim().to_string();
            api_key.allowed_credential_groups =
                normalize_credential_groups(&api_key.allowed_credential_groups);
        }
        let mut credential_groups = normalize_credential_group_catalog(&self.credential_groups);
        if !had_explicit_credential_groups {
            let mut known = credential_groups
                .iter()
                .map(|group| group.name.clone())
                .collect::<BTreeSet<_>>();
            for group in self
                .api_keys
                .iter()
                .flat_map(|api_key| api_key.allowed_credential_groups.iter())
            {
                if known.insert(group.clone()) {
                    credential_groups.push(CredentialGroupConfig {
                        name: group.clone(),
                        display_name: Some(group.clone()),
                        description: None,
                        enabled: true,
                    });
                }
            }
            credential_groups.sort_by(|a, b| a.name.cmp(&b.name));
        }
        self.credential_groups = credential_groups;
    }

    fn validate_api_keys(&self) -> anyhow::Result<()> {
        let mut ids = std::collections::BTreeSet::new();
        let mut keys = std::collections::BTreeSet::new();
        let credential_group_names = self.credential_group_name_set();

        if let Some(api_key) = self.api_key.as_deref() {
            if api_key.trim().is_empty() {
                anyhow::bail!("apiKey 不能为空字符串");
            }
            keys.insert(api_key.trim().to_string());
        }

        for (index, api_key) in self.api_keys.iter().enumerate() {
            let key_name = format!("apiKeys[{index}]");
            if api_key.key.trim().is_empty() {
                anyhow::bail!("{key_name}.key 不能为空字符串");
            }

            let id = api_key
                .id
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .unwrap_or_else(|| format!("api-key-{}", index + 1));
            if !ids.insert(id.clone()) {
                anyhow::bail!("apiKeys 中存在重复 id: {id}");
            }

            if !keys.insert(api_key.key.trim().to_string()) {
                anyhow::bail!("apiKeys 中存在重复 key");
            }

            if normalize_credential_groups(&api_key.allowed_credential_groups).is_empty() {
                anyhow::bail!("{key_name}.allowedCredentialGroups 至少需要一个有效分组");
            }

            for group in normalize_credential_groups(&api_key.allowed_credential_groups) {
                if !credential_group_names.contains(&group) {
                    anyhow::bail!(
                        "{key_name}.allowedCredentialGroups 包含未登记的凭据分组: {group}"
                    );
                }
            }
        }

        Ok(())
    }

    pub fn credential_group_name_set(&self) -> BTreeSet<String> {
        normalize_credential_group_catalog(&self.credential_groups)
            .into_iter()
            .map(|group| group.name)
            .collect()
    }

    pub fn normalize_and_validate_credential_group_catalog(
        groups: Vec<CredentialGroupConfig>,
    ) -> anyhow::Result<Vec<CredentialGroupConfig>> {
        let normalized = normalize_credential_group_catalog(&groups);
        if normalized.is_empty() {
            anyhow::bail!("credentialGroups 至少需要包含 default 分组");
        }
        if !normalized
            .iter()
            .any(|group| group.name == DEFAULT_CREDENTIAL_GROUP && group.enabled)
        {
            anyhow::bail!("credentialGroups 必须包含启用的 default 分组");
        }
        Ok(normalized)
    }

    pub fn api_key_auth_entries(&self) -> anyhow::Result<Vec<ApiKeyAuthEntry>> {
        let mut entries = Vec::new();

        if let Some(key) = self
            .api_key
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            entries.push(ApiKeyAuthEntry {
                id: "legacy".to_string(),
                key: key.to_string(),
                credential_group_scope: CredentialGroupScope::all(),
            });
        }

        for (index, api_key) in self.api_keys.iter().enumerate() {
            let key = api_key.key.trim();
            let groups = normalize_credential_groups(&api_key.allowed_credential_groups);
            if key.is_empty() || groups.is_empty() {
                continue;
            }
            entries.push(ApiKeyAuthEntry {
                id: api_key
                    .id
                    .as_deref()
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_string)
                    .unwrap_or_else(|| format!("api-key-{}", index + 1)),
                key: key.to_string(),
                credential_group_scope: CredentialGroupScope::Groups(groups),
            });
        }

        if entries.is_empty() {
            anyhow::bail!("配置文件中未设置 apiKey 或 apiKeys");
        }

        Ok(entries)
    }

    pub fn primary_api_key_material(&self) -> Option<String> {
        self.api_key
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .or_else(|| {
                self.api_keys
                    .iter()
                    .map(|entry| entry.key.trim())
                    .find(|value| !value.is_empty())
                    .map(str::to_string)
            })
    }

    pub fn resolved_instance_id(&self) -> String {
        if let Some(instance_id) = self
            .instance_id
            .as_ref()
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
        {
            return instance_id.to_string();
        }

        let host = std::env::var("HOSTNAME")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| self.host.clone());

        format!("{host}:{}:{}", self.port, std::process::id())
    }

    pub fn resolved_advertise_http_base_url(&self) -> Option<String> {
        let advertise_host = std::env::var("KIRO_ADVERTISE_HOST")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .or_else(|| {
                std::env::var("POD_IP")
                    .ok()
                    .filter(|value| !value.trim().is_empty())
            })
            .or_else(|| {
                let host = self.host.trim();
                if host.is_empty()
                    || matches!(host, "0.0.0.0" | "::" | "127.0.0.1" | "::1" | "localhost")
                {
                    None
                } else {
                    Some(host.to_string())
                }
            })?;

        let advertise_host = if advertise_host.contains(':') && !advertise_host.starts_with('[') {
            format!("[{advertise_host}]")
        } else {
            advertise_host
        };

        Some(format!("http://{advertise_host}:{}", self.port))
    }

    pub fn resolved_health_port(&self) -> Option<u16> {
        self.health_port.or_else(|| self.port.checked_add(1))
    }

    /// 将当前配置写回原始配置文件
    pub fn save(&self) -> anyhow::Result<()> {
        let path = self
            .config_path
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("配置文件路径未知，无法保存配置"))?;

        let content = serde_json::to_string_pretty(self).context("序列化配置失败")?;
        fs::write(path, content)
            .with_context(|| format!("写入配置文件失败: {}", path.display()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ApiKeyConfig, Config, ConversionRuntimeConfig, KiroRequestBodyGuardConfig,
        RequestWeightingConfig, ServerWebToolsMode, ThinkingSignatureValidationMode,
    };
    use crate::common::auth::CredentialGroupScope;
    use std::fs;

    fn temp_config_path(test_name: &str) -> std::path::PathBuf {
        let dir =
            std::env::temp_dir().join(format!("kiro-config-{test_name}-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        dir.join("config.json")
    }

    #[test]
    fn validate_rejects_invalid_redis_runtime_coordination_timing() {
        let mut config = Config::default();
        config.state_redis_heartbeat_interval_secs = 30;
        config.state_redis_leader_lease_ttl_secs = 30;

        let err = config.validate().unwrap_err().to_string();

        assert!(err.contains("stateRedisHeartbeatIntervalSecs"));
    }

    #[test]
    fn api_key_auth_entries_keep_legacy_key_full_scope() {
        let mut config = Config::default();
        config.api_key = Some(" legacy-key ".to_string());
        config.api_keys = vec![ApiKeyConfig {
            id: Some("cheap".to_string()),
            key: " scoped-key ".to_string(),
            allowed_credential_groups: vec!["LOW-COST".to_string(), "stable".to_string()],
        }];

        let entries = config.api_key_auth_entries().unwrap();

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].id, "legacy");
        assert_eq!(entries[0].key, "legacy-key");
        assert_eq!(entries[0].credential_group_scope, CredentialGroupScope::All);
        assert_eq!(entries[1].id, "cheap");
        assert_eq!(entries[1].key, "scoped-key");
        assert_eq!(
            entries[1].credential_group_scope,
            CredentialGroupScope::Groups(vec!["low-cost".to_string(), "stable".to_string()])
        );
    }

    #[test]
    fn validate_rejects_api_key_without_valid_groups() {
        let mut config = Config::default();
        config.api_keys = vec![ApiKeyConfig {
            id: Some("empty".to_string()),
            key: "scoped-key".to_string(),
            allowed_credential_groups: vec!["  ".to_string()],
        }];

        let err = config.validate().unwrap_err().to_string();

        assert!(err.contains("allowedCredentialGroups"));
    }

    #[test]
    fn load_seeds_credential_group_catalog_from_api_keys_when_missing() {
        let path = temp_config_path("seed-credential-groups");
        fs::write(
            &path,
            r#"{
              "apiKeys": [
                {
                  "id": "stable",
                  "key": "scoped-key",
                  "allowedCredentialGroups": ["Stable", "low-cost"]
                }
              ]
            }"#,
        )
        .unwrap();

        let config = Config::load(&path).unwrap();
        let groups = config
            .credential_groups
            .iter()
            .map(|group| group.name.as_str())
            .collect::<Vec<_>>();

        assert_eq!(groups, vec!["default", "low-cost", "stable"]);
    }

    #[test]
    fn load_rejects_api_key_group_missing_from_explicit_catalog() {
        let path = temp_config_path("missing-explicit-credential-group");
        fs::write(
            &path,
            r#"{
              "credentialGroups": [{ "name": "default" }],
              "apiKeys": [
                {
                  "id": "stable",
                  "key": "scoped-key",
                  "allowedCredentialGroups": ["stable"]
                }
              ]
            }"#,
        )
        .unwrap();

        let err = Config::load(&path).unwrap_err().to_string();

        assert!(err.contains("未登记的凭据分组"));
    }

    #[test]
    fn resolved_instance_id_prefers_explicit_value() {
        let mut config = Config::default();
        config.instance_id = Some("kiro-a".to_string());

        assert_eq!(config.resolved_instance_id(), "kiro-a");
    }

    #[test]
    fn resolved_health_port_defaults_to_next_port() {
        let config = Config {
            port: 8990,
            ..Config::default()
        };

        assert_eq!(config.resolved_health_port(), Some(8991));
    }

    #[test]
    fn validate_rejects_invalid_conversion_runtime() {
        let mut config = Config::default();
        config.conversion_runtime = ConversionRuntimeConfig {
            max_concurrent: 0,
            ..ConversionRuntimeConfig::default()
        };

        let err = config.validate().unwrap_err().to_string();

        assert!(err.contains("conversionRuntime.maxConcurrent"));
    }

    #[test]
    fn effective_runtime_endpoint_base_defaults_to_legacy_q_endpoint() {
        let config = Config::default();

        assert_eq!(
            config.effective_runtime_endpoint_base("eu-central-1"),
            "https://q.eu-central-1.amazonaws.com"
        );
    }

    #[test]
    fn effective_runtime_endpoint_base_uses_configured_kiro_endpoint() {
        let mut config = Config::default();
        config.runtime_endpoint = Some("https://runtime.eu-central-1.kiro.dev/".to_string());

        assert_eq!(
            config.effective_runtime_endpoint_base("eu-central-1"),
            "https://runtime.eu-central-1.kiro.dev"
        );
    }

    #[test]
    fn effective_management_endpoint_base_uses_configured_kiro_endpoint() {
        let mut config = Config::default();
        config.management_endpoint = Some("https://management.us-east-1.kiro.dev/".to_string());

        assert_eq!(
            config.effective_management_endpoint_base("us-east-1"),
            "https://management.us-east-1.kiro.dev"
        );
    }

    #[test]
    fn endpoint_host_extracts_host_from_endpoint_base() {
        assert_eq!(
            Config::endpoint_host("https://runtime.us-east-1.kiro.dev"),
            "runtime.us-east-1.kiro.dev"
        );
    }

    #[test]
    fn validate_rejects_request_weighting_max_below_base() {
        let mut config = Config::default();
        config.request_weighting = RequestWeightingConfig {
            base_weight: 2.0,
            max_weight: 1.5,
            ..RequestWeightingConfig::default()
        };

        let err = config.validate().unwrap_err().to_string();

        assert!(err.contains("requestWeighting.maxWeight"));
    }

    #[test]
    fn validate_rejects_require_proxy_without_enabled_pool() {
        let mut config = Config::default();
        config.proxy_pool.require_proxy = true;

        let err = config.validate().unwrap_err().to_string();

        assert!(err.contains("proxyPool.requireProxy"));
    }

    #[test]
    fn request_weighting_defaults_to_enabled_and_tuned_for_weighted_bucket() {
        let config = Config::default();

        assert_eq!(
            config.thinking_signature_validation_mode,
            ThinkingSignatureValidationMode::Strict
        );
        assert!(!config.response_thinking_signature_compat_enabled);
        assert!(!config.rate_limit_cooldown_enabled);
        assert!(config.suspicious_activity_cooldown_enabled);
        assert_eq!(config.suspicious_activity_cooldown_ms, 7_200_000);
        assert!(config.suspicious_activity_prefer_clean_credentials);
        assert!(config.suspicious_activity_auto_disable_enabled);
        assert_eq!(config.suspicious_activity_auto_disable_threshold, 3);
        assert_eq!(
            config.suspicious_activity_auto_disable_window_ms,
            86_400_000
        );
        assert!(config.suspicious_activity_auto_clear_enabled);
        assert_eq!(config.suspicious_activity_auto_clear_success_threshold, 10);
        assert_eq!(config.suspicious_activity_auto_clear_after_ms, 604_800_000);
        assert!(config.model_cooldown_enabled);
        assert!(config.request_weighting.enabled);
        assert!(config.stream_dispatch_lease_release_enabled);
        assert!((config.rate_limit_bucket_capacity - 6.0).abs() < f64::EPSILON);
        assert!((config.rate_limit_refill_per_second - 2.0).abs() < f64::EPSILON);
        assert!((config.rate_limit_refill_min_per_second - 1.0).abs() < f64::EPSILON);
        assert!((config.rate_limit_refill_recovery_step_per_success - 0.25).abs() < f64::EPSILON);
        assert!((config.rate_limit_refill_backoff_factor - 0.75).abs() < f64::EPSILON);
        assert!((config.request_weighting.max_weight - 2.5).abs() < f64::EPSILON);
        assert!((config.request_weighting.tools_bonus - 0.4).abs() < f64::EPSILON);
        assert_eq!(config.conversion_runtime.max_concurrent, 16);
        assert_eq!(config.conversion_runtime.max_queue, 64);
        assert_eq!(config.conversion_runtime.max_queue_weight, 128);
        assert_eq!(config.conversion_runtime.queue_wait_ms, 60_000);
        assert_eq!(config.conversion_runtime.max_request_weight, 8);
        assert!(config.kiro_request_body_guard.enabled);
        assert_eq!(config.kiro_request_body_guard.max_bytes, 30 * 1024 * 1024);
    }

    #[test]
    fn non_stream_body_read_timeout_defaults_to_guarded_no_retry() {
        let config = Config::default();

        assert!(config.non_stream_body_read_timeout.enabled);
        assert_eq!(config.non_stream_body_read_timeout.timeout_ms, 540_000);
        assert_eq!(
            config
                .non_stream_body_read_timeout
                .eventstream_idle_timeout_ms,
            120_000
        );
        assert!(!config.non_stream_body_read_timeout.retry_on_timeout);
        assert!(
            config
                .non_stream_body_read_timeout
                .eventstream_safe_retry_on_stall
        );
        config.validate().unwrap();
    }

    #[test]
    fn validate_rejects_enabled_non_stream_body_read_timeout_zero() {
        let mut config = Config::default();
        config.non_stream_body_read_timeout.timeout_ms = 0;

        let err = config.validate().unwrap_err().to_string();

        assert!(err.contains("nonStreamBodyReadTimeout.timeoutMs"));
    }

    #[test]
    fn kiro_request_body_guard_defaults_and_rejects_over_limit_only() {
        let guard = KiroRequestBodyGuardConfig::default();

        assert!(guard.enabled);
        assert_eq!(guard.max_bytes, 30 * 1024 * 1024);
        assert!(!guard.should_reject(30 * 1024 * 1024));
        assert!(guard.should_reject(30 * 1024 * 1024 + 1));
    }

    #[test]
    fn validate_rejects_enabled_kiro_request_body_guard_too_small() {
        let mut config = Config::default();
        config.kiro_request_body_guard.max_bytes = 1024;

        let err = config.validate().unwrap_err().to_string();

        assert!(err.contains("kiroRequestBodyGuard.maxBytes"));
    }

    #[test]
    fn thinking_signature_validation_mode_deserializes_supported_values_and_alias() {
        let camel: Config =
            serde_json::from_str(r#"{"thinkingSignatureValidationMode":"warn_only"}"#).unwrap();
        let snake: Config =
            serde_json::from_str(r#"{"thinking_signature_validation_mode":"strip-invalid"}"#)
                .unwrap();

        assert_eq!(
            camel.thinking_signature_validation_mode,
            ThinkingSignatureValidationMode::WarnOnly
        );
        assert_eq!(
            snake.thinking_signature_validation_mode,
            ThinkingSignatureValidationMode::StripInvalid
        );
    }

    #[test]
    fn response_thinking_signature_compat_enabled_deserializes_alias() {
        let camel: Config =
            serde_json::from_str(r#"{"responseThinkingSignatureCompatEnabled":true}"#).unwrap();
        let snake: Config =
            serde_json::from_str(r#"{"response_thinking_signature_compat_enabled":true}"#).unwrap();

        assert!(camel.response_thinking_signature_compat_enabled);
        assert!(snake.response_thinking_signature_compat_enabled);
    }

    #[test]
    fn server_web_tools_mode_defaults_to_max_compat_and_accepts_aliases() {
        let default_config: Config = serde_json::from_str("{}").unwrap();
        let kebab: Config =
            serde_json::from_str(r#"{"serverWebToolsMode":"native-only"}"#).unwrap();
        let snake: Config =
            serde_json::from_str(r#"{"server_web_tools_mode":"disabled"}"#).unwrap();

        assert_eq!(
            default_config.server_web_tools_mode,
            ServerWebToolsMode::MaxCompat
        );
        assert_eq!(kebab.server_web_tools_mode, ServerWebToolsMode::NativeOnly);
        assert_eq!(snake.server_web_tools_mode, ServerWebToolsMode::Disabled);
    }
}
