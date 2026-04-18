use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

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

/// KNA 应用配置
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Config {
    #[serde(default = "default_host")]
    pub host: String,

    #[serde(default = "default_port")]
    pub port: u16,

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

    #[serde(default = "default_kiro_version")]
    pub kiro_version: String,

    #[serde(default)]
    pub machine_id: Option<String>,

    #[serde(default)]
    pub api_key: Option<String>,

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

impl Default for Config {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
            region: default_region(),
            auth_region: None,
            api_region: None,
            kiro_version: default_kiro_version(),
            machine_id: None,
            api_key: None,
            system_version: default_system_version(),
            node_version: default_node_version(),
            tls_backend: default_tls_backend(),
            count_tokens_api_url: None,
            count_tokens_api_key: None,
            count_tokens_auth_type: default_count_tokens_auth_type(),
            proxy_url: None,
            proxy_username: None,
            proxy_password: None,
            admin_api_key: None,
            state_backend: default_state_backend(),
            state_postgres_url: None,
            state_redis_url: None,
            instance_id: None,
            state_redis_heartbeat_interval_secs: default_state_redis_heartbeat_interval_secs(),
            state_redis_leader_lease_ttl_secs: default_state_redis_leader_lease_ttl_secs(),
            state_hot_path_sync_min_interval_ms: default_state_hot_path_sync_min_interval_ms(),
            load_balancing_mode: default_load_balancing_mode(),
            default_max_concurrency: None,
            queue_max_size: 0,
            queue_max_wait_ms: 0,
            rate_limit_cooldown_ms: default_rate_limit_cooldown_ms(),
            rate_limit_bucket_capacity: default_rate_limit_bucket_capacity(),
            rate_limit_refill_per_second: default_rate_limit_refill_per_second(),
            rate_limit_refill_min_per_second: default_rate_limit_refill_min_per_second(),
            rate_limit_refill_recovery_step_per_success:
                default_rate_limit_refill_recovery_step_per_success(),
            rate_limit_refill_backoff_factor: default_rate_limit_refill_backoff_factor(),
            request_weighting: RequestWeightingConfig::default(),
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

        self.request_weighting.validate()?;

        Ok(())
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
    use super::{Config, RequestWeightingConfig};

    #[test]
    fn validate_rejects_invalid_redis_runtime_coordination_timing() {
        let mut config = Config::default();
        config.state_redis_heartbeat_interval_secs = 30;
        config.state_redis_leader_lease_ttl_secs = 30;

        let err = config.validate().unwrap_err().to_string();

        assert!(err.contains("stateRedisHeartbeatIntervalSecs"));
    }

    #[test]
    fn resolved_instance_id_prefers_explicit_value() {
        let mut config = Config::default();
        config.instance_id = Some("kiro-a".to_string());

        assert_eq!(config.resolved_instance_id(), "kiro-a");
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
    fn request_weighting_defaults_to_enabled_and_tuned_for_weighted_bucket() {
        let config = Config::default();

        assert!(config.request_weighting.enabled);
        assert!((config.rate_limit_bucket_capacity - 6.0).abs() < f64::EPSILON);
        assert!((config.rate_limit_refill_per_second - 2.0).abs() < f64::EPSILON);
        assert!((config.rate_limit_refill_min_per_second - 1.0).abs() < f64::EPSILON);
        assert!((config.rate_limit_refill_recovery_step_per_success - 0.25).abs() < f64::EPSILON);
        assert!((config.rate_limit_refill_backoff_factor - 0.75).abs() < f64::EPSILON);
        assert!((config.request_weighting.max_weight - 2.5).abs() < f64::EPSILON);
        assert!((config.request_weighting.tools_bonus - 0.4).abs() < f64::EPSILON);
    }
}
