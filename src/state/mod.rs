use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use parking_lot::Mutex;
use postgres::{Client, NoTls};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::runtime::RuntimeFlavor;

use crate::admin::types::BalanceResponse;
use crate::kiro::model::credentials::{CredentialsConfig, KiroCredentials};
use crate::model::config::{Config, StateBackendKind};

const STATS_FILE_NAME: &str = "kiro_stats.json";
const BALANCE_CACHE_FILE_NAME: &str = "kiro_balance_cache.json";
const POSTGRES_NAMESPACE: &str = "runtime";
const POSTGRES_CREDENTIALS_KEY: &str = "credentials";
const POSTGRES_STATS_KEY: &str = "stats";
const POSTGRES_BALANCE_CACHE_KEY: &str = "balance_cache";
const POSTGRES_DISPATCH_CONFIG_KEY: &str = "dispatch_config";

#[derive(Debug, Clone)]
pub struct PersistedCredentials {
    pub credentials: Vec<KiroCredentials>,
    pub is_multiple_format: bool,
}

impl PersistedCredentials {
    fn empty(is_multiple_format: bool) -> Self {
        Self {
            credentials: Vec::new(),
            is_multiple_format,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsEntryRecord {
    pub success_count: u64,
    pub last_used_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedBalanceRecord {
    pub cached_at: f64,
    pub data: BalanceResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PersistedDispatchConfig {
    pub mode: String,
    pub queue_max_size: usize,
    pub queue_max_wait_ms: u64,
    pub rate_limit_cooldown_ms: u64,
    pub default_max_concurrency: Option<u32>,
    pub rate_limit_bucket_capacity: f64,
    pub rate_limit_refill_per_second: f64,
    pub rate_limit_refill_min_per_second: f64,
    pub rate_limit_refill_recovery_step_per_success: f64,
    pub rate_limit_refill_backoff_factor: f64,
}

impl PersistedDispatchConfig {
    pub fn from_config(config: &Config) -> Self {
        Self {
            mode: config.load_balancing_mode.clone(),
            queue_max_size: config.queue_max_size,
            queue_max_wait_ms: config.queue_max_wait_ms,
            rate_limit_cooldown_ms: config.rate_limit_cooldown_ms,
            default_max_concurrency: config.default_max_concurrency,
            rate_limit_bucket_capacity: config.rate_limit_bucket_capacity,
            rate_limit_refill_per_second: config.rate_limit_refill_per_second,
            rate_limit_refill_min_per_second: config.rate_limit_refill_min_per_second,
            rate_limit_refill_recovery_step_per_success: config
                .rate_limit_refill_recovery_step_per_success,
            rate_limit_refill_backoff_factor: config.rate_limit_refill_backoff_factor,
        }
    }

    pub fn apply_to_config(&self, config: &mut Config) {
        config.load_balancing_mode = self.mode.clone();
        config.queue_max_size = self.queue_max_size;
        config.queue_max_wait_ms = self.queue_max_wait_ms;
        config.rate_limit_cooldown_ms = self.rate_limit_cooldown_ms;
        config.default_max_concurrency = self.default_max_concurrency;
        config.rate_limit_bucket_capacity = self.rate_limit_bucket_capacity;
        config.rate_limit_refill_per_second = self.rate_limit_refill_per_second;
        config.rate_limit_refill_min_per_second = self.rate_limit_refill_min_per_second;
        config.rate_limit_refill_recovery_step_per_success =
            self.rate_limit_refill_recovery_step_per_success;
        config.rate_limit_refill_backoff_factor = self.rate_limit_refill_backoff_factor;
    }
}

#[derive(Clone)]
pub struct StateStore {
    backend: Arc<dyn StateBackend>,
}

impl std::fmt::Debug for StateStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateStore").finish_non_exhaustive()
    }
}

trait StateBackend: Send + Sync {
    fn is_external(&self) -> bool;
    fn load_credentials(&self) -> anyhow::Result<PersistedCredentials>;
    fn load_dispatch_config(&self) -> anyhow::Result<Option<PersistedDispatchConfig>>;
    fn persist_credentials(
        &self,
        credentials: &[KiroCredentials],
        is_multiple_format: bool,
    ) -> anyhow::Result<bool>;
    fn load_stats(&self) -> anyhow::Result<HashMap<String, StatsEntryRecord>>;
    fn save_stats(&self, stats: &HashMap<String, StatsEntryRecord>) -> anyhow::Result<()>;
    fn load_balance_cache(&self) -> anyhow::Result<HashMap<u64, CachedBalanceRecord>>;
    fn save_balance_cache(&self, cache: &HashMap<u64, CachedBalanceRecord>) -> anyhow::Result<()>;
    fn persist_dispatch_config(&self, dispatch: &PersistedDispatchConfig) -> anyhow::Result<()>;
}

impl StateStore {
    pub fn from_config(config: &Config, credentials_path: Option<PathBuf>) -> anyhow::Result<Self> {
        match config.state_backend {
            StateBackendKind::File => Ok(Self::file(
                config.config_path().map(|path| path.to_path_buf()),
                credentials_path,
            )),
            StateBackendKind::Postgres => {
                let postgres_url = config
                    .state_postgres_url
                    .as_deref()
                    .ok_or_else(|| anyhow::anyhow!("stateBackend=postgres 时必须配置 statePostgresUrl"))?;
                Self::postgres(postgres_url)
            }
        }
    }

    pub fn file(config_path: Option<PathBuf>, credentials_path: Option<PathBuf>) -> Self {
        Self {
            backend: Arc::new(FileStateBackend {
                config_path,
                credentials_path,
            }),
        }
    }

    pub fn postgres(postgres_url: &str) -> anyhow::Result<Self> {
        Ok(Self {
            backend: Arc::new(PostgresStateBackend::connect(postgres_url)?),
        })
    }

    pub fn is_external(&self) -> bool {
        self.backend.is_external()
    }

    pub fn load_credentials(&self) -> anyhow::Result<PersistedCredentials> {
        self.backend.load_credentials()
    }

    pub fn load_dispatch_config(&self) -> anyhow::Result<Option<PersistedDispatchConfig>> {
        self.backend.load_dispatch_config()
    }

    pub fn persist_credentials(
        &self,
        credentials: &[KiroCredentials],
        is_multiple_format: bool,
    ) -> anyhow::Result<bool> {
        self.backend
            .persist_credentials(credentials, is_multiple_format)
    }

    pub fn load_stats(&self) -> anyhow::Result<HashMap<String, StatsEntryRecord>> {
        self.backend.load_stats()
    }

    pub fn save_stats(&self, stats: &HashMap<String, StatsEntryRecord>) -> anyhow::Result<()> {
        self.backend.save_stats(stats)
    }

    pub fn load_balance_cache(&self) -> anyhow::Result<HashMap<u64, CachedBalanceRecord>> {
        self.backend.load_balance_cache()
    }

    pub fn save_balance_cache(
        &self,
        cache: &HashMap<u64, CachedBalanceRecord>,
    ) -> anyhow::Result<()> {
        self.backend.save_balance_cache(cache)
    }

    pub fn persist_dispatch_config(
        &self,
        dispatch: &PersistedDispatchConfig,
    ) -> anyhow::Result<()> {
        self.backend.persist_dispatch_config(dispatch)
    }
}

fn run_blocking_state_op<R, F>(operation: F) -> R
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => match handle.runtime_flavor() {
            RuntimeFlavor::MultiThread => tokio::task::block_in_place(operation),
            RuntimeFlavor::CurrentThread => std::thread::spawn(operation)
                .join()
                .expect("state blocking operation thread panicked"),
            _ => std::thread::spawn(operation)
                .join()
                .expect("state blocking operation thread panicked"),
        },
        Err(_) => operation(),
    }
}

#[derive(Debug)]
struct FileStateBackend {
    config_path: Option<PathBuf>,
    credentials_path: Option<PathBuf>,
}

impl FileStateBackend {
    fn cache_dir(&self) -> Option<PathBuf> {
        self.credentials_path
            .as_ref()
            .and_then(|path| path.parent().map(|dir| dir.to_path_buf()))
    }

    fn stats_path(&self) -> Option<PathBuf> {
        self.cache_dir().map(|dir| dir.join(STATS_FILE_NAME))
    }

    fn balance_cache_path(&self) -> Option<PathBuf> {
        self.cache_dir().map(|dir| dir.join(BALANCE_CACHE_FILE_NAME))
    }

    fn write_bytes(path: &Path, bytes: Vec<u8>) -> anyhow::Result<()> {
        std::fs::write(path, &bytes)
            .with_context(|| format!("写入状态文件失败: {}", path.display()))?;
        Ok(())
    }
}

impl StateBackend for FileStateBackend {
    fn is_external(&self) -> bool {
        false
    }

    fn load_credentials(&self) -> anyhow::Result<PersistedCredentials> {
        let path = match &self.credentials_path {
            Some(path) => path,
            None => return Ok(PersistedCredentials::empty(false)),
        };

        let credentials = CredentialsConfig::load(path)
            .with_context(|| format!("加载凭据文件失败: {}", path.display()))?;
        let is_multiple_format = credentials.is_multiple();
        Ok(PersistedCredentials {
            credentials: credentials.into_sorted_credentials(),
            is_multiple_format,
        })
    }

    fn load_dispatch_config(&self) -> anyhow::Result<Option<PersistedDispatchConfig>> {
        Ok(None)
    }

    fn persist_credentials(
        &self,
        credentials: &[KiroCredentials],
        is_multiple_format: bool,
    ) -> anyhow::Result<bool> {
        if !is_multiple_format {
            return Ok(false);
        }

        let path = match &self.credentials_path {
            Some(path) => path,
            None => return Ok(false),
        };

        let json = serde_json::to_vec_pretty(credentials).context("序列化凭据失败")?;
        Self::write_bytes(path, json)?;
        Ok(true)
    }

    fn load_stats(&self) -> anyhow::Result<HashMap<String, StatsEntryRecord>> {
        let path = match self.stats_path() {
            Some(path) => path,
            None => return Ok(HashMap::new()),
        };

        let content = match std::fs::read_to_string(&path) {
            Ok(content) => content,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(HashMap::new()),
            Err(err) => {
                return Err(err).with_context(|| format!("读取统计缓存失败: {}", path.display()))
            }
        };

        serde_json::from_str(&content)
            .with_context(|| format!("解析统计缓存失败: {}", path.display()))
    }

    fn save_stats(&self, stats: &HashMap<String, StatsEntryRecord>) -> anyhow::Result<()> {
        let path = match self.stats_path() {
            Some(path) => path,
            None => return Ok(()),
        };

        let json = serde_json::to_vec_pretty(stats).context("序列化统计缓存失败")?;
        Self::write_bytes(&path, json)
    }

    fn load_balance_cache(&self) -> anyhow::Result<HashMap<u64, CachedBalanceRecord>> {
        let path = match self.balance_cache_path() {
            Some(path) => path,
            None => return Ok(HashMap::new()),
        };

        let content = match std::fs::read_to_string(&path) {
            Ok(content) => content,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(HashMap::new()),
            Err(err) => {
                return Err(err).with_context(|| format!("读取余额缓存失败: {}", path.display()))
            }
        };

        let cache: HashMap<String, CachedBalanceRecord> = serde_json::from_str(&content)
            .with_context(|| format!("解析余额缓存失败: {}", path.display()))?;
        Ok(cache
            .into_iter()
            .filter_map(|(key, value)| key.parse::<u64>().ok().map(|id| (id, value)))
            .collect())
    }

    fn save_balance_cache(&self, cache: &HashMap<u64, CachedBalanceRecord>) -> anyhow::Result<()> {
        let path = match self.balance_cache_path() {
            Some(path) => path,
            None => return Ok(()),
        };

        let serializable: HashMap<String, &CachedBalanceRecord> =
            cache.iter().map(|(key, value)| (key.to_string(), value)).collect();
        let json = serde_json::to_vec_pretty(&serializable).context("序列化余额缓存失败")?;
        Self::write_bytes(&path, json)
    }

    fn persist_dispatch_config(&self, dispatch: &PersistedDispatchConfig) -> anyhow::Result<()> {
        let config_path = match &self.config_path {
            Some(path) => path,
            None => {
                tracing::warn!(
                    "配置文件路径未知，调度配置仅在当前进程生效: mode={}, queueMaxSize={}, queueMaxWaitMs={}, rateLimitCooldownMs={}, defaultMaxConcurrency={:?}, rateLimitBucketCapacity={}, rateLimitRefillPerSecond={}, rateLimitRefillMinPerSecond={}, rateLimitRefillRecoveryStepPerSuccess={}, rateLimitRefillBackoffFactor={}",
                    dispatch.mode,
                    dispatch.queue_max_size,
                    dispatch.queue_max_wait_ms,
                    dispatch.rate_limit_cooldown_ms,
                    dispatch.default_max_concurrency,
                    dispatch.rate_limit_bucket_capacity,
                    dispatch.rate_limit_refill_per_second,
                    dispatch.rate_limit_refill_min_per_second,
                    dispatch.rate_limit_refill_recovery_step_per_success,
                    dispatch.rate_limit_refill_backoff_factor
                );
                return Ok(());
            }
        };

        let mut config = Config::load(config_path)
            .with_context(|| format!("重新加载配置失败: {}", config_path.display()))?;
        dispatch.apply_to_config(&mut config);
        config
            .save()
            .with_context(|| format!("持久化调度配置失败: {}", config_path.display()))?;
        Ok(())
    }
}

struct PostgresStateBackend {
    client: Arc<Mutex<Client>>,
}

impl std::fmt::Debug for PostgresStateBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresStateBackend").finish_non_exhaustive()
    }
}

impl PostgresStateBackend {
    fn connect(postgres_url: &str) -> anyhow::Result<Self> {
        let postgres_url = postgres_url.to_string();
        let client = run_blocking_state_op(move || -> anyhow::Result<Client> {
            let mut client =
                Client::connect(&postgres_url, NoTls).context("连接 PostgreSQL 状态存储失败")?;
            client
                .batch_execute(
                    r#"
                    CREATE TABLE IF NOT EXISTS kiro_state_store (
                        namespace TEXT NOT NULL,
                        key TEXT NOT NULL,
                        value TEXT NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        PRIMARY KEY (namespace, key)
                    )
                    "#,
                )
                .context("初始化 PostgreSQL 状态存储表失败")?;
            Ok(client)
        })?;
        Ok(Self {
            client: Arc::new(Mutex::new(client)),
        })
    }

    fn load_json<T: DeserializeOwned + Send + 'static>(
        &self,
        key: &str,
        label: &str,
    ) -> anyhow::Result<Option<T>> {
        let client = Arc::clone(&self.client);
        let key = key.to_string();
        let label = label.to_string();

        run_blocking_state_op(move || {
            let mut client = client.lock();
            let row = client
                .query_opt(
                    "SELECT value FROM kiro_state_store WHERE namespace = $1 AND key = $2",
                    &[&POSTGRES_NAMESPACE, &key],
                )
                .with_context(|| format!("从 PostgreSQL 读取{label}失败"))?;

            row.map(|row| {
                let payload: String = row.get(0);
                serde_json::from_str(&payload)
                    .with_context(|| format!("解析 PostgreSQL {label}失败"))
            })
            .transpose()
        })
    }

    fn save_json<T: Serialize>(&self, key: &str, value: &T, label: &str) -> anyhow::Result<()> {
        let payload = serde_json::to_string(value).with_context(|| format!("序列化{label}失败"))?;
        let client = Arc::clone(&self.client);
        let key = key.to_string();
        let label = label.to_string();

        run_blocking_state_op(move || {
            let mut client = client.lock();
            client
                .execute(
                    r#"
                    INSERT INTO kiro_state_store (namespace, key, value, updated_at)
                    VALUES ($1, $2, $3, NOW())
                    ON CONFLICT (namespace, key)
                    DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
                    "#,
                    &[&POSTGRES_NAMESPACE, &key, &payload],
                )
                .with_context(|| format!("保存{label}到 PostgreSQL 失败"))?;
            Ok(())
        })
    }
}

impl StateBackend for PostgresStateBackend {
    fn is_external(&self) -> bool {
        true
    }

    fn load_credentials(&self) -> anyhow::Result<PersistedCredentials> {
        Ok(PersistedCredentials {
            credentials: self
                .load_json(POSTGRES_CREDENTIALS_KEY, "凭据列表")?
                .unwrap_or_default(),
            is_multiple_format: true,
        })
    }

    fn load_dispatch_config(&self) -> anyhow::Result<Option<PersistedDispatchConfig>> {
        self.load_json(POSTGRES_DISPATCH_CONFIG_KEY, "调度配置")
    }

    fn persist_credentials(
        &self,
        credentials: &[KiroCredentials],
        _is_multiple_format: bool,
    ) -> anyhow::Result<bool> {
        self.save_json(POSTGRES_CREDENTIALS_KEY, &credentials, "凭据列表")?;
        Ok(true)
    }

    fn load_stats(&self) -> anyhow::Result<HashMap<String, StatsEntryRecord>> {
        Ok(self.load_json(POSTGRES_STATS_KEY, "统计缓存")?.unwrap_or_default())
    }

    fn save_stats(&self, stats: &HashMap<String, StatsEntryRecord>) -> anyhow::Result<()> {
        self.save_json(POSTGRES_STATS_KEY, stats, "统计缓存")
    }

    fn load_balance_cache(&self) -> anyhow::Result<HashMap<u64, CachedBalanceRecord>> {
        Ok(self
            .load_json(POSTGRES_BALANCE_CACHE_KEY, "余额缓存")?
            .unwrap_or_default())
    }

    fn save_balance_cache(&self, cache: &HashMap<u64, CachedBalanceRecord>) -> anyhow::Result<()> {
        self.save_json(POSTGRES_BALANCE_CACHE_KEY, cache, "余额缓存")
    }

    fn persist_dispatch_config(&self, dispatch: &PersistedDispatchConfig) -> anyhow::Result<()> {
        self.save_json(POSTGRES_DISPATCH_CONFIG_KEY, dispatch, "调度配置")
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use uuid::Uuid;

    use super::*;

    fn temp_test_dir(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("kiro-state-{name}-{}", Uuid::new_v4()));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn file_state_store_round_trips_credentials_stats_and_balance_cache() {
        let dir = temp_test_dir("roundtrip");
        let credentials_path = dir.join("credentials.json");
        let store = StateStore::file(None, Some(credentials_path.clone()));

        let credentials = vec![KiroCredentials {
            id: Some(7),
            refresh_token: Some("refresh-token".to_string()),
            disabled: true,
            ..KiroCredentials::default()
        }];
        assert!(store.persist_credentials(&credentials, true).unwrap());

        let boot = store.load_credentials().unwrap();
        assert!(boot.is_multiple_format);
        assert_eq!(boot.credentials.len(), 1);
        assert_eq!(boot.credentials[0].id, Some(7));
        assert!(boot.credentials[0].disabled);

        let mut stats = HashMap::new();
        stats.insert(
            "7".to_string(),
            StatsEntryRecord {
                success_count: 11,
                last_used_at: Some("2026-04-15T00:00:00Z".to_string()),
            },
        );
        store.save_stats(&stats).unwrap();
        assert_eq!(store.load_stats().unwrap().get("7").unwrap().success_count, 11);

        let mut balance_cache = HashMap::new();
        balance_cache.insert(
            7,
            CachedBalanceRecord {
                cached_at: 1234.0,
                data: BalanceResponse {
                    id: 7,
                    subscription_title: Some("KIRO PRO+".to_string()),
                    current_usage: 1.0,
                    usage_limit: 10.0,
                    remaining: 9.0,
                    usage_percentage: 10.0,
                    next_reset_at: Some(5678.0),
                },
            },
        );
        store.save_balance_cache(&balance_cache).unwrap();
        assert_eq!(
            store.load_balance_cache().unwrap().get(&7).unwrap().data.subscription_title,
            Some("KIRO PRO+".to_string())
        );

        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn file_state_store_skips_single_credentials_format_writeback() {
        let dir = temp_test_dir("single-format");
        let credentials_path = dir.join("credentials.json");
        let store = StateStore::file(None, Some(credentials_path.clone()));

        let persisted = store
            .persist_credentials(&[KiroCredentials::default()], false)
            .unwrap();

        assert!(!persisted);
        assert!(!credentials_path.exists());

        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn file_state_store_persists_dispatch_config_to_config_file() {
        let dir = temp_test_dir("dispatch");
        let config_path = dir.join("config.json");
        let store = StateStore::file(Some(config_path.clone()), None);

        let dispatch = PersistedDispatchConfig {
            mode: "balanced".to_string(),
            queue_max_size: 16,
            queue_max_wait_ms: 2000,
            rate_limit_cooldown_ms: 5000,
            default_max_concurrency: Some(4),
            rate_limit_bucket_capacity: 6.0,
            rate_limit_refill_per_second: 1.5,
            rate_limit_refill_min_per_second: 0.4,
            rate_limit_refill_recovery_step_per_success: 0.2,
            rate_limit_refill_backoff_factor: 0.7,
        };

        store.persist_dispatch_config(&dispatch).unwrap();

        let persisted = Config::load(&config_path).unwrap();
        assert_eq!(PersistedDispatchConfig::from_config(&persisted), dispatch);

        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn state_store_from_config_rejects_postgres_without_url() {
        let mut config = Config::default();
        config.state_backend = StateBackendKind::Postgres;

        let err = StateStore::from_config(&config, None).unwrap_err().to_string();

        assert!(err.contains("statePostgresUrl"));
    }
}
