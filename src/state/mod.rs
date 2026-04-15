use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use serde::{Deserialize, Serialize};

use crate::admin::types::BalanceResponse;
use crate::kiro::model::credentials::KiroCredentials;
use crate::model::config::Config;

const STATS_FILE_NAME: &str = "kiro_stats.json";
const BALANCE_CACHE_FILE_NAME: &str = "kiro_balance_cache.json";

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

#[derive(Debug, Clone)]
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

#[derive(Clone)]
pub struct StateStore {
    backend: Arc<dyn StateBackend>,
}

trait StateBackend: Send + Sync {
    fn cache_dir(&self) -> Option<PathBuf>;
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
    pub fn file(config_path: Option<PathBuf>, credentials_path: Option<PathBuf>) -> Self {
        Self {
            backend: Arc::new(FileStateBackend {
                config_path,
                credentials_path,
            }),
        }
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

#[derive(Debug)]
struct FileStateBackend {
    config_path: Option<PathBuf>,
    credentials_path: Option<PathBuf>,
}

impl FileStateBackend {
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
    fn cache_dir(&self) -> Option<PathBuf> {
        self.credentials_path
            .as_ref()
            .and_then(|path| path.parent().map(|dir| dir.to_path_buf()))
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
        config.load_balancing_mode = dispatch.mode.clone();
        config.queue_max_size = dispatch.queue_max_size;
        config.queue_max_wait_ms = dispatch.queue_max_wait_ms;
        config.rate_limit_cooldown_ms = dispatch.rate_limit_cooldown_ms;
        config.default_max_concurrency = dispatch.default_max_concurrency;
        config.rate_limit_bucket_capacity = dispatch.rate_limit_bucket_capacity;
        config.rate_limit_refill_per_second = dispatch.rate_limit_refill_per_second;
        config.rate_limit_refill_min_per_second = dispatch.rate_limit_refill_min_per_second;
        config.rate_limit_refill_recovery_step_per_success =
            dispatch.rate_limit_refill_recovery_step_per_success;
        config.rate_limit_refill_backoff_factor = dispatch.rate_limit_refill_backoff_factor;
        config
            .save()
            .with_context(|| format!("持久化调度配置失败: {}", config_path.display()))?;
        Ok(())
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

        let persisted_credentials: Vec<KiroCredentials> =
            serde_json::from_str(&fs::read_to_string(&credentials_path).unwrap()).unwrap();
        assert_eq!(persisted_credentials.len(), 1);
        assert_eq!(persisted_credentials[0].id, Some(7));
        assert!(persisted_credentials[0].disabled);

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

        store
            .persist_dispatch_config(&PersistedDispatchConfig {
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
            })
            .unwrap();

        let persisted = Config::load(&config_path).unwrap();
        assert_eq!(persisted.load_balancing_mode, "balanced");
        assert_eq!(persisted.queue_max_size, 16);
        assert_eq!(persisted.queue_max_wait_ms, 2000);
        assert_eq!(persisted.rate_limit_cooldown_ms, 5000);
        assert_eq!(persisted.default_max_concurrency, Some(4));
        assert_eq!(persisted.rate_limit_bucket_capacity, 6.0);
        assert_eq!(persisted.rate_limit_refill_per_second, 1.5);
        assert_eq!(persisted.rate_limit_refill_min_per_second, 0.4);
        assert_eq!(persisted.rate_limit_refill_recovery_step_per_success, 0.2);
        assert_eq!(persisted.rate_limit_refill_backoff_factor, 0.7);

        fs::remove_dir_all(&dir).unwrap();
    }
}
