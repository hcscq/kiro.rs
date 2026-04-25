mod admin;
mod admin_ui;
mod anthropic;
mod common;
mod http_client;
mod kiro;
mod model;
mod state;
pub mod token;

use std::{
    collections::HashMap,
    fs,
    future::Future,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use anyhow::Context;
use axum::{Json, Router, http::StatusCode, routing::get};
use chrono::Utc;
use clap::Parser;
use kiro::model::credentials::{CredentialsConfig, KiroCredentials};
use kiro::provider::KiroProvider;
use kiro::token_manager::MultiTokenManager;
use model::arg::{Args, Command as CliCommand, ExportFileStateArgs};
use model::config::{Config, StateBackendKind};
use parking_lot::Mutex;
use serde::Serialize;
use serde_json::json;
use state::{
    CachedBalanceRecord, PersistedCredentials, PersistedDispatchConfig, RuntimeCoordinationStatus,
    StateStore,
};
use tokio::{
    sync::watch,
    task::JoinHandle,
    time::{Duration, sleep},
};

const READINESS_DRAIN_FILE: &str = "/tmp/kiro-rs-drain";
const SHUTDOWN_DRAIN_DELAY: Duration = Duration::from_secs(5);
const BACKGROUND_TASK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Serialize)]
struct FileRollbackExportFiles {
    credentials: String,
    dispatch_config: String,
    rollback_config: String,
    stats: String,
    balance_cache: String,
    manifest: String,
}

#[derive(Debug, Serialize)]
struct FileRollbackExportManifest {
    exported_at: String,
    source_config_path: String,
    source_credentials_seed_path: String,
    source_state_backend: String,
    source_postgres_url_configured: bool,
    source_redis_url_configured: bool,
    credentials_count: usize,
    stats_count: usize,
    balance_cache_entries: usize,
    balance_cache_source: String,
    dispatch_mode: String,
    output_dir: String,
    files: FileRollbackExportFiles,
    recommended_start_command: String,
    omitted_runtime_state: Vec<String>,
    notes: Vec<String>,
}

#[derive(Default)]
struct RuntimeHealth {
    draining: AtomicBool,
}

impl RuntimeHealth {
    fn mark_draining(&self) {
        self.draining.store(true, Ordering::SeqCst);
    }

    fn is_draining(&self) -> bool {
        self.draining.load(Ordering::SeqCst)
    }
}

struct BackgroundTask {
    name: &'static str,
    handle: JoinHandle<()>,
}

#[derive(Clone)]
struct BackgroundTasks {
    shutdown_tx: watch::Sender<bool>,
    tasks: Arc<Mutex<Vec<BackgroundTask>>>,
}

impl BackgroundTasks {
    fn new() -> Self {
        let (shutdown_tx, _) = watch::channel(false);
        Self {
            shutdown_tx,
            tasks: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn subscribe(&self) -> watch::Receiver<bool> {
        self.shutdown_tx.subscribe()
    }

    fn spawn<F>(&self, name: &'static str, task: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let handle = tokio::spawn(task);
        self.tasks.lock().push(BackgroundTask { name, handle });
    }

    async fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
        let tasks = {
            let mut tasks = self.tasks.lock();
            std::mem::take(&mut *tasks)
        };

        for mut task in tasks {
            match tokio::time::timeout(BACKGROUND_TASK_SHUTDOWN_TIMEOUT, &mut task.handle).await {
                Ok(Ok(())) => {}
                Ok(Err(err)) if err.is_cancelled() => {}
                Ok(Err(err)) => {
                    tracing::warn!(task = task.name, "后台任务退出异常: {}", err);
                }
                Err(_) => {
                    tracing::warn!(
                        task = task.name,
                        timeout_secs = BACKGROUND_TASK_SHUTDOWN_TIMEOUT.as_secs(),
                        "等待后台任务退出超时，准备强制中止"
                    );
                    task.handle.abort();
                    match task.handle.await {
                        Ok(()) => {}
                        Err(err) if err.is_cancelled() => {}
                        Err(err) => {
                            tracing::warn!(
                                task = task.name,
                                "强制中止后台任务后仍返回错误: {}",
                                err
                            );
                        }
                    }
                }
            }
        }
    }
}

fn shutdown_requested(shutdown: &watch::Receiver<bool>) -> bool {
    *shutdown.borrow()
}

async fn sleep_until_shutdown_or_elapsed(
    duration: Duration,
    shutdown: &mut watch::Receiver<bool>,
) -> bool {
    tokio::select! {
        _ = sleep(duration) => shutdown_requested(shutdown),
        changed = shutdown.changed() => {
            if let Err(err) = changed {
                tracing::debug!("后台任务 shutdown 通道已关闭: {}", err);
            }
            true
        }
    }
}

async fn tick_until_shutdown_or_elapsed(
    ticker: &mut tokio::time::Interval,
    shutdown: &mut watch::Receiver<bool>,
) -> bool {
    tokio::select! {
        _ = ticker.tick() => shutdown_requested(shutdown),
        changed = shutdown.changed() => {
            if let Err(err) = changed {
                tracing::debug!("后台任务 shutdown 通道已关闭: {}", err);
            }
            true
        }
    }
}

fn export_target_path(output_dir: &Path, file_name: &str) -> PathBuf {
    output_dir.join(file_name)
}

fn ensure_export_target_writable(path: &Path, overwrite: bool) -> anyhow::Result<()> {
    if path.exists() && !overwrite {
        anyhow::bail!(
            "导出目标已存在: {}。如需覆盖请追加 --overwrite",
            path.display()
        );
    }
    Ok(())
}

fn write_json_pretty<T: Serialize>(path: &Path, value: &T, overwrite: bool) -> anyhow::Result<()> {
    ensure_export_target_writable(path, overwrite)?;
    let bytes = serde_json::to_vec_pretty(value)
        .map_err(anyhow::Error::from)
        .with_context(|| format!("序列化导出文件失败: {}", path.display()))?;
    fs::write(path, bytes).with_context(|| format!("写入导出文件失败: {}", path.display()))?;
    Ok(())
}

fn normalize_exported_credentials(mut credentials: Vec<KiroCredentials>) -> Vec<KiroCredentials> {
    credentials.sort_by_key(|credential| (credential.priority, credential.id.unwrap_or(u64::MAX)));
    for credential in &mut credentials {
        credential.canonicalize_auth_method();
    }
    credentials
}

fn build_file_rollback_config(
    source_config: &Config,
    dispatch: &PersistedDispatchConfig,
) -> Config {
    let mut rollback_config = source_config.clone();
    dispatch.apply_to_config(&mut rollback_config);
    rollback_config.state_backend = StateBackendKind::File;
    rollback_config.state_postgres_url = None;
    rollback_config.state_redis_url = None;
    rollback_config
}

fn config_for_primary_state_export(source_config: &Config) -> Config {
    let mut config = source_config.clone();
    config.state_redis_url = None;
    config
}

fn load_balance_cache_for_export(
    source_config: &Config,
    credentials_path: &Path,
    primary_state_store: &StateStore,
) -> anyhow::Result<(HashMap<u64, CachedBalanceRecord>, String)> {
    if source_config.state_redis_url.is_some() {
        match StateStore::from_config(source_config, Some(credentials_path.to_path_buf())) {
            Ok(state_store) => match state_store.load_balance_cache() {
                Ok(cache) => return Ok((cache, "redis".to_string())),
                Err(err) => {
                    tracing::warn!("从 Redis 导出余额缓存失败，将回退到主状态后端: {}", err);
                }
            },
            Err(err) => {
                tracing::warn!("初始化 Redis 余额缓存导出失败，将回退到主状态后端: {}", err);
            }
        }
    }

    Ok((
        primary_state_store.load_balance_cache()?,
        "primary-backend".to_string(),
    ))
}

fn display_path(path: &Path) -> String {
    path.canonicalize()
        .unwrap_or_else(|_| path.to_path_buf())
        .display()
        .to_string()
}

fn export_file_state(
    config_path: &str,
    credentials_path: &str,
    export_args: &ExportFileStateArgs,
) -> anyhow::Result<()> {
    let source_config =
        Config::load(config_path).with_context(|| format!("加载配置失败: {}", config_path))?;
    if source_config.state_backend != StateBackendKind::Postgres {
        anyhow::bail!("export-file-state 仅支持 stateBackend=postgres 的配置");
    }

    let primary_config = config_for_primary_state_export(&source_config);
    let credentials_path_buf = PathBuf::from(credentials_path);
    let primary_state_store =
        StateStore::from_config(&primary_config, Some(credentials_path_buf.clone()))
            .context("初始化 PostgreSQL 状态存储失败")?;

    let persisted_credentials = primary_state_store.load_credentials()?;
    if persisted_credentials.credentials.is_empty() {
        anyhow::bail!("外部状态后端中没有凭据，无法导出 file backend 回滚文件");
    }
    let credentials = normalize_exported_credentials(persisted_credentials.credentials);

    let dispatch = primary_state_store
        .load_dispatch_config()?
        .ok_or_else(|| anyhow::anyhow!("外部状态后端中没有调度配置，无法导出回滚配置"))?;

    let stats = primary_state_store.load_stats()?;
    let (balance_cache, balance_cache_source) =
        load_balance_cache_for_export(&source_config, &credentials_path_buf, &primary_state_store)?;

    let output_dir = PathBuf::from(&export_args.output_dir);
    if output_dir.exists() && !output_dir.is_dir() {
        anyhow::bail!("导出目录不是文件夹: {}", output_dir.display());
    }
    fs::create_dir_all(&output_dir)
        .with_context(|| format!("创建导出目录失败: {}", output_dir.display()))?;

    let credentials_output_path = export_target_path(&output_dir, "credentials.json");
    let dispatch_output_path = export_target_path(&output_dir, "dispatch-config.json");
    let rollback_config_path = export_target_path(&output_dir, "config.rollback.json");
    let stats_output_path = export_target_path(&output_dir, "kiro_stats.json");
    let balance_cache_output_path = export_target_path(&output_dir, "kiro_balance_cache.json");
    let manifest_output_path = export_target_path(&output_dir, "rollback-manifest.json");

    for path in [
        &credentials_output_path,
        &dispatch_output_path,
        &rollback_config_path,
        &stats_output_path,
        &balance_cache_output_path,
        &manifest_output_path,
    ] {
        ensure_export_target_writable(path, export_args.overwrite)?;
    }

    let rollback_config = build_file_rollback_config(&source_config, &dispatch);
    write_json_pretty(
        &rollback_config_path,
        &rollback_config,
        export_args.overwrite,
    )?;

    let file_state_store = StateStore::file(
        Some(rollback_config_path.clone()),
        Some(credentials_output_path.clone()),
    );
    if !file_state_store.persist_credentials(&credentials, true)? {
        anyhow::bail!("写入导出凭据失败");
    }
    file_state_store.save_stats(&stats)?;
    file_state_store.save_balance_cache(&balance_cache)?;
    write_json_pretty(&dispatch_output_path, &dispatch, export_args.overwrite)?;

    let rollback_config_display = display_path(&rollback_config_path);
    let credentials_output_display = display_path(&credentials_output_path);

    let files = FileRollbackExportFiles {
        credentials: credentials_output_display.clone(),
        dispatch_config: display_path(&dispatch_output_path),
        rollback_config: rollback_config_display.clone(),
        stats: display_path(&stats_output_path),
        balance_cache: display_path(&balance_cache_output_path),
        manifest: display_path(&manifest_output_path),
    };

    let manifest = FileRollbackExportManifest {
        exported_at: Utc::now().to_rfc3339(),
        source_config_path: display_path(Path::new(config_path)),
        source_credentials_seed_path: credentials_path_buf.display().to_string(),
        source_state_backend: "postgres".to_string(),
        source_postgres_url_configured: source_config.state_postgres_url.is_some(),
        source_redis_url_configured: source_config.state_redis_url.is_some(),
        credentials_count: credentials.len(),
        stats_count: stats.len(),
        balance_cache_entries: balance_cache.len(),
        balance_cache_source,
        dispatch_mode: dispatch.mode.clone(),
        output_dir: display_path(&output_dir),
        files,
        recommended_start_command: format!(
            "kiro-rs --config {} --credentials {}",
            rollback_config_display,
            credentials_output_display
        ),
        omitted_runtime_state: vec![
            "dispatch leases".to_string(),
            "rate limit cooldown runtime windows".to_string(),
            "shared token bucket runtime tokens".to_string(),
        ],
        notes: vec![
            "该导出面向 external -> file 回滚，已将 stateBackend 切回 file，并清空 statePostgresUrl/stateRedisUrl。".to_string(),
            "调度热态属于瞬时运行态，不应作为 file backend 持久状态导出。".to_string(),
            "如 source config 配置了 Redis，但导出时 Redis 不可达，余额缓存会自动回退到主状态后端视图。".to_string(),
        ],
    };
    write_json_pretty(&manifest_output_path, &manifest, export_args.overwrite)?;

    tracing::info!(
        credentials = credentials.len(),
        stats = stats.len(),
        balance_cache_entries = balance_cache.len(),
        output_dir = %display_path(&output_dir),
        "已导出 file backend 回滚文件"
    );

    Ok(())
}

fn drain_file_present() -> bool {
    Path::new(READINESS_DRAIN_FILE).exists()
}

fn clear_drain_signal() -> std::io::Result<()> {
    match std::fs::remove_file(READINESS_DRAIN_FILE) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

async fn live_handler() -> (StatusCode, Json<serde_json::Value>) {
    (StatusCode::OK, Json(json!({ "status": "ok" })))
}

async fn ready_handler(
    health: Arc<RuntimeHealth>,
    token_manager: Arc<MultiTokenManager>,
) -> (StatusCode, Json<serde_json::Value>) {
    let snapshot = token_manager.snapshot();
    let draining = health.is_draining() || drain_file_present();
    let ready = !draining && snapshot.total > 0;
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    let reason = if draining {
        "draining"
    } else if snapshot.total == 0 {
        "no_credentials"
    } else {
        "not_ready"
    };

    (
        status,
        Json(json!({
            "status": if ready { "ok" } else { reason },
            "credentials_total": snapshot.total,
            "credentials_available": snapshot.available,
            "credentials_dispatchable": snapshot.dispatchable,
        })),
    )
}

async fn shutdown_signal(
    health: Arc<RuntimeHealth>,
    state_store: StateStore,
    background_tasks: BackgroundTasks,
) {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install terminate handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    tracing::info!("shutdown signal received, marking readiness false");
    health.mark_draining();
    if let Err(err) = std::fs::write(READINESS_DRAIN_FILE, b"draining\n") {
        tracing::warn!("写入 readiness drain 标记失败: {}", err);
    }
    background_tasks.shutdown().await;
    match state_store.runtime_coordination_release() {
        Ok(Some(status)) => {
            if let Some(leader_id) = status.leader_id {
                tracing::info!(
                    instance_id = %status.instance_id,
                    leader_id = %leader_id,
                    "shutdown drain: 当前实例不是 leader，已清理本地运行时心跳"
                );
            } else {
                tracing::info!(
                    instance_id = %status.instance_id,
                    "shutdown drain: 已释放 Redis leader 租约并清理本地运行时心跳"
                );
            }
        }
        Ok(None) => {}
        Err(err) => tracing::warn!("shutdown drain: 释放运行时协调状态失败: {}", err),
    }
    sleep(SHUTDOWN_DRAIN_DELAY).await;
}

#[tokio::main]
async fn main() {
    // 解析命令行参数
    let args = Args::parse();

    // 初始化日志
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    if let Some(CliCommand::ExportFileState(command)) = &args.command {
        let config_path = args
            .config
            .clone()
            .unwrap_or_else(|| Config::default_config_path().to_string());
        let credentials_path = args
            .credentials
            .clone()
            .unwrap_or_else(|| KiroCredentials::default_credentials_path().to_string());

        export_file_state(&config_path, &credentials_path, command).unwrap_or_else(|err| {
            tracing::error!("导出 file backend 回滚文件失败: {}", err);
            std::process::exit(1);
        });
        return;
    }

    // 加载配置
    let config_path = args
        .config
        .unwrap_or_else(|| Config::default_config_path().to_string());
    let mut config = Config::load(&config_path).unwrap_or_else(|e| {
        tracing::error!("加载配置失败: {}", e);
        std::process::exit(1);
    });

    // 解析凭据文件路径。file backend 直接读取该文件；external backend 仅将其用作首次导入种子。
    let credentials_path = args
        .credentials
        .unwrap_or_else(|| KiroCredentials::default_credentials_path().to_string());
    let credentials_path_buf: std::path::PathBuf = credentials_path.clone().into();

    let state_store = StateStore::from_config(&config, Some(credentials_path_buf.clone()))
        .unwrap_or_else(|e| {
            tracing::error!("初始化状态存储失败: {}", e);
            std::process::exit(1);
        });

    if let Some(dispatch) = state_store.load_dispatch_config().unwrap_or_else(|e| {
        tracing::error!("加载外部调度配置失败: {}", e);
        std::process::exit(1);
    }) {
        dispatch.apply_to_config(&mut config);
        tracing::info!("已从状态后端加载调度配置");
    } else if state_store.is_external() {
        let dispatch = PersistedDispatchConfig::from_config(&config);
        state_store
            .persist_dispatch_config(&dispatch)
            .unwrap_or_else(|e| {
                tracing::error!("将初始调度配置写入外部状态后端失败: {}", e);
                std::process::exit(1);
            });
        tracing::info!("外部状态后端尚无调度配置，已使用本地配置初始化");
    }

    let mut persisted_credentials = state_store.load_credentials().unwrap_or_else(|e| {
        tracing::error!("加载凭据失败: {}", e);
        std::process::exit(1);
    });

    if state_store.is_external() && persisted_credentials.credentials.is_empty() {
        let credentials_config = CredentialsConfig::load(&credentials_path).unwrap_or_else(|e| {
            tracing::error!("从本地凭据文件导入种子数据失败: {}", e);
            std::process::exit(1);
        });
        let is_multiple_format = credentials_config.is_multiple();
        let credentials = credentials_config.into_sorted_credentials();

        if !credentials.is_empty() {
            state_store
                .persist_credentials(&credentials, is_multiple_format)
                .unwrap_or_else(|e| {
                    tracing::error!("将本地凭据导入外部状态后端失败: {}", e);
                    std::process::exit(1);
                });
            tracing::info!(
                "外部状态后端尚无凭据，已从本地文件导入 {} 个凭据",
                credentials.len()
            );
        }

        persisted_credentials = PersistedCredentials {
            credentials,
            is_multiple_format,
        };
    }

    let is_multiple_format = persisted_credentials.is_multiple_format;
    let credentials_list = persisted_credentials.credentials;
    tracing::info!("已加载 {} 个凭据配置", credentials_list.len());
    let runtime_health = Arc::new(RuntimeHealth::default());
    let background_tasks = BackgroundTasks::new();
    let advertise_http_base_url = config.resolved_advertise_http_base_url();

    if state_store.runtime_coordination_enabled() {
        let initial_status = state_store
            .runtime_coordination_tick()
            .unwrap_or_else(|e| {
                tracing::error!("初始化 Redis 运行时协调失败: {}", e);
                std::process::exit(1);
            })
            .expect("runtime coordination tick should return status when enabled");
        tracing::info!(
            instance_id = %initial_status.instance_id,
            advertise_http_base_url = ?advertise_http_base_url,
            heartbeat_interval_secs = config.state_redis_heartbeat_interval_secs,
            leader_lease_ttl_secs = config.state_redis_leader_lease_ttl_secs,
            "已启用 Redis 运行时协调"
        );
        if advertise_http_base_url.is_none() {
            tracing::warn!("当前实例未解析到可路由的对等地址，follower 无法代理 Admin 写请求");
        }
        log_runtime_coordination_status(&initial_status, None);

        let heartbeat_interval = state_store
            .runtime_coordination_interval()
            .expect("runtime coordination interval should exist");
        let state_store = state_store.clone();
        let mut shutdown = background_tasks.subscribe();
        background_tasks.spawn("runtime_coordination_heartbeat", async move {
            let mut previous_status = initial_status;
            loop {
                if sleep_until_shutdown_or_elapsed(heartbeat_interval, &mut shutdown).await {
                    break;
                }
                match state_store.runtime_coordination_tick() {
                    Ok(Some(status)) => {
                        log_runtime_coordination_status(&status, Some(&previous_status));
                        previous_status = status;
                    }
                    Ok(None) => break,
                    Err(err) => {
                        tracing::error!(
                            instance_id = %previous_status.instance_id,
                            "Redis 运行时协调续租失败: {}",
                            err
                        );
                    }
                }
            }
        });
    }

    if let Some(first_credentials) = credentials_list.first() {
        tracing::debug!(
            credential_id = first_credentials.id.unwrap_or_default(),
            auth_method = first_credentials
                .auth_method
                .as_deref()
                .unwrap_or("unknown"),
            has_access_token = first_credentials.access_token.is_some(),
            has_refresh_token = first_credentials.refresh_token.is_some(),
            has_profile_arn = first_credentials.profile_arn.is_some(),
            "主凭证摘要"
        );
    }

    // 获取 API Key
    let api_key = config.api_key.clone().unwrap_or_else(|| {
        tracing::error!("配置文件中未设置 apiKey");
        std::process::exit(1);
    });

    // 构建代理配置
    let proxy_config = config.proxy_url.as_ref().map(|url| {
        let mut proxy = http_client::ProxyConfig::new(url);
        if let (Some(username), Some(password)) = (&config.proxy_username, &config.proxy_password) {
            proxy = proxy.with_auth(username, password);
        }
        proxy
    });

    if proxy_config.is_some() {
        tracing::info!("已配置 HTTP 代理: {}", config.proxy_url.as_ref().unwrap());
    }

    // 创建 MultiTokenManager 和 KiroProvider
    let token_manager = MultiTokenManager::new(
        config.clone(),
        credentials_list,
        proxy_config.clone(),
        Some(credentials_path_buf),
        is_multiple_format,
    )
    .unwrap_or_else(|e| {
        tracing::error!("创建 Token 管理器失败: {}", e);
        std::process::exit(1);
    });
    let token_manager = Arc::new(token_manager);
    let kiro_provider = KiroProvider::with_proxy(token_manager.clone(), proxy_config.clone());

    if let Err(err) = clear_drain_signal() {
        tracing::warn!("清理 readiness drain 标记失败: {}", err);
    }

    if state_store.is_external() {
        let sync_interval =
            std::time::Duration::from_secs(config.state_redis_heartbeat_interval_secs);
        tracing::info!(
            sync_interval_secs = sync_interval.as_secs(),
            "已启用外部状态定时热同步兜底（凭据、调度配置与统计信息）"
        );

        let token_manager = Arc::clone(&token_manager);
        let mut shutdown = background_tasks.subscribe();
        background_tasks.spawn("external_state_sync", async move {
            let mut ticker = tokio::time::interval(sync_interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            ticker.tick().await;

            loop {
                if tick_until_shutdown_or_elapsed(&mut ticker, &mut shutdown).await {
                    break;
                }
                match token_manager.sync_from_state() {
                    Ok(report) => {
                        if report.dispatch_config_reloaded {
                            tracing::info!("外部状态热同步: 已应用最新调度配置");
                        }
                        if report.stats_reloaded {
                            tracing::info!("外部状态热同步: 已应用最新统计信息");
                        }
                    }
                    Err(err) => {
                        tracing::error!("外部状态热同步失败: {}", err);
                    }
                }
            }
        });
    }

    // 初始化 count_tokens 配置
    token::init_config(token::CountTokensConfig {
        api_url: config.count_tokens_api_url.clone(),
        api_key: config.count_tokens_api_key.clone(),
        auth_type: config.count_tokens_auth_type.clone(),
        proxy: proxy_config,
        tls_backend: config.tls_backend,
    });

    // 构建 Anthropic API 路由（profile_arn 由 provider 层根据实际凭据动态注入）
    let anthropic_app = anthropic::create_router_with_provider(&api_key, Some(kiro_provider));

    // 构建 Admin API 路由（如果配置了非空的 admin_api_key）
    // 安全检查：空字符串被视为未配置，防止空 key 绕过认证
    let admin_key_valid = config
        .admin_api_key
        .as_ref()
        .map(|k| !k.trim().is_empty())
        .unwrap_or(false);

    let base_app = if let Some(admin_key) = &config.admin_api_key {
        if admin_key.trim().is_empty() {
            tracing::warn!("admin_api_key 配置为空，Admin API 未启用");
            anthropic_app
        } else {
            let admin_service = admin::AdminService::new(token_manager.clone());
            let admin_state = admin::AdminState::new(admin_key, admin_service);
            if state_store.is_external() {
                let sync_interval =
                    std::time::Duration::from_secs(config.state_redis_heartbeat_interval_secs);
                let admin_service = admin_state.service.clone();
                let mut shutdown = background_tasks.subscribe();
                background_tasks.spawn("admin_balance_cache_sync", async move {
                    let mut ticker = tokio::time::interval(sync_interval);
                    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    ticker.tick().await;

                    loop {
                        if tick_until_shutdown_or_elapsed(&mut ticker, &mut shutdown).await {
                            break;
                        }
                        if let Err(err) = admin_service.sync_balance_cache_from_state() {
                            tracing::error!("Admin 余额缓存热同步失败: {}", err);
                        }
                    }
                });
            }
            let admin_app = admin::create_admin_router(admin_state);

            // 创建 Admin UI 路由
            let admin_ui_app = admin_ui::create_admin_ui_router();

            tracing::info!("Admin API 已启用");
            tracing::info!("Admin UI 已启用: /admin");
            anthropic_app
                .nest("/api/admin", admin_app)
                .nest("/admin", admin_ui_app)
        }
    } else {
        anthropic_app
    };

    let health_app = Router::new()
        .route("/health", get(live_handler))
        .route("/healthz", get(live_handler))
        .route("/livez", get(live_handler))
        .route(
            "/readyz",
            get({
                let runtime_health = runtime_health.clone();
                let token_manager = token_manager.clone();
                move || {
                    let runtime_health = runtime_health.clone();
                    let token_manager = token_manager.clone();
                    async move { ready_handler(runtime_health, token_manager).await }
                }
            }),
        );
    let app = health_app.merge(base_app);

    // 启动服务器
    let addr = format!("{}:{}", config.host, config.port);
    tracing::info!("启动 Anthropic API 端点: {}", addr);
    tracing::info!("API Key: {}***", &api_key[..(api_key.len() / 2)]);
    tracing::info!("可用 API:");
    tracing::info!("  GET  /v1/models");
    tracing::info!("  POST /v1/messages");
    tracing::info!("  POST /v1/messages/count_tokens");
    if admin_key_valid {
        tracing::info!("Admin API:");
        tracing::info!("  GET  /api/admin/credentials");
        tracing::info!("  POST /api/admin/credentials/:index/disabled");
        tracing::info!("  POST /api/admin/credentials/:index/priority");
        tracing::info!("  POST /api/admin/credentials/:index/reset");
        tracing::info!("  GET  /api/admin/credentials/:index/balance");
        tracing::info!("Admin UI:");
        tracing::info!("  GET  /admin");
    }

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(
            runtime_health,
            state_store,
            background_tasks,
        ))
        .await
        .unwrap();
}

fn log_runtime_coordination_status(
    status: &RuntimeCoordinationStatus,
    previous: Option<&RuntimeCoordinationStatus>,
) {
    if previous.is_some_and(|current| {
        current.is_leader == status.is_leader && current.leader_id == status.leader_id
    }) {
        return;
    }

    if status.is_leader {
        tracing::info!(
            instance_id = %status.instance_id,
            "Redis 运行时协调: 当前实例持有 leader 租约"
        );
        return;
    }

    if let Some(leader_id) = &status.leader_id {
        tracing::info!(
            instance_id = %status.instance_id,
            leader_id = %leader_id,
            "Redis 运行时协调: 当前实例处于 follower"
        );
        return;
    }

    tracing::warn!(
        instance_id = %status.instance_id,
        "Redis 运行时协调: 当前未观察到 leader"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::config::RequestWeightingConfig;

    #[test]
    fn build_file_rollback_config_switches_to_file_backend() {
        let mut config = Config::default();
        config.state_backend = StateBackendKind::Postgres;
        config.state_postgres_url = Some("postgres://postgres:postgres@localhost:5432/kiro".into());
        config.state_redis_url = Some("redis://127.0.0.1:6379/0".into());
        config.load_balancing_mode = "priority".into();
        config.queue_max_size = 0;
        config.queue_max_wait_ms = 0;
        config.rate_limit_cooldown_ms = 2000;

        let dispatch = PersistedDispatchConfig {
            mode: "balanced".into(),
            queue_max_size: 16,
            queue_max_wait_ms: 1500,
            rate_limit_cooldown_ms: 5000,
            model_cooldown_enabled: true,
            default_max_concurrency: Some(3),
            rate_limit_bucket_capacity: 5.0,
            rate_limit_refill_per_second: 1.5,
            rate_limit_refill_min_per_second: 0.3,
            rate_limit_refill_recovery_step_per_success: 0.2,
            rate_limit_refill_backoff_factor: 0.4,
            request_weighting: RequestWeightingConfig {
                max_weight: 4.0,
                tools_bonus: 1.0,
                ..RequestWeightingConfig::default()
            },
            account_type_policies: std::collections::BTreeMap::new(),
            account_type_dispatch_policies: std::collections::BTreeMap::new(),
        };

        let rollback = build_file_rollback_config(&config, &dispatch);

        assert_eq!(rollback.state_backend, StateBackendKind::File);
        assert!(rollback.state_postgres_url.is_none());
        assert!(rollback.state_redis_url.is_none());
        assert_eq!(rollback.load_balancing_mode, "balanced");
        assert_eq!(rollback.queue_max_size, 16);
        assert_eq!(rollback.queue_max_wait_ms, 1500);
        assert_eq!(rollback.rate_limit_cooldown_ms, 5000);
        assert_eq!(rollback.default_max_concurrency, Some(3));
        assert_eq!(rollback.rate_limit_bucket_capacity, 5.0);
        assert_eq!(rollback.rate_limit_refill_per_second, 1.5);
        assert_eq!(rollback.rate_limit_refill_min_per_second, 0.3);
        assert_eq!(rollback.rate_limit_refill_recovery_step_per_success, 0.2);
        assert_eq!(rollback.rate_limit_refill_backoff_factor, 0.4);
        assert_eq!(rollback.request_weighting.max_weight, 4.0);
        assert_eq!(rollback.request_weighting.tools_bonus, 1.0);
    }
}
