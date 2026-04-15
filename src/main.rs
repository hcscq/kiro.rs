mod admin;
mod admin_ui;
mod anthropic;
mod common;
mod http_client;
mod kiro;
mod model;
mod state;
pub mod token;

use std::sync::Arc;

use clap::Parser;
use kiro::model::credentials::{CredentialsConfig, KiroCredentials};
use kiro::provider::KiroProvider;
use kiro::token_manager::MultiTokenManager;
use model::arg::Args;
use model::config::Config;
use state::{PersistedCredentials, PersistedDispatchConfig, RuntimeCoordinationStatus, StateStore};

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
            heartbeat_interval_secs = config.state_redis_heartbeat_interval_secs,
            leader_lease_ttl_secs = config.state_redis_leader_lease_ttl_secs,
            "已启用 Redis 运行时协调"
        );
        log_runtime_coordination_status(&initial_status, None);

        let heartbeat_interval = state_store
            .runtime_coordination_interval()
            .expect("runtime coordination interval should exist");
        let state_store = state_store.clone();
        tokio::spawn(async move {
            let mut previous_status = initial_status;
            loop {
                tokio::time::sleep(heartbeat_interval).await;
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

    // 获取第一个凭据用于日志显示
    let first_credentials = credentials_list.first().cloned().unwrap_or_default();
    tracing::debug!("主凭证: {:?}", first_credentials);

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

    if state_store.is_external() {
        let sync_interval = std::time::Duration::from_secs(config.state_redis_heartbeat_interval_secs);
        tracing::info!(
            sync_interval_secs = sync_interval.as_secs(),
            "已启用外部状态热同步（凭据、调度配置与统计信息）"
        );

        let token_manager = Arc::clone(&token_manager);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(sync_interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            ticker.tick().await;

            loop {
                ticker.tick().await;
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

    let app = if let Some(admin_key) = &config.admin_api_key {
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
                tokio::spawn(async move {
                    let mut ticker = tokio::time::interval(sync_interval);
                    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    ticker.tick().await;

                    loop {
                        ticker.tick().await;
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
    axum::serve(listener, app).await.unwrap();
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
