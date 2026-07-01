use std::fs::{self, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use chrono::Utc;
use futures::stream::{FuturesUnordered, StreamExt};
use serde::Serialize;
use uuid::Uuid;

use crate::anthropic::converter::{ConversionLogContext, convert_request_with_context};
use crate::anthropic::probe::UpstreamProbe;
use crate::anthropic::types::{Message, MessagesRequest};
use crate::http_client::ProxyConfig;
use crate::kiro::model::credentials::{CredentialsConfig, KiroCredentials};
use crate::kiro::model::requests::kiro::KiroRequest;
use crate::kiro::provider::{KiroProvider, ProbeCredentialSelector, ProbeResponse};
use crate::kiro::token_manager::MultiTokenManager;
use crate::model::arg::QuotaProbeArgs;
use crate::model::config::{Config, StateBackendKind};
use crate::state::{PersistedCredentials, StateStore};
use crate::token;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProbeMode {
    Fixed,
    RampRpm,
    RampTpm,
}

impl ProbeMode {
    fn parse(value: &str) -> anyhow::Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "fixed" => Ok(Self::Fixed),
            "ramp-rpm" | "ramp_rpm" | "ramp" => Ok(Self::RampRpm),
            "ramp-tpm" | "ramp_tpm" => Ok(Self::RampTpm),
            other => anyhow::bail!("unsupported quota-probe mode: {other}"),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Fixed => "fixed",
            Self::RampRpm => "ramp-rpm",
            Self::RampTpm => "ramp-tpm",
        }
    }
}

#[derive(Debug, Clone)]
struct ProbeStep {
    index: usize,
    target_rpm: f64,
    requested_tpm: Option<f64>,
    target_tpm: f64,
    requested_input_tokens: u32,
    estimated_input_tokens: u64,
}

#[derive(Debug, Clone)]
struct ProbeCredential {
    id: u64,
    auth_account_type: Option<String>,
    account_type: Option<String>,
    resolved_account_type: Option<String>,
    priority: u32,
    email: Option<String>,
    user_id: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProbeObservation {
    timestamp: String,
    probe_run_id: String,
    mode: String,
    step_index: usize,
    target_rpm: f64,
    request_index: u32,
    credential_id: u64,
    auth_account_type: Option<String>,
    account_type: Option<String>,
    resolved_account_type: Option<String>,
    model: String,
    effective_model: Option<String>,
    region: String,
    requested_tpm: Option<f64>,
    target_tpm: f64,
    configured_input_tokens: u32,
    estimated_input_tokens: u64,
    max_tokens: i32,
    status_code: Option<u16>,
    retry_after: Option<String>,
    content_type: Option<String>,
    success: bool,
    rate_limited: bool,
    latency_ms: u128,
    request_body_bytes: usize,
    response_body_bytes: usize,
    response_excerpt: Option<String>,
    error: Option<String>,
}

pub async fn run_quota_probe(
    config_path: &str,
    credentials_path: &str,
    args: &QuotaProbeArgs,
) -> anyhow::Result<()> {
    let mode = ProbeMode::parse(&args.mode)?;
    validate_args(args, mode)?;

    let mut config = Config::load(config_path).context("load config for quota probe")?;
    let source_credentials_path = PathBuf::from(credentials_path);
    let source_state_store =
        StateStore::from_config(&config, Some(source_credentials_path.clone()))
            .context("initialize source state store")?;

    if let Some(dispatch) = source_state_store
        .load_dispatch_config()
        .context("load source dispatch config")?
    {
        dispatch.apply_to_config(&mut config);
    }

    let persisted_credentials = load_credentials_snapshot(&source_state_store, credentials_path)
        .context("load credentials")?;
    let credentials = persisted_credentials.credentials;
    let candidates = select_probe_credentials(&credentials, args);
    if candidates.is_empty() {
        anyhow::bail!("no credentials matched quota-probe filters");
    }

    let steps = build_steps(args, mode);
    if args.dry_run {
        print_dry_run(&candidates, &steps, args, mode);
        return Ok(());
    }

    let mut probe_config = config.clone();
    configure_probe_runtime(&mut probe_config)?;
    let temp_credentials_path = temp_probe_credentials_path();
    let probe_credentials = credentials_for_probe(&credentials, &candidates);
    fs::write(
        &temp_credentials_path,
        serde_json::to_string_pretty(&probe_credentials)?,
    )
    .with_context(|| {
        format!(
            "write temporary quota-probe credentials to {}",
            temp_credentials_path.display()
        )
    })?;

    let proxy_config = build_proxy_config(&probe_config);
    let token_manager = Arc::new(MultiTokenManager::new(
        probe_config.clone(),
        probe_credentials,
        proxy_config.clone(),
        Some(temp_credentials_path.clone()),
        true,
    )?);
    let provider = Arc::new(KiroProvider::with_proxy(token_manager, proxy_config));
    let mut writer = observation_writer(args.output.as_deref())?;
    let run_id = Uuid::new_v4().to_string();

    tracing::info!(
        run_id,
        mode = mode.as_str(),
        credentials = candidates.len(),
        steps = steps.len(),
        model = %args.model,
        "starting quota probe"
    );

    for credential in &candidates {
        for step in &steps {
            let should_stop = run_probe_step(
                provider.clone(),
                &mut writer,
                &run_id,
                mode,
                step,
                credential,
                args,
            )
            .await?;
            writer.flush()?;
            if should_stop {
                tracing::warn!(
                    credential_id = credential.id,
                    step_index = step.index,
                    target_rpm = step.target_rpm,
                    "quota probe stopped current credential after 429"
                );
                break;
            }
        }
    }

    let _ = fs::remove_file(&temp_credentials_path);
    Ok(())
}

fn validate_args(args: &QuotaProbeArgs, mode: ProbeMode) -> anyhow::Result<()> {
    if args.requests_per_step == 0 {
        anyhow::bail!("requests-per-step must be > 0");
    }
    if !args.rpm.is_finite() || args.rpm <= 0.0 {
        anyhow::bail!("rpm must be a positive finite number");
    }
    match mode {
        ProbeMode::Fixed => {}
        ProbeMode::RampRpm => {
            let max_rpm = args
                .max_rpm
                .ok_or_else(|| anyhow::anyhow!("ramp-rpm mode requires --max-rpm"))?;
            if !max_rpm.is_finite() || max_rpm < args.rpm {
                anyhow::bail!("max-rpm must be >= rpm");
            }
            if !args.rpm_step.is_finite() || args.rpm_step <= 0.0 {
                anyhow::bail!("rpm-step must be a positive finite number");
            }
        }
        ProbeMode::RampTpm => {
            let max_tpm = args
                .max_tpm
                .ok_or_else(|| anyhow::anyhow!("ramp-tpm mode requires --max-tpm"))?;
            if !args.tpm.is_finite() || args.tpm <= 0.0 {
                anyhow::bail!("tpm must be a positive finite number");
            }
            if !max_tpm.is_finite() || max_tpm < args.tpm {
                anyhow::bail!("max-tpm must be >= tpm");
            }
            if !args.tpm_step.is_finite() || args.tpm_step <= 0.0 {
                anyhow::bail!("tpm-step must be a positive finite number");
            }
        }
    }
    if args.concurrency == 0 {
        anyhow::bail!("concurrency must be > 0");
    }
    if args.max_tokens <= 0 {
        anyhow::bail!("max-tokens must be > 0");
    }
    if args.timeout_seconds == 0 {
        anyhow::bail!("timeout-seconds must be > 0");
    }
    Ok(())
}

fn load_credentials_snapshot(
    state_store: &StateStore,
    credentials_path: &str,
) -> anyhow::Result<PersistedCredentials> {
    let persisted = state_store.load_credentials()?;
    if !persisted.credentials.is_empty() {
        return Ok(persisted);
    }

    let config = CredentialsConfig::load(credentials_path)?;
    let is_multiple_format = config.is_multiple();
    Ok(PersistedCredentials {
        credentials: config.into_sorted_credentials(),
        is_multiple_format,
    })
}

fn select_probe_credentials(
    credentials: &[KiroCredentials],
    args: &QuotaProbeArgs,
) -> Vec<ProbeCredential> {
    let auth_filter = args
        .auth_account_type
        .as_deref()
        .map(normalize_filter_value);
    let account_filter = args.account_type.as_deref().map(normalize_filter_value);
    let mut selected = Vec::new();

    for credential in credentials {
        let Some(id) = credential.id else {
            continue;
        };
        if args.credential_id.is_some_and(|target| target != id) {
            continue;
        }
        let auth_account_type = credential.detected_auth_account_type();
        if auth_filter.as_deref().is_some_and(|target| {
            auth_account_type.as_deref().map(normalize_filter_value) != Some(target.to_string())
        }) {
            continue;
        }
        let resolved_account_type = credential.resolved_account_type();
        if account_filter.as_deref().is_some_and(|target| {
            credential
                .account_type
                .as_deref()
                .or(resolved_account_type.as_deref())
                .map(normalize_filter_value)
                != Some(target.to_string())
        }) {
            continue;
        }
        if credential.disabled {
            continue;
        }
        selected.push(ProbeCredential {
            id,
            auth_account_type,
            account_type: credential.account_type.clone(),
            resolved_account_type,
            priority: credential.priority,
            email: credential.email.clone(),
            user_id: credential.user_id.clone(),
        });
    }

    selected.sort_by_key(|credential| (credential.priority, credential.id));
    selected
}

fn normalize_filter_value(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn build_steps(args: &QuotaProbeArgs, mode: ProbeMode) -> Vec<ProbeStep> {
    match mode {
        ProbeMode::Fixed => {
            let estimated_input_tokens = estimated_tokens_for_requested(args.input_tokens);
            vec![ProbeStep {
                index: 0,
                target_rpm: args.rpm,
                requested_tpm: None,
                target_tpm: args.rpm * estimated_input_tokens as f64,
                requested_input_tokens: args.input_tokens,
                estimated_input_tokens,
            }]
        }
        ProbeMode::RampRpm => {
            let mut steps = Vec::new();
            let mut rpm = args.rpm;
            let max_rpm = args.max_rpm.unwrap_or(args.rpm);
            let estimated_input_tokens = estimated_tokens_for_requested(args.input_tokens);
            while rpm <= max_rpm + f64::EPSILON {
                steps.push(ProbeStep {
                    index: steps.len(),
                    target_rpm: rpm,
                    requested_tpm: None,
                    target_tpm: rpm * estimated_input_tokens as f64,
                    requested_input_tokens: args.input_tokens,
                    estimated_input_tokens,
                });
                rpm += args.rpm_step;
            }
            steps
        }
        ProbeMode::RampTpm => {
            let mut steps = Vec::new();
            let mut tpm = args.tpm;
            let max_tpm = args.max_tpm.unwrap_or(args.tpm);
            while tpm <= max_tpm + f64::EPSILON {
                let requested_input_tokens = requested_input_tokens_for_tpm(tpm, args.rpm);
                let estimated_input_tokens = estimated_tokens_for_requested(requested_input_tokens);
                steps.push(ProbeStep {
                    index: steps.len(),
                    target_rpm: args.rpm,
                    requested_tpm: Some(tpm),
                    target_tpm: args.rpm * estimated_input_tokens as f64,
                    requested_input_tokens,
                    estimated_input_tokens,
                });
                tpm += args.tpm_step;
            }
            steps
        }
    }
}

fn requested_input_tokens_for_tpm(target_tpm: f64, rpm: f64) -> u32 {
    let requested = (target_tpm / rpm.max(0.001)).ceil();
    requested.clamp(1.0, u32::MAX as f64) as u32
}

fn estimated_tokens_for_requested(requested_input_tokens: u32) -> u64 {
    token::count_tokens(&build_prompt(requested_input_tokens))
}

fn print_dry_run(
    credentials: &[ProbeCredential],
    steps: &[ProbeStep],
    args: &QuotaProbeArgs,
    mode: ProbeMode,
) {
    println!(
        "quota-probe dry-run: mode={} model={} requests_per_step={} concurrency={}",
        mode.as_str(),
        args.model,
        args.requests_per_step,
        args.concurrency
    );
    println!("credentials:");
    for credential in credentials {
        println!(
            "  id={} auth={:?} account={:?} resolved={:?} email={:?} user_id={:?}",
            credential.id,
            credential.auth_account_type,
            credential.account_type,
            credential.resolved_account_type,
            credential.email,
            credential.user_id
        );
    }
    println!("steps:");
    for step in steps {
        println!(
            "  index={} target_rpm={} requested_tpm={:?} target_tpm={} requested_input_tokens={} estimated_input_tokens={}",
            step.index,
            step.target_rpm,
            step.requested_tpm,
            step.target_tpm,
            step.requested_input_tokens,
            step.estimated_input_tokens
        );
    }
}

fn configure_probe_runtime(config: &mut Config) -> anyhow::Result<()> {
    config.state_backend = StateBackendKind::File;
    config.state_redis_url = None;
    config.state_postgres_url = None;
    config.load_balancing_mode = "priority".to_string();
    config.default_max_concurrency = Some(10_000);
    config.queue_max_size = 0;
    config.queue_max_wait_ms = 0;
    config.rate_limit_cooldown_enabled = false;
    config.rate_limit_bucket_capacity = 0.0;
    config.rate_limit_refill_per_second = 0.0;
    config.account_type_dispatch_policies.clear();
    config.auth_account_type_dispatch_policies.clear();
    config
        .auth_account_type_account_type_dispatch_policies
        .clear();
    config.validate()
}

fn temp_probe_credentials_path() -> PathBuf {
    std::env::temp_dir().join(format!(
        "kiro-rs-quota-probe-{}-credentials.json",
        Uuid::new_v4()
    ))
}

fn credentials_for_probe(
    credentials: &[KiroCredentials],
    selected: &[ProbeCredential],
) -> Vec<KiroCredentials> {
    let selected_ids: std::collections::HashSet<u64> =
        selected.iter().map(|credential| credential.id).collect();
    credentials
        .iter()
        .filter(|credential| {
            credential.id.is_some_and(|id| selected_ids.contains(&id)) && !credential.disabled
        })
        .cloned()
        .collect()
}

fn build_proxy_config(config: &Config) -> Option<ProxyConfig> {
    config.proxy_url.as_ref().map(|url| {
        let mut proxy = ProxyConfig::new(url);
        if let (Some(username), Some(password)) = (&config.proxy_username, &config.proxy_password) {
            proxy = proxy.with_auth(username, password);
        }
        proxy
    })
}

fn observation_writer(output: Option<&str>) -> anyhow::Result<Box<dyn Write + Send>> {
    match output {
        Some(path) => {
            if let Some(parent) = Path::new(path).parent()
                && !parent.as_os_str().is_empty()
            {
                fs::create_dir_all(parent)?;
            }
            let file = OpenOptions::new().create(true).append(true).open(path)?;
            Ok(Box::new(BufWriter::new(file)))
        }
        None => Ok(Box::new(BufWriter::new(io::stdout()))),
    }
}

fn build_kiro_request_body(
    args: &QuotaProbeArgs,
    requested_input_tokens: u32,
) -> anyhow::Result<String> {
    let payload = MessagesRequest {
        model: args.model.clone(),
        max_tokens: args.max_tokens,
        messages: vec![Message {
            role: "user".to_string(),
            content: serde_json::Value::String(build_prompt(requested_input_tokens)),
        }],
        stream: false,
        system: None,
        tools: None,
        tool_choice: None,
        thinking: None,
        output_config: None,
        metadata: None,
    };
    let body_bytes = serde_json::to_vec(&payload)?.len();
    let conversion = convert_request_with_context(
        &payload,
        UpstreamProbe::default(),
        ConversionLogContext::for_request(
            Some("quota-probe"),
            "quota_probe",
            &args.model,
            Some(body_bytes),
            payload.messages.len(),
        ),
    )?;
    let kiro_request = KiroRequest {
        conversation_state: conversion.conversation_state,
        additional_model_request_fields: conversion.additional_model_request_fields,
        profile_arn: None,
    };
    Ok(serde_json::to_string(&kiro_request)?)
}

fn build_prompt(input_tokens: u32) -> String {
    let target_chars = input_tokens.max(1).saturating_mul(4) as usize;
    let seed = "quota calibration probe. respond with a short acknowledgement. ";
    let mut prompt = String::with_capacity(target_chars + seed.len());
    while prompt.len() < target_chars {
        prompt.push_str(seed);
    }
    prompt.truncate(target_chars);
    prompt
}

async fn run_probe_step(
    provider: Arc<KiroProvider>,
    writer: &mut Box<dyn Write + Send>,
    run_id: &str,
    mode: ProbeMode,
    step: &ProbeStep,
    credential: &ProbeCredential,
    args: &QuotaProbeArgs,
) -> anyhow::Result<bool> {
    let interval = request_interval(step.target_rpm);
    let timeout_duration = Duration::from_secs(args.timeout_seconds);
    let request_body = build_kiro_request_body(args, step.requested_input_tokens)?;
    let mut in_flight = FuturesUnordered::new();
    let mut next_request_index = 0u32;
    let mut stop_after_429 = false;

    while next_request_index < args.requests_per_step || !in_flight.is_empty() {
        while next_request_index < args.requests_per_step
            && in_flight.len() < args.concurrency
            && !stop_after_429
        {
            let request_index = next_request_index;
            next_request_index += 1;
            let provider = provider.clone();
            let request_body = request_body.to_string();
            let credential_id = credential.id;
            let omit_agent_mode_header = false;
            in_flight.push(async move {
                let started_at = Instant::now();
                let response = provider
                    .call_api_probe_once(
                        &request_body,
                        ProbeCredentialSelector { credential_id },
                        timeout_duration,
                        omit_agent_mode_header,
                    )
                    .await;
                (request_index, started_at, response)
            });
            tokio::time::sleep(interval).await;
        }

        if let Some((request_index, _started_at, response)) = in_flight.next().await {
            let observation = build_observation(
                run_id,
                mode,
                step,
                request_index,
                credential,
                args,
                response,
            );
            if observation.rate_limited && args.stop_on_429 {
                stop_after_429 = true;
            }
            serde_json::to_writer(&mut *writer, &observation)?;
            writer.write_all(b"\n")?;
        }
    }

    Ok(stop_after_429)
}

fn request_interval(target_rpm: f64) -> Duration {
    let requests_per_second = (target_rpm / 60.0).max(0.001);
    let interval_seconds = 1.0 / requests_per_second;
    Duration::from_secs_f64(interval_seconds.max(0.001))
}

fn build_observation(
    run_id: &str,
    mode: ProbeMode,
    step: &ProbeStep,
    request_index: u32,
    credential: &ProbeCredential,
    args: &QuotaProbeArgs,
    response: ProbeResponse,
) -> ProbeObservation {
    let status_code = response.status_code;
    let success = status_code.is_some_and(|status| (200..300).contains(&status));
    let response_excerpt = response_excerpt(&response, args.error_excerpt_chars);
    ProbeObservation {
        timestamp: Utc::now().to_rfc3339(),
        probe_run_id: run_id.to_string(),
        mode: mode.as_str().to_string(),
        step_index: step.index,
        target_rpm: step.target_rpm,
        request_index,
        credential_id: response.credential_id,
        auth_account_type: response
            .auth_account_type
            .or_else(|| credential.auth_account_type.clone()),
        account_type: response
            .account_type
            .or_else(|| credential.account_type.clone()),
        resolved_account_type: response
            .resolved_account_type
            .or_else(|| credential.resolved_account_type.clone()),
        model: response.model.unwrap_or_else(|| args.model.clone()),
        effective_model: response.effective_model,
        region: response.region,
        requested_tpm: step.requested_tpm,
        target_tpm: step.target_tpm,
        configured_input_tokens: step.requested_input_tokens,
        estimated_input_tokens: step.estimated_input_tokens,
        max_tokens: args.max_tokens,
        status_code,
        retry_after: response.retry_after,
        content_type: response.content_type,
        success,
        rate_limited: status_code == Some(429),
        latency_ms: response.latency_ms,
        request_body_bytes: response.request_body_bytes,
        response_body_bytes: response.response_body_bytes,
        response_excerpt,
        error: response.error,
    }
}

fn response_excerpt(response: &ProbeResponse, max_chars: usize) -> Option<String> {
    if response.body.is_empty() || max_chars == 0 {
        return None;
    }
    if response.error.is_none()
        && response
            .status_code
            .is_some_and(|status| (200..300).contains(&status))
    {
        return None;
    }
    let text = String::from_utf8_lossy(&response.body);
    Some(summarize_text(&text, max_chars))
}

fn summarize_text(value: &str, max_chars: usize) -> String {
    let collapsed = value.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.chars().count() <= max_chars {
        return collapsed;
    }
    let mut out = collapsed.chars().take(max_chars).collect::<String>();
    out.push_str("...");
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn quota_probe_args() -> QuotaProbeArgs {
        QuotaProbeArgs {
            mode: "fixed".to_string(),
            model: "claude-sonnet-4.5".to_string(),
            credential_id: None,
            auth_account_type: None,
            account_type: None,
            requests_per_step: 10,
            rpm: 6.0,
            max_rpm: None,
            rpm_step: 6.0,
            tpm: 6000.0,
            max_tpm: None,
            tpm_step: 6000.0,
            concurrency: 1,
            input_tokens: 256,
            max_tokens: 64,
            timeout_seconds: 180,
            output: None,
            error_excerpt_chars: 512,
            stop_on_429: false,
            dry_run: false,
        }
    }

    #[test]
    fn request_interval_uses_global_target_rpm() {
        assert_eq!(request_interval(60.0), Duration::from_secs(1));
    }

    #[test]
    fn ramp_tpm_steps_scale_input_tokens_at_fixed_rpm() {
        let args = QuotaProbeArgs {
            mode: "ramp-tpm".to_string(),
            max_tpm: Some(12_000.0),
            ..quota_probe_args()
        };

        let steps = build_steps(&args, ProbeMode::RampTpm);

        assert_eq!(steps.len(), 2);
        assert_eq!(steps[0].target_rpm, 6.0);
        assert_eq!(steps[0].requested_tpm, Some(6000.0));
        assert_eq!(steps[0].requested_input_tokens, 1000);
        assert_eq!(steps[0].estimated_input_tokens, 1000);
        assert_eq!(steps[0].target_tpm, 6000.0);
        assert_eq!(steps[1].requested_tpm, Some(12000.0));
        assert_eq!(steps[1].requested_input_tokens, 2000);
        assert_eq!(steps[1].estimated_input_tokens, 2000);
        assert_eq!(steps[1].target_tpm, 12000.0);
    }
}
