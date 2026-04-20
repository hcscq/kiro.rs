use std::{env, sync::Arc};

use axum::{
    http::StatusCode,
    response::{IntoResponse, Json, Response},
};
use parking_lot::Mutex;

use super::{extractor::MAX_ANTHROPIC_BODY_SIZE_BYTES, types::ErrorResponse};

const BODY_BUDGET_ENV: &str = "KIRO_ANTHROPIC_INFLIGHT_BODY_BUDGET_MB";

pub(crate) struct InflightBodyBudget {
    limit_bytes: u64,
    used_bytes: Mutex<u64>,
}

impl InflightBodyBudget {
    pub(crate) fn new(limit_bytes: u64) -> Option<Arc<Self>> {
        if limit_bytes == 0 {
            return None;
        }

        Some(Arc::new(Self {
            limit_bytes,
            used_bytes: Mutex::new(0),
        }))
    }

    pub(crate) fn limit_bytes(&self) -> u64 {
        self.limit_bytes
    }

    pub(crate) fn used_bytes(&self) -> u64 {
        *self.used_bytes.lock()
    }

    pub(crate) fn try_acquire(
        self: &Arc<Self>,
        reservation_bytes: u64,
    ) -> Option<InflightBodyBudgetPermit> {
        if reservation_bytes == 0 {
            return Some(InflightBodyBudgetPermit {
                budget: Arc::clone(self),
                reservation_bytes: 0,
            });
        }

        let mut used_bytes = self.used_bytes.lock();
        if reservation_bytes > self.limit_bytes
            || *used_bytes > self.limit_bytes.saturating_sub(reservation_bytes)
        {
            return None;
        }

        *used_bytes += reservation_bytes;
        drop(used_bytes);

        Some(InflightBodyBudgetPermit {
            budget: Arc::clone(self),
            reservation_bytes,
        })
    }
}

pub(crate) struct InflightBodyBudgetPermit {
    budget: Arc<InflightBodyBudget>,
    reservation_bytes: u64,
}

impl Drop for InflightBodyBudgetPermit {
    fn drop(&mut self) {
        if self.reservation_bytes == 0 {
            return;
        }

        let mut used_bytes = self.budget.used_bytes.lock();
        *used_bytes = used_bytes.saturating_sub(self.reservation_bytes);
    }
}

pub(crate) fn load_budget_from_env() -> Option<Arc<InflightBodyBudget>> {
    let raw = env::var(BODY_BUDGET_ENV).ok()?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    let limit_mb = match trimmed.parse::<u64>() {
        Ok(limit_mb) => limit_mb,
        Err(err) => {
            tracing::warn!(
                env = BODY_BUDGET_ENV,
                value = trimmed,
                error = %err,
                "无法解析 Anthropic in-flight body budget 配置，已禁用"
            );
            return None;
        }
    };

    let limit_bytes = limit_mb.saturating_mul(1024 * 1024);
    let budget = InflightBodyBudget::new(limit_bytes);
    if let Some(budget) = &budget {
        tracing::info!(
            env = BODY_BUDGET_ENV,
            limit_mb,
            limit_bytes = budget.limit_bytes(),
            "Anthropic in-flight request-body budget 已启用"
        );
    }
    budget
}

pub(crate) fn request_body_budget_exhausted_response() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(ErrorResponse::new(
            "service_unavailable",
            "Anthropic request buffering budget is exhausted. Retry later or reduce concurrent large uploads.",
        )),
    )
        .into_response()
}

pub(crate) fn request_body_budget_reservation_bytes(
    path: &str,
    content_length_header: Option<u64>,
) -> Option<u64> {
    let normalized_path = path.trim_end_matches('/');
    let multiplier = match normalized_path {
        "/messages" | "/v1/messages" | "/cc/v1/messages" => 2,
        "/messages/count_tokens" | "/v1/messages/count_tokens" | "/cc/v1/messages/count_tokens" => {
            1
        }
        _ => return None,
    };

    let base_bytes = content_length_header.unwrap_or(MAX_ANTHROPIC_BODY_SIZE_BYTES as u64);
    Some(base_bytes.saturating_mul(multiplier))
}
