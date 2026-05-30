use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use serde_json::Value;
use tokio::{sync::Notify, task, time::timeout};

use crate::model::config::ConversionRuntimeConfig;

use super::{
    converter::{ConversionError, ConversionResult, convert_request_with_probe},
    probe::UpstreamProbe,
    types::MessagesRequest,
};

const BODY_512_KIB: usize = 512 * 1024;
const BODY_4_MIB: usize = 4 * 1024 * 1024;
const BODY_16_MIB: usize = 16 * 1024 * 1024;
const IMAGE_WEIGHT_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ConversionWorkload {
    pub weight: usize,
    pub body_bytes: usize,
    pub message_count: usize,
    pub image_count: usize,
    pub document_count: usize,
    pub source_data_bytes: usize,
}

impl ConversionWorkload {
    pub(crate) fn estimate(
        payload: &MessagesRequest,
        body_bytes: usize,
        max_request_weight: usize,
    ) -> Self {
        let mut stats = WorkloadContentStats::default();
        for message in &payload.messages {
            inspect_content_value(&message.content, &mut stats);
        }

        let mut weight = 1usize;
        if body_bytes >= BODY_512_KIB {
            weight += 1;
        }
        if body_bytes >= BODY_4_MIB {
            weight += 1;
        }
        if body_bytes >= BODY_16_MIB {
            weight += 2;
        }
        if stats.image_count > 0 {
            weight += stats.image_count.div_ceil(4).min(4);
        }
        if stats.document_count > 0 {
            weight += stats.document_count.div_ceil(2).min(4);
        }
        if stats.source_data_bytes > 0 {
            weight += stats.source_data_bytes.div_ceil(IMAGE_WEIGHT_BYTES).min(4);
        }
        if payload.messages.len() >= 100 {
            weight += 1;
        }
        if payload.messages.len() >= 500 {
            weight += 1;
        }

        Self {
            weight: weight.clamp(1, max_request_weight.max(1)),
            body_bytes,
            message_count: payload.messages.len(),
            image_count: stats.image_count,
            document_count: stats.document_count,
            source_data_bytes: stats.source_data_bytes,
        }
    }
}

#[derive(Debug, Default)]
struct WorkloadContentStats {
    image_count: usize,
    document_count: usize,
    source_data_bytes: usize,
}

fn inspect_content_value(value: &Value, stats: &mut WorkloadContentStats) {
    match value {
        Value::Array(items) => {
            for item in items {
                inspect_content_value(item, stats);
            }
        }
        Value::Object(obj) => inspect_content_object(obj, stats),
        _ => {}
    }
}

fn inspect_content_object(obj: &serde_json::Map<String, Value>, stats: &mut WorkloadContentStats) {
    match obj.get("type").and_then(Value::as_str) {
        Some("image") => {
            stats.image_count += 1;
            add_source_data_len(obj.get("source"), stats);
        }
        Some("image_url") => {
            stats.image_count += 1;
            match obj.get("image_url") {
                Some(Value::String(url)) => stats.source_data_bytes += url.len(),
                Some(Value::Object(image_url)) => {
                    if let Some(url) = image_url.get("url").and_then(Value::as_str) {
                        stats.source_data_bytes += url.len();
                    }
                }
                _ => {}
            }
        }
        Some("document") => {
            stats.document_count += 1;
            add_source_data_len(obj.get("source"), stats);
        }
        _ => {
            add_source_data_len(obj.get("source"), stats);
        }
    }
}

fn add_source_data_len(source: Option<&Value>, stats: &mut WorkloadContentStats) {
    let Some(source) = source.and_then(Value::as_object) else {
        return;
    };
    if let Some(data) = source.get("data").and_then(Value::as_str) {
        stats.source_data_bytes += data.len();
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ConversionRuntimeBucketStats {
    pub le_1_ms: u64,
    pub le_10_ms: u64,
    pub le_100_ms: u64,
    pub le_1000_ms: u64,
    pub le_5000_ms: u64,
    pub gt_5000_ms: u64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ConversionRuntimeMetricSnapshot {
    pub accepted_total: u64,
    pub completed_total: u64,
    pub queue_full_total: u64,
    pub wait_timeout_total: u64,
    pub worker_join_error_total: u64,
    pub conversion_error_total: u64,
    pub max_observed_waiting: usize,
    pub max_observed_waiting_weight: usize,
    pub max_observed_request_weight: usize,
    pub wait_ms: ConversionRuntimeBucketStats,
    pub convert_ms: ConversionRuntimeBucketStats,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ConversionRuntimeStats {
    pub max_concurrent: usize,
    pub available_permits: usize,
    pub used_permits: usize,
    pub waiting: usize,
    pub waiting_weight: usize,
    pub max_queue: usize,
    pub max_queue_weight: usize,
    pub queue_wait_ms: u64,
    pub max_request_weight: usize,
    pub in_flight: usize,
    pub in_flight_weight: usize,
    pub metrics: ConversionRuntimeMetricSnapshot,
}

impl ConversionRuntimeStats {
    pub(crate) fn is_saturated(self) -> bool {
        if self.available_permits > 0 {
            return false;
        }
        self.max_queue == 0
            || self.waiting >= self.max_queue
            || (self.max_queue_weight > 0 && self.waiting_weight >= self.max_queue_weight)
    }
}

#[derive(Debug)]
pub(crate) enum ConversionRuntimeError {
    QueueFull {
        stats: ConversionRuntimeStats,
        workload: ConversionWorkload,
    },
    WaitTimeout {
        stats: ConversionRuntimeStats,
        workload: ConversionWorkload,
    },
    WorkerJoin(String),
    Conversion(ConversionError),
}

pub(crate) struct ConversionRuntime {
    config: ConversionRuntimeConfig,
    limiter: Arc<WeightedLimiter>,
    waiting: AtomicUsize,
    waiting_weight: AtomicUsize,
    in_flight: AtomicUsize,
    in_flight_weight: AtomicUsize,
    metrics: Arc<ConversionRuntimeMetrics>,
}

impl ConversionRuntime {
    pub(crate) fn new(config: ConversionRuntimeConfig) -> Self {
        Self {
            limiter: Arc::new(WeightedLimiter::new(config.max_concurrent)),
            config,
            waiting: AtomicUsize::new(0),
            waiting_weight: AtomicUsize::new(0),
            in_flight: AtomicUsize::new(0),
            in_flight_weight: AtomicUsize::new(0),
            metrics: Arc::new(ConversionRuntimeMetrics::default()),
        }
    }

    pub(crate) fn stats(&self) -> ConversionRuntimeStats {
        let used_permits = self.limiter.used_permits();
        ConversionRuntimeStats {
            max_concurrent: self.config.max_concurrent,
            available_permits: self.limiter.available_permits(),
            used_permits,
            waiting: self.waiting.load(Ordering::Relaxed),
            waiting_weight: self.waiting_weight.load(Ordering::Relaxed),
            max_queue: self.config.max_queue,
            max_queue_weight: self.config.max_queue_weight,
            queue_wait_ms: self.config.queue_wait_ms,
            max_request_weight: self.effective_max_request_weight(),
            in_flight: self.in_flight.load(Ordering::Relaxed),
            in_flight_weight: self.in_flight_weight.load(Ordering::Relaxed),
            metrics: self.metrics.snapshot(),
        }
    }

    pub(crate) async fn convert(
        &self,
        payload: &MessagesRequest,
        probe: UpstreamProbe,
        body_bytes: Option<usize>,
    ) -> Result<ConversionResult, ConversionRuntimeError> {
        let workload = ConversionWorkload::estimate(
            payload,
            body_bytes.unwrap_or_default(),
            self.effective_max_request_weight(),
        );
        let (admission, wait_ms) = self.acquire_permit(workload).await?;
        self.metrics.accepted_total.fetch_add(1, Ordering::Relaxed);
        self.metrics.wait_ms.record(wait_ms);
        self.metrics
            .record_max_observed_request_weight(workload.weight);

        let payload = payload.clone();
        let convert_started_at = Instant::now();
        let result = task::spawn_blocking(move || convert_request_with_probe(&payload, probe))
            .await
            .map_err(|err| {
                self.metrics
                    .worker_join_error_total
                    .fetch_add(1, Ordering::Relaxed);
                ConversionRuntimeError::WorkerJoin(err.to_string())
            })?;

        let convert_ms = elapsed_ms(convert_started_at);
        self.metrics.convert_ms.record(convert_ms);
        self.metrics.completed_total.fetch_add(1, Ordering::Relaxed);
        drop(admission);

        result.map_err(|err| {
            self.metrics
                .conversion_error_total
                .fetch_add(1, Ordering::Relaxed);
            ConversionRuntimeError::Conversion(err)
        })
    }

    fn effective_max_request_weight(&self) -> usize {
        self.config
            .max_request_weight
            .max(1)
            .min(self.config.max_concurrent.max(1))
    }

    async fn acquire_permit(
        &self,
        workload: ConversionWorkload,
    ) -> Result<(ConversionAdmissionPermit<'_>, u64), ConversionRuntimeError> {
        let started_at = Instant::now();
        if let Some(weighted) = self.limiter.clone().try_acquire(workload.weight) {
            return Ok((
                self.admit(weighted, workload.weight),
                elapsed_ms(started_at),
            ));
        }

        if self.config.max_queue == 0 {
            self.metrics
                .queue_full_total
                .fetch_add(1, Ordering::Relaxed);
            return Err(ConversionRuntimeError::QueueFull {
                stats: self.stats(),
                workload,
            });
        }

        let previous_waiting = self.waiting.fetch_add(1, Ordering::AcqRel);
        let previous_waiting_weight = self
            .waiting_weight
            .fetch_add(workload.weight, Ordering::AcqRel);
        self.metrics
            .record_max_observed_waiting(previous_waiting.saturating_add(1));
        let current_waiting_weight = previous_waiting_weight.saturating_add(workload.weight);
        self.metrics
            .record_max_observed_waiting_weight(current_waiting_weight);

        if previous_waiting >= self.config.max_queue
            || (self.config.max_queue_weight > 0
                && current_waiting_weight > self.config.max_queue_weight)
        {
            self.waiting.fetch_sub(1, Ordering::AcqRel);
            self.waiting_weight
                .fetch_sub(workload.weight, Ordering::AcqRel);
            self.metrics
                .queue_full_total
                .fetch_add(1, Ordering::Relaxed);
            return Err(ConversionRuntimeError::QueueFull {
                stats: self.stats(),
                workload,
            });
        }

        let deadline = Instant::now() + Duration::from_millis(self.config.queue_wait_ms);
        let acquire_result = loop {
            let notified = self.limiter.notified();
            if let Some(weighted) = self.limiter.clone().try_acquire(workload.weight) {
                break Ok(weighted);
            }

            let now = Instant::now();
            if now >= deadline {
                break Err(());
            }

            if timeout(deadline.saturating_duration_since(now), notified)
                .await
                .is_err()
            {
                break Err(());
            }
        };

        self.waiting.fetch_sub(1, Ordering::AcqRel);
        self.waiting_weight
            .fetch_sub(workload.weight, Ordering::AcqRel);

        match acquire_result {
            Ok(weighted) => Ok((
                self.admit(weighted, workload.weight),
                elapsed_ms(started_at),
            )),
            Err(_) => {
                self.metrics
                    .wait_timeout_total
                    .fetch_add(1, Ordering::Relaxed);
                Err(ConversionRuntimeError::WaitTimeout {
                    stats: self.stats(),
                    workload,
                })
            }
        }
    }

    fn admit(&self, permit: WeightedPermit, weight: usize) -> ConversionAdmissionPermit<'_> {
        self.in_flight.fetch_add(1, Ordering::AcqRel);
        self.in_flight_weight.fetch_add(weight, Ordering::AcqRel);
        ConversionAdmissionPermit {
            _permit: permit,
            in_flight: &self.in_flight,
            in_flight_weight: &self.in_flight_weight,
            weight,
        }
    }
}

impl Default for ConversionRuntime {
    fn default() -> Self {
        Self::new(ConversionRuntimeConfig::default())
    }
}

#[derive(Debug)]
struct WeightedLimiter {
    max_permits: usize,
    used_permits: AtomicUsize,
    notify: Notify,
}

impl WeightedLimiter {
    fn new(max_permits: usize) -> Self {
        Self {
            max_permits: max_permits.max(1),
            used_permits: AtomicUsize::new(0),
            notify: Notify::new(),
        }
    }

    fn used_permits(&self) -> usize {
        self.used_permits.load(Ordering::Relaxed)
    }

    fn available_permits(&self) -> usize {
        self.max_permits.saturating_sub(self.used_permits())
    }

    fn notified(&self) -> tokio::sync::futures::Notified<'_> {
        self.notify.notified()
    }

    fn try_acquire(self: Arc<Self>, weight: usize) -> Option<WeightedPermit> {
        let weight = weight.clamp(1, self.max_permits);
        let mut used = self.used_permits.load(Ordering::Acquire);
        loop {
            if used > self.max_permits.saturating_sub(weight) {
                return None;
            }
            match self.used_permits.compare_exchange_weak(
                used,
                used + weight,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(WeightedPermit {
                        limiter: self,
                        weight,
                    });
                }
                Err(observed) => used = observed,
            }
        }
    }

    fn release(&self, weight: usize) {
        self.used_permits.fetch_sub(weight, Ordering::AcqRel);
        self.notify.notify_waiters();
    }
}

#[derive(Debug)]
struct WeightedPermit {
    limiter: Arc<WeightedLimiter>,
    weight: usize,
}

impl Drop for WeightedPermit {
    fn drop(&mut self) {
        self.limiter.release(self.weight);
    }
}

struct ConversionAdmissionPermit<'a> {
    _permit: WeightedPermit,
    in_flight: &'a AtomicUsize,
    in_flight_weight: &'a AtomicUsize,
    weight: usize,
}

impl Drop for ConversionAdmissionPermit<'_> {
    fn drop(&mut self) {
        self.in_flight.fetch_sub(1, Ordering::AcqRel);
        self.in_flight_weight
            .fetch_sub(self.weight, Ordering::AcqRel);
    }
}

#[derive(Debug, Default)]
struct ConversionRuntimeMetrics {
    accepted_total: AtomicU64,
    completed_total: AtomicU64,
    queue_full_total: AtomicU64,
    wait_timeout_total: AtomicU64,
    worker_join_error_total: AtomicU64,
    conversion_error_total: AtomicU64,
    max_observed_waiting: AtomicUsize,
    max_observed_waiting_weight: AtomicUsize,
    max_observed_request_weight: AtomicUsize,
    wait_ms: DurationBuckets,
    convert_ms: DurationBuckets,
}

impl ConversionRuntimeMetrics {
    fn snapshot(&self) -> ConversionRuntimeMetricSnapshot {
        ConversionRuntimeMetricSnapshot {
            accepted_total: self.accepted_total.load(Ordering::Relaxed),
            completed_total: self.completed_total.load(Ordering::Relaxed),
            queue_full_total: self.queue_full_total.load(Ordering::Relaxed),
            wait_timeout_total: self.wait_timeout_total.load(Ordering::Relaxed),
            worker_join_error_total: self.worker_join_error_total.load(Ordering::Relaxed),
            conversion_error_total: self.conversion_error_total.load(Ordering::Relaxed),
            max_observed_waiting: self.max_observed_waiting.load(Ordering::Relaxed),
            max_observed_waiting_weight: self.max_observed_waiting_weight.load(Ordering::Relaxed),
            max_observed_request_weight: self.max_observed_request_weight.load(Ordering::Relaxed),
            wait_ms: self.wait_ms.snapshot(),
            convert_ms: self.convert_ms.snapshot(),
        }
    }

    fn record_max_observed_waiting(&self, value: usize) {
        record_max_usize(&self.max_observed_waiting, value);
    }

    fn record_max_observed_waiting_weight(&self, value: usize) {
        record_max_usize(&self.max_observed_waiting_weight, value);
    }

    fn record_max_observed_request_weight(&self, value: usize) {
        record_max_usize(&self.max_observed_request_weight, value);
    }
}

#[derive(Debug, Default)]
struct DurationBuckets {
    le_1_ms: AtomicU64,
    le_10_ms: AtomicU64,
    le_100_ms: AtomicU64,
    le_1000_ms: AtomicU64,
    le_5000_ms: AtomicU64,
    gt_5000_ms: AtomicU64,
}

impl DurationBuckets {
    fn record(&self, ms: u64) {
        let counter = if ms <= 1 {
            &self.le_1_ms
        } else if ms <= 10 {
            &self.le_10_ms
        } else if ms <= 100 {
            &self.le_100_ms
        } else if ms <= 1_000 {
            &self.le_1000_ms
        } else if ms <= 5_000 {
            &self.le_5000_ms
        } else {
            &self.gt_5000_ms
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> ConversionRuntimeBucketStats {
        ConversionRuntimeBucketStats {
            le_1_ms: self.le_1_ms.load(Ordering::Relaxed),
            le_10_ms: self.le_10_ms.load(Ordering::Relaxed),
            le_100_ms: self.le_100_ms.load(Ordering::Relaxed),
            le_1000_ms: self.le_1000_ms.load(Ordering::Relaxed),
            le_5000_ms: self.le_5000_ms.load(Ordering::Relaxed),
            gt_5000_ms: self.gt_5000_ms.load(Ordering::Relaxed),
        }
    }
}

fn record_max_usize(counter: &AtomicUsize, value: usize) {
    let mut current = counter.load(Ordering::Relaxed);
    while value > current {
        match counter.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(observed) => current = observed,
        }
    }
}

fn elapsed_ms(started_at: Instant) -> u64 {
    started_at.elapsed().as_millis().min(u64::MAX as u128) as u64
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{ConversionWorkload, MessagesRequest};

    fn request_with_content(content: serde_json::Value) -> MessagesRequest {
        MessagesRequest {
            model: "claude-test".to_string(),
            max_tokens: 1024,
            messages: vec![super::super::types::Message {
                role: "user".to_string(),
                content,
            }],
            stream: false,
            system: None,
            tools: None,
            tool_choice: None,
            thinking: None,
            output_config: None,
            metadata: None,
        }
    }

    #[test]
    fn estimates_text_only_request_as_weight_one() {
        let payload = request_with_content(json!("hello"));

        let workload = ConversionWorkload::estimate(&payload, 128, 8);

        assert_eq!(workload.weight, 1);
        assert_eq!(workload.message_count, 1);
        assert_eq!(workload.image_count, 0);
        assert_eq!(workload.source_data_bytes, 0);
    }

    #[test]
    fn estimates_large_body_without_static_rejection() {
        let payload = request_with_content(json!("hello"));

        let workload = ConversionWorkload::estimate(&payload, 22 * 1024 * 1024, 8);

        assert!(workload.weight > 1);
        assert_eq!(workload.weight, 5);
        assert_eq!(workload.body_bytes, 22 * 1024 * 1024);
    }

    #[test]
    fn image_source_size_contributes_to_weight_and_clamps() {
        let image_data = "a".repeat(4 * 1024 * 1024 + 1);
        let payload = request_with_content(json!([
            {"type":"image","source":{"type":"base64","media_type":"image/png","data":image_data}}
        ]));

        let workload = ConversionWorkload::estimate(&payload, 1024, 4);

        assert_eq!(workload.image_count, 1);
        assert_eq!(workload.source_data_bytes, 4 * 1024 * 1024 + 1);
        assert_eq!(workload.weight, 4);
    }
}
