use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError},
    task,
    time::timeout,
};

use crate::model::config::ConversionRuntimeConfig;

use super::{
    converter::{ConversionError, ConversionResult, convert_request_with_probe},
    probe::UpstreamProbe,
    types::MessagesRequest,
};

#[derive(Debug, Clone, Copy)]
pub(crate) struct ConversionRuntimeStats {
    pub max_concurrent: usize,
    pub available_permits: usize,
    pub waiting: usize,
    pub max_queue: usize,
    pub queue_wait_ms: u64,
}

impl ConversionRuntimeStats {
    pub(crate) fn is_saturated(self) -> bool {
        self.available_permits == 0
            && if self.max_queue == 0 {
                true
            } else {
                self.waiting >= self.max_queue
            }
    }
}

#[derive(Debug)]
pub(crate) enum ConversionRuntimeError {
    QueueFull(ConversionRuntimeStats),
    WaitTimeout(ConversionRuntimeStats),
    WorkerClosed,
    WorkerJoin(String),
    Conversion(ConversionError),
}

pub(crate) struct ConversionRuntime {
    config: ConversionRuntimeConfig,
    semaphore: Arc<Semaphore>,
    waiting: AtomicUsize,
}

impl ConversionRuntime {
    pub(crate) fn new(config: ConversionRuntimeConfig) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(config.max_concurrent)),
            config,
            waiting: AtomicUsize::new(0),
        }
    }

    pub(crate) fn stats(&self) -> ConversionRuntimeStats {
        ConversionRuntimeStats {
            max_concurrent: self.config.max_concurrent,
            available_permits: self.semaphore.available_permits(),
            waiting: self.waiting.load(Ordering::Relaxed),
            max_queue: self.config.max_queue,
            queue_wait_ms: self.config.queue_wait_ms,
        }
    }

    pub(crate) async fn convert(
        &self,
        payload: &MessagesRequest,
        probe: UpstreamProbe,
    ) -> Result<ConversionResult, ConversionRuntimeError> {
        let _permit = self.acquire_permit().await?;
        let payload = payload.clone();
        let result = task::spawn_blocking(move || convert_request_with_probe(&payload, probe))
            .await
            .map_err(|err| ConversionRuntimeError::WorkerJoin(err.to_string()))?;

        result.map_err(ConversionRuntimeError::Conversion)
    }

    async fn acquire_permit(&self) -> Result<OwnedSemaphorePermit, ConversionRuntimeError> {
        match self.semaphore.clone().try_acquire_owned() {
            Ok(permit) => return Ok(permit),
            Err(TryAcquireError::Closed) => return Err(ConversionRuntimeError::WorkerClosed),
            Err(TryAcquireError::NoPermits) => {}
        }

        if self.config.max_queue == 0 {
            return Err(ConversionRuntimeError::QueueFull(self.stats()));
        }

        let previous_waiting = self.waiting.fetch_add(1, Ordering::AcqRel);
        if previous_waiting >= self.config.max_queue {
            self.waiting.fetch_sub(1, Ordering::AcqRel);
            return Err(ConversionRuntimeError::QueueFull(self.stats()));
        }

        let acquire = self.semaphore.clone().acquire_owned();
        let result = timeout(Duration::from_millis(self.config.queue_wait_ms), acquire).await;
        self.waiting.fetch_sub(1, Ordering::AcqRel);

        match result {
            Ok(Ok(permit)) => Ok(permit),
            Ok(Err(_)) => Err(ConversionRuntimeError::WorkerClosed),
            Err(_) => Err(ConversionRuntimeError::WaitTimeout(self.stats())),
        }
    }
}

impl Default for ConversionRuntime {
    fn default() -> Self {
        Self::new(ConversionRuntimeConfig::default())
    }
}
