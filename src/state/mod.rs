use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use chrono::DateTime;
use parking_lot::Mutex;
use postgres::{Client, NoTls};
use redis::{Client as RedisClient, Commands, Script};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::runtime::RuntimeFlavor;

use crate::admin::types::BalanceResponse;
use crate::kiro::model::credentials::{CredentialsConfig, KiroCredentials};
use crate::model::config::{Config, RequestWeightingConfig, StateBackendKind};
use crate::model::model_policy::{
    AccountTypeDispatchPolicy, ModelSupportPolicy, normalize_account_type_dispatch_policies,
    normalize_account_type_policies,
};

const STATS_FILE_NAME: &str = "kiro_stats.json";
const BALANCE_CACHE_FILE_NAME: &str = "kiro_balance_cache.json";
const POSTGRES_NAMESPACE: &str = "runtime";
const POSTGRES_CREDENTIALS_KEY: &str = "credentials";
const POSTGRES_STATS_KEY: &str = "stats";
const POSTGRES_BALANCE_CACHE_KEY: &str = "balance_cache";
const POSTGRES_DISPATCH_CONFIG_KEY: &str = "dispatch_config";
const REDIS_BALANCE_CACHE_KEY: &str = "kiro:runtime:balance_cache";
const REDIS_BALANCE_CACHE_TTL_SECS: u64 = 86_400;
const REDIS_DISPATCH_RUNTIME_NAMESPACE: &str = "kiro:runtime:dispatch";
const REDIS_DISPATCH_RUNTIME_STATE_KEY_SUFFIX: &str = "state";
const REDIS_DISPATCH_RUNTIME_LEASES_KEY_SUFFIX: &str = "leases";
const REDIS_RUNTIME_COORDINATION_NAMESPACE: &str = "kiro:runtime:coordination";
const REDIS_RUNTIME_INSTANCE_KEY_PREFIX: &str = "instances";
const REDIS_RUNTIME_LEADER_KEY: &str = "leader";
const REDIS_RUNTIME_REFRESH_NAMESPACE: &str = "kiro:runtime:refresh";
const REDIS_STATE_CHANGE_NAMESPACE: &str = "kiro:runtime:state_change";
const REDIS_STATE_CHANGE_CREDENTIALS_KEY: &str = "credentials_revision";
const REDIS_STATE_CHANGE_DISPATCH_KEY: &str = "dispatch_revision";
const REDIS_STATE_CHANGE_BALANCE_CACHE_KEY: &str = "balance_cache_revision";
const REDIS_RUNTIME_LEADER_SCRIPT: &str = r#"
local current = redis.call('GET', KEYS[1])
if not current then
  redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2], 'NX')
  return {'acquired', ARGV[1]}
end
if current == ARGV[1] then
  redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2], 'XX')
  return {'renewed', ARGV[1]}
end
return {'held', current}
"#;
const REDIS_RUNTIME_RELEASE_SCRIPT: &str = r#"
redis.call('DEL', KEYS[2])
local current = redis.call('GET', KEYS[1])
if current == ARGV[1] then
  redis.call('DEL', KEYS[1])
  return {'released', ''}
end
if current then
  return {'held', current}
end
return {'empty', ''}
"#;
const REDIS_RUNTIME_REFRESH_LEASE_SCRIPT: &str = r#"
local current = redis.call('GET', KEYS[1])
if not current then
  redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2], 'NX')
  return {'acquired', ARGV[1]}
end
if current == ARGV[1] then
  redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2], 'XX')
  return {'renewed', ARGV[1]}
end
return {'held', current}
"#;
const REDIS_RUNTIME_REFRESH_RELEASE_SCRIPT: &str = r#"
local current = redis.call('GET', KEYS[1])
if current == ARGV[1] then
  redis.call('DEL', KEYS[1])
  return {'released', current}
end
if current then
  return {'held', current}
end
return {'empty', ''}
"#;
const REDIS_DISPATCH_RUNTIME_RESERVE_SCRIPT: &str = r#"
local now_ms = tonumber(ARGV[1])
local lease_id = ARGV[2]
local lease_expires_at_ms = tonumber(ARGV[3])
local max_concurrency = tonumber(ARGV[4])
local bucket_enabled = ARGV[5] == '1'
local capacity = tonumber(ARGV[6])
local refill_per_second = tonumber(ARGV[7])
local min_refill_per_second = tonumber(ARGV[8])
local requested_tokens = tonumber(ARGV[9])

if not requested_tokens or requested_tokens <= 0 then
  requested_tokens = 1.0
end
if bucket_enabled and capacity and capacity > 0 then
  requested_tokens = math.min(requested_tokens, capacity)
end

redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now_ms)

local active_requests = redis.call('ZCARD', KEYS[2])
local values = redis.call(
  'HMGET',
  KEYS[1],
  'cooldown_until_ms',
  'rate_limit_hit_streak',
  'bucket_tokens',
  'bucket_current_refill_per_second',
  'bucket_last_refill_at_ms',
  'bucket_capacity',
  'bucket_base_refill_per_second'
)

local cooldown_until_ms = tonumber(values[1])
local rate_limit_hit_streak = tonumber(values[2]) or 0
local bucket_tokens = tonumber(values[3])
local bucket_current_refill_per_second = tonumber(values[4])
local bucket_last_refill_at_ms = tonumber(values[5])
local stored_capacity = tonumber(values[6])
local stored_base_refill_per_second = tonumber(values[7])

if cooldown_until_ms and cooldown_until_ms <= now_ms then
  cooldown_until_ms = nil
  redis.call('HDEL', KEYS[1], 'cooldown_until_ms')
end

local function normalize_bucket()
  if not bucket_enabled then
    bucket_tokens = nil
    bucket_current_refill_per_second = nil
    bucket_last_refill_at_ms = nil
    stored_capacity = nil
    stored_base_refill_per_second = nil
    redis.call(
      'HDEL',
      KEYS[1],
      'bucket_tokens',
      'bucket_current_refill_per_second',
      'bucket_last_refill_at_ms',
      'bucket_capacity',
      'bucket_base_refill_per_second'
    )
    return
  end

  if not bucket_tokens or not bucket_current_refill_per_second then
    bucket_tokens = capacity
    bucket_current_refill_per_second = refill_per_second
    bucket_last_refill_at_ms = now_ms
    stored_capacity = capacity
    stored_base_refill_per_second = refill_per_second
    return
  end

  if not bucket_last_refill_at_ms then
    bucket_last_refill_at_ms = now_ms
  end
  if not stored_capacity or stored_capacity <= 0 then
    stored_capacity = capacity
  end
  if not stored_base_refill_per_second or stored_base_refill_per_second <= 0 then
    stored_base_refill_per_second = refill_per_second
  end

  if now_ms > bucket_last_refill_at_ms then
    local elapsed_seconds = (now_ms - bucket_last_refill_at_ms) / 1000.0
    bucket_tokens = math.min(
      stored_capacity,
      bucket_tokens + elapsed_seconds * bucket_current_refill_per_second
    )
    bucket_last_refill_at_ms = now_ms
  end

  if stored_capacity ~= capacity or stored_base_refill_per_second ~= refill_per_second then
    local token_ratio = 1.0
    if stored_capacity > 0 then
      token_ratio = math.max(0.0, math.min(1.0, bucket_tokens / stored_capacity))
    end

    local refill_ratio = 1.0
    if stored_base_refill_per_second > 0 then
      refill_ratio = math.max(
        0.0,
        math.min(1.0, bucket_current_refill_per_second / stored_base_refill_per_second)
      )
    end

    bucket_tokens = math.max(0.0, math.min(capacity, token_ratio * capacity))
    bucket_current_refill_per_second = math.max(
      min_refill_per_second,
      math.min(refill_per_second, refill_per_second * refill_ratio)
    )
    bucket_last_refill_at_ms = now_ms
    stored_capacity = capacity
    stored_base_refill_per_second = refill_per_second
  end
end

normalize_bucket()

local next_ready_at_ms = ''
if cooldown_until_ms and cooldown_until_ms > now_ms then
  next_ready_at_ms = tostring(cooldown_until_ms)
end
if bucket_enabled and bucket_tokens and bucket_tokens < requested_tokens then
  local wait_seconds = (requested_tokens - bucket_tokens) / bucket_current_refill_per_second
  local bucket_ready_at_ms = now_ms + math.ceil(wait_seconds * 1000.0)
  if next_ready_at_ms == '' or bucket_ready_at_ms > tonumber(next_ready_at_ms) then
    next_ready_at_ms = tostring(bucket_ready_at_ms)
  end
end

if (cooldown_until_ms and cooldown_until_ms > now_ms)
  or (bucket_enabled and bucket_tokens and bucket_tokens < requested_tokens)
  or (max_concurrency >= 0 and active_requests >= max_concurrency) then
  if bucket_enabled then
    redis.call(
      'HSET',
      KEYS[1],
      'bucket_tokens',
      bucket_tokens,
      'bucket_current_refill_per_second',
      bucket_current_refill_per_second,
      'bucket_last_refill_at_ms',
      bucket_last_refill_at_ms,
      'bucket_capacity',
      stored_capacity,
      'bucket_base_refill_per_second',
      stored_base_refill_per_second,
      'rate_limit_hit_streak',
      rate_limit_hit_streak
    )
  else
    redis.call('HSET', KEYS[1], 'rate_limit_hit_streak', rate_limit_hit_streak)
  end

  return {
    'unavailable',
    tostring(active_requests),
    cooldown_until_ms and tostring(cooldown_until_ms) or '',
    tostring(rate_limit_hit_streak),
    bucket_tokens and tostring(bucket_tokens) or '',
    stored_capacity and tostring(stored_capacity) or '',
    bucket_current_refill_per_second and tostring(bucket_current_refill_per_second) or '',
    stored_base_refill_per_second and tostring(stored_base_refill_per_second) or '',
    next_ready_at_ms
  }
end

if bucket_enabled then
  bucket_tokens = math.max(0.0, bucket_tokens - requested_tokens)
  redis.call(
    'HSET',
    KEYS[1],
    'bucket_tokens',
    bucket_tokens,
    'bucket_current_refill_per_second',
    bucket_current_refill_per_second,
    'bucket_last_refill_at_ms',
    bucket_last_refill_at_ms,
    'bucket_capacity',
    stored_capacity,
    'bucket_base_refill_per_second',
    stored_base_refill_per_second,
    'rate_limit_hit_streak',
    rate_limit_hit_streak
  )
else
  redis.call('HSET', KEYS[1], 'rate_limit_hit_streak', rate_limit_hit_streak)
end

redis.call('ZADD', KEYS[2], lease_expires_at_ms, lease_id)
redis.call('PEXPIRE', KEYS[2], math.max(1, lease_expires_at_ms - now_ms))
active_requests = redis.call('ZCARD', KEYS[2])

return {
  'reserved',
  tostring(active_requests),
  cooldown_until_ms and tostring(cooldown_until_ms) or '',
  tostring(rate_limit_hit_streak),
  bucket_tokens and tostring(bucket_tokens) or '',
  stored_capacity and tostring(stored_capacity) or '',
  bucket_current_refill_per_second and tostring(bucket_current_refill_per_second) or '',
  stored_base_refill_per_second and tostring(stored_base_refill_per_second) or '',
  ''
}
"#;
const REDIS_DISPATCH_RUNTIME_SUCCESS_SCRIPT: &str = r#"
local now_ms = tonumber(ARGV[1])
local bucket_enabled = ARGV[2] == '1'
local capacity = tonumber(ARGV[3])
local refill_per_second = tonumber(ARGV[4])
local min_refill_per_second = tonumber(ARGV[5])
local recovery_step_per_success = tonumber(ARGV[6])

local values = redis.call(
  'HMGET',
  KEYS[1],
  'cooldown_until_ms',
  'rate_limit_hit_streak',
  'bucket_tokens',
  'bucket_current_refill_per_second',
  'bucket_last_refill_at_ms',
  'bucket_capacity',
  'bucket_base_refill_per_second'
)

local cooldown_until_ms = tonumber(values[1])
local bucket_tokens = tonumber(values[3])
local bucket_current_refill_per_second = tonumber(values[4])
local bucket_last_refill_at_ms = tonumber(values[5])
local stored_capacity = tonumber(values[6])
local stored_base_refill_per_second = tonumber(values[7])

if cooldown_until_ms and cooldown_until_ms <= now_ms then
  cooldown_until_ms = nil
  redis.call('HDEL', KEYS[1], 'cooldown_until_ms')
end

local function normalize_bucket()
  if not bucket_enabled then
    bucket_tokens = nil
    bucket_current_refill_per_second = nil
    bucket_last_refill_at_ms = nil
    stored_capacity = nil
    stored_base_refill_per_second = nil
    redis.call(
      'HDEL',
      KEYS[1],
      'bucket_tokens',
      'bucket_current_refill_per_second',
      'bucket_last_refill_at_ms',
      'bucket_capacity',
      'bucket_base_refill_per_second'
    )
    return
  end

  if not bucket_tokens or not bucket_current_refill_per_second then
    bucket_tokens = capacity
    bucket_current_refill_per_second = refill_per_second
    bucket_last_refill_at_ms = now_ms
    stored_capacity = capacity
    stored_base_refill_per_second = refill_per_second
    return
  end

  if not bucket_last_refill_at_ms then
    bucket_last_refill_at_ms = now_ms
  end
  if not stored_capacity or stored_capacity <= 0 then
    stored_capacity = capacity
  end
  if not stored_base_refill_per_second or stored_base_refill_per_second <= 0 then
    stored_base_refill_per_second = refill_per_second
  end

  if now_ms > bucket_last_refill_at_ms then
    local elapsed_seconds = (now_ms - bucket_last_refill_at_ms) / 1000.0
    bucket_tokens = math.min(
      stored_capacity,
      bucket_tokens + elapsed_seconds * bucket_current_refill_per_second
    )
  end
  bucket_current_refill_per_second = math.min(
    refill_per_second,
    bucket_current_refill_per_second + recovery_step_per_success
  )
  bucket_last_refill_at_ms = now_ms
  stored_capacity = capacity
  stored_base_refill_per_second = refill_per_second
end

normalize_bucket()
redis.call('HSET', KEYS[1], 'rate_limit_hit_streak', 0)

if bucket_enabled then
  redis.call(
    'HSET',
    KEYS[1],
    'bucket_tokens',
    bucket_tokens,
    'bucket_current_refill_per_second',
    bucket_current_refill_per_second,
    'bucket_last_refill_at_ms',
    bucket_last_refill_at_ms,
    'bucket_capacity',
    stored_capacity,
    'bucket_base_refill_per_second',
    stored_base_refill_per_second
  )
end

return {
  'updated',
  '',
  cooldown_until_ms and tostring(cooldown_until_ms) or '',
  '0',
  bucket_tokens and tostring(bucket_tokens) or '',
  stored_capacity and tostring(stored_capacity) or '',
  bucket_current_refill_per_second and tostring(bucket_current_refill_per_second) or '',
  stored_base_refill_per_second and tostring(stored_base_refill_per_second) or '',
  ''
}
"#;
const REDIS_DISPATCH_RUNTIME_RATE_LIMIT_SCRIPT: &str = r#"
local now_ms = tonumber(ARGV[1])
local cooldown_ms = tonumber(ARGV[2])
local bucket_enabled = ARGV[3] == '1'
local capacity = tonumber(ARGV[4])
local refill_per_second = tonumber(ARGV[5])
local min_refill_per_second = tonumber(ARGV[6])
local backoff_factor = tonumber(ARGV[7])

local values = redis.call(
  'HMGET',
  KEYS[1],
  'bucket_tokens',
  'bucket_current_refill_per_second',
  'bucket_last_refill_at_ms',
  'bucket_capacity',
  'bucket_base_refill_per_second',
  'rate_limit_hit_streak'
)

local bucket_tokens = tonumber(values[1])
local bucket_current_refill_per_second = tonumber(values[2])
local bucket_last_refill_at_ms = tonumber(values[3])
local stored_capacity = tonumber(values[4])
local stored_base_refill_per_second = tonumber(values[5])
local rate_limit_hit_streak = tonumber(values[6]) or 0

local function normalize_bucket()
  if not bucket_enabled then
    bucket_tokens = nil
    bucket_current_refill_per_second = nil
    bucket_last_refill_at_ms = nil
    stored_capacity = nil
    stored_base_refill_per_second = nil
    redis.call(
      'HDEL',
      KEYS[1],
      'bucket_tokens',
      'bucket_current_refill_per_second',
      'bucket_last_refill_at_ms',
      'bucket_capacity',
      'bucket_base_refill_per_second'
    )
    return
  end

  if not bucket_tokens or not bucket_current_refill_per_second then
    bucket_tokens = capacity
    bucket_current_refill_per_second = refill_per_second
    bucket_last_refill_at_ms = now_ms
    stored_capacity = capacity
    stored_base_refill_per_second = refill_per_second
    return
  end

  if not bucket_last_refill_at_ms then
    bucket_last_refill_at_ms = now_ms
  end
  if not stored_capacity or stored_capacity <= 0 then
    stored_capacity = capacity
  end
  if not stored_base_refill_per_second or stored_base_refill_per_second <= 0 then
    stored_base_refill_per_second = refill_per_second
  end

  if now_ms > bucket_last_refill_at_ms then
    local elapsed_seconds = (now_ms - bucket_last_refill_at_ms) / 1000.0
    bucket_tokens = math.min(
      stored_capacity,
      bucket_tokens + elapsed_seconds * bucket_current_refill_per_second
    )
  end
  bucket_tokens = 0.0
  bucket_current_refill_per_second = math.max(
    min_refill_per_second,
    math.min(refill_per_second, bucket_current_refill_per_second * backoff_factor)
  )
  bucket_last_refill_at_ms = now_ms
  stored_capacity = capacity
  stored_base_refill_per_second = refill_per_second
end

normalize_bucket()

rate_limit_hit_streak = rate_limit_hit_streak + 1
local cooldown_until_ms = ''
if cooldown_ms > 0 then
  cooldown_until_ms = tostring(now_ms + cooldown_ms)
  redis.call('HSET', KEYS[1], 'cooldown_until_ms', cooldown_until_ms)
else
  redis.call('HDEL', KEYS[1], 'cooldown_until_ms')
end
redis.call('HSET', KEYS[1], 'rate_limit_hit_streak', rate_limit_hit_streak)

if bucket_enabled then
  redis.call(
    'HSET',
    KEYS[1],
    'bucket_tokens',
    bucket_tokens,
    'bucket_current_refill_per_second',
    bucket_current_refill_per_second,
    'bucket_last_refill_at_ms',
    bucket_last_refill_at_ms,
    'bucket_capacity',
    stored_capacity,
    'bucket_base_refill_per_second',
    stored_base_refill_per_second
  )
end

local next_ready_at_ms = cooldown_until_ms
if bucket_enabled and bucket_tokens and bucket_tokens < 1.0 then
  local wait_seconds = (1.0 - bucket_tokens) / bucket_current_refill_per_second
  local bucket_ready_at_ms = now_ms + math.ceil(wait_seconds * 1000.0)
  if next_ready_at_ms == '' or bucket_ready_at_ms > tonumber(next_ready_at_ms) then
    next_ready_at_ms = tostring(bucket_ready_at_ms)
  end
end

return {
  'updated',
  '',
  cooldown_until_ms,
  tostring(rate_limit_hit_streak),
  bucket_tokens and tostring(bucket_tokens) or '',
  stored_capacity and tostring(stored_capacity) or '',
  bucket_current_refill_per_second and tostring(bucket_current_refill_per_second) or '',
  stored_base_refill_per_second and tostring(stored_base_refill_per_second) or '',
  next_ready_at_ms
}
"#;
const REDIS_DISPATCH_RUNTIME_DEFER_SCRIPT: &str = r#"
local now_ms = tonumber(ARGV[1])
local cooldown_ms = tonumber(ARGV[2])
local target_until = now_ms + cooldown_ms
local current_until = tonumber(redis.call('HGET', KEYS[1], 'cooldown_until_ms'))
if current_until and current_until > target_until then
  target_until = current_until
end
redis.call('HSET', KEYS[1], 'cooldown_until_ms', target_until)

local rate_limit_hit_streak = tonumber(redis.call('HGET', KEYS[1], 'rate_limit_hit_streak')) or 0

return {
  'updated',
  '',
  tostring(target_until),
  tostring(rate_limit_hit_streak),
  '',
  '',
  '',
  '',
  tostring(target_until)
}
"#;
const REDIS_DISPATCH_RUNTIME_RESET_SCRIPT: &str = r#"
local now_ms = tonumber(ARGV[1])
local bucket_enabled = ARGV[2] == '1'
local capacity = tonumber(ARGV[3])
local refill_per_second = tonumber(ARGV[4])

redis.call('HDEL', KEYS[1], 'cooldown_until_ms')
redis.call('HSET', KEYS[1], 'rate_limit_hit_streak', 0)

if bucket_enabled then
  redis.call(
    'HSET',
    KEYS[1],
    'bucket_tokens',
    capacity,
    'bucket_current_refill_per_second',
    refill_per_second,
    'bucket_last_refill_at_ms',
    now_ms,
    'bucket_capacity',
    capacity,
    'bucket_base_refill_per_second',
    refill_per_second
  )
  return {
    'updated',
    '',
    '',
    '0',
    tostring(capacity),
    tostring(capacity),
    tostring(refill_per_second),
    tostring(refill_per_second),
    ''
  }
end

redis.call(
  'HDEL',
  KEYS[1],
  'bucket_tokens',
  'bucket_current_refill_per_second',
  'bucket_last_refill_at_ms',
  'bucket_capacity',
  'bucket_base_refill_per_second'
)

return {'updated', '', '', '0', '', '', '', '', ''}
"#;

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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StatsMergeRecord {
    pub success_count_delta: u64,
    pub last_used_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedBalanceRecord {
    pub cached_at: f64,
    pub data: BalanceResponse,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DispatchRuntimeBucketPolicy {
    pub capacity: f64,
    pub refill_per_second: f64,
    pub min_refill_per_second: f64,
    pub recovery_step_per_success: f64,
    pub backoff_factor: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct DispatchRuntimeCredential {
    pub id: u64,
    pub bucket_policy: Option<DispatchRuntimeBucketPolicy>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DispatchRuntimeSnapshot {
    pub active_requests: usize,
    pub cooldown_until_epoch_ms: Option<u64>,
    pub rate_limit_hit_streak: u32,
    pub bucket_tokens: Option<f64>,
    pub bucket_capacity: Option<f64>,
    pub bucket_current_refill_per_second: Option<f64>,
    pub bucket_base_refill_per_second: Option<f64>,
    pub next_ready_at_epoch_ms: Option<u64>,
}

impl DispatchRuntimeSnapshot {
    fn normalized_request_weight(request_weight: f64) -> f64 {
        if request_weight.is_finite() && request_weight > 0.0 {
            request_weight
        } else {
            1.0
        }
    }

    pub fn requested_bucket_tokens(&self, request_weight: f64) -> Option<f64> {
        self.bucket_capacity
            .map(|capacity| Self::normalized_request_weight(request_weight).min(capacity.max(0.0)))
    }

    pub fn next_ready_at_epoch_ms_for(
        &self,
        request_weight: f64,
        now_epoch_ms: u64,
    ) -> Option<u64> {
        let cooldown_ready_at = self
            .cooldown_until_epoch_ms
            .filter(|until| *until > now_epoch_ms);
        let bucket_ready_at = match (
            self.bucket_tokens,
            self.requested_bucket_tokens(request_weight),
            self.bucket_current_refill_per_second,
        ) {
            (Some(tokens), Some(requested_tokens), Some(refill_per_second))
                if requested_tokens > 0.0
                    && refill_per_second > 0.0
                    && tokens < requested_tokens =>
            {
                let wait_ms = ((requested_tokens - tokens) / refill_per_second * 1000.0)
                    .ceil()
                    .max(0.0) as u64;
                Some(now_epoch_ms.saturating_add(wait_ms))
            }
            _ => self
                .next_ready_at_epoch_ms
                .filter(|ready_at| *ready_at > now_epoch_ms),
        };

        match (cooldown_ready_at, bucket_ready_at) {
            (Some(cooldown_ready_at), Some(bucket_ready_at)) => {
                Some(cooldown_ready_at.max(bucket_ready_at))
            }
            (Some(cooldown_ready_at), None) => Some(cooldown_ready_at),
            (None, Some(bucket_ready_at)) => Some(bucket_ready_at),
            (None, None) => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchLeaseReservationStatus {
    Reserved,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DispatchLeaseReservation {
    pub status: DispatchLeaseReservationStatus,
    pub snapshot: DispatchRuntimeSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PersistedDispatchConfig {
    pub mode: String,
    pub queue_max_size: usize,
    pub queue_max_wait_ms: u64,
    pub rate_limit_cooldown_ms: u64,
    #[serde(default)]
    pub model_cooldown_enabled: bool,
    pub default_max_concurrency: Option<u32>,
    pub rate_limit_bucket_capacity: f64,
    pub rate_limit_refill_per_second: f64,
    pub rate_limit_refill_min_per_second: f64,
    pub rate_limit_refill_recovery_step_per_success: f64,
    pub rate_limit_refill_backoff_factor: f64,
    #[serde(default)]
    pub request_weighting: RequestWeightingConfig,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub account_type_policies: BTreeMap<String, ModelSupportPolicy>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub account_type_dispatch_policies: BTreeMap<String, AccountTypeDispatchPolicy>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateChangeKind {
    Credentials,
    DispatchConfig,
    BalanceCache,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StateChangeRevisions {
    pub credentials: u64,
    pub dispatch_config: u64,
    pub balance_cache: u64,
}

impl PersistedDispatchConfig {
    pub fn from_config(config: &Config) -> Self {
        let mut account_type_policies = config.account_type_policies.clone();
        normalize_account_type_policies(&mut account_type_policies);
        let mut account_type_dispatch_policies = config.account_type_dispatch_policies.clone();
        normalize_account_type_dispatch_policies(&mut account_type_dispatch_policies);
        Self {
            mode: config.load_balancing_mode.clone(),
            queue_max_size: config.queue_max_size,
            queue_max_wait_ms: config.queue_max_wait_ms,
            rate_limit_cooldown_ms: config.rate_limit_cooldown_ms,
            model_cooldown_enabled: config.model_cooldown_enabled,
            default_max_concurrency: config.default_max_concurrency,
            rate_limit_bucket_capacity: config.rate_limit_bucket_capacity,
            rate_limit_refill_per_second: config.rate_limit_refill_per_second,
            rate_limit_refill_min_per_second: config.rate_limit_refill_min_per_second,
            rate_limit_refill_recovery_step_per_success: config
                .rate_limit_refill_recovery_step_per_success,
            rate_limit_refill_backoff_factor: config.rate_limit_refill_backoff_factor,
            request_weighting: config.request_weighting.clone(),
            account_type_policies,
            account_type_dispatch_policies,
        }
    }

    pub fn apply_to_config(&self, config: &mut Config) {
        config.load_balancing_mode = self.mode.clone();
        config.queue_max_size = self.queue_max_size;
        config.queue_max_wait_ms = self.queue_max_wait_ms;
        config.rate_limit_cooldown_ms = self.rate_limit_cooldown_ms;
        config.model_cooldown_enabled = self.model_cooldown_enabled;
        config.default_max_concurrency = self.default_max_concurrency;
        config.rate_limit_bucket_capacity = self.rate_limit_bucket_capacity;
        config.rate_limit_refill_per_second = self.rate_limit_refill_per_second;
        config.rate_limit_refill_min_per_second = self.rate_limit_refill_min_per_second;
        config.rate_limit_refill_recovery_step_per_success =
            self.rate_limit_refill_recovery_step_per_success;
        config.rate_limit_refill_backoff_factor = self.rate_limit_refill_backoff_factor;
        config.request_weighting = self.request_weighting.clone();
        config.account_type_policies = self.account_type_policies.clone();
        config.account_type_dispatch_policies = self.account_type_dispatch_policies.clone();
    }
}

#[derive(Clone)]
enum BalanceCacheStore {
    Primary(Arc<dyn StateBackend>),
    Redis(Arc<RedisBalanceCacheBackend>),
}

impl BalanceCacheStore {
    fn load_balance_cache(&self) -> anyhow::Result<HashMap<u64, CachedBalanceRecord>> {
        match self {
            Self::Primary(backend) => backend.load_balance_cache(),
            Self::Redis(backend) => backend.load_balance_cache(),
        }
    }

    fn save_balance_cache(&self, cache: &HashMap<u64, CachedBalanceRecord>) -> anyhow::Result<()> {
        match self {
            Self::Primary(backend) => backend.save_balance_cache(cache),
            Self::Redis(backend) => backend.save_balance_cache(cache),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeCoordinationStatus {
    pub instance_id: String,
    pub leader_id: Option<String>,
    pub leader_http_base_url: Option<String>,
    pub is_leader: bool,
}

#[derive(Debug, Clone)]
pub enum CredentialCompareAndSwapResult {
    Applied,
    Conflict { current: KiroCredentials },
    Missing,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeRefreshLease {
    credential_id: u64,
    lease_value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeRefreshLeaseAcquisition {
    Acquired(RuntimeRefreshLease),
    HeldByPeer { owner_instance_id: Option<String> },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct RuntimeRefreshLeaseRecord {
    lease_id: String,
    instance_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RuntimeInstanceHeartbeat {
    instance_id: String,
    observed_at_epoch_secs: u64,
    #[serde(default)]
    advertise_http_base_url: Option<String>,
}

#[derive(Clone)]
pub struct StateStore {
    primary_backend: Arc<dyn StateBackend>,
    balance_cache_backend: BalanceCacheStore,
    dispatch_runtime_backend: Option<Arc<RedisDispatchRuntimeBackend>>,
    runtime_coordinator: Option<Arc<RedisRuntimeCoordinator>>,
    runtime_refresh_backend: Option<Arc<RedisRuntimeRefreshBackend>>,
    state_change_backend: Option<Arc<RedisStateChangeBackend>>,
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
    fn merge_stats(
        &self,
        updates: &HashMap<String, StatsMergeRecord>,
    ) -> anyhow::Result<HashMap<String, StatsEntryRecord>>;
    fn load_balance_cache(&self) -> anyhow::Result<HashMap<u64, CachedBalanceRecord>>;
    fn save_balance_cache(&self, cache: &HashMap<u64, CachedBalanceRecord>) -> anyhow::Result<()>;
    fn persist_dispatch_config(&self, dispatch: &PersistedDispatchConfig) -> anyhow::Result<()>;
    fn compare_and_swap_refreshed_credential(
        &self,
        id: u64,
        expected_refresh_token: Option<&str>,
        credential: &KiroCredentials,
        is_multiple_format: bool,
    ) -> anyhow::Result<CredentialCompareAndSwapResult> {
        let _ = (id, expected_refresh_token, credential, is_multiple_format);
        anyhow::bail!("当前状态后端不支持凭据级 compare-and-swap 更新")
    }
}

impl StateStore {
    pub fn from_config(config: &Config, credentials_path: Option<PathBuf>) -> anyhow::Result<Self> {
        config.validate()?;

        let primary_backend: Arc<dyn StateBackend> = match config.state_backend {
            StateBackendKind::File => Arc::new(FileStateBackend {
                config_path: config.config_path().map(|path| path.to_path_buf()),
                credentials_path,
            }),
            StateBackendKind::Postgres => {
                let postgres_url = config.state_postgres_url.as_deref().ok_or_else(|| {
                    anyhow::anyhow!("stateBackend=postgres 时必须配置 statePostgresUrl")
                })?;
                Arc::new(PostgresStateBackend::connect(postgres_url)?)
            }
        };

        Self::with_redis_support(primary_backend, config)
    }

    pub fn file(config_path: Option<PathBuf>, credentials_path: Option<PathBuf>) -> Self {
        let primary_backend: Arc<dyn StateBackend> = Arc::new(FileStateBackend {
            config_path,
            credentials_path,
        });
        Self {
            primary_backend: primary_backend.clone(),
            balance_cache_backend: BalanceCacheStore::Primary(primary_backend),
            dispatch_runtime_backend: None,
            runtime_coordinator: None,
            runtime_refresh_backend: None,
            state_change_backend: None,
        }
    }

    fn with_redis_support(
        primary_backend: Arc<dyn StateBackend>,
        config: &Config,
    ) -> anyhow::Result<Self> {
        let balance_cache_backend = match config.state_redis_url.as_deref() {
            Some(redis_url) => BalanceCacheStore::Redis(Arc::new(
                RedisBalanceCacheBackend::connect(redis_url, REDIS_BALANCE_CACHE_KEY)?,
            )),
            None => BalanceCacheStore::Primary(primary_backend.clone()),
        };

        let runtime_coordinator = match config.state_redis_url.as_deref() {
            Some(redis_url) => Some(Arc::new(RedisRuntimeCoordinator::connect(
                redis_url,
                REDIS_RUNTIME_COORDINATION_NAMESPACE,
                config.resolved_instance_id(),
                config.resolved_advertise_http_base_url(),
                Duration::from_secs(config.state_redis_heartbeat_interval_secs),
                Duration::from_secs(config.state_redis_leader_lease_ttl_secs),
            )?)),
            None => None,
        };
        let dispatch_runtime_backend = match config.state_redis_url.as_deref() {
            Some(redis_url) => Some(Arc::new(RedisDispatchRuntimeBackend::connect(
                redis_url,
                REDIS_DISPATCH_RUNTIME_NAMESPACE,
            )?)),
            None => None,
        };
        let runtime_refresh_backend = match config.state_redis_url.as_deref() {
            Some(redis_url) => Some(Arc::new(RedisRuntimeRefreshBackend::connect(
                redis_url,
                REDIS_RUNTIME_REFRESH_NAMESPACE,
            )?)),
            None => None,
        };
        let state_change_backend = match config.state_redis_url.as_deref() {
            Some(redis_url) => Some(Arc::new(RedisStateChangeBackend::connect(
                redis_url,
                REDIS_STATE_CHANGE_NAMESPACE,
            )?)),
            None => None,
        };

        Ok(Self {
            primary_backend,
            balance_cache_backend,
            dispatch_runtime_backend,
            runtime_coordinator,
            runtime_refresh_backend,
            state_change_backend,
        })
    }

    pub fn is_external(&self) -> bool {
        self.primary_backend.is_external()
    }

    pub fn load_credentials(&self) -> anyhow::Result<PersistedCredentials> {
        self.primary_backend.load_credentials()
    }

    pub fn load_dispatch_config(&self) -> anyhow::Result<Option<PersistedDispatchConfig>> {
        self.primary_backend.load_dispatch_config()
    }

    pub fn persist_credentials(
        &self,
        credentials: &[KiroCredentials],
        is_multiple_format: bool,
    ) -> anyhow::Result<bool> {
        self.primary_backend
            .persist_credentials(credentials, is_multiple_format)
    }

    pub fn load_stats(&self) -> anyhow::Result<HashMap<String, StatsEntryRecord>> {
        self.primary_backend.load_stats()
    }

    pub fn save_stats(&self, stats: &HashMap<String, StatsEntryRecord>) -> anyhow::Result<()> {
        self.primary_backend.save_stats(stats)
    }

    pub fn merge_stats(
        &self,
        updates: &HashMap<String, StatsMergeRecord>,
    ) -> anyhow::Result<HashMap<String, StatsEntryRecord>> {
        self.primary_backend.merge_stats(updates)
    }

    pub fn load_balance_cache(&self) -> anyhow::Result<HashMap<u64, CachedBalanceRecord>> {
        self.balance_cache_backend.load_balance_cache()
    }

    pub fn save_balance_cache(
        &self,
        cache: &HashMap<u64, CachedBalanceRecord>,
    ) -> anyhow::Result<()> {
        self.balance_cache_backend.save_balance_cache(cache)
    }

    pub fn persist_dispatch_config(
        &self,
        dispatch: &PersistedDispatchConfig,
    ) -> anyhow::Result<()> {
        self.primary_backend.persist_dispatch_config(dispatch)
    }

    pub fn dispatch_runtime_enabled(&self) -> bool {
        self.dispatch_runtime_backend.is_some()
    }

    pub fn load_dispatch_runtime_snapshots(
        &self,
        credentials: &[DispatchRuntimeCredential],
        now_epoch_ms: u64,
    ) -> anyhow::Result<HashMap<u64, DispatchRuntimeSnapshot>> {
        self.dispatch_runtime_backend
            .as_ref()
            .map(|backend| backend.load_snapshots(credentials, now_epoch_ms))
            .transpose()
            .map(|value| value.unwrap_or_default())
    }

    pub fn try_reserve_dispatch_lease(
        &self,
        id: u64,
        lease_id: &str,
        max_concurrency: Option<usize>,
        bucket_policy: Option<DispatchRuntimeBucketPolicy>,
        requested_tokens: f64,
        now_epoch_ms: u64,
        lease_ttl_ms: u64,
    ) -> anyhow::Result<Option<DispatchLeaseReservation>> {
        self.dispatch_runtime_backend
            .as_ref()
            .map(|backend| {
                backend.try_reserve_lease(
                    id,
                    lease_id,
                    max_concurrency,
                    bucket_policy,
                    requested_tokens,
                    now_epoch_ms,
                    lease_ttl_ms,
                )
            })
            .transpose()
    }

    pub fn renew_dispatch_lease(
        &self,
        id: u64,
        lease_id: &str,
        lease_ttl_ms: u64,
    ) -> anyhow::Result<bool> {
        self.dispatch_runtime_backend
            .as_ref()
            .map(|backend| backend.renew_lease(id, lease_id, lease_ttl_ms))
            .transpose()
            .map(|value| value.unwrap_or(false))
    }

    pub fn release_dispatch_lease(
        &self,
        id: u64,
        lease_id: &str,
        now_epoch_ms: u64,
    ) -> anyhow::Result<()> {
        if let Some(backend) = &self.dispatch_runtime_backend {
            backend.release_lease(id, lease_id, now_epoch_ms)?;
        }
        Ok(())
    }

    pub fn record_dispatch_success(
        &self,
        id: u64,
        bucket_policy: Option<DispatchRuntimeBucketPolicy>,
        now_epoch_ms: u64,
    ) -> anyhow::Result<Option<DispatchRuntimeSnapshot>> {
        self.dispatch_runtime_backend
            .as_ref()
            .map(|backend| backend.record_success(id, bucket_policy, now_epoch_ms))
            .transpose()
    }

    pub fn record_dispatch_rate_limited(
        &self,
        id: u64,
        bucket_policy: Option<DispatchRuntimeBucketPolicy>,
        cooldown_ms: u64,
        now_epoch_ms: u64,
    ) -> anyhow::Result<Option<DispatchRuntimeSnapshot>> {
        self.dispatch_runtime_backend
            .as_ref()
            .map(|backend| {
                backend.record_rate_limited(id, bucket_policy, cooldown_ms, now_epoch_ms)
            })
            .transpose()
    }

    pub fn defer_dispatch_credential(
        &self,
        id: u64,
        cooldown_ms: u64,
        now_epoch_ms: u64,
    ) -> anyhow::Result<Option<DispatchRuntimeSnapshot>> {
        self.dispatch_runtime_backend
            .as_ref()
            .map(|backend| backend.defer_credential(id, cooldown_ms, now_epoch_ms))
            .transpose()
    }

    pub fn reset_dispatch_runtime(
        &self,
        id: u64,
        bucket_policy: Option<DispatchRuntimeBucketPolicy>,
        now_epoch_ms: u64,
    ) -> anyhow::Result<Option<DispatchRuntimeSnapshot>> {
        self.dispatch_runtime_backend
            .as_ref()
            .map(|backend| backend.reset_runtime(id, bucket_policy, now_epoch_ms))
            .transpose()
    }

    pub fn clear_dispatch_cooldowns(&self, ids: &[u64]) -> anyhow::Result<()> {
        if let Some(backend) = &self.dispatch_runtime_backend {
            backend.clear_cooldowns(ids)?;
        }
        Ok(())
    }

    pub fn runtime_coordination_enabled(&self) -> bool {
        self.runtime_coordinator.is_some()
    }

    pub fn runtime_refresh_lease_enabled(&self) -> bool {
        self.runtime_refresh_backend.is_some()
    }

    pub fn runtime_coordination_interval(&self) -> Option<Duration> {
        self.runtime_coordinator
            .as_ref()
            .map(|coordinator| coordinator.heartbeat_interval())
    }

    pub fn runtime_coordination_status(&self) -> anyhow::Result<Option<RuntimeCoordinationStatus>> {
        self.runtime_coordinator
            .as_ref()
            .map(|coordinator| coordinator.current_status())
            .transpose()
    }

    pub fn runtime_coordination_tick(&self) -> anyhow::Result<Option<RuntimeCoordinationStatus>> {
        self.runtime_coordinator
            .as_ref()
            .map(|coordinator| coordinator.tick())
            .transpose()
    }

    pub fn runtime_coordination_release(
        &self,
    ) -> anyhow::Result<Option<RuntimeCoordinationStatus>> {
        self.runtime_coordinator
            .as_ref()
            .map(|coordinator| coordinator.release())
            .transpose()
    }

    pub fn try_acquire_runtime_refresh_lease(
        &self,
        credential_id: u64,
        instance_id: &str,
        lease_ttl: Duration,
    ) -> anyhow::Result<Option<RuntimeRefreshLeaseAcquisition>> {
        self.runtime_refresh_backend
            .as_ref()
            .map(|backend| backend.try_acquire(credential_id, instance_id, lease_ttl))
            .transpose()
    }

    pub fn release_runtime_refresh_lease(
        &self,
        lease: &RuntimeRefreshLease,
    ) -> anyhow::Result<Option<bool>> {
        self.runtime_refresh_backend
            .as_ref()
            .map(|backend| backend.release(lease))
            .transpose()
    }

    pub fn compare_and_swap_refreshed_credential(
        &self,
        id: u64,
        expected_refresh_token: Option<&str>,
        credential: &KiroCredentials,
        is_multiple_format: bool,
    ) -> anyhow::Result<CredentialCompareAndSwapResult> {
        let result = self.primary_backend.compare_and_swap_refreshed_credential(
            id,
            expected_refresh_token,
            credential,
            is_multiple_format,
        )?;
        if matches!(result, CredentialCompareAndSwapResult::Applied) {
            if let Err(err) = self.bump_state_change_revision(StateChangeKind::Credentials) {
                tracing::warn!("更新共享凭据修订号失败: {}", err);
            }
        }
        Ok(result)
    }

    pub fn state_change_revisions(&self) -> anyhow::Result<StateChangeRevisions> {
        self.state_change_backend
            .as_ref()
            .map(|backend| backend.load_revisions())
            .transpose()
            .map(|value| value.unwrap_or_default())
    }

    pub fn bump_state_change_revision(&self, kind: StateChangeKind) -> anyhow::Result<u64> {
        self.state_change_backend
            .as_ref()
            .map(|backend| backend.bump_revision(kind))
            .transpose()
            .map(|value| value.unwrap_or_default())
    }
}

fn newer_timestamp(current: Option<String>, candidate: Option<String>) -> Option<String> {
    match (current, candidate) {
        (None, other) | (other, None) => other,
        (Some(current), Some(candidate)) => {
            let current_parsed = DateTime::parse_from_rfc3339(&current).ok();
            let candidate_parsed = DateTime::parse_from_rfc3339(&candidate).ok();

            match (current_parsed, candidate_parsed) {
                (Some(current_parsed), Some(candidate_parsed)) => {
                    if candidate_parsed > current_parsed {
                        Some(candidate)
                    } else {
                        Some(current)
                    }
                }
                _ => {
                    if candidate > current {
                        Some(candidate)
                    } else {
                        Some(current)
                    }
                }
            }
        }
    }
}

fn merge_stats_records(
    mut stats: HashMap<String, StatsEntryRecord>,
    updates: &HashMap<String, StatsMergeRecord>,
) -> HashMap<String, StatsEntryRecord> {
    for (key, update) in updates {
        let entry = stats
            .entry(key.clone())
            .or_insert_with(|| StatsEntryRecord {
                success_count: 0,
                last_used_at: None,
            });
        entry.success_count = entry
            .success_count
            .saturating_add(update.success_count_delta);
        entry.last_used_at =
            newer_timestamp(entry.last_used_at.take(), update.last_used_at.clone());
    }

    stats
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

fn apply_refreshed_credential_compare_and_swap(
    credentials: &mut [KiroCredentials],
    id: u64,
    expected_refresh_token: Option<&str>,
    credential: &KiroCredentials,
) -> CredentialCompareAndSwapResult {
    let Some(current) = credentials.iter_mut().find(|item| item.id == Some(id)) else {
        return CredentialCompareAndSwapResult::Missing;
    };

    if current.refresh_token.as_deref() != expected_refresh_token {
        return CredentialCompareAndSwapResult::Conflict {
            current: current.clone(),
        };
    }

    *current = credential.clone();
    CredentialCompareAndSwapResult::Applied
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
        self.cache_dir()
            .map(|dir| dir.join(BALANCE_CACHE_FILE_NAME))
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

    fn compare_and_swap_refreshed_credential(
        &self,
        id: u64,
        expected_refresh_token: Option<&str>,
        credential: &KiroCredentials,
        is_multiple_format: bool,
    ) -> anyhow::Result<CredentialCompareAndSwapResult> {
        if !is_multiple_format {
            return Ok(CredentialCompareAndSwapResult::Missing);
        }

        let path = match &self.credentials_path {
            Some(path) => path.clone(),
            None => return Ok(CredentialCompareAndSwapResult::Missing),
        };

        let mut credentials = CredentialsConfig::load(&path)
            .with_context(|| format!("加载凭据文件失败: {}", path.display()))?
            .into_sorted_credentials();
        let result = apply_refreshed_credential_compare_and_swap(
            &mut credentials,
            id,
            expected_refresh_token,
            credential,
        );

        if matches!(result, CredentialCompareAndSwapResult::Applied) {
            let json = serde_json::to_vec_pretty(&credentials).context("序列化凭据失败")?;
            Self::write_bytes(&path, json)?;
        }

        Ok(result)
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
                return Err(err).with_context(|| format!("读取统计缓存失败: {}", path.display()));
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

    fn merge_stats(
        &self,
        updates: &HashMap<String, StatsMergeRecord>,
    ) -> anyhow::Result<HashMap<String, StatsEntryRecord>> {
        let merged = merge_stats_records(self.load_stats()?, updates);
        self.save_stats(&merged)?;
        Ok(merged)
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
                return Err(err).with_context(|| format!("读取余额缓存失败: {}", path.display()));
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

        let serializable: HashMap<String, &CachedBalanceRecord> = cache
            .iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect();
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
        f.debug_struct("PostgresStateBackend")
            .finish_non_exhaustive()
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

    fn compare_and_swap_refreshed_credential(
        &self,
        id: u64,
        expected_refresh_token: Option<&str>,
        credential: &KiroCredentials,
        _is_multiple_format: bool,
    ) -> anyhow::Result<CredentialCompareAndSwapResult> {
        let client = Arc::clone(&self.client);
        let expected_refresh_token = expected_refresh_token.map(str::to_owned);
        let mut next_credential = credential.clone();
        next_credential.canonicalize_auth_method();
        next_credential.normalize_model_capabilities();

        run_blocking_state_op(move || {
            let mut client = client.lock();
            let mut transaction = client
                .transaction()
                .context("开启 PostgreSQL 凭据 compare-and-swap 事务失败")?;
            let row = transaction
                .query_opt(
                    "SELECT value FROM kiro_state_store WHERE namespace = $1 AND key = $2 FOR UPDATE",
                    &[&POSTGRES_NAMESPACE, &POSTGRES_CREDENTIALS_KEY],
                )
                .context("锁定 PostgreSQL 凭据列表失败")?;

            let Some(row) = row else {
                transaction
                    .commit()
                    .context("提交空 PostgreSQL 凭据 compare-and-swap 事务失败")?;
                return Ok(CredentialCompareAndSwapResult::Missing);
            };

            let payload: String = row.get(0);
            let mut credentials: Vec<KiroCredentials> =
                serde_json::from_str(&payload).context("解析 PostgreSQL 凭据列表失败")?;
            let result = apply_refreshed_credential_compare_and_swap(
                &mut credentials,
                id,
                expected_refresh_token.as_deref(),
                &next_credential,
            );

            if matches!(result, CredentialCompareAndSwapResult::Applied) {
                let payload = serde_json::to_string(&credentials)
                    .context("序列化 PostgreSQL 凭据列表失败")?;
                transaction
                    .execute(
                        r#"
                        UPDATE kiro_state_store
                        SET value = $3, updated_at = NOW()
                        WHERE namespace = $1 AND key = $2
                        "#,
                        &[&POSTGRES_NAMESPACE, &POSTGRES_CREDENTIALS_KEY, &payload],
                    )
                    .context("更新 PostgreSQL 凭据列表失败")?;
            }

            transaction
                .commit()
                .context("提交 PostgreSQL 凭据 compare-and-swap 事务失败")?;
            Ok(result)
        })
    }

    fn load_stats(&self) -> anyhow::Result<HashMap<String, StatsEntryRecord>> {
        Ok(self
            .load_json(POSTGRES_STATS_KEY, "统计缓存")?
            .unwrap_or_default())
    }

    fn save_stats(&self, stats: &HashMap<String, StatsEntryRecord>) -> anyhow::Result<()> {
        self.save_json(POSTGRES_STATS_KEY, stats, "统计缓存")
    }

    fn merge_stats(
        &self,
        updates: &HashMap<String, StatsMergeRecord>,
    ) -> anyhow::Result<HashMap<String, StatsEntryRecord>> {
        let client = Arc::clone(&self.client);
        let updates = updates.clone();

        run_blocking_state_op(move || {
            let mut client = client.lock();
            let mut transaction = client
                .transaction()
                .context("开启 PostgreSQL 统计事务失败")?;
            let empty_payload = "{}".to_string();

            transaction
                .execute(
                    r#"
                    INSERT INTO kiro_state_store (namespace, key, value, updated_at)
                    VALUES ($1, $2, $3, NOW())
                    ON CONFLICT (namespace, key) DO NOTHING
                    "#,
                    &[&POSTGRES_NAMESPACE, &POSTGRES_STATS_KEY, &empty_payload],
                )
                .context("初始化 PostgreSQL 统计缓存行失败")?;

            let row = transaction
                .query_one(
                    "SELECT value FROM kiro_state_store WHERE namespace = $1 AND key = $2 FOR UPDATE",
                    &[&POSTGRES_NAMESPACE, &POSTGRES_STATS_KEY],
                )
                .context("锁定 PostgreSQL 统计缓存失败")?;
            let payload: String = row.get(0);
            let stats: HashMap<String, StatsEntryRecord> =
                serde_json::from_str(&payload).context("解析 PostgreSQL 统计缓存失败")?;
            let merged = merge_stats_records(stats, &updates);
            let merged_payload =
                serde_json::to_string(&merged).context("序列化 PostgreSQL 合并统计失败")?;

            transaction
                .execute(
                    r#"
                    UPDATE kiro_state_store
                    SET value = $3, updated_at = NOW()
                    WHERE namespace = $1 AND key = $2
                    "#,
                    &[&POSTGRES_NAMESPACE, &POSTGRES_STATS_KEY, &merged_payload],
                )
                .context("写回 PostgreSQL 合并统计失败")?;

            transaction
                .commit()
                .context("提交 PostgreSQL 统计事务失败")?;
            Ok(merged)
        })
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

#[derive(Debug)]
struct RedisBalanceCacheBackend {
    client: RedisClient,
    key: String,
}

impl RedisBalanceCacheBackend {
    fn connect(redis_url: &str, key: &str) -> anyhow::Result<Self> {
        let client = RedisClient::open(redis_url).context("初始化 Redis 余额缓存客户端失败")?;
        Ok(Self {
            client,
            key: key.to_string(),
        })
    }

    fn load_balance_cache(&self) -> anyhow::Result<HashMap<u64, CachedBalanceRecord>> {
        let client = self.client.clone();
        let key = self.key.clone();

        run_blocking_state_op(
            move || -> anyhow::Result<HashMap<u64, CachedBalanceRecord>> {
                let mut connection = client.get_connection().context("连接 Redis 余额缓存失败")?;
                let payload: Option<String> = connection
                    .get(&key)
                    .with_context(|| format!("从 Redis 读取余额缓存失败: {key}"))?;

                let Some(payload) = payload else {
                    return Ok(HashMap::new());
                };

                let cache: HashMap<String, CachedBalanceRecord> = serde_json::from_str(&payload)
                    .with_context(|| format!("解析 Redis 余额缓存失败: {key}"))?;
                Ok(cache
                    .into_iter()
                    .filter_map(|(raw_key, value)| {
                        raw_key.parse::<u64>().ok().map(|id| (id, value))
                    })
                    .collect())
            },
        )
    }

    fn save_balance_cache(&self, cache: &HashMap<u64, CachedBalanceRecord>) -> anyhow::Result<()> {
        let serializable: HashMap<String, &CachedBalanceRecord> = cache
            .iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect();
        let payload = serde_json::to_string(&serializable).context("序列化 Redis 余额缓存失败")?;
        let client = self.client.clone();
        let key = self.key.clone();

        run_blocking_state_op(move || -> anyhow::Result<()> {
            let mut connection = client.get_connection().context("连接 Redis 余额缓存失败")?;
            let _: () = connection
                .set_ex(&key, payload, REDIS_BALANCE_CACHE_TTL_SECS)
                .with_context(|| format!("写入 Redis 余额缓存失败: {key}"))?;
            Ok(())
        })
    }
}

#[derive(Debug)]
struct RedisStateChangeBackend {
    client: RedisClient,
    namespace: String,
}

impl RedisStateChangeBackend {
    fn connect(redis_url: &str, namespace: &str) -> anyhow::Result<Self> {
        let client = RedisClient::open(redis_url).context("初始化 Redis 状态变更客户端失败")?;
        Ok(Self {
            client,
            namespace: namespace.to_string(),
        })
    }

    fn revision_key(&self, kind: StateChangeKind) -> String {
        let key = match kind {
            StateChangeKind::Credentials => REDIS_STATE_CHANGE_CREDENTIALS_KEY,
            StateChangeKind::DispatchConfig => REDIS_STATE_CHANGE_DISPATCH_KEY,
            StateChangeKind::BalanceCache => REDIS_STATE_CHANGE_BALANCE_CACHE_KEY,
        };
        format!("{}:{}", self.namespace, key)
    }

    fn load_revisions(&self) -> anyhow::Result<StateChangeRevisions> {
        let client = self.client.clone();
        let credentials_key = self.revision_key(StateChangeKind::Credentials);
        let dispatch_key = self.revision_key(StateChangeKind::DispatchConfig);
        let balance_cache_key = self.revision_key(StateChangeKind::BalanceCache);

        run_blocking_state_op(move || -> anyhow::Result<StateChangeRevisions> {
            let mut connection = client.get_connection().context("连接 Redis 状态变更失败")?;
            let credentials = connection
                .get::<_, Option<u64>>(&credentials_key)
                .with_context(|| format!("读取 Redis 状态变更版本失败: {credentials_key}"))?
                .unwrap_or_default();
            let dispatch_config = connection
                .get::<_, Option<u64>>(&dispatch_key)
                .with_context(|| format!("读取 Redis 状态变更版本失败: {dispatch_key}"))?
                .unwrap_or_default();
            let balance_cache = connection
                .get::<_, Option<u64>>(&balance_cache_key)
                .with_context(|| format!("读取 Redis 状态变更版本失败: {balance_cache_key}"))?
                .unwrap_or_default();
            Ok(StateChangeRevisions {
                credentials,
                dispatch_config,
                balance_cache,
            })
        })
    }

    fn bump_revision(&self, kind: StateChangeKind) -> anyhow::Result<u64> {
        let client = self.client.clone();
        let key = self.revision_key(kind);

        run_blocking_state_op(move || -> anyhow::Result<u64> {
            let mut connection = client.get_connection().context("连接 Redis 状态变更失败")?;
            connection
                .incr(&key, 1_u64)
                .with_context(|| format!("递增 Redis 状态变更版本失败: {key}"))
        })
    }
}

#[derive(Debug)]
struct RedisRuntimeCoordinator {
    client: RedisClient,
    namespace: String,
    instance_id: String,
    advertise_http_base_url: Option<String>,
    heartbeat_interval: Duration,
    leader_lease_ttl: Duration,
}

impl RedisRuntimeCoordinator {
    fn connect(
        redis_url: &str,
        namespace: &str,
        instance_id: String,
        advertise_http_base_url: Option<String>,
        heartbeat_interval: Duration,
        leader_lease_ttl: Duration,
    ) -> anyhow::Result<Self> {
        let client = RedisClient::open(redis_url).context("初始化 Redis 运行时协调客户端失败")?;
        Ok(Self {
            client,
            namespace: namespace.to_string(),
            instance_id,
            advertise_http_base_url,
            heartbeat_interval,
            leader_lease_ttl,
        })
    }

    fn heartbeat_interval(&self) -> Duration {
        self.heartbeat_interval
    }

    fn tick(&self) -> anyhow::Result<RuntimeCoordinationStatus> {
        self.publish_heartbeat()?;
        self.acquire_or_renew_leader()
    }

    fn current_status(&self) -> anyhow::Result<RuntimeCoordinationStatus> {
        let client = self.client.clone();
        let leader_key = self.leader_key();
        let namespace = self.namespace.clone();
        let instance_id = self.instance_id.clone();

        run_blocking_state_op(move || -> anyhow::Result<RuntimeCoordinationStatus> {
            let mut connection = client
                .get_connection()
                .context("连接 Redis 运行时协调失败")?;
            let leader_id: Option<String> = connection
                .get(&leader_key)
                .with_context(|| format!("读取 Redis Leader 状态失败: {leader_key}"))?;
            let leader_http_base_url = Self::load_advertise_http_base_url(
                &mut connection,
                &namespace,
                leader_id.as_deref(),
            )?;

            Ok(RuntimeCoordinationStatus {
                is_leader: leader_id.as_deref() == Some(instance_id.as_str()),
                leader_http_base_url,
                leader_id,
                instance_id,
            })
        })
    }

    fn publish_heartbeat(&self) -> anyhow::Result<()> {
        let client = self.client.clone();
        let instance_key = self.instance_key();
        let ttl_secs = self
            .leader_lease_ttl_secs()
            .max(self.heartbeat_interval_secs().saturating_mul(3));
        let payload = serde_json::to_string(&RuntimeInstanceHeartbeat {
            instance_id: self.instance_id.clone(),
            observed_at_epoch_secs: current_epoch_secs(),
            advertise_http_base_url: self.advertise_http_base_url.clone(),
        })
        .context("序列化 Redis 运行时心跳失败")?;

        run_blocking_state_op(move || -> anyhow::Result<()> {
            let mut connection = client
                .get_connection()
                .context("连接 Redis 运行时协调失败")?;
            let _: () = connection
                .set_ex(&instance_key, payload, ttl_secs)
                .with_context(|| format!("写入 Redis 实例心跳失败: {instance_key}"))?;
            Ok(())
        })
    }

    fn acquire_or_renew_leader(&self) -> anyhow::Result<RuntimeCoordinationStatus> {
        let client = self.client.clone();
        let leader_key = self.leader_key();
        let namespace = self.namespace.clone();
        let instance_id = self.instance_id.clone();
        let lease_ttl_secs = self.leader_lease_ttl_secs();

        run_blocking_state_op(move || -> anyhow::Result<RuntimeCoordinationStatus> {
            let mut connection = client
                .get_connection()
                .context("连接 Redis 运行时协调失败")?;
            let response: Vec<String> = Script::new(REDIS_RUNTIME_LEADER_SCRIPT)
                .key(&leader_key)
                .arg(&instance_id)
                .arg(lease_ttl_secs)
                .invoke(&mut connection)
                .with_context(|| format!("更新 Redis Leader 租约失败: {leader_key}"))?;

            let status = response.first().map(String::as_str).unwrap_or_default();
            let leader_id = response.get(1).cloned();
            let leader_http_base_url = Self::load_advertise_http_base_url(
                &mut connection,
                &namespace,
                leader_id.as_deref(),
            )?;
            Ok(RuntimeCoordinationStatus {
                instance_id: instance_id.clone(),
                is_leader: matches!(status, "acquired" | "renewed"),
                leader_http_base_url,
                leader_id,
            })
        })
    }

    fn release(&self) -> anyhow::Result<RuntimeCoordinationStatus> {
        let client = self.client.clone();
        let leader_key = self.leader_key();
        let instance_key = self.instance_key();
        let namespace = self.namespace.clone();
        let instance_id = self.instance_id.clone();

        run_blocking_state_op(move || -> anyhow::Result<RuntimeCoordinationStatus> {
            let mut connection = client
                .get_connection()
                .context("连接 Redis 运行时协调失败")?;
            let response: Vec<String> = Script::new(REDIS_RUNTIME_RELEASE_SCRIPT)
                .key(&leader_key)
                .key(&instance_key)
                .arg(&instance_id)
                .invoke(&mut connection)
                .with_context(|| format!("释放 Redis Leader 租约失败: {leader_key}"))?;

            let status = response.first().map(String::as_str).unwrap_or_default();
            let leader_id = response.get(1).filter(|value| !value.is_empty()).cloned();
            let leader_http_base_url = if status == "released" {
                None
            } else {
                Self::load_advertise_http_base_url(
                    &mut connection,
                    &namespace,
                    leader_id.as_deref(),
                )?
            };
            Ok(RuntimeCoordinationStatus {
                instance_id,
                is_leader: false,
                leader_http_base_url,
                leader_id: if status == "released" {
                    None
                } else {
                    leader_id
                },
            })
        })
    }

    fn heartbeat_interval_secs(&self) -> u64 {
        self.heartbeat_interval.as_secs()
    }

    fn leader_lease_ttl_secs(&self) -> u64 {
        self.leader_lease_ttl.as_secs()
    }

    fn leader_key(&self) -> String {
        format!("{}:{REDIS_RUNTIME_LEADER_KEY}", self.namespace)
    }

    fn instance_key(&self) -> String {
        format!(
            "{}:{REDIS_RUNTIME_INSTANCE_KEY_PREFIX}:{}",
            self.namespace, self.instance_id
        )
    }

    fn instance_key_for(namespace: &str, instance_id: &str) -> String {
        format!("{namespace}:{REDIS_RUNTIME_INSTANCE_KEY_PREFIX}:{instance_id}")
    }

    fn load_advertise_http_base_url(
        connection: &mut redis::Connection,
        namespace: &str,
        leader_id: Option<&str>,
    ) -> anyhow::Result<Option<String>> {
        let Some(leader_id) = leader_id else {
            return Ok(None);
        };

        let instance_key = Self::instance_key_for(namespace, leader_id);
        let payload: Option<String> = connection
            .get(&instance_key)
            .with_context(|| format!("读取 Redis 实例心跳失败: {instance_key}"))?;
        let Some(payload) = payload else {
            return Ok(None);
        };

        let heartbeat: RuntimeInstanceHeartbeat = serde_json::from_str(&payload)
            .with_context(|| format!("解析 Redis 实例心跳失败: {instance_key}"))?;
        Ok(heartbeat.advertise_http_base_url)
    }
}

fn current_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[derive(Debug)]
struct RedisRuntimeRefreshBackend {
    client: RedisClient,
    namespace: String,
}

impl RedisRuntimeRefreshBackend {
    fn connect(redis_url: &str, namespace: &str) -> anyhow::Result<Self> {
        let client = RedisClient::open(redis_url).context("初始化 Redis 凭据刷新协调客户端失败")?;
        Ok(Self {
            client,
            namespace: namespace.to_string(),
        })
    }

    fn try_acquire(
        &self,
        credential_id: u64,
        instance_id: &str,
        lease_ttl: Duration,
    ) -> anyhow::Result<RuntimeRefreshLeaseAcquisition> {
        let client = self.client.clone();
        let lease_key = self.lease_key(credential_id);
        let lease_record = RuntimeRefreshLeaseRecord {
            lease_id: uuid::Uuid::new_v4().to_string(),
            instance_id: instance_id.to_string(),
        };
        let lease_value =
            serde_json::to_string(&lease_record).context("序列化 Redis 凭据刷新租约失败")?;
        let lease_ttl_ms = lease_ttl.as_millis().clamp(1, u128::from(u64::MAX)) as u64;

        run_blocking_state_op(move || -> anyhow::Result<RuntimeRefreshLeaseAcquisition> {
            let mut connection = client
                .get_connection()
                .context("连接 Redis 凭据刷新协调失败")?;
            let response: Vec<String> = Script::new(REDIS_RUNTIME_REFRESH_LEASE_SCRIPT)
                .key(&lease_key)
                .arg(&lease_value)
                .arg(lease_ttl_ms)
                .invoke(&mut connection)
                .with_context(|| format!("获取 Redis 凭据刷新租约失败: {lease_key}"))?;

            let status = response.first().map(String::as_str).unwrap_or_default();
            let payload = response.get(1).cloned().unwrap_or_default();
            if matches!(status, "acquired" | "renewed") {
                return Ok(RuntimeRefreshLeaseAcquisition::Acquired(
                    RuntimeRefreshLease {
                        credential_id,
                        lease_value,
                    },
                ));
            }

            let owner_instance_id = serde_json::from_str::<RuntimeRefreshLeaseRecord>(&payload)
                .ok()
                .map(|record| record.instance_id);
            Ok(RuntimeRefreshLeaseAcquisition::HeldByPeer { owner_instance_id })
        })
    }

    fn release(&self, lease: &RuntimeRefreshLease) -> anyhow::Result<bool> {
        let client = self.client.clone();
        let lease_key = self.lease_key(lease.credential_id);
        let lease_value = lease.lease_value.clone();

        run_blocking_state_op(move || -> anyhow::Result<bool> {
            let mut connection = client
                .get_connection()
                .context("连接 Redis 凭据刷新协调失败")?;
            let response: Vec<String> = Script::new(REDIS_RUNTIME_REFRESH_RELEASE_SCRIPT)
                .key(&lease_key)
                .arg(&lease_value)
                .invoke(&mut connection)
                .with_context(|| format!("释放 Redis 凭据刷新租约失败: {lease_key}"))?;
            Ok(matches!(
                response.first().map(String::as_str).unwrap_or_default(),
                "released" | "empty"
            ))
        })
    }

    fn lease_key(&self, credential_id: u64) -> String {
        format!("{}:credential:{credential_id}:lease", self.namespace)
    }
}

pub fn current_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

#[derive(Debug, Clone, Default)]
struct DispatchRuntimeRecord {
    active_requests: usize,
    cooldown_until_epoch_ms: Option<u64>,
    rate_limit_hit_streak: u32,
    bucket_tokens: Option<f64>,
    bucket_capacity: Option<f64>,
    bucket_current_refill_per_second: Option<f64>,
    bucket_base_refill_per_second: Option<f64>,
    bucket_last_refill_at_epoch_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
struct DispatchRuntimeBucketState {
    tokens: f64,
    capacity: f64,
    current_refill_per_second: f64,
    base_refill_per_second: f64,
}

impl DispatchRuntimeRecord {
    fn normalize(
        &self,
        bucket_policy: Option<DispatchRuntimeBucketPolicy>,
        now_epoch_ms: u64,
    ) -> DispatchRuntimeSnapshot {
        let cooldown_until_epoch_ms = self
            .cooldown_until_epoch_ms
            .filter(|until| *until > now_epoch_ms);
        let bucket = self.normalize_bucket(bucket_policy, now_epoch_ms);
        let bucket_ready_at_epoch_ms = bucket.and_then(|state| {
            if state.tokens >= 1.0 {
                return None;
            }
            let missing_tokens = 1.0 - state.tokens;
            let wait_ms = (missing_tokens / state.current_refill_per_second * 1000.0).ceil();
            Some(now_epoch_ms.saturating_add(wait_ms.max(0.0) as u64))
        });

        let next_ready_at_epoch_ms = match (cooldown_until_epoch_ms, bucket_ready_at_epoch_ms) {
            (Some(cooldown_until), Some(bucket_ready_at)) => {
                Some(cooldown_until.max(bucket_ready_at))
            }
            (Some(cooldown_until), None) => Some(cooldown_until),
            (None, Some(bucket_ready_at)) => Some(bucket_ready_at),
            (None, None) => None,
        };

        DispatchRuntimeSnapshot {
            active_requests: self.active_requests,
            cooldown_until_epoch_ms,
            rate_limit_hit_streak: self.rate_limit_hit_streak,
            bucket_tokens: bucket.map(|state| state.tokens),
            bucket_capacity: bucket.map(|state| state.capacity),
            bucket_current_refill_per_second: bucket.map(|state| state.current_refill_per_second),
            bucket_base_refill_per_second: bucket.map(|state| state.base_refill_per_second),
            next_ready_at_epoch_ms,
        }
    }

    fn normalize_bucket(
        &self,
        bucket_policy: Option<DispatchRuntimeBucketPolicy>,
        now_epoch_ms: u64,
    ) -> Option<DispatchRuntimeBucketState> {
        let policy = bucket_policy?;
        let Some(mut tokens) = self.bucket_tokens else {
            return Some(DispatchRuntimeBucketState {
                tokens: policy.capacity,
                capacity: policy.capacity,
                current_refill_per_second: policy.refill_per_second,
                base_refill_per_second: policy.refill_per_second,
            });
        };

        let mut current_refill_per_second = self
            .bucket_current_refill_per_second
            .unwrap_or(policy.refill_per_second);
        let last_refill_at_epoch_ms = self.bucket_last_refill_at_epoch_ms.unwrap_or(now_epoch_ms);
        let stored_capacity = self
            .bucket_capacity
            .filter(|value| *value > 0.0)
            .unwrap_or(policy.capacity);
        let stored_base_refill_per_second = self
            .bucket_base_refill_per_second
            .filter(|value| *value > 0.0)
            .unwrap_or(policy.refill_per_second);

        if now_epoch_ms > last_refill_at_epoch_ms {
            let elapsed_seconds = (now_epoch_ms - last_refill_at_epoch_ms) as f64 / 1000.0;
            tokens = (tokens + elapsed_seconds * current_refill_per_second).min(stored_capacity);
        }

        let token_ratio = if stored_capacity > 0.0 {
            (tokens / stored_capacity).clamp(0.0, 1.0)
        } else {
            1.0
        };
        let refill_ratio = if stored_base_refill_per_second > 0.0 {
            (current_refill_per_second / stored_base_refill_per_second).clamp(0.0, 1.0)
        } else {
            1.0
        };

        tokens = (token_ratio * policy.capacity).clamp(0.0, policy.capacity);
        current_refill_per_second = (policy.refill_per_second * refill_ratio)
            .clamp(policy.min_refill_per_second, policy.refill_per_second);

        Some(DispatchRuntimeBucketState {
            tokens,
            capacity: policy.capacity,
            current_refill_per_second,
            base_refill_per_second: policy.refill_per_second,
        })
    }
}

#[derive(Debug)]
struct RedisDispatchRuntimeBackend {
    client: RedisClient,
    namespace: String,
}

impl RedisDispatchRuntimeBackend {
    fn connect(redis_url: &str, namespace: &str) -> anyhow::Result<Self> {
        let client = RedisClient::open(redis_url).context("初始化 Redis 调度热态客户端失败")?;
        Ok(Self {
            client,
            namespace: namespace.to_string(),
        })
    }

    fn state_key(&self, id: u64) -> String {
        format!(
            "{}:credential:{}:{}",
            self.namespace, id, REDIS_DISPATCH_RUNTIME_STATE_KEY_SUFFIX
        )
    }

    fn leases_key(&self, id: u64) -> String {
        format!(
            "{}:credential:{}:{}",
            self.namespace, id, REDIS_DISPATCH_RUNTIME_LEASES_KEY_SUFFIX
        )
    }

    fn load_snapshots(
        &self,
        credentials: &[DispatchRuntimeCredential],
        now_epoch_ms: u64,
    ) -> anyhow::Result<HashMap<u64, DispatchRuntimeSnapshot>> {
        if credentials.is_empty() {
            return Ok(HashMap::new());
        }

        let client = self.client.clone();
        let namespace = self.namespace.clone();
        let credentials = credentials.to_vec();

        run_blocking_state_op(
            move || -> anyhow::Result<HashMap<u64, DispatchRuntimeSnapshot>> {
                let mut connection = client.get_connection().context("连接 Redis 调度热态失败")?;
                let mut snapshots = HashMap::with_capacity(credentials.len());

                for credential in credentials {
                    let state_key = format!(
                        "{}:credential:{}:{}",
                        namespace, credential.id, REDIS_DISPATCH_RUNTIME_STATE_KEY_SUFFIX
                    );
                    let leases_key = format!(
                        "{}:credential:{}:{}",
                        namespace, credential.id, REDIS_DISPATCH_RUNTIME_LEASES_KEY_SUFFIX
                    );
                    let record =
                        Self::load_record(&mut connection, &state_key, &leases_key, now_epoch_ms)?;
                    snapshots.insert(
                        credential.id,
                        record.normalize(credential.bucket_policy, now_epoch_ms),
                    );
                }

                Ok(snapshots)
            },
        )
    }

    fn try_reserve_lease(
        &self,
        id: u64,
        lease_id: &str,
        max_concurrency: Option<usize>,
        bucket_policy: Option<DispatchRuntimeBucketPolicy>,
        requested_tokens: f64,
        now_epoch_ms: u64,
        lease_ttl_ms: u64,
    ) -> anyhow::Result<DispatchLeaseReservation> {
        let client = self.client.clone();
        let state_key = self.state_key(id);
        let leases_key = self.leases_key(id);
        let lease_id = lease_id.to_string();
        let max_concurrency = max_concurrency
            .and_then(|value| i64::try_from(value).ok())
            .unwrap_or(-1);
        let (bucket_enabled, capacity, refill_per_second, min_refill_per_second) =
            Self::bucket_script_args(bucket_policy);
        let lease_expires_at_ms = now_epoch_ms.saturating_add(lease_ttl_ms.max(1));

        run_blocking_state_op(move || -> anyhow::Result<DispatchLeaseReservation> {
            let mut connection = client.get_connection().context("连接 Redis 调度热态失败")?;
            let response: Vec<String> = Script::new(REDIS_DISPATCH_RUNTIME_RESERVE_SCRIPT)
                .key(&state_key)
                .key(&leases_key)
                .arg(now_epoch_ms)
                .arg(&lease_id)
                .arg(lease_expires_at_ms)
                .arg(max_concurrency)
                .arg(if bucket_enabled { 1 } else { 0 })
                .arg(capacity)
                .arg(refill_per_second)
                .arg(min_refill_per_second)
                .arg(requested_tokens)
                .invoke(&mut connection)
                .with_context(|| format!("更新 Redis 调度占位失败: {state_key}"))?;
            Self::parse_reservation_response(response)
        })
    }

    fn renew_lease(&self, id: u64, lease_id: &str, lease_ttl_ms: u64) -> anyhow::Result<bool> {
        let client = self.client.clone();
        let leases_key = self.leases_key(id);
        let lease_id = lease_id.to_string();
        let expires_at_ms = current_epoch_ms().saturating_add(lease_ttl_ms.max(1));

        run_blocking_state_op(move || -> anyhow::Result<bool> {
            let mut connection = client.get_connection().context("连接 Redis 调度热态失败")?;
            let updated: usize = redis::cmd("ZADD")
                .arg(&leases_key)
                .arg("XX")
                .arg("CH")
                .arg(expires_at_ms)
                .arg(&lease_id)
                .query(&mut connection)
                .with_context(|| format!("续租 Redis 调度占位失败: {leases_key}"))?;

            if updated > 0 {
                let _: bool = redis::cmd("PEXPIRE")
                    .arg(&leases_key)
                    .arg(lease_ttl_ms.max(1))
                    .query(&mut connection)
                    .with_context(|| format!("更新 Redis 调度占位 TTL 失败: {leases_key}"))?;
                return Ok(true);
            }

            Ok(false)
        })
    }

    fn release_lease(&self, id: u64, lease_id: &str, now_epoch_ms: u64) -> anyhow::Result<()> {
        let client = self.client.clone();
        let leases_key = self.leases_key(id);
        let lease_id = lease_id.to_string();

        run_blocking_state_op(move || -> anyhow::Result<()> {
            let mut connection = client.get_connection().context("连接 Redis 调度热态失败")?;
            let _: usize = redis::cmd("ZREMRANGEBYSCORE")
                .arg(&leases_key)
                .arg("-inf")
                .arg(now_epoch_ms)
                .query(&mut connection)
                .with_context(|| format!("清理过期 Redis 调度占位失败: {leases_key}"))?;
            let _: usize = redis::cmd("ZREM")
                .arg(&leases_key)
                .arg(&lease_id)
                .query(&mut connection)
                .with_context(|| format!("释放 Redis 调度占位失败: {leases_key}"))?;
            let remaining: usize = redis::cmd("ZCARD")
                .arg(&leases_key)
                .query(&mut connection)
                .with_context(|| format!("读取 Redis 调度占位计数失败: {leases_key}"))?;
            if remaining == 0 {
                let _: usize = redis::cmd("DEL")
                    .arg(&leases_key)
                    .query(&mut connection)
                    .with_context(|| format!("清理空 Redis 调度占位键失败: {leases_key}"))?;
            }
            Ok(())
        })
    }

    fn record_success(
        &self,
        id: u64,
        bucket_policy: Option<DispatchRuntimeBucketPolicy>,
        now_epoch_ms: u64,
    ) -> anyhow::Result<DispatchRuntimeSnapshot> {
        let client = self.client.clone();
        let state_key = self.state_key(id);
        let (bucket_enabled, capacity, refill_per_second, min_refill_per_second) =
            Self::bucket_script_args(bucket_policy);
        let recovery_step_per_success = bucket_policy
            .map(|policy| policy.recovery_step_per_success)
            .unwrap_or_default();

        run_blocking_state_op(move || -> anyhow::Result<DispatchRuntimeSnapshot> {
            let mut connection = client.get_connection().context("连接 Redis 调度热态失败")?;
            let response: Vec<String> = Script::new(REDIS_DISPATCH_RUNTIME_SUCCESS_SCRIPT)
                .key(&state_key)
                .arg(now_epoch_ms)
                .arg(if bucket_enabled { 1 } else { 0 })
                .arg(capacity)
                .arg(refill_per_second)
                .arg(min_refill_per_second)
                .arg(recovery_step_per_success)
                .invoke(&mut connection)
                .with_context(|| format!("更新 Redis 调度成功态失败: {state_key}"))?;
            let (_, snapshot) = Self::parse_runtime_response(response)?;
            Ok(snapshot)
        })
    }

    fn record_rate_limited(
        &self,
        id: u64,
        bucket_policy: Option<DispatchRuntimeBucketPolicy>,
        cooldown_ms: u64,
        now_epoch_ms: u64,
    ) -> anyhow::Result<DispatchRuntimeSnapshot> {
        let client = self.client.clone();
        let state_key = self.state_key(id);
        let (bucket_enabled, capacity, refill_per_second, min_refill_per_second) =
            Self::bucket_script_args(bucket_policy);
        let backoff_factor = bucket_policy
            .map(|policy| policy.backoff_factor)
            .unwrap_or(1.0);

        run_blocking_state_op(move || -> anyhow::Result<DispatchRuntimeSnapshot> {
            let mut connection = client.get_connection().context("连接 Redis 调度热态失败")?;
            let response: Vec<String> = Script::new(REDIS_DISPATCH_RUNTIME_RATE_LIMIT_SCRIPT)
                .key(&state_key)
                .arg(now_epoch_ms)
                .arg(cooldown_ms)
                .arg(if bucket_enabled { 1 } else { 0 })
                .arg(capacity)
                .arg(refill_per_second)
                .arg(min_refill_per_second)
                .arg(backoff_factor)
                .invoke(&mut connection)
                .with_context(|| format!("更新 Redis 调度限流态失败: {state_key}"))?;
            let (_, snapshot) = Self::parse_runtime_response(response)?;
            Ok(snapshot)
        })
    }

    fn defer_credential(
        &self,
        id: u64,
        cooldown_ms: u64,
        now_epoch_ms: u64,
    ) -> anyhow::Result<DispatchRuntimeSnapshot> {
        let client = self.client.clone();
        let state_key = self.state_key(id);

        run_blocking_state_op(move || -> anyhow::Result<DispatchRuntimeSnapshot> {
            let mut connection = client.get_connection().context("连接 Redis 调度热态失败")?;
            let response: Vec<String> = Script::new(REDIS_DISPATCH_RUNTIME_DEFER_SCRIPT)
                .key(&state_key)
                .arg(now_epoch_ms)
                .arg(cooldown_ms)
                .invoke(&mut connection)
                .with_context(|| format!("更新 Redis 调度临时冷却失败: {state_key}"))?;
            let (_, snapshot) = Self::parse_runtime_response(response)?;
            Ok(snapshot)
        })
    }

    fn reset_runtime(
        &self,
        id: u64,
        bucket_policy: Option<DispatchRuntimeBucketPolicy>,
        now_epoch_ms: u64,
    ) -> anyhow::Result<DispatchRuntimeSnapshot> {
        let client = self.client.clone();
        let state_key = self.state_key(id);
        let (bucket_enabled, capacity, refill_per_second, _) =
            Self::bucket_script_args(bucket_policy);

        run_blocking_state_op(move || -> anyhow::Result<DispatchRuntimeSnapshot> {
            let mut connection = client.get_connection().context("连接 Redis 调度热态失败")?;
            let response: Vec<String> = Script::new(REDIS_DISPATCH_RUNTIME_RESET_SCRIPT)
                .key(&state_key)
                .arg(now_epoch_ms)
                .arg(if bucket_enabled { 1 } else { 0 })
                .arg(capacity)
                .arg(refill_per_second)
                .invoke(&mut connection)
                .with_context(|| format!("重置 Redis 调度运行态失败: {state_key}"))?;
            let (_, snapshot) = Self::parse_runtime_response(response)?;
            Ok(snapshot)
        })
    }

    fn clear_cooldowns(&self, ids: &[u64]) -> anyhow::Result<()> {
        if ids.is_empty() {
            return Ok(());
        }

        let client = self.client.clone();
        let keys: Vec<String> = ids.iter().map(|id| self.state_key(*id)).collect();
        run_blocking_state_op(move || -> anyhow::Result<()> {
            let mut connection = client.get_connection().context("连接 Redis 调度热态失败")?;
            for key in keys {
                let _: usize = redis::cmd("HDEL")
                    .arg(&key)
                    .arg("cooldown_until_ms")
                    .query(&mut connection)
                    .with_context(|| format!("清理 Redis 调度冷却失败: {key}"))?;
            }
            Ok(())
        })
    }

    fn load_record(
        connection: &mut redis::Connection,
        state_key: &str,
        leases_key: &str,
        now_epoch_ms: u64,
    ) -> anyhow::Result<DispatchRuntimeRecord> {
        let _: usize = redis::cmd("ZREMRANGEBYSCORE")
            .arg(leases_key)
            .arg("-inf")
            .arg(now_epoch_ms)
            .query(connection)
            .with_context(|| format!("清理过期 Redis 调度占位失败: {leases_key}"))?;
        let active_requests: usize = redis::cmd("ZCARD")
            .arg(leases_key)
            .query(connection)
            .with_context(|| format!("读取 Redis 调度占位计数失败: {leases_key}"))?;
        let values: Vec<Option<String>> = redis::cmd("HMGET")
            .arg(state_key)
            .arg("cooldown_until_ms")
            .arg("rate_limit_hit_streak")
            .arg("bucket_tokens")
            .arg("bucket_current_refill_per_second")
            .arg("bucket_last_refill_at_ms")
            .arg("bucket_capacity")
            .arg("bucket_base_refill_per_second")
            .query(connection)
            .with_context(|| format!("读取 Redis 调度热态失败: {state_key}"))?;

        Ok(DispatchRuntimeRecord {
            active_requests,
            cooldown_until_epoch_ms: Self::parse_optional_u64(values.first()),
            rate_limit_hit_streak: Self::parse_optional_u32(values.get(1)).unwrap_or_default(),
            bucket_tokens: Self::parse_optional_f64(values.get(2)),
            bucket_current_refill_per_second: Self::parse_optional_f64(values.get(3)),
            bucket_last_refill_at_epoch_ms: Self::parse_optional_u64(values.get(4)),
            bucket_capacity: Self::parse_optional_f64(values.get(5)),
            bucket_base_refill_per_second: Self::parse_optional_f64(values.get(6)),
        })
    }

    fn bucket_script_args(
        bucket_policy: Option<DispatchRuntimeBucketPolicy>,
    ) -> (bool, f64, f64, f64) {
        match bucket_policy {
            Some(policy) => (
                true,
                policy.capacity,
                policy.refill_per_second,
                policy.min_refill_per_second,
            ),
            None => (false, 0.0, 0.0, 0.0),
        }
    }

    fn parse_reservation_response(
        response: Vec<String>,
    ) -> anyhow::Result<DispatchLeaseReservation> {
        let (status, snapshot) = Self::parse_runtime_response(response)?;
        let status = match status.as_str() {
            "reserved" => DispatchLeaseReservationStatus::Reserved,
            "unavailable" => DispatchLeaseReservationStatus::Unavailable,
            other => anyhow::bail!("未知的 Redis 调度占位结果: {other}"),
        };

        Ok(DispatchLeaseReservation { status, snapshot })
    }

    fn parse_runtime_response(
        response: Vec<String>,
    ) -> anyhow::Result<(String, DispatchRuntimeSnapshot)> {
        if response.len() < 9 {
            anyhow::bail!("Redis 调度热态响应字段不足: {:?}", response);
        }

        let status = response[0].clone();
        let active_requests = Self::parse_string_u64(&response[1]).unwrap_or_default();
        let cooldown_until_epoch_ms = Self::parse_string_u64(&response[2]);
        let rate_limit_hit_streak = Self::parse_string_u32(&response[3]).unwrap_or_default();
        let bucket_tokens = Self::parse_string_f64(&response[4]);
        let bucket_capacity = Self::parse_string_f64(&response[5]);
        let bucket_current_refill_per_second = Self::parse_string_f64(&response[6]);
        let bucket_base_refill_per_second = Self::parse_string_f64(&response[7]);
        let next_ready_at_epoch_ms = Self::parse_string_u64(&response[8]);

        Ok((
            status,
            DispatchRuntimeSnapshot {
                active_requests: usize::try_from(active_requests).unwrap_or(usize::MAX),
                cooldown_until_epoch_ms,
                rate_limit_hit_streak,
                bucket_tokens,
                bucket_capacity,
                bucket_current_refill_per_second,
                bucket_base_refill_per_second,
                next_ready_at_epoch_ms,
            },
        ))
    }

    fn parse_optional_u64(raw: Option<&Option<String>>) -> Option<u64> {
        raw.and_then(|value| value.as_deref())
            .and_then(Self::parse_string_u64)
    }

    fn parse_optional_u32(raw: Option<&Option<String>>) -> Option<u32> {
        raw.and_then(|value| value.as_deref())
            .and_then(Self::parse_string_u32)
    }

    fn parse_optional_f64(raw: Option<&Option<String>>) -> Option<f64> {
        raw.and_then(|value| value.as_deref())
            .and_then(Self::parse_string_f64)
    }

    fn parse_string_u64(raw: &str) -> Option<u64> {
        if raw.is_empty() {
            return None;
        }
        raw.parse::<u64>().ok()
    }

    fn parse_string_u32(raw: &str) -> Option<u32> {
        if raw.is_empty() {
            return None;
        }
        raw.parse::<u32>().ok()
    }

    fn parse_string_f64(raw: &str) -> Option<f64> {
        if raw.is_empty() {
            return None;
        }
        raw.parse::<f64>().ok()
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
        assert_eq!(
            store.load_stats().unwrap().get("7").unwrap().success_count,
            11
        );

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
            store
                .load_balance_cache()
                .unwrap()
                .get(&7)
                .unwrap()
                .data
                .subscription_title,
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
    fn file_state_store_compare_and_swap_refreshed_credential_updates_only_matching_token() {
        let dir = temp_test_dir("credential-cas");
        let credentials_path = dir.join("credentials.json");
        let store = StateStore::file(None, Some(credentials_path.clone()));

        let original = KiroCredentials {
            id: Some(7),
            refresh_token: Some("refresh-old".to_string()),
            access_token: Some("access-old".to_string()),
            expires_at: Some("2026-04-15T00:00:00Z".to_string()),
            ..KiroCredentials::default()
        };
        assert!(
            store
                .persist_credentials(&[original.clone()], true)
                .unwrap()
        );

        let updated = KiroCredentials {
            id: Some(7),
            refresh_token: Some("refresh-new".to_string()),
            access_token: Some("access-new".to_string()),
            expires_at: Some("2026-04-15T01:00:00Z".to_string()),
            ..KiroCredentials::default()
        };
        let applied = store
            .compare_and_swap_refreshed_credential(7, Some("refresh-old"), &updated, true)
            .unwrap();
        assert!(matches!(applied, CredentialCompareAndSwapResult::Applied));

        let reloaded = store.load_credentials().unwrap();
        assert_eq!(
            reloaded.credentials[0].refresh_token.as_deref(),
            Some("refresh-new")
        );
        assert_eq!(
            reloaded.credentials[0].access_token.as_deref(),
            Some("access-new")
        );

        let stale_update = KiroCredentials {
            id: Some(7),
            refresh_token: Some("refresh-stale".to_string()),
            access_token: Some("access-stale".to_string()),
            expires_at: Some("2026-04-15T02:00:00Z".to_string()),
            ..KiroCredentials::default()
        };
        let conflict = store
            .compare_and_swap_refreshed_credential(7, Some("refresh-old"), &stale_update, true)
            .unwrap();
        match conflict {
            CredentialCompareAndSwapResult::Conflict { current } => {
                assert_eq!(current.refresh_token.as_deref(), Some("refresh-new"));
                assert_eq!(current.access_token.as_deref(), Some("access-new"));
            }
            other => panic!("expected CAS conflict, got {:?}", other),
        }

        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn file_state_store_merges_stats_deltas() {
        let dir = temp_test_dir("merge-stats");
        let credentials_path = dir.join("credentials.json");
        let store = StateStore::file(None, Some(credentials_path));

        let mut initial = HashMap::new();
        initial.insert(
            "7".to_string(),
            StatsEntryRecord {
                success_count: 11,
                last_used_at: Some("2026-04-15T00:00:00Z".to_string()),
            },
        );
        store.save_stats(&initial).unwrap();

        let mut updates = HashMap::new();
        updates.insert(
            "7".to_string(),
            StatsMergeRecord {
                success_count_delta: 2,
                last_used_at: Some("2026-04-15T01:00:00Z".to_string()),
            },
        );
        updates.insert(
            "8".to_string(),
            StatsMergeRecord {
                success_count_delta: 1,
                last_used_at: Some("2026-04-15T00:30:00Z".to_string()),
            },
        );

        let merged = store.merge_stats(&updates).unwrap();
        assert_eq!(merged.get("7").unwrap().success_count, 13);
        assert_eq!(
            merged.get("7").unwrap().last_used_at.as_deref(),
            Some("2026-04-15T01:00:00Z")
        );
        assert_eq!(merged.get("8").unwrap().success_count, 1);

        let reloaded = store.load_stats().unwrap();
        assert_eq!(reloaded.get("7").unwrap().success_count, 13);
        assert_eq!(
            reloaded.get("7").unwrap().last_used_at.as_deref(),
            Some("2026-04-15T01:00:00Z")
        );

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
            model_cooldown_enabled: true,
            default_max_concurrency: Some(4),
            rate_limit_bucket_capacity: 6.0,
            rate_limit_refill_per_second: 1.5,
            rate_limit_refill_min_per_second: 0.4,
            rate_limit_refill_recovery_step_per_success: 0.2,
            rate_limit_refill_backoff_factor: 0.7,
            request_weighting: RequestWeightingConfig {
                max_weight: 4.0,
                tools_bonus: 1.0,
                ..RequestWeightingConfig::default()
            },
            account_type_policies: BTreeMap::new(),
            account_type_dispatch_policies: BTreeMap::new(),
        };

        store.persist_dispatch_config(&dispatch).unwrap();

        let persisted = Config::load(&config_path).unwrap();
        assert_eq!(PersistedDispatchConfig::from_config(&persisted), dispatch);

        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn persisted_dispatch_config_defaults_request_weighting_to_enabled_when_missing() {
        let dispatch: PersistedDispatchConfig = serde_json::from_str(
            r#"{
                "mode":"balanced",
                "queue_max_size":16,
                "queue_max_wait_ms":1500,
                "rate_limit_cooldown_ms":5000,
                "default_max_concurrency":3,
                "rate_limit_bucket_capacity":6.0,
                "rate_limit_refill_per_second":1.5,
                "rate_limit_refill_min_per_second":0.4,
                "rate_limit_refill_recovery_step_per_success":0.2,
                "rate_limit_refill_backoff_factor":0.7
            }"#,
        )
        .unwrap();

        assert!(!dispatch.model_cooldown_enabled);
        assert!(dispatch.request_weighting.enabled);
    }

    #[test]
    fn state_store_from_config_rejects_postgres_without_url() {
        let mut config = Config::default();
        config.state_backend = StateBackendKind::Postgres;

        let err = StateStore::from_config(&config, None)
            .unwrap_err()
            .to_string();

        assert!(err.contains("statePostgresUrl"));
    }

    #[test]
    fn state_store_from_config_rejects_invalid_redis_url() {
        let mut config = Config::default();
        config.state_redis_url = Some("not-a-valid-redis-url".to_string());

        let err = StateStore::from_config(&config, None)
            .unwrap_err()
            .to_string();

        assert!(err.contains("Redis"));
    }

    #[test]
    fn state_store_from_config_rejects_invalid_runtime_coordination_timing() {
        let mut config = Config::default();
        config.state_redis_heartbeat_interval_secs = 30;
        config.state_redis_leader_lease_ttl_secs = 30;

        let err = StateStore::from_config(&config, None)
            .unwrap_err()
            .to_string();

        assert!(err.contains("stateRedisHeartbeatIntervalSecs"));
    }

    #[test]
    fn redis_balance_cache_round_trips_when_test_url_is_set() {
        let Ok(redis_url) = std::env::var("TEST_REDIS_URL") else {
            return;
        };

        let key = format!("kiro:test:balance-cache:{}", Uuid::new_v4());
        let backend = RedisBalanceCacheBackend::connect(&redis_url, &key).unwrap();

        let mut cache = HashMap::new();
        cache.insert(
            9,
            CachedBalanceRecord {
                cached_at: 2345.0,
                data: BalanceResponse {
                    id: 9,
                    subscription_title: Some("KIRO MAX".to_string()),
                    current_usage: 2.0,
                    usage_limit: 20.0,
                    remaining: 18.0,
                    usage_percentage: 10.0,
                    next_reset_at: Some(6789.0),
                },
            },
        );

        backend.save_balance_cache(&cache).unwrap();
        let loaded = backend.load_balance_cache().unwrap();
        assert_eq!(
            loaded.get(&9).unwrap().data.subscription_title,
            Some("KIRO MAX".to_string())
        );

        let client = backend.client.clone();
        run_blocking_state_op(move || -> anyhow::Result<()> {
            let mut connection = client.get_connection().context("连接 Redis 测试清理失败")?;
            let _: usize = redis::cmd("DEL")
                .arg(&key)
                .query(&mut connection)
                .context("删除 Redis 测试键失败")?;
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn redis_runtime_coordinator_elects_single_leader_when_test_url_is_set() {
        let Ok(redis_url) = std::env::var("TEST_REDIS_URL") else {
            return;
        };

        let namespace = format!("kiro:test:runtime-coordination:{}", Uuid::new_v4());
        let instance_a_url = "http://instance-a:8080".to_string();
        let instance_b_url = "http://instance-b:8080".to_string();
        let coordinator_a = RedisRuntimeCoordinator::connect(
            &redis_url,
            &namespace,
            "instance-a".to_string(),
            Some(instance_a_url.clone()),
            Duration::from_secs(1),
            Duration::from_secs(3),
        )
        .unwrap();
        let coordinator_b = RedisRuntimeCoordinator::connect(
            &redis_url,
            &namespace,
            "instance-b".to_string(),
            Some(instance_b_url.clone()),
            Duration::from_secs(1),
            Duration::from_secs(3),
        )
        .unwrap();

        let status_a = coordinator_a.tick().unwrap();
        let status_b = coordinator_b.tick().unwrap();

        assert_ne!(status_a.is_leader, status_b.is_leader);

        let leader_id = if status_a.is_leader {
            status_a.instance_id.clone()
        } else {
            status_b.instance_id.clone()
        };

        assert_eq!(status_a.leader_id.as_deref(), Some(leader_id.as_str()));
        assert_eq!(status_b.leader_id.as_deref(), Some(leader_id.as_str()));

        let renewed = if status_a.is_leader {
            coordinator_a.tick().unwrap()
        } else {
            coordinator_b.tick().unwrap()
        };
        assert!(renewed.is_leader);
        assert_eq!(renewed.leader_id.as_deref(), Some(leader_id.as_str()));
        assert_eq!(
            renewed.leader_http_base_url.as_deref(),
            Some(if status_a.is_leader {
                instance_a_url.as_str()
            } else {
                instance_b_url.as_str()
            })
        );

        let observer = RedisRuntimeCoordinator::connect(
            &redis_url,
            &namespace,
            "instance-c".to_string(),
            Some("http://instance-c:8080".to_string()),
            Duration::from_secs(1),
            Duration::from_secs(3),
        )
        .unwrap();
        let observed = observer.current_status().unwrap();
        assert!(!observed.is_leader);
        assert_eq!(observed.leader_id.as_deref(), Some(leader_id.as_str()));
        assert_eq!(
            observed.leader_http_base_url.as_deref(),
            Some(if status_a.is_leader {
                instance_a_url.as_str()
            } else {
                instance_b_url.as_str()
            })
        );

        let cleanup_keys = vec![
            coordinator_a.leader_key(),
            coordinator_a.instance_key(),
            coordinator_b.instance_key(),
        ];
        let client = coordinator_a.client.clone();
        run_blocking_state_op(move || -> anyhow::Result<()> {
            let mut connection = client.get_connection().context("连接 Redis 测试清理失败")?;
            let _: usize = redis::cmd("DEL")
                .arg(cleanup_keys)
                .query(&mut connection)
                .context("删除 Redis 运行时协调测试键失败")?;
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn redis_runtime_coordinator_release_leader_promotes_follower_on_next_tick() {
        let Ok(redis_url) = std::env::var("TEST_REDIS_URL") else {
            return;
        };

        let namespace = format!("kiro:test:runtime-release:{}", Uuid::new_v4());
        let leader = RedisRuntimeCoordinator::connect(
            &redis_url,
            &namespace,
            "instance-a".to_string(),
            Some("http://instance-a:8080".to_string()),
            Duration::from_secs(1),
            Duration::from_secs(3),
        )
        .unwrap();
        let follower = RedisRuntimeCoordinator::connect(
            &redis_url,
            &namespace,
            "instance-b".to_string(),
            Some("http://instance-b:8080".to_string()),
            Duration::from_secs(1),
            Duration::from_secs(3),
        )
        .unwrap();

        let leader_status = leader.tick().unwrap();
        assert!(leader_status.is_leader);

        let follower_status = follower.tick().unwrap();
        assert!(!follower_status.is_leader);
        assert_eq!(
            follower_status.leader_id.as_deref(),
            Some(leader_status.instance_id.as_str())
        );

        let released = leader.release().unwrap();
        assert!(!released.is_leader);
        assert!(released.leader_id.is_none());

        let promoted = follower.tick().unwrap();
        assert!(promoted.is_leader);
        assert_eq!(
            promoted.leader_id.as_deref(),
            Some(follower_status.instance_id.as_str())
        );

        let cleanup_keys = vec![
            leader.leader_key(),
            leader.instance_key(),
            follower.instance_key(),
        ];
        let client = leader.client.clone();
        run_blocking_state_op(move || -> anyhow::Result<()> {
            let mut connection = client.get_connection().context("连接 Redis 测试清理失败")?;
            let _: usize = redis::cmd("DEL")
                .arg(cleanup_keys)
                .query(&mut connection)
                .context("删除 Redis 运行时协调测试键失败")?;
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn redis_runtime_refresh_backend_allows_single_owner_when_test_url_is_set() {
        let Ok(redis_url) = std::env::var("TEST_REDIS_URL") else {
            return;
        };

        let namespace = format!("kiro:test:runtime-refresh:{}", Uuid::new_v4());
        let backend = RedisRuntimeRefreshBackend::connect(&redis_url, &namespace).unwrap();

        let lease = match backend
            .try_acquire(7, "instance-a", Duration::from_secs(30))
            .unwrap()
        {
            RuntimeRefreshLeaseAcquisition::Acquired(lease) => lease,
            other => panic!("expected lease acquisition, got {:?}", other),
        };

        let held = backend
            .try_acquire(7, "instance-b", Duration::from_secs(30))
            .unwrap();
        match held {
            RuntimeRefreshLeaseAcquisition::HeldByPeer { owner_instance_id } => {
                assert_eq!(owner_instance_id.as_deref(), Some("instance-a"));
            }
            other => panic!("expected peer hold, got {:?}", other),
        }

        assert!(backend.release(&lease).unwrap());

        let reacquired = backend
            .try_acquire(7, "instance-b", Duration::from_secs(30))
            .unwrap();
        let second_lease = match reacquired {
            RuntimeRefreshLeaseAcquisition::Acquired(lease) => lease,
            other => panic!("expected reacquired lease, got {:?}", other),
        };
        assert!(backend.release(&second_lease).unwrap());

        let cleanup_key = backend.lease_key(7);
        let client = backend.client.clone();
        run_blocking_state_op(move || -> anyhow::Result<()> {
            let mut connection = client.get_connection().context("连接 Redis 测试清理失败")?;
            let _: usize = redis::cmd("DEL")
                .arg(&cleanup_key)
                .query(&mut connection)
                .context("删除 Redis 凭据刷新租约测试键失败")?;
            Ok(())
        })
        .unwrap();
    }
}
