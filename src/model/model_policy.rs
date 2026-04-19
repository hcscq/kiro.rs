use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ModelSupportPolicy {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_models: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub blocked_models: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AccountTypeDispatchPolicy {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_concurrency: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate_limit_bucket_capacity: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate_limit_refill_per_second: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeModelRestriction {
    pub model: String,
    pub expires_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedModelSelector {
    pub exact: String,
    pub family: String,
}

impl ModelSupportPolicy {
    pub fn normalize(&mut self) {
        self.allowed_models = normalize_model_entries(&self.allowed_models);
        self.blocked_models = normalize_model_entries(&self.blocked_models);
    }

    pub fn is_empty(&self) -> bool {
        self.allowed_models.is_empty() && self.blocked_models.is_empty()
    }

    pub fn matches_allowed(&self, selector: &NormalizedModelSelector) -> bool {
        self.allowed_models
            .iter()
            .any(|entry| matches_model_entry(entry, selector))
    }

    pub fn matches_blocked(&self, selector: &NormalizedModelSelector) -> bool {
        self.blocked_models
            .iter()
            .any(|entry| matches_model_entry(entry, selector))
    }
}

impl AccountTypeDispatchPolicy {
    pub fn normalize(&mut self) {
        self.max_concurrency = self.max_concurrency.filter(|limit| *limit > 0);
        self.rate_limit_bucket_capacity =
            normalize_non_negative_finite(self.rate_limit_bucket_capacity);
        self.rate_limit_refill_per_second =
            normalize_non_negative_finite(self.rate_limit_refill_per_second);
    }

    pub fn is_empty(&self) -> bool {
        self.max_concurrency.is_none()
            && self.rate_limit_bucket_capacity.is_none()
            && self.rate_limit_refill_per_second.is_none()
    }

    pub fn effective_max_concurrency(&self) -> Option<u32> {
        self.max_concurrency.filter(|limit| *limit > 0)
    }

    pub fn rate_limit_bucket_capacity_override(&self) -> Option<f64> {
        normalize_non_negative_finite(self.rate_limit_bucket_capacity)
    }

    pub fn rate_limit_refill_per_second_override(&self) -> Option<f64> {
        normalize_non_negative_finite(self.rate_limit_refill_per_second)
    }
}

impl RuntimeModelRestriction {
    pub fn new(model: impl Into<String>, expires_at: DateTime<Utc>) -> Self {
        Self {
            model: model.into(),
            expires_at: expires_at.to_rfc3339(),
        }
    }

    pub fn normalize(&mut self) -> bool {
        let Some(model) = normalize_model_token(&self.model) else {
            return false;
        };
        let Some(expires_at) = self.expires_at_datetime() else {
            return false;
        };
        self.model = model;
        self.expires_at = expires_at.to_rfc3339();
        true
    }

    pub fn expires_at_datetime(&self) -> Option<DateTime<Utc>> {
        DateTime::parse_from_rfc3339(&self.expires_at)
            .ok()
            .map(|value| value.with_timezone(&Utc))
    }

    pub fn is_active_at(&self, now: DateTime<Utc>) -> bool {
        self.expires_at_datetime()
            .is_some_and(|expires_at| expires_at > now)
    }
}

pub fn normalize_account_type(value: &str) -> Option<String> {
    let normalized = value.trim().to_ascii_lowercase();
    (!normalized.is_empty()).then_some(normalized)
}

pub fn normalize_model_token(value: &str) -> Option<String> {
    let mut normalized = value.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return None;
    }

    let has_thinking_suffix = normalized.ends_with("-thinking");
    if has_thinking_suffix {
        normalized.truncate(normalized.len() - "-thinking".len());
    }

    let normalized = normalize_known_model_alias(&normalized);
    if normalized.is_empty() {
        return None;
    }

    Some(if has_thinking_suffix {
        format!("{normalized}-thinking")
    } else {
        normalized
    })
}

pub fn normalize_model_selector(value: &str) -> Option<NormalizedModelSelector> {
    let exact = normalize_model_token(value)?;
    let family = exact
        .strip_suffix("-thinking")
        .unwrap_or(exact.as_str())
        .to_string();
    Some(NormalizedModelSelector { exact, family })
}

pub fn normalize_model_entries(values: &[String]) -> Vec<String> {
    let mut normalized: Vec<String> = values
        .iter()
        .filter_map(|value| normalize_model_token(value))
        .collect();
    normalized.sort();
    normalized.dedup();
    normalized
}

pub fn normalize_account_type_policies(
    policies: &mut BTreeMap<String, ModelSupportPolicy>,
) -> BTreeMap<String, ModelSupportPolicy> {
    let mut normalized = BTreeMap::new();
    for (key, mut policy) in std::mem::take(policies) {
        let Some(account_type) = normalize_account_type(&key) else {
            continue;
        };
        policy.normalize();
        if policy.is_empty() {
            continue;
        }
        normalized.insert(account_type, policy);
    }
    *policies = normalized.clone();
    normalized
}

pub fn normalize_account_type_dispatch_policies(
    policies: &mut BTreeMap<String, AccountTypeDispatchPolicy>,
) -> BTreeMap<String, AccountTypeDispatchPolicy> {
    let mut normalized = BTreeMap::new();
    for (key, mut policy) in std::mem::take(policies) {
        let Some(account_type) = normalize_account_type(&key) else {
            continue;
        };
        policy.normalize();
        if policy.is_empty() {
            continue;
        }
        normalized.insert(account_type, policy);
    }
    *policies = normalized.clone();
    normalized
}

pub fn matches_model_entry(entry: &str, selector: &NormalizedModelSelector) -> bool {
    entry == selector.exact || entry == selector.family
}

fn normalize_non_negative_finite(value: Option<f64>) -> Option<f64> {
    value.filter(|value| value.is_finite() && *value >= 0.0)
}

fn normalize_known_model_alias(model: &str) -> String {
    model
        .replace("claude-opus-4-7", "claude-opus-4.7")
        .replace("claude-opus-4-6", "claude-opus-4.6")
        .replace("claude-opus-4-5", "claude-opus-4.5")
        .replace("claude-sonnet-4-6", "claude-sonnet-4.6")
        .replace("claude-sonnet-4-5", "claude-sonnet-4.5")
        .replace("claude-haiku-4-5", "claude-haiku-4.5")
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};

    use super::{
        AccountTypeDispatchPolicy, ModelSupportPolicy, RuntimeModelRestriction,
        matches_model_entry, normalize_account_type, normalize_account_type_dispatch_policies,
        normalize_model_entries, normalize_model_selector, normalize_model_token,
    };
    use std::collections::BTreeMap;

    #[test]
    fn normalize_model_token_keeps_known_aliases_consistent() {
        assert_eq!(
            normalize_model_token("claude-opus-4-6"),
            Some("claude-opus-4.6".to_string())
        );
        assert_eq!(
            normalize_model_token("claude-opus-4-6-thinking"),
            Some("claude-opus-4.6-thinking".to_string())
        );
    }

    #[test]
    fn matches_model_entry_allows_family_to_match_thinking_variant() {
        let selector = normalize_model_selector("claude-opus-4-6-thinking").unwrap();
        assert!(matches_model_entry("claude-opus-4.6", &selector));
        assert!(matches_model_entry("claude-opus-4.6-thinking", &selector));
        assert!(!matches_model_entry("claude-sonnet-4.6", &selector));
    }

    #[test]
    fn normalize_model_entries_deduplicates_blank_and_alias_values() {
        let normalized = normalize_model_entries(&[
            "".to_string(),
            "claude-opus-4-6".to_string(),
            "claude-opus-4.6".to_string(),
            "CLAUDE-OPUS-4-6".to_string(),
        ]);

        assert_eq!(normalized, vec!["claude-opus-4.6".to_string()]);
    }

    #[test]
    fn normalize_account_type_lowercases_and_trims() {
        assert_eq!(
            normalize_account_type("  Power-Team "),
            Some("power-team".to_string())
        );
        assert_eq!(normalize_account_type("   "), None);
    }

    #[test]
    fn runtime_model_restriction_filters_invalid_or_expired_values() {
        let future = (Utc::now() + Duration::minutes(30)).to_rfc3339();
        let past = (Utc::now() - Duration::minutes(30)).to_rfc3339();

        let mut active = RuntimeModelRestriction {
            model: "claude-opus-4-7".to_string(),
            expires_at: future,
        };
        let mut expired = RuntimeModelRestriction {
            model: "claude-opus-4-7".to_string(),
            expires_at: past,
        };

        assert!(active.normalize());
        assert_eq!(active.model, "claude-opus-4.7");
        assert!(active.is_active_at(Utc::now()));
        assert!(expired.normalize());
        assert!(!expired.is_active_at(Utc::now()));
    }

    #[test]
    fn model_support_policy_detects_empty_after_normalize() {
        let mut policy = ModelSupportPolicy {
            allowed_models: vec![" ".to_string()],
            blocked_models: vec![],
        };

        policy.normalize();

        assert!(policy.is_empty());
    }

    #[test]
    fn account_type_dispatch_policy_preserves_zero_bucket_override() {
        let mut policy = AccountTypeDispatchPolicy {
            max_concurrency: Some(20),
            rate_limit_bucket_capacity: Some(0.0),
            rate_limit_refill_per_second: Some(0.0),
        };

        policy.normalize();

        assert_eq!(policy.effective_max_concurrency(), Some(20));
        assert_eq!(policy.rate_limit_bucket_capacity_override(), Some(0.0));
        assert_eq!(policy.rate_limit_refill_per_second_override(), Some(0.0));
    }

    #[test]
    fn normalize_account_type_dispatch_policies_drops_empty_entries() {
        let mut policies = BTreeMap::from([
            (
                " POWER ".to_string(),
                AccountTypeDispatchPolicy {
                    max_concurrency: Some(32),
                    rate_limit_bucket_capacity: Some(0.0),
                    rate_limit_refill_per_second: Some(0.0),
                },
            ),
            (
                "   ".to_string(),
                AccountTypeDispatchPolicy {
                    max_concurrency: Some(8),
                    rate_limit_bucket_capacity: None,
                    rate_limit_refill_per_second: None,
                },
            ),
            (
                "free".to_string(),
                AccountTypeDispatchPolicy {
                    max_concurrency: None,
                    rate_limit_bucket_capacity: None,
                    rate_limit_refill_per_second: None,
                },
            ),
        ]);

        let normalized = normalize_account_type_dispatch_policies(&mut policies);

        assert_eq!(normalized.len(), 1);
        assert_eq!(
            normalized
                .get("power")
                .and_then(AccountTypeDispatchPolicy::effective_max_concurrency),
            Some(32)
        );
    }
}
