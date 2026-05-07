use super::model_policy::{AccountTypeDispatchPolicy, ModelSupportPolicy};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BuiltInAccountTypePreset {
    pub id: &'static str,
    pub display_name: &'static str,
    pub description: &'static str,
    pub subscription_title_examples: &'static [&'static str],
    pub recommended_allowed_models: &'static [&'static str],
    pub recommended_blocked_models: &'static [&'static str],
    pub recommended_max_concurrency: Option<u32>,
    pub recommended_rate_limit_bucket_capacity: Option<f64>,
    pub recommended_rate_limit_refill_per_second: Option<f64>,
}

impl BuiltInAccountTypePreset {
    pub fn recommended_policy(&self) -> Option<ModelSupportPolicy> {
        let policy = ModelSupportPolicy {
            allowed_models: self
                .recommended_allowed_models
                .iter()
                .map(|value| (*value).to_string())
                .collect(),
            blocked_models: self
                .recommended_blocked_models
                .iter()
                .map(|value| (*value).to_string())
                .collect(),
        };

        (!policy.is_empty()).then_some(policy)
    }

    pub fn recommended_dispatch_policy(&self) -> Option<AccountTypeDispatchPolicy> {
        let policy = AccountTypeDispatchPolicy {
            max_concurrency: self.recommended_max_concurrency,
            rate_limit_bucket_capacity: self.recommended_rate_limit_bucket_capacity,
            rate_limit_refill_per_second: self.recommended_rate_limit_refill_per_second,
        };

        (!policy.is_empty()).then_some(policy)
    }
}

const BUILT_IN_ACCOUNT_TYPE_PRESETS: [BuiltInAccountTypePreset; 6] = [
    BuiltInAccountTypePreset {
        id: "free",
        display_name: "KIRO Free",
        description: "免费档位的保守基线。建议显式屏蔽所有 Opus 家族，避免请求误路由到高价模型。",
        subscription_title_examples: &["KIRO FREE"],
        recommended_allowed_models: &[],
        recommended_blocked_models: &[
            "claude-opus-4.7",
            "claude-opus-4.6",
            "claude-opus-4.5-20251101",
        ],
        recommended_max_concurrency: None,
        recommended_rate_limit_bucket_capacity: None,
        recommended_rate_limit_refill_per_second: None,
    },
    BuiltInAccountTypePreset {
        id: "pro",
        display_name: "KIRO Pro",
        description: "标准付费档位的保守基线。建议默认不要承接 Opus 4.7，必要时再派生灰度类型。",
        subscription_title_examples: &["KIRO PRO"],
        recommended_allowed_models: &[],
        recommended_blocked_models: &["claude-opus-4.7"],
        recommended_max_concurrency: None,
        recommended_rate_limit_bucket_capacity: None,
        recommended_rate_limit_refill_per_second: None,
    },
    BuiltInAccountTypePreset {
        id: "power",
        display_name: "KIRO Power",
        description: "Power 档位实测单卡可稳定承接更高并发。建议保留 Opus 4.6 及以下，并用账号类型调度策略关闭本地 bucket 覆盖、并发上限提升到 32。",
        subscription_title_examples: &["KIRO POWER"],
        recommended_allowed_models: &[],
        recommended_blocked_models: &["claude-opus-4.7"],
        recommended_max_concurrency: Some(32),
        recommended_rate_limit_bucket_capacity: Some(0.0),
        recommended_rate_limit_refill_per_second: Some(0.0),
    },
    BuiltInAccountTypePreset {
        id: "pro-plus",
        display_name: "KIRO Pro+",
        description: "高价值标准档位。通常适合作为 Opus 4.7 主力池，建议保持空白基线，仅在衍生类型中做金丝雀限制。",
        subscription_title_examples: &["KIRO PRO+"],
        recommended_allowed_models: &[],
        recommended_blocked_models: &[],
        recommended_max_concurrency: None,
        recommended_rate_limit_bucket_capacity: None,
        recommended_rate_limit_refill_per_second: None,
    },
    BuiltInAccountTypePreset {
        id: "max",
        display_name: "KIRO Max",
        description: "高优先级档位。通常无需额外模型限制，适合单独派生更激进的实验池。",
        subscription_title_examples: &["KIRO MAX"],
        recommended_allowed_models: &[],
        recommended_blocked_models: &[],
        recommended_max_concurrency: None,
        recommended_rate_limit_bucket_capacity: None,
        recommended_rate_limit_refill_per_second: None,
    },
    BuiltInAccountTypePreset {
        id: "ultra",
        display_name: "KIRO Ultra",
        description: "最高档位。通常作为最宽松的主力池，若要做灰度建议从该标准类型复制出衍生类型。",
        subscription_title_examples: &["KIRO ULTRA"],
        recommended_allowed_models: &[],
        recommended_blocked_models: &[],
        recommended_max_concurrency: None,
        recommended_rate_limit_bucket_capacity: None,
        recommended_rate_limit_refill_per_second: None,
    },
];

pub fn built_in_account_type_presets() -> &'static [BuiltInAccountTypePreset] {
    &BUILT_IN_ACCOUNT_TYPE_PRESETS
}

pub fn find_built_in_account_type_preset(id: &str) -> Option<&'static BuiltInAccountTypePreset> {
    let normalized = id.trim().to_ascii_lowercase();
    BUILT_IN_ACCOUNT_TYPE_PRESETS
        .iter()
        .find(|preset| preset.id == normalized)
}

pub fn infer_standard_account_type(
    subscription_title: &str,
) -> Option<&'static BuiltInAccountTypePreset> {
    let normalized = subscription_title.trim().to_ascii_uppercase();
    if normalized.is_empty() {
        return None;
    }

    let compact: String = normalized
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || *ch == '+')
        .collect();

    if compact.contains("ULTRA") {
        return find_built_in_account_type_preset("ultra");
    }
    if compact.contains("MAX") {
        return find_built_in_account_type_preset("max");
    }
    if compact.contains("PRO+") || compact.contains("PROPLUS") {
        return find_built_in_account_type_preset("pro-plus");
    }
    if compact.contains("POWER") {
        return find_built_in_account_type_preset("power");
    }
    if compact.contains("FREE") {
        return find_built_in_account_type_preset("free");
    }
    if compact.contains("PRO") {
        return find_built_in_account_type_preset("pro");
    }

    None
}

pub fn infer_standard_account_type_id(subscription_title: &str) -> Option<&'static str> {
    infer_standard_account_type(subscription_title).map(|preset| preset.id)
}

pub fn infer_standard_account_type_id_from_subscription(
    subscription_title: Option<&str>,
    subscription_type: Option<&str>,
) -> Option<&'static str> {
    subscription_title
        .and_then(infer_standard_account_type_id)
        .or_else(|| subscription_type.and_then(infer_standard_account_type_id))
}

#[cfg(test)]
mod tests {
    use super::{
        built_in_account_type_presets, find_built_in_account_type_preset,
        infer_standard_account_type_id,
    };

    #[test]
    fn infer_standard_account_type_prefers_specific_paid_tiers_before_pro() {
        assert_eq!(
            infer_standard_account_type_id("KIRO PRO+"),
            Some("pro-plus")
        );
        assert_eq!(
            infer_standard_account_type_id("  kiro power team "),
            Some("power")
        );
        assert_eq!(infer_standard_account_type_id("KIRO MAX"), Some("max"));
        assert_eq!(infer_standard_account_type_id("KIRO ULTRA"), Some("ultra"));
        assert_eq!(infer_standard_account_type_id("KIRO PRO"), Some("pro"));
        assert_eq!(
            infer_standard_account_type_id("Q_DEVELOPER_STANDALONE_PRO_PLUS"),
            Some("pro-plus")
        );
        assert_eq!(infer_standard_account_type_id("KIRO FREE"), Some("free"));
    }

    #[test]
    fn built_in_account_type_presets_expose_recommended_policies_only_when_needed() {
        let free = find_built_in_account_type_preset("free").unwrap();
        assert!(free.recommended_policy().is_some());

        let power = find_built_in_account_type_preset("power").unwrap();
        let dispatch = power.recommended_dispatch_policy().unwrap();
        assert_eq!(dispatch.max_concurrency, Some(32));
        assert_eq!(dispatch.rate_limit_bucket_capacity, Some(0.0));
        assert_eq!(dispatch.rate_limit_refill_per_second, Some(0.0));

        let pro_plus = find_built_in_account_type_preset("pro-plus").unwrap();
        assert!(pro_plus.recommended_policy().is_none());
        assert!(pro_plus.recommended_dispatch_policy().is_none());

        assert_eq!(built_in_account_type_presets().len(), 6);
    }
}
