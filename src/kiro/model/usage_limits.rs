//! 使用额度查询数据模型
//!
//! 包含 getUsageLimits API 的响应类型定义

use serde::Deserialize;

/// 使用额度查询响应
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UsageLimitsResponse {
    /// 下次重置日期 (Unix 时间戳)
    #[serde(default)]
    pub next_date_reset: Option<f64>,

    /// 订阅信息
    #[serde(default)]
    pub subscription_info: Option<SubscriptionInfo>,

    /// 用户信息（Enterprise 账号可能没有 email，但会返回 userId）
    #[serde(default)]
    pub user_info: Option<UserInfo>,

    /// 使用量明细列表
    #[serde(default)]
    pub usage_breakdown_list: Vec<UsageBreakdown>,

    /// 超额使用配置
    #[serde(default)]
    pub overage_configuration: Option<OverageConfiguration>,
}

/// 用户信息
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UserInfo {
    /// 用户邮箱
    #[serde(default)]
    pub email: Option<String>,

    /// 用户 ID
    #[serde(default)]
    pub user_id: Option<String>,
}

/// 订阅信息
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionInfo {
    /// 订阅标题 (KIRO PRO+ / KIRO FREE 等)
    #[serde(default)]
    pub subscription_title: Option<String>,

    /// 订阅内部类型 (如 Q_DEVELOPER_STANDALONE_PRO)
    #[serde(default, rename = "type")]
    pub subscription_type: Option<String>,

    /// 超额使用能力 (OVERAGE_CAPABLE / OVERAGE_INCAPABLE)
    #[serde(default)]
    pub overage_capability: Option<String>,
}

/// 使用量明细
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct UsageBreakdown {
    /// 当前使用量
    #[serde(default)]
    pub current_usage: i64,

    /// 当前使用量（精确值）
    #[serde(default)]
    pub current_usage_with_precision: f64,

    /// 奖励额度列表
    #[serde(default)]
    pub bonuses: Option<Vec<Bonus>>,

    /// 免费试用信息
    #[serde(default)]
    pub free_trial_info: Option<FreeTrialInfo>,

    /// 下次重置日期 (Unix 时间戳)
    #[serde(default)]
    pub next_date_reset: Option<f64>,

    /// 使用限额
    #[serde(default)]
    pub usage_limit: i64,

    /// 使用限额（精确值）
    #[serde(default)]
    pub usage_limit_with_precision: f64,

    /// 当前超额使用量
    #[serde(default)]
    pub current_overages: f64,

    /// 当前超额使用量（精确值）
    #[serde(default)]
    pub current_overages_with_precision: f64,

    /// 超额上限
    #[serde(default)]
    pub overage_cap: f64,

    /// 超额上限（精确值）
    #[serde(default)]
    pub overage_cap_with_precision: f64,

    /// 当前超额费用
    #[serde(default)]
    pub overage_charges: f64,

    /// 超额费率
    #[serde(default)]
    pub overage_rate: Option<f64>,

    /// 费用币种
    #[serde(default)]
    pub currency: Option<String>,

    /// 计量单位
    #[serde(default)]
    pub unit: Option<String>,
}

/// 超额使用配置
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OverageConfiguration {
    /// 新版 API 返回 ENABLED / DISABLED
    #[serde(default)]
    pub overage_status: Option<String>,

    /// 旧版 API 返回 true / false
    #[serde(default)]
    pub overage_enabled: Option<bool>,
}

/// 奖励额度
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Bonus {
    /// 当前使用量
    #[serde(default)]
    pub current_usage: f64,

    /// 使用限额
    #[serde(default)]
    pub usage_limit: f64,

    /// 状态 (ACTIVE / EXPIRED)
    #[serde(default)]
    pub status: Option<String>,
}

impl Bonus {
    /// 检查 bonus 是否处于激活状态
    pub fn is_active(&self) -> bool {
        self.status
            .as_deref()
            .map(|s| s.eq_ignore_ascii_case("ACTIVE"))
            .unwrap_or(false)
    }
}

/// 免费试用信息
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct FreeTrialInfo {
    /// 当前使用量
    #[serde(default)]
    pub current_usage: i64,

    /// 当前使用量（精确值）
    #[serde(default)]
    pub current_usage_with_precision: f64,

    /// 免费试用过期时间 (Unix 时间戳)
    #[serde(default)]
    pub free_trial_expiry: Option<f64>,

    /// 免费试用状态 (ACTIVE / EXPIRED)
    #[serde(default)]
    pub free_trial_status: Option<String>,

    /// 使用限额
    #[serde(default)]
    pub usage_limit: i64,

    /// 使用限额（精确值）
    #[serde(default)]
    pub usage_limit_with_precision: f64,
}

// ============ 便捷方法实现 ============

impl FreeTrialInfo {
    /// 检查免费试用是否处于激活状态
    pub fn is_active(&self) -> bool {
        self.free_trial_status
            .as_deref()
            .map(|s| s.eq_ignore_ascii_case("ACTIVE"))
            .unwrap_or(false)
    }
}

impl UsageLimitsResponse {
    /// 获取订阅标题
    pub fn subscription_title(&self) -> Option<&str> {
        self.subscription_info
            .as_ref()
            .and_then(|info| info.subscription_title.as_deref())
    }

    /// 获取订阅内部类型
    pub fn subscription_type(&self) -> Option<&str> {
        self.subscription_info
            .as_ref()
            .and_then(|info| info.subscription_type.as_deref())
    }

    /// 获取超额使用能力
    pub fn overage_capability(&self) -> Option<&str> {
        self.subscription_info
            .as_ref()
            .and_then(|info| info.overage_capability.as_deref())
    }

    /// 获取用户邮箱
    pub fn email(&self) -> Option<&str> {
        self.user_info
            .as_ref()
            .and_then(|info| info.email.as_deref())
    }

    /// 获取用户 ID
    pub fn user_id(&self) -> Option<&str> {
        self.user_info
            .as_ref()
            .and_then(|info| info.user_id.as_deref())
    }

    /// 是否支持开启超额使用
    pub fn is_overage_capable(&self) -> bool {
        self.overage_capability()
            .is_some_and(|value| value == "OVERAGE_CAPABLE")
    }

    /// 获取超额状态字符串
    pub fn overage_status(&self) -> Option<&str> {
        self.overage_configuration
            .as_ref()
            .and_then(|config| config.overage_status.as_deref())
    }

    /// 获取超额状态布尔值
    pub fn overage_enabled(&self) -> Option<bool> {
        if let Some(status) = self.overage_status() {
            return match status {
                "ENABLED" => Some(true),
                "DISABLED" => Some(false),
                _ => None,
            };
        }

        self.overage_configuration
            .as_ref()
            .and_then(|config| config.overage_enabled)
    }

    /// 本地更新超额状态，用于 setUserPreference 成功后更新缓存视图
    pub fn set_overage_enabled_local(&mut self, enabled: bool) {
        let config = self
            .overage_configuration
            .get_or_insert_with(OverageConfiguration::default);
        config.overage_status = Some(if enabled { "ENABLED" } else { "DISABLED" }.to_string());
        config.overage_enabled = Some(enabled);
    }

    /// 获取第一个使用量明细
    fn primary_breakdown(&self) -> Option<&UsageBreakdown> {
        self.usage_breakdown_list.first()
    }

    /// 获取总使用限额（精确值）
    ///
    /// 累加基础额度、激活的免费试用额度和激活的奖励额度
    pub fn usage_limit(&self) -> f64 {
        let Some(breakdown) = self.primary_breakdown() else {
            return 0.0;
        };

        let mut total =
            precise_or_integer(breakdown.usage_limit_with_precision, breakdown.usage_limit);

        // 累加激活的 free trial 额度
        if let Some(trial) = &breakdown.free_trial_info {
            if trial.is_active() {
                total += precise_or_integer(trial.usage_limit_with_precision, trial.usage_limit);
            }
        }

        // 累加激活的 bonus 额度
        if let Some(bonuses) = &breakdown.bonuses {
            for bonus in bonuses {
                if bonus.is_active() {
                    total += bonus.usage_limit_value();
                }
            }
        }

        total
    }

    /// 获取总当前使用量（精确值）
    ///
    /// 累加基础使用量、激活的免费试用使用量和激活的奖励使用量
    pub fn current_usage(&self) -> f64 {
        let Some(breakdown) = self.primary_breakdown() else {
            return 0.0;
        };

        let mut total = precise_or_integer(
            breakdown.current_usage_with_precision,
            breakdown.current_usage,
        );

        // 累加激活的 free trial 使用量
        if let Some(trial) = &breakdown.free_trial_info {
            if trial.is_active() {
                total +=
                    precise_or_integer(trial.current_usage_with_precision, trial.current_usage);
            }
        }

        // 累加激活的 bonus 使用量
        if let Some(bonuses) = &breakdown.bonuses {
            for bonus in bonuses {
                if bonus.is_active() {
                    total += bonus.current_usage_value();
                }
            }
        }

        total
    }

    /// 获取超额上限
    pub fn overage_cap(&self) -> f64 {
        self.primary_breakdown()
            .map(|breakdown| {
                precise_or_float(breakdown.overage_cap_with_precision, breakdown.overage_cap)
            })
            .unwrap_or(0.0)
    }

    /// 获取当前超额使用量
    pub fn current_overages(&self) -> f64 {
        self.primary_breakdown()
            .map(|breakdown| {
                precise_or_float(
                    breakdown.current_overages_with_precision,
                    breakdown.current_overages,
                )
            })
            .unwrap_or(0.0)
    }

    /// 获取超额费用
    pub fn overage_charges(&self) -> f64 {
        self.primary_breakdown()
            .map(|breakdown| breakdown.overage_charges)
            .unwrap_or(0.0)
    }

    /// 获取超额费率
    pub fn overage_rate(&self) -> Option<f64> {
        self.primary_breakdown()
            .and_then(|breakdown| breakdown.overage_rate)
    }

    /// 获取费用币种
    pub fn currency(&self) -> Option<&str> {
        self.primary_breakdown()
            .and_then(|breakdown| breakdown.currency.as_deref())
    }

    /// 获取计量单位
    pub fn unit(&self) -> Option<&str> {
        self.primary_breakdown()
            .and_then(|breakdown| breakdown.unit.as_deref())
    }

    /// 获取实际可用限额。超额开启时包含 overageCap。
    pub fn effective_usage_limit(&self) -> f64 {
        let base_limit = self.usage_limit();
        if self.overage_enabled().unwrap_or(false) {
            base_limit + self.overage_cap()
        } else {
            base_limit
        }
    }
}

fn precise_or_integer(precision: f64, integer: i64) -> f64 {
    if precision > 0.0 {
        precision
    } else {
        integer as f64
    }
}

fn precise_or_float(precision: f64, fallback: f64) -> f64 {
    if precision > 0.0 { precision } else { fallback }
}

impl Bonus {
    fn current_usage_value(&self) -> f64 {
        self.current_usage
    }

    fn usage_limit_value(&self) -> f64 {
        self.usage_limit
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_usage_limits_accepts_null_bonuses() {
        let payload = r#"{
            "usageBreakdownList": [{
                "currentUsageWithPrecision": 10.0,
                "usageLimitWithPrecision": 100.0,
                "bonuses": null
            }]
        }"#;

        let response: UsageLimitsResponse =
            serde_json::from_str(payload).expect("null bonuses should deserialize");

        assert_eq!(response.current_usage(), 10.0);
        assert_eq!(response.usage_limit(), 100.0);
    }

    #[test]
    fn test_usage_limits_active_status_is_case_insensitive() {
        let payload = r#"{
            "usageBreakdownList": [{
                "currentUsageWithPrecision": 10.0,
                "usageLimitWithPrecision": 100.0,
                "bonuses": [{
                    "currentUsage": 2.0,
                    "usageLimit": 20.0,
                    "status": "active"
                }],
                "freeTrialInfo": {
                    "currentUsageWithPrecision": 3.0,
                    "usageLimitWithPrecision": 30.0,
                    "freeTrialStatus": "active"
                }
            }]
        }"#;

        let response: UsageLimitsResponse =
            serde_json::from_str(payload).expect("lowercase statuses should deserialize");

        assert_eq!(response.current_usage(), 15.0);
        assert_eq!(response.usage_limit(), 150.0);
    }

    #[test]
    fn test_usage_limits_reads_overage_status_and_effective_limit() {
        let payload = r#"{
            "subscriptionInfo": {
                "subscriptionTitle": "KIRO PRO+",
                "type": "Q_DEVELOPER_STANDALONE_PRO_PLUS",
                "overageCapability": "OVERAGE_CAPABLE"
            },
            "overageConfiguration": {
                "overageStatus": "ENABLED"
            },
            "usageBreakdownList": [{
                "currentUsageWithPrecision": 120.0,
                "usageLimitWithPrecision": 100.0,
                "currentOveragesWithPrecision": 20.0,
                "overageCapWithPrecision": 50.0,
                "overageCharges": 0.8,
                "overageRate": 0.04
            }]
        }"#;

        let response: UsageLimitsResponse =
            serde_json::from_str(payload).expect("overage fields should deserialize");

        assert!(response.is_overage_capable());
        assert_eq!(response.overage_enabled(), Some(true));
        assert_eq!(response.usage_limit(), 100.0);
        assert_eq!(response.effective_usage_limit(), 150.0);
        assert_eq!(response.current_overages(), 20.0);
        assert_eq!(response.overage_charges(), 0.8);
        assert_eq!(response.overage_rate(), Some(0.04));
    }

    #[test]
    fn test_usage_limits_accepts_legacy_overage_enabled() {
        let payload = r#"{
            "overageConfiguration": {
                "overageEnabled": true
            },
            "usageBreakdownList": [{
                "currentUsage": 1,
                "usageLimit": 10,
                "overageCap": 5.0
            }]
        }"#;

        let mut response: UsageLimitsResponse =
            serde_json::from_str(payload).expect("legacy overageEnabled should deserialize");

        assert_eq!(response.overage_enabled(), Some(true));
        assert_eq!(response.current_usage(), 1.0);
        assert_eq!(response.usage_limit(), 10.0);
        assert_eq!(response.effective_usage_limit(), 15.0);

        response.set_overage_enabled_local(false);
        assert_eq!(response.overage_status(), Some("DISABLED"));
        assert_eq!(response.overage_enabled(), Some(false));
        assert_eq!(response.effective_usage_limit(), 10.0);
    }

    #[test]
    fn test_usage_limits_reads_user_info() {
        let payload = r#"{
            "userInfo": {
                "email": "enterprise-user@example.com",
                "userId": "u-123"
            }
        }"#;

        let response: UsageLimitsResponse =
            serde_json::from_str(payload).expect("userInfo should deserialize");

        assert_eq!(response.email(), Some("enterprise-user@example.com"));
        assert_eq!(response.user_id(), Some("u-123"));
    }
}
