//! Kiro ListAvailableProfiles API response model.

use serde::Deserialize;

/// Single profile entry returned by ListAvailableProfiles.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub struct AvailableProfile {
    pub arn: String,
    #[serde(default)]
    pub profile_name: Option<String>,
    #[serde(default)]
    pub profile_type: Option<String>,
}

/// ListAvailableProfiles response.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListAvailableProfilesResponse {
    #[serde(default)]
    pub profiles: Vec<AvailableProfile>,
    #[serde(default)]
    #[allow(dead_code)]
    pub next_token: Option<String>,
}

impl ListAvailableProfilesResponse {
    pub fn selected_profile_arn(&self, api_region: &str) -> Option<String> {
        let api_region = api_region.trim();
        let mut candidates: Vec<&AvailableProfile> = self
            .profiles
            .iter()
            .filter(|profile| !profile.arn.trim().is_empty())
            .collect();

        if candidates.is_empty() {
            return None;
        }

        if !api_region.is_empty() {
            let region_matches: Vec<&AvailableProfile> = candidates
                .iter()
                .copied()
                .filter(|profile| profile_arn_region(&profile.arn) == Some(api_region))
                .collect();
            if !region_matches.is_empty() {
                candidates = region_matches;
            }
        }

        candidates
            .iter()
            .copied()
            .find(is_kiro_profile)
            .or_else(|| candidates.first().copied())
            .map(|profile| profile.arn.trim().to_string())
    }
}

fn is_kiro_profile(profile: &&AvailableProfile) -> bool {
    profile
        .profile_type
        .as_deref()
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("kiro"))
        || profile
            .profile_name
            .as_deref()
            .is_some_and(|value| value.to_ascii_lowercase().contains("kiro"))
}

fn profile_arn_region(profile_arn: &str) -> Option<&str> {
    let mut segments = profile_arn.trim().split(':');
    let arn = segments.next()?;
    let partition = segments.next()?;
    let service = segments.next()?;
    let region = segments.next()?;
    if arn == "arn"
        && !partition.is_empty()
        && service == "codewhisperer"
        && !region.trim().is_empty()
    {
        Some(region.trim())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::ListAvailableProfilesResponse;

    #[test]
    fn selects_kiro_profile_in_requested_region() {
        let response: ListAvailableProfilesResponse = serde_json::from_str(
            r#"{
                "profiles": [
                    {
                        "arn": "arn:aws:codewhisperer:eu-west-1:123:profile/OTHER",
                        "profileName": "KiroProfile-eu-west-1"
                    },
                    {
                        "arn": "arn:aws:codewhisperer:us-east-1:123:profile/KIRO",
                        "profileName": "KiroProfile-us-east-1"
                    }
                ]
            }"#,
        )
        .unwrap();

        assert_eq!(
            response.selected_profile_arn("us-east-1").as_deref(),
            Some("arn:aws:codewhisperer:us-east-1:123:profile/KIRO")
        );
    }

    #[test]
    fn falls_back_to_first_non_empty_profile() {
        let response: ListAvailableProfilesResponse = serde_json::from_str(
            r#"{
                "profiles": [
                    {"arn": ""},
                    {"arn": "arn:aws:codewhisperer:us-east-1:123:profile/FIRST"}
                ]
            }"#,
        )
        .unwrap();

        assert_eq!(
            response.selected_profile_arn("eu-west-1").as_deref(),
            Some("arn:aws:codewhisperer:us-east-1:123:profile/FIRST")
        );
    }
}
