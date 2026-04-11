use axum::http::HeaderMap;

pub const UPSTREAM_PROBE_HEADER: &str = "x-kiro-upstream-probe";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UpstreamProbe {
    pub omit_origin: bool,
    pub origin_override: Option<String>,
    pub omit_agent_task_type: bool,
    pub omit_chat_trigger_type: bool,
    pub omit_agent_mode_header: bool,
}

impl UpstreamProbe {
    pub fn is_enabled(&self) -> bool {
        self.omit_origin
            || self.origin_override.is_some()
            || self.omit_agent_task_type
            || self.omit_chat_trigger_type
            || self.omit_agent_mode_header
    }

    pub fn apply_origin(&self, origin: &mut Option<String>) {
        if let Some(origin_override) = &self.origin_override {
            *origin = Some(origin_override.clone());
        } else if self.omit_origin {
            *origin = None;
        }
    }
}

pub fn parse_upstream_probe(headers: &HeaderMap) -> UpstreamProbe {
    let Some(raw) = headers.get(UPSTREAM_PROBE_HEADER) else {
        return UpstreamProbe::default();
    };

    let Ok(raw) = raw.to_str() else {
        return UpstreamProbe::default();
    };

    let mut probe = UpstreamProbe::default();
    for token in raw
        .split(|c: char| c == ',' || c.is_ascii_whitespace())
        .map(str::trim)
        .filter(|part| !part.is_empty())
    {
        let token_lower = token.to_ascii_lowercase();

        match token_lower.as_str() {
            "raw" | "identity-raw" => {
                probe.omit_origin = true;
                probe.origin_override = None;
                probe.omit_agent_task_type = true;
                probe.omit_chat_trigger_type = true;
                probe.omit_agent_mode_header = true;
            }
            "no-origin" => {
                probe.omit_origin = true;
                probe.origin_override = None;
            }
            "origin-cli" => {
                probe.omit_origin = false;
                probe.origin_override = Some("CLI".to_string());
            }
            "origin-ai-editor" => {
                probe.omit_origin = false;
                probe.origin_override = Some("AI_EDITOR".to_string());
            }
            "no-agent-task" => probe.omit_agent_task_type = true,
            "no-chat-trigger" => probe.omit_chat_trigger_type = true,
            "no-agent-mode-header" => probe.omit_agent_mode_header = true,
            _ => {
                if let Some(raw_origin) = token_lower.strip_prefix("set-origin=") {
                    if let Some(origin_override) = normalize_origin_override(raw_origin) {
                        probe.omit_origin = false;
                        probe.origin_override = Some(origin_override);
                    }
                }
            }
        }
    }

    probe
}

fn normalize_origin_override(raw: &str) -> Option<String> {
    let normalized = raw.trim().replace(['-', ' '], "_").to_ascii_uppercase();
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

#[cfg(test)]
mod tests {
    use super::{UPSTREAM_PROBE_HEADER, UpstreamProbe, parse_upstream_probe};
    use axum::http::{HeaderMap, HeaderValue};

    #[test]
    fn test_parse_upstream_probe_raw_preset() {
        let mut headers = HeaderMap::new();
        headers.insert(UPSTREAM_PROBE_HEADER, HeaderValue::from_static("raw"));

        assert_eq!(
            parse_upstream_probe(&headers),
            UpstreamProbe {
                omit_origin: true,
                origin_override: None,
                omit_agent_task_type: true,
                omit_chat_trigger_type: true,
                omit_agent_mode_header: true,
            }
        );
    }

    #[test]
    fn test_parse_upstream_probe_partial_flags() {
        let mut headers = HeaderMap::new();
        headers.insert(
            UPSTREAM_PROBE_HEADER,
            HeaderValue::from_static("no-origin, no-agent-mode-header"),
        );

        assert_eq!(
            parse_upstream_probe(&headers),
            UpstreamProbe {
                omit_origin: true,
                origin_override: None,
                omit_agent_task_type: false,
                omit_chat_trigger_type: false,
                omit_agent_mode_header: true,
            }
        );
    }

    #[test]
    fn test_parse_upstream_probe_set_origin_cli() {
        let mut headers = HeaderMap::new();
        headers.insert(
            UPSTREAM_PROBE_HEADER,
            HeaderValue::from_static("set-origin=cli, no-agent-mode-header"),
        );

        assert_eq!(
            parse_upstream_probe(&headers),
            UpstreamProbe {
                omit_origin: false,
                origin_override: Some("CLI".to_string()),
                omit_agent_task_type: false,
                omit_chat_trigger_type: false,
                omit_agent_mode_header: true,
            }
        );
    }

    #[test]
    fn test_parse_upstream_probe_last_origin_directive_wins() {
        let mut headers = HeaderMap::new();
        headers.insert(
            UPSTREAM_PROBE_HEADER,
            HeaderValue::from_static("no-origin set-origin=ai-editor"),
        );

        assert_eq!(
            parse_upstream_probe(&headers),
            UpstreamProbe {
                omit_origin: false,
                origin_override: Some("AI_EDITOR".to_string()),
                omit_agent_task_type: false,
                omit_chat_trigger_type: false,
                omit_agent_mode_header: false,
            }
        );
    }
}
