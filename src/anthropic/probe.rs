use axum::http::HeaderMap;

pub const UPSTREAM_PROBE_HEADER: &str = "x-kiro-upstream-probe";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UpstreamProbe {
    pub omit_origin: bool,
    pub omit_agent_task_type: bool,
    pub omit_chat_trigger_type: bool,
    pub omit_agent_mode_header: bool,
}

impl UpstreamProbe {
    pub fn is_enabled(self) -> bool {
        self.omit_origin
            || self.omit_agent_task_type
            || self.omit_chat_trigger_type
            || self.omit_agent_mode_header
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
        .map(|part| part.trim().to_ascii_lowercase())
        .filter(|part| !part.is_empty())
    {
        match token.as_str() {
            "raw" | "identity-raw" => {
                probe.omit_origin = true;
                probe.omit_agent_task_type = true;
                probe.omit_chat_trigger_type = true;
                probe.omit_agent_mode_header = true;
            }
            "no-origin" => probe.omit_origin = true,
            "no-agent-task" => probe.omit_agent_task_type = true,
            "no-chat-trigger" => probe.omit_chat_trigger_type = true,
            "no-agent-mode-header" => probe.omit_agent_mode_header = true,
            _ => {}
        }
    }

    probe
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
                omit_agent_task_type: false,
                omit_chat_trigger_type: false,
                omit_agent_mode_header: true,
            }
        );
    }
}
