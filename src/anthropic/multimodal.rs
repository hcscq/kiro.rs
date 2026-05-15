use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::OnceLock;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use futures::StreamExt;
use reqwest::header::{ACCEPT, CONTENT_TYPE, HeaderMap, LOCATION};
use reqwest::redirect::Policy;
use serde_json::{Map, Value};
use tokio::net::lookup_host;
use url::Url;

use super::types::MessagesRequest;

const MAX_REMOTE_IMAGE_BYTES: usize = 8 * 1024 * 1024;
const MAX_IMAGE_REDIRECTS: usize = 3;
const IMAGE_FETCH_TIMEOUT_SECS: u64 = 10;
static IMAGE_FETCH_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct MultimodalNormalizeStats {
    pub remote_images: usize,
    pub data_url_images: usize,
    pub openai_image_url_blocks: usize,
    pub anthropic_url_blocks: usize,
}

#[derive(Debug)]
pub(crate) struct MultimodalNormalizeError {
    message: String,
}

impl MultimodalNormalizeError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for MultimodalNormalizeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for MultimodalNormalizeError {}

pub(crate) async fn normalize_multimodal_urls(
    req: &mut MessagesRequest,
) -> Result<MultimodalNormalizeStats, MultimodalNormalizeError> {
    let mut stats = MultimodalNormalizeStats::default();
    let client = image_fetch_client()?;

    for message in &mut req.messages {
        let Value::Array(blocks) = &mut message.content else {
            continue;
        };
        for block in blocks {
            normalize_content_block(block, client, &mut stats).await?;
        }
    }

    Ok(stats)
}

async fn normalize_content_block(
    block: &mut Value,
    client: &reqwest::Client,
    stats: &mut MultimodalNormalizeStats,
) -> Result<(), MultimodalNormalizeError> {
    let Value::Object(obj) = block else {
        return Ok(());
    };

    match obj.get("type").and_then(Value::as_str) {
        Some("image_url") => {
            let reference = openai_image_url_reference(obj)?;
            let source = image_reference_to_base64_source(reference, None, client, stats).await?;
            obj.insert("type".to_string(), Value::String("image".to_string()));
            obj.remove("image_url");
            obj.insert("source".to_string(), Value::Object(source));
            stats.openai_image_url_blocks += 1;
        }
        Some("image") => {
            let Some(source) = obj.get_mut("source").and_then(Value::as_object_mut) else {
                return Ok(());
            };
            if source.get("type").and_then(Value::as_str) != Some("url") {
                return Ok(());
            }

            let reference = source
                .get("url")
                .and_then(Value::as_str)
                .or_else(|| source.get("data").and_then(Value::as_str))
                .ok_or_else(|| {
                    MultimodalNormalizeError::new("image source type=url requires a url field")
                })?;
            let declared_media_type = source
                .get("media_type")
                .and_then(Value::as_str)
                .map(str::to_string);
            let new_source =
                image_reference_to_base64_source(reference, declared_media_type, client, stats)
                    .await?;
            *source = new_source;
            stats.anthropic_url_blocks += 1;
        }
        _ => {}
    }

    Ok(())
}

fn openai_image_url_reference(obj: &Map<String, Value>) -> Result<&str, MultimodalNormalizeError> {
    match obj.get("image_url") {
        Some(Value::String(url)) if !url.trim().is_empty() => Ok(url),
        Some(Value::Object(image_url)) => image_url
            .get("url")
            .and_then(Value::as_str)
            .filter(|url| !url.trim().is_empty())
            .ok_or_else(|| MultimodalNormalizeError::new("image_url block requires image_url.url")),
        _ => Err(MultimodalNormalizeError::new(
            "image_url block requires a string or object",
        )),
    }
}

async fn image_reference_to_base64_source(
    reference: &str,
    declared_media_type: Option<String>,
    client: &reqwest::Client,
    stats: &mut MultimodalNormalizeStats,
) -> Result<Map<String, Value>, MultimodalNormalizeError> {
    let reference = reference.trim();
    let (media_type, data) = if let Some((media_type, data)) = parse_image_data_url(reference)? {
        stats.data_url_images += 1;
        (media_type, data)
    } else {
        let fetched = fetch_remote_image(reference, declared_media_type, client).await?;
        stats.remote_images += 1;
        fetched
    };

    let mut source = Map::new();
    source.insert("type".to_string(), Value::String("base64".to_string()));
    source.insert("media_type".to_string(), Value::String(media_type));
    source.insert("data".to_string(), Value::String(data));
    Ok(source)
}

fn parse_image_data_url(
    reference: &str,
) -> Result<Option<(String, String)>, MultimodalNormalizeError> {
    if !reference
        .get(..5)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("data:"))
    {
        return Ok(None);
    }

    let Some((metadata, data)) = reference.split_once(',') else {
        return Err(MultimodalNormalizeError::new("invalid image data URL"));
    };
    let metadata = metadata.strip_prefix("data:").unwrap_or(metadata);
    let mut parts = metadata.split(';');
    let media_type = parts.next().unwrap_or("").trim().to_ascii_lowercase();
    let is_base64 = parts.any(|part| part.eq_ignore_ascii_case("base64"));

    let Some(media_type) = supported_image_media_type(&media_type) else {
        return Err(MultimodalNormalizeError::new(
            "image data URL must be base64 encoded png/jpeg/gif/webp",
        ));
    };

    if !is_base64 {
        return Err(MultimodalNormalizeError::new(
            "image data URL must be base64 encoded png/jpeg/gif/webp",
        ));
    }

    Ok(Some((media_type.to_string(), data.trim().to_string())))
}

fn image_fetch_client() -> Result<&'static reqwest::Client, MultimodalNormalizeError> {
    if let Some(client) = IMAGE_FETCH_CLIENT.get() {
        return Ok(client);
    }

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(IMAGE_FETCH_TIMEOUT_SECS))
        .redirect(Policy::none())
        .build()
        .map_err(|err| {
            MultimodalNormalizeError::new(format!("failed to create image fetch client: {err}"))
        })?;

    let _ = IMAGE_FETCH_CLIENT.set(client);
    IMAGE_FETCH_CLIENT
        .get()
        .ok_or_else(|| MultimodalNormalizeError::new("failed to cache image fetch client"))
}

async fn fetch_remote_image(
    url: &str,
    declared_media_type: Option<String>,
    client: &reqwest::Client,
) -> Result<(String, String), MultimodalNormalizeError> {
    let mut current = parse_and_validate_remote_image_url(url).await?;

    for redirect_count in 0..=MAX_IMAGE_REDIRECTS {
        let response = client
            .get(current.clone())
            .header(ACCEPT, "image/png,image/jpeg,image/gif,image/webp")
            .send()
            .await
            .map_err(|err| {
                MultimodalNormalizeError::new(format!("failed to fetch image: {err}"))
            })?;

        if response.status().is_redirection() {
            if redirect_count == MAX_IMAGE_REDIRECTS {
                return Err(MultimodalNormalizeError::new(
                    "image URL redirected too many times",
                ));
            }
            let location = response
                .headers()
                .get(LOCATION)
                .and_then(|value| value.to_str().ok())
                .ok_or_else(|| {
                    MultimodalNormalizeError::new("image URL redirect missing Location header")
                })?;
            current = current.join(location).map_err(|err| {
                MultimodalNormalizeError::new(format!("invalid image redirect URL: {err}"))
            })?;
            validate_remote_image_url(&current).await?;
            continue;
        }

        if !response.status().is_success() {
            return Err(MultimodalNormalizeError::new(format!(
                "image URL returned HTTP {}",
                response.status()
            )));
        }

        let header_media_type = content_type_media_type(response.headers());
        let bytes = read_limited_response_body(response).await?;
        let media_type = select_image_media_type(
            declared_media_type.as_deref(),
            header_media_type.as_deref(),
            &bytes,
        )?;
        return Ok((media_type, BASE64_STANDARD.encode(bytes)));
    }

    Err(MultimodalNormalizeError::new(
        "image URL redirected too many times",
    ))
}

async fn parse_and_validate_remote_image_url(raw: &str) -> Result<Url, MultimodalNormalizeError> {
    let url = Url::parse(raw)
        .map_err(|err| MultimodalNormalizeError::new(format!("invalid image URL: {err}")))?;
    validate_remote_image_url(&url).await?;
    Ok(url)
}

async fn validate_remote_image_url(url: &Url) -> Result<(), MultimodalNormalizeError> {
    match url.scheme() {
        "http" | "https" => {}
        _ => {
            return Err(MultimodalNormalizeError::new(
                "image URL must use http or https",
            ));
        }
    }

    let host = url
        .host_str()
        .ok_or_else(|| MultimodalNormalizeError::new("image URL missing host"))?;
    if host.eq_ignore_ascii_case("localhost") || host.ends_with(".localhost") {
        return Err(MultimodalNormalizeError::new(
            "image URL host is not allowed",
        ));
    }

    if let Ok(ip) = host.parse::<IpAddr>() {
        validate_public_ip(ip)?;
        return Ok(());
    }

    let port = url
        .port_or_known_default()
        .ok_or_else(|| MultimodalNormalizeError::new("image URL missing known port for scheme"))?;
    let addrs = lookup_host((host, port)).await.map_err(|err| {
        MultimodalNormalizeError::new(format!("failed to resolve image URL host: {err}"))
    })?;

    let mut saw_addr = false;
    for addr in addrs {
        saw_addr = true;
        validate_public_ip(addr.ip())?;
    }

    if !saw_addr {
        return Err(MultimodalNormalizeError::new(
            "image URL host did not resolve",
        ));
    }

    Ok(())
}

fn validate_public_ip(ip: IpAddr) -> Result<(), MultimodalNormalizeError> {
    if is_blocked_ip(ip) {
        return Err(MultimodalNormalizeError::new(
            "image URL resolves to a private or reserved address",
        ));
    }
    Ok(())
}

fn is_blocked_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => is_blocked_ipv4(ip),
        IpAddr::V6(ip) => is_blocked_ipv6(ip),
    }
}

fn is_blocked_ipv4(ip: Ipv4Addr) -> bool {
    let octets = ip.octets();
    ip.is_private()
        || ip.is_loopback()
        || ip.is_link_local()
        || ip.is_broadcast()
        || ip.is_documentation()
        || ip.is_unspecified()
        || ip.is_multicast()
        || octets[0] == 0
        || octets[0] >= 224
        || (octets[0] == 100 && (octets[1] & 0b1100_0000) == 64)
        || (octets[0] == 169 && octets[1] == 254)
}

fn is_blocked_ipv6(ip: Ipv6Addr) -> bool {
    ip.is_loopback()
        || ip.is_unspecified()
        || ip.is_multicast()
        || ip.is_unique_local()
        || ip.is_unicast_link_local()
        || ip.to_ipv4_mapped().is_some_and(is_blocked_ipv4)
}

async fn read_limited_response_body(
    response: reqwest::Response,
) -> Result<Vec<u8>, MultimodalNormalizeError> {
    let mut out = Vec::new();
    let mut stream = response.bytes_stream();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|err| {
            MultimodalNormalizeError::new(format!("failed to read image body: {err}"))
        })?;
        if out.len() + chunk.len() > MAX_REMOTE_IMAGE_BYTES {
            return Err(MultimodalNormalizeError::new("image URL body is too large"));
        }
        out.extend_from_slice(&chunk);
    }

    Ok(out)
}

fn content_type_media_type(headers: &HeaderMap) -> Option<String> {
    headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(str::trim)
        .map(str::to_ascii_lowercase)
        .filter(|value| supported_image_media_type(value).is_some())
}

fn select_image_media_type(
    declared: Option<&str>,
    header: Option<&str>,
    bytes: &[u8],
) -> Result<String, MultimodalNormalizeError> {
    if let Some(media_type) = detect_image_media_type(bytes) {
        return Ok(media_type);
    }
    if let Some(media_type) = header.and_then(supported_image_media_type) {
        return Ok(media_type.to_string());
    }
    if let Some(media_type) = declared.and_then(|value| supported_image_media_type(value.trim())) {
        return Ok(media_type.to_string());
    }
    Err(MultimodalNormalizeError::new(
        "image URL did not contain a supported png/jpeg/gif/webp image",
    ))
}

fn detect_image_media_type(bytes: &[u8]) -> Option<String> {
    match image::guess_format(bytes).ok()? {
        image::ImageFormat::Png => Some("image/png".to_string()),
        image::ImageFormat::Jpeg => Some("image/jpeg".to_string()),
        image::ImageFormat::Gif => Some("image/gif".to_string()),
        image::ImageFormat::WebP => Some("image/webp".to_string()),
        _ => None,
    }
}

fn supported_image_media_type(media_type: &str) -> Option<&'static str> {
    match media_type
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "image/png" => Some("image/png"),
        "image/jpeg" | "image/jpg" => Some("image/jpeg"),
        "image/gif" => Some("image/gif"),
        "image/webp" => Some("image/webp"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::anthropic::types::Message;

    fn request_with_content(content: Value) -> MessagesRequest {
        MessagesRequest {
            model: "claude-sonnet-4-6".to_string(),
            max_tokens: 64,
            messages: vec![Message {
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

    #[tokio::test]
    async fn test_normalize_openai_image_url_data_url() {
        let mut req = request_with_content(serde_json::json!([
            {"type":"text","text":"describe"},
            {"type":"image_url","image_url":{"url":"data:image/png;base64,aGVsbG8="}}
        ]));

        let stats = normalize_multimodal_urls(&mut req)
            .await
            .expect("data URL should normalize");

        assert_eq!(stats.openai_image_url_blocks, 1);
        assert_eq!(stats.data_url_images, 1);
        let block = &req.messages[0].content.as_array().unwrap()[1];
        assert_eq!(block["type"], "image");
        assert_eq!(block["source"]["type"], "base64");
        assert_eq!(block["source"]["media_type"], "image/png");
        assert_eq!(block["source"]["data"], "aGVsbG8=");
    }

    #[tokio::test]
    async fn test_normalize_openai_image_url_string_data_url() {
        let mut req = request_with_content(serde_json::json!([
            {"type":"image_url","image_url":"data:image/jpg;base64,aGVsbG8="}
        ]));

        let stats = normalize_multimodal_urls(&mut req)
            .await
            .expect("string image_url data URL should normalize");

        assert_eq!(stats.openai_image_url_blocks, 1);
        let block = &req.messages[0].content.as_array().unwrap()[0];
        assert_eq!(block["type"], "image");
        assert_eq!(block["source"]["type"], "base64");
        assert_eq!(block["source"]["media_type"], "image/jpeg");
        assert_eq!(block["source"]["data"], "aGVsbG8=");
    }

    #[tokio::test]
    async fn test_normalize_anthropic_url_source_data_url() {
        let mut req = request_with_content(serde_json::json!([
            {"type":"image","source":{"type":"url","url":"data:image/jpeg;base64,aGVsbG8="}}
        ]));

        let stats = normalize_multimodal_urls(&mut req)
            .await
            .expect("Anthropic URL source data URL should normalize");

        assert_eq!(stats.anthropic_url_blocks, 1);
        let block = &req.messages[0].content.as_array().unwrap()[0];
        assert_eq!(block["source"]["type"], "base64");
        assert_eq!(block["source"]["media_type"], "image/jpeg");
        assert_eq!(block["source"]["data"], "aGVsbG8=");
    }

    #[tokio::test]
    async fn test_reject_private_remote_image_url() {
        let mut req = request_with_content(serde_json::json!([
            {"type":"image_url","image_url":{"url":"http://127.0.0.1/image.png"}}
        ]));

        let err = normalize_multimodal_urls(&mut req)
            .await
            .expect_err("private URL should be rejected");

        assert!(err.to_string().contains("private or reserved"));
    }
}
