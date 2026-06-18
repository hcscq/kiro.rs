#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-agent-stack-state-ext}"
NODE_SELECTOR="${NODE_SELECTOR:-kiro.rs/egress-proxy=true}"
PROXY_PORT="${PROXY_PORT:-3128}"
PROXY_SCHEME="${PROXY_SCHEME:-http}"
KIRO_SERVICE="${KIRO_SERVICE:-kiro-rs}"
KIRO_PORT="${KIRO_PORT:-8990}"
CONFIG_SECRET="${CONFIG_SECRET:-kiro-rs-config}"
ADMIN_API_KEY_JSON_PATH="${ADMIN_API_KEY_JSON_PATH:-adminApiKey}"
PROBE_URL="${PROBE_URL:-https://api.ipify.org}"
FAILURE_THRESHOLD="${FAILURE_THRESHOLD:-3}"
COOLDOWN_SECS="${COOLDOWN_SECS:-300}"
REQUIRE_PROXY="${REQUIRE_PROXY:-false}"
ASSIGNMENT_STRATEGY="${ASSIGNMENT_STRATEGY:-weighted_least_assigned}"
PRUNE_MISSING="${PRUNE_MISSING:-false}"
DRY_RUN="${DRY_RUN:-false}"

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "missing required command: $1" >&2
    exit 1
  }
}

need_cmd kubectl
need_cmd jq
need_cmd curl

admin_api_key="$(
  kubectl -n "$NAMESPACE" get secret "$CONFIG_SECRET" -o jsonpath='{.data.config\.json}' |
    base64 -d |
    jq -r --arg path "$ADMIN_API_KEY_JSON_PATH" '.[$path] // empty'
)"

if [ -z "$admin_api_key" ]; then
  echo "admin API key not found in secret ${NAMESPACE}/${CONFIG_SECRET}" >&2
  exit 1
fi

kiro_service_ip="$(kubectl -n "$NAMESPACE" get svc "$KIRO_SERVICE" -o jsonpath='{.spec.clusterIP}')"
if [ -z "$kiro_service_ip" ]; then
  echo "kiro service ${NAMESPACE}/${KIRO_SERVICE} has no ClusterIP" >&2
  exit 1
fi

probe_pod="$(
  kubectl -n "$NAMESPACE" get pods -l app.kubernetes.io/name=kiro-rs \
    -o jsonpath='{range .items[?(@.status.phase=="Running")]}{.metadata.name}{"\n"}{end}' |
    head -n 1
)"
if [ -z "$probe_pod" ]; then
  echo "no running kiro-rs pod found for proxy probes" >&2
  exit 1
fi

current="$(
  curl -fsS -H "x-api-key: ${admin_api_key}" \
    "http://${kiro_service_ip}:${KIRO_PORT}/api/admin/config/load-balancing"
)"

nodes_json="$(kubectl get nodes -l "$NODE_SELECTOR" -o json)"

discovered='[]'
while IFS= read -r node; do
  [ -n "$node" ] || continue
  name="$(jq -r '.metadata.name' <<<"$node")"
  id="$(jq -r '.metadata.labels["kiro.rs/egress-id"] // .metadata.name' <<<"$node")"
  service="$(jq -r --arg id "$id" '.metadata.labels["kiro.rs/proxy-service"] // ("kiro-egress-proxy-" + $id)' <<<"$node")"
  expected_ip="$(jq -r '.metadata.labels["kiro.rs/expected-egress-ip"] // empty' <<<"$node")"
  weight="$(jq -r '.metadata.labels["kiro.rs/proxy-weight"] // "1"' <<<"$node")"
  enabled="$(jq -r '.metadata.labels["kiro.rs/proxy-enabled"] // "true"' <<<"$node")"

  if ! [[ "$weight" =~ ^[0-9]+$ ]] || [ "$weight" -le 0 ]; then
    echo "skip node ${name}: invalid kiro.rs/proxy-weight=${weight}" >&2
    continue
  fi

  if ! kubectl -n "$NAMESPACE" get svc "$service" >/dev/null 2>&1; then
    echo "skip node ${name}: service ${NAMESPACE}/${service} not found" >&2
    continue
  fi

  ready_endpoint_count="$(
    kubectl -n "$NAMESPACE" get endpointslices -l "kubernetes.io/service-name=${service}" -o json |
      jq '[.items[].endpoints[]? | select(.conditions.ready == true)] | length'
  )"
  if [ "$ready_endpoint_count" -eq 0 ]; then
    echo "skip node ${name}: service ${service} has no ready endpoints" >&2
    continue
  fi

  proxy_url="${PROXY_SCHEME}://${service}.${NAMESPACE}.svc.cluster.local:${PROXY_PORT}"
  if ! observed_ip="$(
    kubectl -n "$NAMESPACE" exec "$probe_pod" -- sh -c \
      "curl -fsS --connect-timeout 5 --max-time 20 --proxy '$proxy_url' '$PROBE_URL'" |
      tr -d '\r\n'
  )"; then
    echo "skip node ${name}: proxy probe through ${proxy_url} failed" >&2
    continue
  fi

  if [ -z "$observed_ip" ]; then
    echo "skip node ${name}: proxy probe through ${proxy_url} returned empty egress IP" >&2
    continue
  fi

  if [ -n "$expected_ip" ] && [ "$observed_ip" != "$expected_ip" ]; then
    echo "skip node ${name}: expected egress ${expected_ip}, observed ${observed_ip}" >&2
    continue
  fi
  if [ -z "$expected_ip" ]; then
    expected_ip="$observed_ip"
  fi

  discovered="$(
    jq -c \
      --arg id "$id" \
      --arg url "$proxy_url" \
      --arg expected "$expected_ip" \
      --argjson weight "$weight" \
      --argjson enabled "$enabled" \
      '. + [{
        id: $id,
        url: $url,
        weight: $weight,
        enabled: $enabled,
        expectedEgressIp: $expected
      }]' <<<"$discovered"
  )"
done < <(jq -c '.items[]' <<<"$nodes_json")

if [ "$(jq 'length' <<<"$discovered")" -eq 0 ]; then
  echo "no valid proxy nodes discovered" >&2
  exit 1
fi

next_proxy_pool="$(
  jq -c \
    --argjson discovered "$discovered" \
    --argjson requireProxy "$REQUIRE_PROXY" \
    --arg assignmentStrategy "$ASSIGNMENT_STRATEGY" \
    --argjson failureThreshold "$FAILURE_THRESHOLD" \
    --argjson cooldownSecs "$COOLDOWN_SECS" \
    --arg probeUrl "$PROBE_URL" \
    --argjson pruneMissing "$PRUNE_MISSING" '
      .proxyPool as $existing |
      ($existing.proxies // []) as $existingProxies |
      ($discovered | map(.id)) as $discoveredIds |
      (
        if $pruneMissing then
          $existingProxies | map(select(.id as $id | $discoveredIds | index($id)))
        else
          $existingProxies
        end
      ) as $base |
      ($base | map(select(.id as $id | ($discoveredIds | index($id) | not)))) as $manual |
      {
        enabled: true,
        requireProxy: $requireProxy,
        assignmentStrategy: $assignmentStrategy,
        proxies: ($manual + $discovered),
        failover: {
          enabled: (($existing.failover.enabled // true) | not | not),
          failureThreshold: $failureThreshold,
          cooldownSecs: $cooldownSecs,
          probeUrl: $probeUrl
        }
      }
    ' <<<"$current"
)"

payload="$(jq -c --argjson proxyPool "$next_proxy_pool" '{proxyPool: $proxyPool}' <<<"{}")"

if [ "$DRY_RUN" = "true" ]; then
  jq . <<<"$payload"
  exit 0
fi

curl -fsS -X PUT \
  -H "x-api-key: ${admin_api_key}" \
  -H "content-type: application/json" \
  --data-binary "$payload" \
  "http://${kiro_service_ip}:${KIRO_PORT}/api/admin/config/load-balancing" |
  jq '{proxyPool: .proxyPool}'
