# Proxy pool node discovery

`scripts/sync-k8s-proxy-pool.sh` discovers Kubernetes nodes that are approved as
Kiro egress proxy nodes, verifies their proxy Service, probes the observed egress
IP from a running `kiro-rs` pod, then updates `proxyPool` through the Admin API.

## Node labels

Only nodes with this label are considered:

```bash
kubectl label node <node> kiro.rs/egress-proxy=true
```

Recommended labels:

```bash
kubectl label node <node> \
  kiro.rs/egress-id=rainyun-kbmxbmzt \
  kiro.rs/proxy-service=kiro-egress-proxy-kbmxbmzt \
  kiro.rs/expected-egress-ip=154.9.227.230 \
  kiro.rs/proxy-weight=1 \
  kiro.rs/proxy-enabled=true
```

The script expects each proxy Service to point to exactly one stable egress node.
Do not put multiple egress nodes behind one Service when credential egress IP
stability matters.

## Run

Dry run:

```bash
DRY_RUN=true ./scripts/sync-k8s-proxy-pool.sh
```

Apply:

```bash
./scripts/sync-k8s-proxy-pool.sh
```

Useful environment overrides:

```bash
NAMESPACE=agent-stack-state-ext
NODE_SELECTOR='kiro.rs/egress-proxy=true'
PROXY_PORT=3128
REQUIRE_PROXY=false
PRUNE_MISSING=false
PROBE_URL=https://api.ipify.org
```

`PRUNE_MISSING=false` preserves manually configured proxy entries. Set it to
`true` only when the labeled node set should be the complete source of truth.

## Automatic sync in Kubernetes

Create or refresh the script ConfigMap, then apply the CronJob template:

```bash
kubectl -n agent-stack-state-ext create configmap kiro-proxy-pool-sync-script \
  --from-file=sync-k8s-proxy-pool.sh=./scripts/sync-k8s-proxy-pool.sh \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl apply -f k8s/proxy-pool-sync-cronjob.yaml
```

The CronJob runs every five minutes. Adding the required node label plus the
recommended proxy labels is enough for the next run to validate the node and add
or update its `proxyPool.proxies[]` entry through the Admin API.

The template installs `bash`, `curl`, `jq`, and `kubectl` in an `alpine:3.21`
container at job start. For production clusters without outbound package access,
replace the image with an internal operations image that already contains those
tools.
