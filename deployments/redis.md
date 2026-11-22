# Self-Hosted Bitnami Redis 7.2 Cluster — `rag-search-orchestrator`

This document describes the dedicated, in-cluster Bitnami Redis 7.2 Cluster used by `rag-search-orchestrator` for **Tier 1 normalized query caching** ($\le 8\text{ ms}$ P99 fast path) and **pipelined `MGET` parent container hydration** ($3\text{–}6\text{ ms}$).

---

## 1. Cluster Topology & Latency Rationale

- **Topology**: 6-node Redis 7.2 Cluster (`3` Primary shards + `3` Secondary replicas) managed via `deployments/helm/redis-cluster/values.yaml`.
- **Persistence**: `5Gi` RWO PersistentVolumeClaim per node with AOF (`appendonly yes`, `appendfsync everysec`) and RDB snapshots.
- **Anti-Affinity**: Soft pod anti-affinity distributes shards and replicas across Kubernetes nodes/AZs.
- **Why In-Cluster Redis**: Co-locating the Redis Cluster inside the `rag-search` namespace eliminates cross-VPC/NAT hops and TLS proxy overhead, delivering sub-millisecond ($P99 < 1\text{ ms}$) network round-trips for slot-grouped `MGET` hydration and SHA-256 query cache lookups.

---

## 2. Internal Headless DNS Routing

`go-redis` Universal/Cluster client discovers the full slot topology (`16,384` slots) from the headless StatefulSet seed endpoints:

```text
rag-search-orchestrator-redis-0.rag-search-orchestrator-redis-headless.rag-search.svc.cluster.local:6379
rag-search-orchestrator-redis-1.rag-search-orchestrator-redis-headless.rag-search.svc.cluster.local:6379
rag-search-orchestrator-redis-2.rag-search-orchestrator-redis-headless.rag-search.svc.cluster.local:6379
```

In `.env.prod` and `deployments/helm/values.yaml`, these are configured as a comma-separated list in `REDIS_ADDRS` (and `REDIS_CLUSTER_ADDRS` is also supported as a fallback by `internal/config/config.go`).

Authentication is shared via the existing Kubernetes Secret `rag-search-orchestrator-redis` (key `REDIS_PASSWORD`) — never stored in Git or `values.yaml`.

---

## 3. Local Development & Deployment Commands

### Local Development (Docker Compose)
```bash
# Start standalone Redis 7.2 with AOF persistence and password auth
REDIS_PASSWORD=localdevpassword docker compose -f deployments/docker/docker-compose.redis.yml up -d

# Stop local Redis
docker compose -f deployments/docker/docker-compose.redis.yml down
```

### Kubernetes Deployment (One-Click Scripts)
```bash
# Linux / macOS
./scripts/deploy_redis_cluster.sh

# Windows PowerShell
./scripts/deploy_redis_cluster.ps1
```

### GitOps Sync (ArgoCD)
```bash
kubectl apply -f deployments/argocd/redis-cluster-application.yaml
```

---

## 4. Day-2 Operational Runbooks

### Inspect Cluster Health & Slot Distribution
```bash
REDIS_PASS=$(kubectl get secret rag-search-orchestrator-redis -n rag-search -o jsonpath='{.data.REDIS_PASSWORD}' | base64 -d)
kubectl exec -n rag-search rag-search-orchestrator-redis-0 -- \
  env REDISCLI_AUTH="$REDIS_PASS" redis-cli cluster info
kubectl exec -n rag-search rag-search-orchestrator-redis-0 -- \
  env REDISCLI_AUTH="$REDIS_PASS" redis-cli cluster nodes
```
Verify `cluster_state:ok` and `cluster_slots_assigned:16384`.

### Real-Time Latency Verification
```bash
kubectl exec -it -n rag-search rag-search-orchestrator-redis-0 -- \
  env REDISCLI_AUTH="$REDIS_PASS" redis-cli --latency -i 1
```
Expected in-cluster `avg` / `P99` is $< 1\text{ ms}$.

### Prometheus Metrics Scraping
Each Redis pod runs the Bitnami `redis-exporter` sidecar on port `9121` with annotations:
- `prometheus.io/scrape: "true"`
- `prometheus.io/port: "9121"`

Verify metrics directly via port-forward:
```bash
kubectl port-forward -n rag-search svc/rag-search-orchestrator-redis-metrics 9121:9121
curl -s http://localhost:9121/metrics | head -n 20
```
