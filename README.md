# `rag-search-orchestrator`

Mission-critical, low-latency **Go 1.26** retrieval orchestration microservice for the disaggregated RAG platform. It executes a deterministic, zero-allocation 6-stage retrieval cascade with sub-millisecond native `uint32` Roaring Bitmap entitlement enforcement before any candidate reaches GPU reranking.

---

## Key Capabilities & SLOs

- **Cold-Path Latency**: $\le 75\text{ ms}$ P99 end-to-end
- **Cache-Hit Latency**: $\le 8\text{ ms}$ P99 (Tier 1 Redis Cluster lookup + Tier 2 live Roaring Bitmap ACL verification + parent hydration)
- **Throughput Target**: $5,000\text{ RPS}$ across 14–32 horizontally autoscaled pods
- **Zero-Alloc Critical Path**: `sync.Pool` candidate buffers (`pkg/pool/candidate_pool.go`) and a lock-free MPSC ring buffer (`internal/infrastructure/telemetry/ring_buffer_emitter.go`) for asynchronous HIPAA/SOC 2 audit spans
- **Security & Isolation**:
  - Native `uint32` Roaring Bitmap ACL intersection ($< 1\text{ ms}$, zero runtime UUID-to-integer translation) **before** Triton Cross-Encoder GPU inference
  - Structured JSON logging (`log/slog`) with CI-enforced redaction of `Authorization` headers, bearer tokens, raw `caller_acl_bitmap` bytes, credentials, and paths
  - Distroless non-root container (`gcr.io/distroless/static:nonroot`, UID/GID `65532`, read-only root filesystem)

---

## 6-Stage Retrieval Pipeline

```text
Request (REST :8080 / gRPC :50051)
  │
  ▼
1. Query Normalization & Tier 1 Cache (Redis 7.2 Cluster)
   ├─ Cache Hit ──▶ Tier 2 Live Roaring Bitmap ACL Filter
   │                  ├─ >= top_k survive ──▶ Rerank (if needed) ──▶ Hydrate Top-5
   │                  └─ < top_k survive  ──▶ Bypass to live fan-out (no cache eviction)
   └─ Cache Miss
        │
        ▼
2. Dense Query Embedding (Triton FP16 gRPC, <= 15 ms)
        │
        ▼
3. Parallel Multi-Index Fan-Out (20 ms hard deadline, single-index fallback)
   ├─ HNSW Dense Replicas (Top-40)
   └─ BM25 Sparse Shards  (Top-40)
        │
        ▼
4. Reciprocal Rank Fusion (RRF, k=60) ──▶ Top-20 Candidates
        │
        ▼
5. Native uint32 Roaring Bitmap ACL Filter (< 1 ms, zero heap allocs)
        │
        ▼
6. Bounded Triton Cross-Encoder Reranker (Top-10, <= 30 ms)
   └─ Circuit Breaker (> 35 ms latency, > 5% errors, or logit < tau) ──▶ Promote RRF Top-K
        │
        ▼
7. Parent Chunk Hydration (Redis MGET, Top-5 chunks, 800-1,500 tokens)
   + Asynchronous Lock-Free Ring Buffer Audit Telemetry
```

---

## APIs & Endpoints

- **HTTP / OpenAPI 3.1 (`:8080`)**:
  - `POST /v1/search` — Execute retrieval cascade and return Top-K enriched parent chunks
  - `POST /v1/search/diagnostic` — Execute retrieval with stage timings, candidate counts, and reranker logits (`ScoreTrace`)
  - `GET /healthz` — Liveness probe
  - `GET /ready` — Readiness probe (checks Redis, Triton, HNSW/BM25 pools, and drain state)
  - `GET /metrics` — Prometheus metrics endpoint
  - `GET /openapi.yaml` & `GET /docs` — OpenAPI 3.1 spec and Swagger UI
- **gRPC / Protobuf v3 (`:50051`)**:
  - `SearchService.Search`
  - `SearchService.DiagnosticSearch`

Full contracts are documented in [`.ai/doc-api.md`](.ai/doc-api.md) and [`api/openapi/openapi.yaml`](api/openapi/openapi.yaml).

---

## Local Development & Testing

### 1. Configure Environment

```bash
cp .env.example .env
```

Process-level environment variables always take precedence over `.env`. Non-secret production defaults in `.env.prod` are synced to `deployments/helm/values.yaml` via:

```bash
python scripts/sync_env_to_deployments.py --sync
```

### 2. Run Locally

```bash
go run ./cmd/search-orchestrator
```

### 3. Quality Gates & Tests

```bash
make fmt            # gofmt -s -w .
make vet            # go vet ./...
make lint           # golangci-lint run ./...
make test           # go test -v -race ./...
make test-chaos     # go test -tags=chaos -v -race ./test/chaos/...
make bench          # unit benchmarks with -benchmem
make bench-perf     # performance benchmarks (-tags=performance)
make audit-logging  # secured-logging AST audit & regression tests
make check          # full pre-commit verification suite
```

---

## Deployment & Operations

- **Container Image**: Built with the multi-stage [`Dockerfile`](Dockerfile) (`golang:1.26-alpine` builder $\rightarrow$ `gcr.io/distroless/static:nonroot`).
- **Kubernetes & Helm**: Chart located at [`deployments/helm/`](deployments/helm/) with HPA (14–32 replicas), PDB (`minAvailable: 10`), `NetworkPolicy`, `ServiceMonitor`, `PrometheusRule`, and optional Argo Rollouts progressive canary (`AnalysisTemplate` SLO gates).
- **Redis Cluster**: Deployed via [`deployments/argocd/redis-cluster-application.yaml`](deployments/argocd/redis-cluster-application.yaml) or [`scripts/deploy_redis_cluster.sh`](scripts/deploy_redis_cluster.sh) / [`scripts/deploy_redis_cluster.ps1`](scripts/deploy_redis_cluster.ps1).
- **CI/CD & Secret Management**: Automated via [`.github/workflows/ci.yml`](.github/workflows/ci.yml) and [`.github/workflows/deploy.yml`](.github/workflows/deploy.yml). See [`deployments/SECRET_MANAGEMENT.md`](deployments/SECRET_MANAGEMENT.md) for secret injection and zero-downtime `REDIS_PASSWORD` rotation procedures.

---

## Architecture & Specifications

- [`.ai/architecture.md`](.ai/architecture.md) — 5-layer backend architecture, folder layout, and observability rules
- [`.ai/doc-api.md`](.ai/doc-api.md) — Authoritative REST, gRPC, Redis cache/hydration, telemetry, and Prometheus contracts
- [`.ai/security.md`](.ai/security.md) — Threat model, Roaring Bitmap ACL isolation, and secured logging invariants
- [`.ai/code-standards.md`](.ai/code-standards.md) — Go coding standards, `sync.Pool` patterns, and circuit breaking
