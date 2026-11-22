# Phase 5: 5,000 RPS Benchmarks, Chaos & Production Cutover

## 1. Goals & Scope

Complete **Phase 5** from `.ai/progress-tracker.md` and `tasks/RAG Search Orchestrator Implementation Plan.pdf`:

1. **Performance Benchmarks (`test/performance/`, `//go:build performance`)**:
   - Add Go benchmark and load-simulation tests validating:
     - Single-pod sustained throughput (~350 RPS/pod, scaling to 5,000 RPS across 14–16 pods) with P99 <= 75ms cold-path and <= 8ms Tier 1/2 cache-hit latency (including normalized string cache coherence: Tier 1 hit + Tier 2 Roaring Bitmap intersection).
     - `sync.Pool` candidate buffer reuse and zero-alloc Roaring Bitmap ACL (over `100,000` `uint32` IDs, `< 1ms`) / RRF hot paths (`-benchmem`, `testing.AllocsPerRun`, and `-cpuprofile`/`-memprofile` `pprof` verification).
   - Add `bench-perf` (`-tags=performance`) and `test-chaos` (`-tags=chaos`) targets to `Makefile` so `make check` (`go test -v -race ./...`) stays fast in CI.

2. **Chaos & Fault-Injection Suite (`test/chaos/`, `//go:build chaos`)**:
   - **Triton 50ms Latency Injection**: Inject >= 50ms latency into mock Triton Cross-Encoder; drive enough requests (>= `DefaultMinRequests` = 20 or configured threshold) to trip `StateOpen` (`DefaultRerankTimeout` = 35ms), assert `search_circuit_breaker_tripped_total` increments, and verify Top-10 ACL-filtered RRF candidates are returned (`reranker_fallback: true`) within the 75ms P99 SLO once tripped.
   - **Dense / Sparse Shard Dropouts**: Inject `DeadlineExceeded` / `Unavailable` on HNSW dense or BM25 sparse gRPC servers; assert 20ms fan-out deadline triggers single-index fallback without failing the search request.
   - **Redis Partition / Timeout**: Inject Redis errors/timeouts on Tier 1 cache and parent hydration; assert graceful fallback (live fan-out on cache failure, bounded hydration error handling) with zero panics or secret leakage.

3. **Distroless Container & Helm / ArgoCD Production Manifests (`Dockerfile`, `deployments/helm/`)**:
   - `Dockerfile`: Multi-stage Go (`golang:1.26-alpine`, `CGO_ENABLED=0`) build -> `gcr.io/distroless/static:nonroot` (UID `65532`), zero shell utilities, read-only rootfs compatible.
   - `deployments/helm/`:
     - `Chart.yaml`, `values.yaml` (default `image.repository: ghcr.io/rag-platform/rag-search-orchestrator`; passes `helm lint deployments/helm` in CI; parameterizes `main.go` env vars such as `REDIS_ADDRS`, `REDIS_PASSWORD` via `Secret`/`secretKeyRef`, `HNSW_TARGETS`, `BM25_TARGETS`, `TRITON_EMBED_TARGETS`, `TRITON_RERANK_TARGETS`, `GRPC_POOL_SIZE`, `SHUTDOWN_DRAIN_DELAY`, `LOG_LEVEL`).
     - `templates/deployment.yaml`: Standard Kubernetes `Deployment` (default when `canary.enabled: false`), 14 baseline replicas, `4000m`/`8000m` CPU, `2Gi`/`4Gi` memory, `GOMEMLIMIT=3600MiB`, `GOGC=100`, `topologySpreadConstraints` across 3 AZs, `securityContext` (`runAsNonRoot: true`, `runAsUser: 65532`, `readOnlyRootFilesystem: true`, `capabilities: drop: ["ALL"]`), graceful drain via `SHUTDOWN_DRAIN_DELAY=5s` (no shell `preStop: sleep 5` on distroless, or add a `--prestop-sleep=5s` binary flag in `main.go`) + `terminationGracePeriodSeconds: 25`, `/healthz` & `/ready` probes.
     - `templates/rollout.yaml`, `templates/analysistemplate.yaml` (optional), & `templates/argocd-application.yaml`: Optional Argo Rollouts `Rollout` CRD (10% 10m -> 25% 15m -> 100%, automated analysis for error rate <= 0.01% and P99 <= 75ms) and ArgoCD `Application` gated by `canary.enabled: true` so `helm lint` passes without external CRDs.
     - `templates/hpa.yaml`: `minReplicas: 14`, `maxReplicas: 32`.
     - `templates/pdb.yaml`: `minAvailable: 10`.
     - `templates/service.yaml` (`:8080` HTTP, `:50051` gRPC).
     - `templates/networkpolicy.yaml`: Restricted ingress (Edge Gateway + Prometheus `/metrics`) and egress (Redis `:6379`, HNSW, BM25, Triton, `rag-telemetry-audit-daemon`, DNS `:53`).
     - `templates/prometheusrule.yaml` & `templates/servicemonitor.yaml`: Gated by `monitoring.enabled` (for clusters without Prometheus Operator CRDs); alert rules from `.ai/architecture.md` (P99 Latency Violation, Triton Reranker Saturation, Search Shard Deadline Exceeded; gate `Cache Invalidation Coherence Lag` behind `monitoring.bitmapSyncAlert.enabled: false` since `caller_acl_bitmap` is supplied per-request and `search_last_bitmap_sync_timestamp_seconds` defaults to `0`).

4. **Documentation & Quality Gates**:
   - Verify `.ai/doc-api.md` and `api/openapi/openapi.yaml` remain synchronized.
   - Update `.ai/progress-tracker.md` upon completion.
   - Pass `make check` (`gofmt -s`, `go vet`, `golangci-lint`, `govulncheck`, `gosec`, `audit-logging`, `go test -race ./...`) plus `test/performance` and `test/chaos`.
   - Commit incrementally with at least 2 conventional commits (`feat:`, `tests:`, `docs:`).
