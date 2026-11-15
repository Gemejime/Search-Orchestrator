# Architecture Context - RAG Search Orchestrator (`rag-search-orchestrator`)

## System Stack & Specifications

| Layer | Technology | Role & Operational Characteristics |
| :--- | :--- | :--- |
| **Language Runtime** | Go 1.23+ (64-bit) | Low-latency concurrent network I/O, `GOMEMLIMIT=3600MiB`, `GOGC=100` |
| **Memory Discipline** | `sync.Pool` + Lock-Free Ring Buffer | Zero-alloc candidate slice reuse & non-blocking audit trace emission |
| **Inbound Transport** | gRPC (Protobuf v3 / HTTP/2) & REST | Multiplexed HTTP/2 gRPC (`:50051`, 30s TCP keepalive) + OpenAPI 3.1 HTTP (`:8080`) |
| **Entitlement ACL** | Native `uint32` Roaring Bitmaps | In-memory bitwise AND intersection ($<1\text{ ms}$, zero UUID translation) |
| **Cache & Hydration** | Redis 7.2 Cluster | Tier 1 normalized query cache & pipelined `MGET` parent container hydration (3–6ms) |
| **Dense & Sparse Search** | HNSW Read Replicas & BM25 Shards | Parallel unary gRPC fan-out with $20\text{ ms}$ `context.WithTimeout` deadline |
| **GPU Inference** | Triton Inference Server (gRPC) | FP16 query embedding ($\le 15\text{ ms}$) & dynamic-batched Cross-Encoder ($\le 30\text{ ms}$, max batch 32, 4ms timeout) |
| **Compliance Audit** | `rag-telemetry-audit-daemon` | Asynchronous HIPAA/SOC 2 provenance & access trace ingestion via ring buffer |
| **Containerization** | Distroless (`gcr.io/distroless/static:nonroot`) | Non-root UID `65532`, read-only rootfs, zero shell utilities |

---

## Production-Grade 5-Layer Backend Architecture

Aligned with `tasks/Production Grade Backend Concepts.pdf`:

### Layer 1: API & Routing
- **Entry Points (`cmd/search-orchestrator/main.go`)**:
  - Initializes 12-factor configuration, warms up gRPC channel connection pools (Triton, HNSW replicas, BM25 shards) and Redis 7.2 Cluster client, registers Prometheus metrics, and starts HTTP (`:8080`) and gRPC (`:50051`) listeners.
  - Handles graceful termination (`SIGINT`, `SIGTERM`) with a 5-second `preStop` deregistration sleep and gRPC `GracefulStop()` draining pending candidates and ring buffers within a 15-second deadline.
- **Endpoints & Routing (`internal/transport/grpc/` & `internal/transport/http/`)**:
  - gRPC `SearchService`: `Search` and `DiagnosticSearch` RPCs over multiplexed HTTP/2.
  - HTTP REST Gateway: `POST /v1/search`, `POST /v1/search/diagnostic`, `GET /healthz`, `GET /ready`, `GET /metrics`, `GET /openapi.yaml`, `GET /docs`.
- **Middleware & Interceptors (`internal/transport/grpc/interceptor/`)**:
  - Request/trace ID correlation, tenant context validation, Prometheus metric counters, and panic recovery.
- **Input Validation & Sanitization**:
  - Strict validation of `tenant_id`, non-empty normalized query length bounds, valid compressed `caller_acl_bitmap` bytes, and bounded `top_k` query limits.
- **Rate Limiting & Throttling**:
  - Per-tenant/caller token-bucket protection and bounded candidate cascade protecting downstream GPU pools from saturation.
- **Idempotency**:
  - Read-only retrieval operations are inherently idempotent; normalized cache keys (`SHA256(len_bytes(Normalize(QueryText)) + ":" + Normalize(QueryText) + ":" + len_bytes(TenantID) + ":" + TenantID + ":" + len_bytes(EmbeddingModelVersion) + ":" + EmbeddingModelVersion)`) ensure deterministic Tier 1 cache resolution.

### Layer 2: Data Persistence & State
- **Caching (Tier 1 & Tier 2)**:
  - Tier 1: Redis 7.2 Cluster stores TTL-bounded `uint32` chunk/doc candidate references keyed by normalized query hash.
  - Tier 2: Mandatory in-memory Roaring Bitmap intersection against `caller_acl_bitmap` on every cache hit before parent hydration.
- **Parent Container Hydration**:
  - Pipelined Redis `MGET` on pre-warmed parent keys for Top-5 surviving chunks (800–1,500 tokens), bypassing database transaction pools.
- **Connection Pooling**:
  - Pre-warmed gRPC channel pools for HNSW, BM25, and Triton; Redis Cluster connection pool with bounded timeouts.
- **Pagination & Query Limits**:
  - Hard-capped deterministic cascade: Top-40 dense + Top-40 sparse $\rightarrow$ Top-20 RRF $\rightarrow$ ACL filter $\rightarrow$ Top-10 cross-encoder $\rightarrow$ Top-5 hydrated chunks.
- **Resource Identification**:
  - Immutable monotonic `uint32 doc_id` assigned upstream by `rag-kb-catalog-service`; high-entropy ULIDs/UUIDs for `trace_id` and `request_id`.

### Layer 3: Architecture & Scale
- **Parallel Fan-Out & Concurrency Control**:
  - Concurrent goroutines for dense HNSW and sparse BM25 unary gRPC calls bounded by `context.WithTimeout(ctx, 20*time.Millisecond)` and `sync.Pool` slice buffers (`pkg/pool/candidate_pool.go`) to prevent GC stalls.
  - Automatic single-index fallback: if one search modality times out or errors, RRF proceeds with surviving candidates from the healthy modality.
- **Deterministic Circuit Breaking**:
  - `internal/infrastructure/triton/circuit_breaker.go` trips when Triton Cross-Encoder P99 latency exceeds $35\text{ ms}$ or errors exceed $5\%$ (and bypasses reranking if remaining request `ctx.Deadline()` budget is $<15\text{ ms}$), serving Top-10 ACL-filtered RRF candidates directly.
- **High Availability & Redundancy**:
  - 14–16 baseline pods (HPA: 14–32) with `topologySpreadConstraints` across 3 AZs and `PodDisruptionBudget` (`minAvailable: 10`).
  - ArgoCD progressive canary rollout: 10% (10 mins) $\rightarrow$ 25% (15 mins) $\rightarrow$ 100% promotion.
- **Creational Design Patterns**:
  - Explicit constructor-based dependency injection in `cmd/search-orchestrator/main.go` wiring domain services, repository ports, and infrastructure adapters.

### Layer 4: Security & Identity
- **AuthN vs AuthZ**:
  - Edge API Gateway authenticates callers; `rag-search-orchestrator` enforces tenant isolation (`tenant_id`) and document-level AuthZ via `caller_acl_bitmap` Roaring Bitmap intersection **before** candidates reach Triton Cross-Encoder GPU buffers.
- **Configuration & Secrets Management**:
  - 12-factor environment configuration; credentials injected via Kubernetes Secrets / external secret stores (never in source control).
- **Secured Logging**:
  - Structured JSON logs (`log/slog`) with strict redaction of auth headers, bearer tokens, raw `caller_acl_bitmap` bytes, credentials, and PII/PHI.
- **Container Hardening**:
  - `gcr.io/distroless/static:nonroot`, UID `65532`, `readOnlyRootFilesystem: true`, `capabilities: drop: ["ALL"]`.

### Layer 5: Observability & Quality
- **Structured Logging**: JSON logs with `trace_id`, `request_id`, `tenant_id`, `cache_hit`, `stage`, and `duration_ms`.
- **Primary Prometheus Telemetry Metrics**:
  - `search_orchestrator_requests_total` (labels: `tenant_id`, `status_code`, `cache_hit`)
  - `search_orchestrator_latency_seconds` (histogram, exponential buckets 5ms–250ms)
  - `search_fanout_latency_seconds` (histogram by component: `hnsw_dense`, `bm25_sparse`, `triton_embed`, `triton_rerank`, `redis_hydrate`)
  - `search_circuit_breaker_tripped_total` (counter)
  - `search_roaring_bitmap_eval_duration_seconds` (histogram, microsecond-level ACL intersection)
- **Production Alert Rules**:
  - *P99 Latency Violation (Page)*: `histogram_quantile(0.99, sum(rate(search_orchestrator_latency_seconds_bucket[5m])) by (le)) > 0.075` for `>3m`.
  - *Triton Reranker Saturation (Warning)*: `rate(search_circuit_breaker_tripped_total[2m]) > 5` for `>1m`.
  - *Search Shard Deadline Exceeded (Warning)*: `sum(rate(search_fanout_stage_errors_total{error="DeadlineExceeded"}[5m])) / sum(rate(search_orchestrator_requests_total[5m])) > 0.01`.
  - *Cache Invalidation Coherence Lag (Warning — if local Pub/Sub bitmap cache enabled)*: `time() - search_last_bitmap_sync_timestamp_seconds > 5` for `>2m`. *(Not applicable when `caller_acl_bitmap` is supplied purely per-request by the Edge Gateway.)*
- **Health Checks & Graceful Error Handling**:
  - `/healthz` (liveness) and `/ready` (readiness probing Redis, Triton, HNSW/BM25 pools, and drain state); sanitized client error responses.

---

## Adjusted Folder Structure

```text
rag-search-orchestrator/
├── cmd/
│   └── search-orchestrator/
│       └── main.go                     # Dependency wiring, connection pool warmup, health probes
├── api/
│   ├── proto/v1/
│   │   ├── search_service.proto        # Pure retrieval & ranking gRPC definitions
│   │   └── types.proto                 # Enriched chunk, score trace, and bitmap models
│   └── openapi/
│       └── openapi.yaml                # Public REST retrieval proxy specifications (OpenAPI 3.1)
├── internal/
│   ├── domain/                         # Core business logic (pure Go, zero network dependencies)
│   │   ├── entity/
│   │   │   ├── query_context.go        # Tenant identity, normalized text, ACL bitmap
│   │   │   ├── search_candidate.go     # Candidate with native uint32 doc_id and chunk keys
│   │   │   └── fused_result.go         # Post-RRF and post-rerank data structures
│   │   ├── ranking/
│   │   │   ├── rrf_engine.go           # Standard Reciprocal Rank Fusion (k=60)
│   │   │   └── cascade_filter.go       # Fixed candidate cascade (Top-80 -> Top-20 -> Top-10)
│   │   ├── acl/
│   │   │   └── roaring_evaluator.go    # Fast bitwise uint32 bitmap intersection
│   │   └── repository/                 # Domain interface ports
│   │       ├── dense_search_port.go
│   │       ├── sparse_search_port.go
│   │       ├── triton_inference_port.go
│   │       └── parent_chunk_port.go
│   ├── application/                    # Application use cases
│   │   ├── usecase/
│   │   │   ├── execute_search.go       # Primary staged retrieval cascade
│   │   │   └── execute_diagnostic_search.go # Detailed score waterfalls and timing breakdowns
│   │   └── dto/
│   │       ├── search_request.go
│   │       └── search_response.go
│   ├── infrastructure/                 # Adapters and external network clients
│   │   ├── cache/
│   │   │   └── redis/
│   │   │       ├── query_text_cache.go # Tier 1 normalized query string cache
│   │   │       └── parent_chunk_reader.go # Redis Cluster MGET parent chunk reader
│   │   ├── search/
│   │   │   ├── dense/
│   │   │   │   └── hnsw_grpc_client.go # Resilient gRPC client with 20ms context timeout
│   │   │   └── sparse/
│   │   │       └── bm25_grpc_client.go # Sharded gRPC client with 20ms context timeout
│   │   ├── triton/
│   │   │   ├── embedding_client.go     # Triton client for FP16 query embedding
│   │   │   ├── reranker_client.go      # Triton client for cross-encoder passage scoring
│   │   │   └── circuit_breaker.go      # Trip logic bypassing reranker under GPU queue stalls
│   │   └── telemetry/
│   │       └── ring_buffer_emitter.go  # Zero-alloc trace publisher to audit daemon
│   └── transport/                      # Inbound protocol handlers
│       ├── grpc/
│       │   ├── search_handler.go       # High-throughput gRPC search implementation
│       │   └── interceptor/            # Tenant context, metric counters, panic recovery
│       └── http/
│           ├── router.go               # HTTP routing definitions
│           └── search_controller.go    # Public REST gateway controller
├── pkg/
│   └── pool/
│       └── candidate_pool.go           # sync.Pool reusable slice buffers
├── deployments/
│   └── helm/                           # Scaled charts (14-16 replicas, 4-8 CPU / 2-4 GiB limits)
├── test/
│   ├── unit/                           # RRF math, cascade boundaries, bitmap operations
│   ├── integration/                    # Testcontainers suites (Redis, mock Triton, mock shards)
│   ├── performance/                    # 5,000 RPS benchmarks and Go pprof allocations
│   └── chaos/                          # Triton timeouts, sparse shard dropouts, Redis partitions
├── Makefile
├── go.mod
└── go.sum
```
