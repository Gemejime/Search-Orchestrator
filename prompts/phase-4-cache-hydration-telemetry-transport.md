# Phase 4: Tier 1/2 Redis Cache, Hydration, Telemetry & Transport

## 1. Goals

1. **Tier 1 Query Cache & Parent Chunk Hydration (`internal/infrastructure/cache/redis/`)**:
   - `query_text_cache.go`: Implement `repository.QueryCachePort` backed by `redis.UniversalClient` (supporting both Redis 7.2 Cluster in production and single-node Redis in local/Testcontainers). Keyed by `QueryContext.Tier1CacheKey()` (`qcache:<tenant_id>:<sha256(...)>`), storing compact serialized `Tier1CacheEntry` (`RerankerFallback` + pre-ACL `[]CachedCandidate`) with configurable TTL (default `5m`) and bounded write timeout so cache `SET`s never stall request budgets.
   - `parent_chunk_reader.go`: Implement `repository.ParentChunkPort` (`HydrateParents`) using cluster-safe per-slot `MGET` batching (CRC16 hash-slot grouping to prevent `CROSSSLOT` errors) or pipelined `GET` commands for `parent:<tenant_id>:<doc_id>:<chunk_id>` keys, targeting the `3–6ms` (`<= 5ms` P99 SLO) within the `10ms` `DefaultHydrateTimeout`. Deserialize parent container payload (`text` + `metadata`), preserve input candidate order and `FinalScore`, and handle missing/expired keys (`redis.Nil`) deterministically.

2. **Zero-Alloc MPSC Lock-Free Ring Buffer & Prometheus Metrics (`internal/infrastructure/telemetry/`)**:
   - `ring_buffer_emitter.go`: Implement `repository.TelemetryEmitterPort` using a bounded **MPSC** lock-free ring buffer (fixed-capacity power-of-2 slots with per-slot atomic `sequence` / Vyukov pattern) for `SearchAuditSpan`, guaranteeing `-race` safety under concurrent producers, zero heap allocations on `Emit()`, non-blocking drop-on-full semantics with an atomic drop counter, and a background consumer draining to an `AuditSink` interface (with graceful flush on shutdown).
   - `prometheus_metrics.go`: Implement `usecase.MetricsObserver` and transport-level Prometheus collectors aligned with `.ai/doc-api.md`:
     - `search_orchestrator_requests_total` (`tenant_id`, `status_code`, `cache_hit`)
     - `search_orchestrator_latency_seconds` (exponential buckets `5ms` to `250ms`)
     - `search_fanout_latency_seconds` (`component`: `hnsw_dense`, `bm25_sparse`, `triton_embed`, `triton_rerank`, `redis_hydrate`)
     - `search_fanout_stage_errors_total` (`component`, `error`)
     - `search_circuit_breaker_tripped_total`
     - `search_roaring_bitmap_eval_duration_seconds`
     - `search_last_bitmap_sync_timestamp_seconds` (Gauge)

3. **Inbound gRPC & HTTP/REST Transport (`internal/transport/`)**:
   - Generate Go Protobuf/gRPC stubs (`buf generate`) for `api/proto/v1/search_service.proto` and `api/proto/v1/types.proto`.
   - `grpc/search_handler.go`: Implement `SearchServiceServer` (`Search`, `DiagnosticSearch`) with `trace_id` extraction from gRPC metadata (or generation), optional `request_id` generation, strict input validation (`query_text` length, `caller_acl_bitmap` size bounds, `top_k` `0..5`), sanitized gRPC status error mapping (`InvalidArgument`, `Unavailable`, `Internal`), and DTO mapping.
   - `grpc/interceptor/`: Structured JSON `log/slog` logging interceptor (strictly redacting `Authorization`, bearer tokens, raw `caller_acl_bitmap` bytes, credentials, and raw query PII), `trace_id`/`request_id` correlation, panic recovery, and Prometheus metrics.
   - `http/router.go` & `http/search_controller.go`: OpenAPI 3.1 REST endpoints (`POST /v1/search`, `POST /v1/search/diagnostic`, `GET /healthz`, `GET /ready`, `GET /metrics`, `GET /openapi.yaml` served via `//go:embed` for distroless `readOnlyRootFilesystem: true`, `GET /docs`), `X-Request-ID` / `X-Tenant-ID` / `X-Trace-ID` header handling, `draining` state (`503 {"status":"draining"}` on `/ready` during `preStop`), and `http.Server` timeouts (`ReadHeaderTimeout`, `ReadTimeout`, `WriteTimeout`, `IdleTimeout`).

4. **Connection Pool Warmup & Service Entrypoint (`internal/infrastructure/grpcutil/pool.go`, `cmd/search-orchestrator/main.go`)**:
   - Implement `ConnPool.Warmup(ctx context.Context) error` in `internal/infrastructure/grpcutil/pool.go`.
   - `cmd/search-orchestrator/main.go`: 12-factor environment configuration, `ConnPool.Warmup(ctx)` for HNSW/BM25/Triton channels, Redis ping check, `rag-telemetry-audit-daemon` `AuditSink` wiring, HTTP (`:8080`) + gRPC (`:50051`, 30s keepalive + `KeepaliveEnforcementPolicy`) servers, and graceful shutdown (`draining = true` + configurable `SHUTDOWN_DRAIN_DELAY` defaulting to `5s` + `GracefulStop()` + HTTP shutdown + ring-buffer flush within `15s`).

5. **Unit, Integration & Quality Gates (`test/unit/`, `test/integration/`)**:
   - Fix Phase 3 `bufconn` cold-dial flakiness in `test/unit/grpc_clients_test.go` via `ConnPool.Warmup`.
   - Unit tests for MPSC ring-buffer zero-alloc `Emit` and concurrent `-race` safety, Redis cache/hydration serialization and `CROSSSLOT`-safe batching, Tier 2 bypass-without-eviction, HTTP/gRPC validation/error sanitization/readiness, and secured logging redaction.
   - Testcontainers integration suite (`test/integration/`) with real Redis + mock gRPC downstreams.
   - Pass `make check` (`gofmt -s`, `go vet`, `golangci-lint`, `govulncheck`, `gosec`, `audit-logging`, `go test -race ./...`).

6. **Documentation & Synchronization**:
   - Keep `.ai/doc-api.md` and `api/openapi/openapi.yaml` synchronized with any contract/metric/config refinements.
   - Update `.ai/progress-tracker.md` upon completion.

## 2. Security & Invariants

- Zero `fmt.Print*` in `cmd/`, `internal/`, `pkg/`.
- Structured JSON `log/slog` with `trace_id`, `request_id`, `tenant_id`, and strict redaction of `Authorization`, bearer tokens, raw `caller_acl_bitmap`, credentials, and PII.
- Tier 2 live Roaring Bitmap verification on every Tier 1 cache hit before hydration (bypass to live fan-out without evicting cache if `< top_k` survive).
- Git commits: At least 2 conventional commits (`feat:`, `fix:`, `tests:`, `docs:`).
