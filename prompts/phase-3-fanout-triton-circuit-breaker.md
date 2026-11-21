# Phase 3: Parallel Fan-Out, Triton Inference & Circuit Breaker

## 1. Goals

1. **Consume Existing Domain Ports (`internal/domain/repository/`)**:

   - Use the existing pure-Go ports from Phases 1–2 (`DenseSearchPort`, `SparseSearchPort`, `TritonEmbeddingPort`, `TritonRerankerPort`, `ParentChunkPort`, `QueryCachePort`, and `TelemetryEmitterPort` in `internal/domain/repository/parent_chunk_port.go`).
   - Keep `internal/domain/` free of network, gRPC, or Prometheus dependencies. If stage-level metric recording hooks are needed in application/infrastructure layers, define minimal observer interfaces without polluting domain entities.

2. **Downstream gRPC Contracts, Pooled Clients & Circuit Breaker (`internal/infrastructure/`)**:

   - **Downstream Protobuf Stubs**: Add/generate internal client contracts for HNSW dense search, BM25 sparse search, and Triton `GRPCInferenceService` (e.g., under `api/proto/` or `internal/infrastructure/` with `Makefile` codegen targets) so `bufconn` tests and clients compile cleanly.
   - **Channel Connection Pooling & Keepalives**: Configure pooled `grpc.ClientConn` channels with `30s` TCP keepalives (`keepalive.ClientParameters`) across HNSW, BM25, and Triton clients to prevent HTTP/2 single-connection saturation at ~350 RPS/pod.
   - `search/dense/hnsw_grpc_client.go`: Pooled unary gRPC client for HNSW read replicas enforcing Top-40 cap and strict `20ms` `context.WithTimeout` deadline (capped by parent `ctx.Deadline()`).
   - `search/sparse/bm25_grpc_client.go`: Sharded unary gRPC scatter-gather client for BM25 text shards enforcing Top-40 cap and strict `20ms` `context.WithTimeout` deadline. Tolerate partial shard failures: merge surviving shard results deterministically (`score desc`, `doc_id asc`, `chunk_id asc`) and fail only if all shards fail.
   - `triton/embedding_client.go`: Triton `GRPCInferenceService` client for FP16 query embedding enforcing a strict `15ms` `context.WithTimeout` deadline. Align with `ModelStreamInfer` (streaming gRPC per implementation plan) or document an explicit ADR if using pooled unary `ModelInfer`.
   - `triton/reranker_client.go`: Triton `GRPCInferenceService` (`ModelInfer`) client for batched Top-10 Cross-Encoder passage scoring enforcing a strict `35ms` hard `context.WithTimeout` deadline (targeting `<= 30ms` P99).
   - `triton/circuit_breaker.go`: Low-contention (atomic/ring-window) deterministic circuit breaker:
     - Trips open when slow calls (`> 35ms`) or rolling errors exceed `5%` over a sliding window once a `minRequests` floor (e.g., 20 requests) is met, or on consecutive `> 35ms` stalls.
     - Bypasses reranking when remaining `ctx.Deadline()` budget is insufficient (configurable, default `< 25ms` to protect the `<= 75ms` cold-path P99 SLO).
     - Enforces a single-flight `HalfOpen` probe (`maxProbes = 1`) to avoid thundering-herd stampedes on Triton during recovery.
     - Promotes ACL-filtered Top-10 RRF candidates directly on trip/bypass and records `search_circuit_breaker_tripped_total`.

3. **Application Layer (`internal/application/`)**:

   - `dto/search_request.go` & `dto/search_response.go`: Application request/response DTOs decoupling transport from domain entities.
   - `usecase/execute_search.go`: Primary staged retrieval cascade using `pkg/pool.CandidatePool` and `acl.RoaringEvaluatorPool` (`defer eval.Close()`, `defer pool.Put*()` without retaining pooled slices past return):
     1. Validate `QueryContext` & decode `RoaringEvaluator` from `RoaringEvaluatorPool` (fail-closed).
     2. Optional Tier 1/2 cache hook (`QueryCachePort`): on hit, evaluate `RoaringEvaluator` against cached candidates; if `< QueryContext.TopK` survive, bypass to live fan-out without evicting the shared tenant cache entry.
     3. Dense query embedding via `TritonEmbeddingPort` (`15ms` cap). If embedding fails or times out, degrade gracefully to **sparse-only** retrieval (preserving 99.99% availability).
     4. Concurrent Dense (Top-40) + Sparse (Top-40) fan-out with strict `20ms` `context.WithTimeout` and automatic single-index fallback if one modality times out or errors. Return an error only if **both** modalities fail (zero matches or zero ACL-permitted results return a valid empty `SearchResponse`).
     5. RRF (`k=60`, `80 -> 20`) + Roaring Bitmap ACL filter (`< 1ms`, zero-alloc via `RoaringEvaluator.FilterCandidates`) -> Top-10 permitted candidates.
     6. Bounded Triton Cross-Encoder reranking guarded by `CircuitBreaker` + logit threshold `tau` evaluation (`CascadeFilter.SelectTopKWithThreshold`).
     7. Parent chunk hydration (Top-5 via `ParentChunkPort`; metadata-only fallback stub allowed **only** when `ParentChunkPort == nil` for pre-Phase 4 wiring—never swallow real `ParentChunkPort` errors) + optional `QueryCachePort.Set` (storing caller-independent pre-ACL candidates `>= 5` with `RerankerFallback` state) + optional `TelemetryEmitterPort.Emit` (`SearchAuditSpan` with `PermittedDocIDs [20]uint32`, `PermittedCount`, `QueryHash`, `TotalLatencyUs`) + stage metric hooks (`search_fanout_latency_seconds`, `search_fanout_stage_errors_total`, `search_roaring_bitmap_eval_duration_seconds`).
   - `usecase/execute_diagnostic_search.go`: Executes the same deterministic cascade while recording `ScoreTrace` (`dense_count`, `sparse_count`, `rrf_count`, `acl_permitted_count`, `circuit_breaker_tripped`, `rerank_logits` as `[]CandidateLogit`, and `stage_timings_ms` keyed by `hnsw_dense`, `bm25_sparse`, `triton_embed`, `rrf_acl`, `triton_rerank`, `redis_hydrate`).

4. **Unit & Resilience Tests (`test/unit/`)**:

   - Parallel fan-out `20ms` timeout & single-index fallback tests (dense timeout, embedding failure -> sparse-only, sparse partial-shard timeout, both failing -> error, zero ACL-permitted -> empty response).
   - Circuit breaker state transition tests (`>35ms` latency / `>5%` error rate trip with `minRequests` guard, `<25ms` remaining deadline bypass, `HalfOpen` single-flight probe recovery).
   - `ExecuteSearch` & `ExecuteDiagnosticSearch` tests verifying strict ACL isolation before Triton reranking, `tau` threshold fallback/backfill, `CandidatePool` buffer return, Tier 1/2 cache bypass when `< TopK` permitted, and `ScoreTrace` accuracy.
   - Mock gRPC server tests (`bufconn`) for `hnsw_grpc_client`, `bm25_grpc_client` (multi-shard scatter-gather + partial shard failure), `embedding_client`, and `reranker_client`.

5. **Documentation & Synchronization**:

   - Keep `.ai/doc-api.md` and `api/openapi/openapi.yaml` synchronized if any contract, metric, or DTO details are refined.
   - Update `.ai/progress-tracker.md` upon completion.

## 2. Security & Invariants

- Zero network dependencies in `internal/domain/`.
- Zero `fmt.Print*` in production packages (`cmd/`, `internal/`, `pkg/`); structured JSON logging via `log/slog` with `trace_id`, `request_id`, `tenant_id`, and strict redaction (never log raw `caller_acl_bitmap`, auth headers, credentials, or raw query PII).
- Pre-inference entitlement isolation: unpermitted `uint32 doc_id` candidates MUST NEVER reach Triton Cross-Encoder reranking, and caller-specific ACL results MUST NEVER be written into shared Tier 1 `QueryCachePort` entries.
- Git commits: At least 2 conventional commits (`feat:`, `tests:`, `docs:`).

## 3. Verification

- `gofmt -s -l .` (clean)
- `go vet ./...`
- `golangci-lint run ./...`
- `govulncheck ./...`
- `gosec -exclude-generated ./...`
- `go run -C .github/scripts . ../../internal ../../pkg`
- `go test -v ./...` (and `-race` where CGO is available)
