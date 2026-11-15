# Code Standards - RAG Search Orchestrator (`rag-search-orchestrator`)

## General Principles

- **Focused Retrieval Boundary**: Decouple retrieval orchestration, RRF ranking, Roaring Bitmap ACL filtering, and parent container hydration from prompt chat dialect compilation (Anthropic XML, Llama-3 headers) and interactive HyDE LLM expansion.
- **Fail-Fast on Corrupt Input, Graceful Degradation on Downstream Stalls**: Reject malformed tenant IDs, empty queries, or invalid `caller_acl_bitmap` payloads immediately (`400 Bad Request` / `codes.InvalidArgument`). On downstream search shard timeouts ($>20\text{ ms}$) or Triton Cross-Encoder stalls ($>35\text{ ms}$ / $>5\%$ errors), degrade gracefully via single-index fallback or deterministic Top-10 RRF fallback.
- **Root-Cause Focus**: Never mask race conditions or latency spikes with arbitrary `time.Sleep` calls; use bounded `context.WithTimeout` deadlines, pre-warmed gRPC channel pools, `sync.Pool` buffers, and deterministic circuit breakers.

---

## Go 1.23+ & Code Quality Standards

- **Runtime Target**: Go 1.23+ (64-bit) with `GOMEMLIMIT=3600MiB` and `GOGC=100` in production.
- **Linter & Formatter**:
  - Formatter: `gofmt -s -w .`
  - Vet: `go vet ./...`
  - Linter: `golangci-lint run` (enforcing `errcheck`, `govet`, `staticcheck`, `unused`, `gosec`, `revive`)
- **Standard Library Utilization**:
  - Prefer Go standard library (`net/http`, `context`, `log/slog`, `sync`, `time`, `crypto/sha256`).
  - Context propagation: All functions performing I/O, RPCs, or Redis calls MUST accept `context.Context` as the first argument and respect `ctx.Done()`.
- **Idiomatic Go Structuring**:
  - Keep `internal/domain/` pure Go with **zero network dependencies**; communicate with infrastructure via interfaces in `internal/domain/repository/`.
  - Avoid global mutable state; wire dependencies via constructor functions (`NewExecuteSearchUseCase(...)`, `NewRoaringEvaluator(...)`).
  - Package names must be lowercase, short, and singular (e.g., `entity`, `ranking`, `acl`, `repository`, `usecase`, `redis`, `triton`, `pool`).

---

## Staged Cascade & Zero-Allocation Invariants

1. **Fixed Candidate Cascade Bounds (`internal/domain/ranking/`)**:
   - Retrieve: Top-40 Dense (HNSW) + Top-40 Sparse (BM25) = max 80 candidates.
   - Blend: Reciprocal Rank Fusion (`k=60`) $\rightarrow$ Top-20 candidates.
   - Authorize: Native `uint32` Roaring Bitmap bitwise AND intersection $\rightarrow$ permitted candidates.
   - Rerank: Top-10 permitted candidates $\rightarrow$ Triton Cross-Encoder (FP16 batch).
   - Hydrate: Top-5 surviving chunks (logit $\ge \tau$, or promoted RRF fallback) $\rightarrow$ Redis `MGET` parent containers (800–1,500 tokens).
2. **Native `uint32` Roaring Bitmap Evaluation (`internal/domain/acl/`)**:
   - Bitwise intersections over `100,000+` `uint32` IDs MUST execute in **$<1\text{ ms}$ with zero heap allocations**.
   - Never maintain runtime UUID-to-integer translation maps on the hot path.
3. **Reusable Slice Buffers (`pkg/pool/candidate_pool.go`)**:
   - Use `sync.Pool` for candidate slices and intermediate RRF scoring buffers on the hot path; always reset slice lengths (`buf = buf[:0]`) before returning to the pool.
4. **Lock-Free Telemetry Ring Buffer (`internal/infrastructure/telemetry/`)**:
   - Emit compliance trace spans to `rag-telemetry-audit-daemon` via a non-blocking, lock-free ring buffer with zero telemetry heap allocations on the critical search path.

---

## Concurrency, Deadlines & Circuit Breaker Invariants

1. **Parallel Fan-Out Deadline**:
   - Dense HNSW and sparse BM25 unary gRPC calls MUST run concurrently under a strict `context.WithTimeout(ctx, 20*time.Millisecond)` deadline.
   - If one modality fails or exceeds the deadline, log a structured warning and proceed with single-index fallback.
2. **Triton Cross-Encoder Circuit Breaker**:
   - Trip the circuit breaker when Triton reranker latency exceeds $35\text{ ms}$ or error rate exceeds $5\%$.
   - When tripped or when surviving logits are below threshold $\tau$, promote Top-10 un-reranked RRF candidates directly.

---

## Secured Logging Invariants

- **Structured JSON Logging**: All logs must be emitted as machine-parseable JSON lines via `log/slog`. Never use `fmt.Print*` in production packages (`cmd/`, `internal/`, `pkg/`).
- **Mandatory Correlation IDs**: Every log message must include `trace_id`, `request_id`, and `tenant_id` (plus `cache_hit` and `duration_ms` where applicable).
- **Strict Redaction**:
  - Never log `Authorization` headers, bearer tokens, Redis/Triton passwords, or internal connection strings/DSNs.
  - Never log raw `caller_acl_bitmap` byte slices or unredacted tenant PII/PHI.
  - Sanitize error messages before returning them to clients (no internal file paths, hostnames, or stack traces).

---

## Doc API & OpenAPI Mandatory Maintenance

- **Continuous Synchronization**: Whenever any endpoint, request/response field, Protobuf message, Redis cache key, or metric is added or modified—no matter how small—[`.ai/doc-api.md`](doc-api.md) and `api/openapi/openapi.yaml` **MUST** be updated immediately in the same change step.

---

## Git Commit Standards

- **Git Commit Frequency**: Commit code as you build. Every working session or phase must contain **at least two git commits**.
- **Conventional Commit Patterns**: All git commit messages must strictly adhere to the following prefixes:
  - `feat:` for new features 
  - `fix:` for bug fixes
  - `refactor:` for code refactoring
  - `docs:` for documentation updates
  - `tests:` for tests addition and modification
  - `chore:` for maintenance tasks and environment setup
