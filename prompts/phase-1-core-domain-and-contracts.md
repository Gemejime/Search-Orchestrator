# Phase 0 Completion & Phase 1: Core Domain, Contracts & Staged Cascade

## 1. Goals
1. **Complete Phase 0 Deliverables**:
   - Add `.golangci.yml` enforcing `errcheck`, `govet`, `staticcheck`, `unused`, `gosec`, and `revive`.
   - Add `Makefile` with `fmt`, `vet`, `lint`, `test`, `bench`, `proto`, `vuln`, `sec`, `audit-logging`, and `check` targets:
     - `audit-logging` must dynamically scan whichever of `cmd/`, `internal/`, `pkg/` exist (matching `.github/workflows/ci.yml`), forbid `fmt.Print*`, and run `go run -C .github/scripts . ...`.
     - Ensure targets work cleanly in cross-platform/Windows environments (avoid bash-only constructs where Go or portable commands can be used).
   - Mark Phase 0 as `Completed` in `.ai/progress-tracker.md`.

2. **Implement Phase 1 Deliverables**:
   - Initialize `go.mod` (`github.com/rag-platform/rag-search-orchestrator`, Go `1.23`).
   - Author `buf.yaml`, `buf.gen.yaml`, `api/proto/v1/types.proto`, and `api/proto/v1/search_service.proto` with sequential Protobuf v3 field numbers (`1..N`) and generate Go stubs via `buf generate` *(requires `buf` installed locally, or remote `buf` plugins; install `buf` before running `make proto`)*.
     - Align `ScoreTrace` fields (`dense_count`, `sparse_count`, `rrf_count`, `acl_permitted_count`, `circuit_breaker_tripped`, `rerank_logits`, `stage_timings_ms`) and resolve `rerank_logits` keying (`map<string, double>` by `chunk_id` vs. `map<uint32, double>` by `doc_id` if multiple chunks per `doc_id` can enter Top-10).
   - Author `api/openapi/openapi.yaml` (OpenAPI 3.1) covering all 7 endpoints in `.ai/doc-api.md` (`/v1/search`, `/v1/search/diagnostic`, `/healthz`, `/ready`, `/metrics`, `/openapi.yaml`, `/docs`), aligned with `types.proto`, and passing `.spectral.yaml`.
   - Implement pure Go domain entities in `internal/domain/entity/`:
     - `query_context.go`: `tenant_id`, `embedding_model_version`, deterministic `Normalize(QueryText)` (Unicode NFC, trim leading/trailing whitespace, collapse internal whitespace runs to a single space, lowercase), canonical length-prefixed SHA-256 cache key builder (`SHA256(len_bytes(NormQuery) + ":" + NormQuery + ":" + len_bytes(TenantID) + ":" + TenantID + ":" + len_bytes(EmbeddingModelVersion) + ":" + EmbeddingModelVersion)`), `caller_acl_bitmap` bytes, and `top_k` bounds (`0` defaults to `5` for Protobuf v3 compatibility; `1..5` valid; `<0` or `>5` rejected).
     - `search_candidate.go`: native `uint32 doc_id`, `chunk_id`, `parent_key`, modality rank/score, metadata.
     - `fused_result.go`: post-RRF and post-rerank structures, `ScoreTrace`, `EnrichedChunk`.
   - Implement staged cascade & RRF in `internal/domain/ranking/`:
     - `rrf_engine.go`: Reciprocal Rank Fusion with `k=60`; enforce `[:40]` per-modality input cap; deduplicate intra-modality by keeping the best (lowest 1-based) rank per `(doc_id, chunk_id)` before summing; deduplicate cross-modality by `(doc_id, chunk_id)`; deterministic tie-breaking (higher RRF score -> lower `doc_id` -> lexicographically smaller `chunk_id`).
     - `cascade_filter.go`: fixed bounds (`max 40 dense + max 40 sparse = 80 -> Top-20 RRF -> ACL predicate/interface hook -> Top-10 rerank input -> Top-5 hydrated output`); explicit logit threshold `tau` configuration/parameter with deterministic fallback/backfill (if fewer than `top_k` candidates pass `logit >= tau`, backfill remaining slots up to `top_k` with deduplicated **ACL-permitted** RRF candidates—never unpermitted candidates).
   - Write comprehensive unit & benchmark tests in `test/unit/` covering:
     - `QueryContext` validation (`top_k=0 -> 5`, `<0` / `>5` error, empty `tenant_id`/`query_text`/`embedding_model_version`), `Normalize`, and canonical SHA-256 cache key determinism.
     - RRF `k=60` math, `[:40]` cap, intra-modality & cross-modality `(doc_id, chunk_id)` deduplication, and deterministic tie-breaking.
     - Cascade bounds (`80 -> 20 -> ACL -> 10 -> 5`), ACL hook enforcement before reranking and during fallback, and `tau` threshold fallback/backfill behavior.
   - Synchronize `.ai/doc-api.md` (including `ScoreTrace` and normalization/key details) and `.ai/progress-tracker.md`.

## 2. Security & Invariants
- Zero network dependencies in `internal/domain/`.
- Zero `fmt.Print*` in production packages (`cmd/`, `internal/`, `pkg/`); no raw `caller_acl_bitmap` or PII logging.
- Strict input validation: non-empty `tenant_id`, non-empty `embedding_model_version`, non-empty normalized `query_text`, bounded `top_k` (`0` -> default `5`, `1..5` allowed, otherwise error).
- Git commits: At least 2 conventional commits (`chore:`, `feat:`, `tests:`, `docs:`).

## 3. Verification
- `gofmt -s -l .` (clean)
- `go vet ./...`
- `golangci-lint run`
- `govulncheck ./...`
- `gosec -exclude-generated ./...`
- `go test -v -race ./...`
- `go test -run=^$ -bench=. -benchmem ./test/unit/...`
- `go run -C .github/scripts . ../../internal`
- Spectral OpenAPI 3.1 validation against `.spectral.yaml` (via `spectral lint api/openapi/openapi.yaml` locally if installed, or CI `openapi-and-doc-api-contract-sync`)
