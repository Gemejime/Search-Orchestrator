# Progress Tracker & Implementation Roadmap - RAG Search Orchestrator (`rag-search-orchestrator`)

## 1. Calibrated Implementation Phases

This roadmap tracks the implementation milestones detailed in `tasks/RAG Search Orchestrator Implementation Plan.pdf`:

| Phase | Focus Area | Deliverables & Exit Gates | Status |
| :--- | :--- | :--- | :--- |
| **Phase 0: Repository & AI Spec Alignment** | Governance, CI/CD & Contracts | - `.gitignore` excluding `tasks/`, `.spectral.yaml`, `.golangci.yml`, `Makefile`, and GitHub Actions CI (`lint-and-typecheck`, `security-and-logging-audit`, `OpenAPI 3.1 & Doc API Contract Sync`, `test-suites`).<br>- Updated `.ai.md`, `.ai/*`, and `GEMINI.md` aligned with `rag-search-orchestrator` and `Production Grade Backend Concepts.pdf`. | Completed |
| **Phase 1: Core Domain, Contracts & Staged Cascade** | Pure Go Domain & API Contracts | - `go.mod`, `api/proto/v1/{search_service,types}.proto`, and `api/openapi/openapi.yaml`.<br>- `internal/domain/entity/` (`query_context.go`, `search_candidate.go`, `fused_result.go`).<br>- `internal/domain/ranking/` (`rrf_engine.go` with `k=60`, `cascade_filter.go` enforcing 80 -> 20 -> 10 bounds).<br>- Unit tests verifying staged cascade determinism. | In Progress |
| **Phase 2: Native `uint32` Roaring Bitmap ACL & Zero-Alloc Pools** | Sub-ms Entitlements & Memory | - `internal/domain/acl/roaring_evaluator.go` performing native `uint32` bitwise AND intersections.<br>- `pkg/pool/candidate_pool.go` (`sync.Pool` reusable slice buffers).<br>- Security & benchmark tests over `100,000` `uint32` IDs asserting zero heap allocations and $<1\text{ ms}$ execution. | Not Started |
| **Phase 3: Parallel Fan-Out, Triton Inference & Circuit Breaker** | gRPC Clients & Resilience | - `internal/infrastructure/search/dense/hnsw_grpc_client.go` & `sparse/bm25_grpc_client.go` with strict $20\text{ ms}$ `context.WithTimeout` and single-index fallback.<br>- `internal/infrastructure/triton/` (`embedding_client.go`, `reranker_client.go`, `circuit_breaker.go` tripping at $>35\text{ ms}$ or $>5\%$ errors).<br>- `internal/application/usecase/` (`execute_search.go`, `execute_diagnostic_search.go`). | Not Started |
| **Phase 4: Tier 1/2 Redis Cache, Hydration, Telemetry & Transport** | Caching, Ring Buffer & APIs | - `internal/infrastructure/cache/redis/` (`query_text_cache.go` SHA-256 normalized key, `parent_chunk_reader.go` pipelined `MGET`).<br>- `internal/infrastructure/telemetry/ring_buffer_emitter.go` (lock-free zero-alloc emitter to `rag-telemetry-audit-daemon`).<br>- `internal/transport/grpc/` & `http/` + `cmd/search-orchestrator/main.go`.<br>- Integration tests (Testcontainers: Redis, mock Triton, mock shards). | Not Started |
| **Phase 5: 5,000 RPS Benchmarks, Chaos & Production Cutover** | Scale Validation & Helm/Canary | - `test/performance/` (5,000 RPS benchmarks, `pprof` zero-throttling verification).<br>- `test/chaos/` (Triton 50ms latency injection, sparse shard dropouts, Redis partitions).<br>- `deployments/helm/` (14–16 pods, 4000m/8000m CPU, 2Gi/4Gi RAM, `GOMEMLIMIT=3600MiB`, `PDB minAvailable: 10`, ArgoCD progressive canary). | Not Started |

---

## 2. Architecture Decision Records (ADRs)

- **ADR-001: Native `uint32` Roaring Bitmap Entitlement Filtering**: Eliminates runtime UUID-to-integer translation tables by assigning immutable `uint32 doc_id` values at ingestion time (`rag-kb-catalog-service`) and evaluating bitwise AND intersections directly in orchestrator heap memory ($<1\text{ ms}$, zero allocations).
- **ADR-002: Fixed Staged Candidate Cascade vs. Dynamic Knee Pruning & HyDE**: Enforces deterministic narrowing (`Top-40 dense + Top-40 sparse -> Top-20 RRF (k=60) -> Roaring ACL -> Top-10 Cross-Encoder -> Top-5 Hydrated`) and replaces multi-second synchronous HyDE LLM fallback loops with immediate promotion of top ACL-filtered un-reranked RRF candidates.
- **ADR-003: Normalized Text Query Caching vs. Floating-Point Vector Hashing**: Replaces unstable float vector hashing with `SHA256(len_bytes(Normalize(QueryText)) + ":" + Normalize(QueryText) + ":" + len_bytes(TenantID) + ":" + TenantID + ":" + len_bytes(EmbeddingModelVersion) + ":" + EmbeddingModelVersion)` in Redis 7.2 Cluster (Tier 1), paired with mandatory live Roaring Bitmap verification (Tier 2) for $\le 8\text{ ms}$ P99 cache hits.
- **ADR-004: Decoupled Context Delivery Contract**: Removes dynamic chat prompt dialect compilation (Anthropic XML, Llama-3 headers) from `rag-search-orchestrator`, returning clean enriched parent chunks (800–1,500 tokens) over Protobuf v3 / REST to the consuming Edge Gateway.
- **ADR-005: Zero-Alloc Lock-Free Ring Buffer Compliance Telemetry**: Emits provenance and access audit spans asynchronously to `rag-telemetry-audit-daemon` via an in-memory lock-free ring buffer, ensuring zero telemetry heap allocations on the critical retrieval path.

---

## 3. Operational Runbooks

### Runbook 1: Triton GPU Reranker Queue Saturation
1. Monitor `search_fanout_latency_seconds{component="triton_rerank"}` and Triton pending queue depth.
2. If reranker P99 latency exceeds $35\text{ ms}$ (or errors exceed $5\%$), the internal circuit breaker automatically steps down cross-encoder evaluation and serves Top-10 ACL-filtered RRF candidates directly.
3. Autoscale the Triton GPU worker replica pool if GPU VRAM and tensor core utilization exceed $85\%$.

### Runbook 2: Diagnosing Elevated Search Fan-Out Timeouts
1. Inspect `search_fanout_latency_seconds{component="hnsw_dense"}` / `{component="bm25_sparse"}` and `search_fanout_stage_errors_total{error="DeadlineExceeded"}`.
2. If timeouts exceed $1\%$ of queries, verify HNSW replica node memory paging or BM25 index garbage collection spikes.
3. The service degrades gracefully to single-modality candidate retrieval; scale read-replica partitions or rebalance traffic across surviving cluster nodes.

### Runbook 3: Ingestion `uint32 doc_id` Range Exhaustion
1. Monitor `catalog_allocated_doc_id_max` in `rag-kb-catalog-service`.
2. If ID consumption approaches the 32-bit unsigned ceiling ($4.29 \times 10^9$), trigger partition sharding and tenant namespace segmentation across dedicated bitmap ranges.

---

## 4. Git Commit Standards

- **Git Commit Frequency**: Commit code as you build. Every working session or phase must contain **at least two git commits**.
- **Conventional Commit Patterns**: All git commit messages must strictly adhere to the following prefixes:
  - `feat:` for new features 
  - `fix:` for bug fixes
  - `refactor:` for code refactoring
  - `docs:` for documentation updates
  - `tests:` for tests addition and modification
  - `chore:` for maintenance tasks and environment setup
