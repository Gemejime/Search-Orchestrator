# RAG Search Orchestrator (`rag-search-orchestrator`) - Project Overview

## Overview

The **`rag-search-orchestrator`** is the mission-critical, high-throughput retrieval brain of the disaggregated RAG platform, implemented as a low-latency Go microservice. It coordinates query normalization, two-tier semantic/normalized caching, dense query embedding inference, parallel fan-out across dense HNSW vector read replicas and sparse BM25 text shards, Reciprocal Rank Fusion (RRF), real-time native `uint32` Roaring Bitmap entitlement filtering, bounded Triton Cross-Encoder reranking with deterministic circuit breaking, parent container hydration, and zero-allocation compliance telemetry handoff.

```
REVISED RETRIEVAL PIPELINE
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                                    Edge API Gateway                                    │
│            (Handles auth, rate limiting, and prompt chat dialect compilation)          │
└───────────────────────────────────────────┬────────────────────────────────────────────┘
                                            │ gRPC POST /v1/search (Protobuf v3)
                                            ▼
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                                 rag-search-orchestrator                                │
│                                                                                        │
│ 1. Normalization & Tier 1 Query Cache:                                                 │
│    ├── Hash(NormalizedQueryText + TenantID + Version) ──► Redis 7.2 Cluster            │
│    └── Hit ──► Tier 2 Fast-Path Bitmap Entitlement Evaluation ──► Direct Return (≤8ms) │
│                                                                                        │
│ 2. Embedding Inference:                                                                │
│    └── Dense Query Vector Generation ──► Triton gRPC (FP16 Small Pool, 10–15ms P99)    │
│                                                                                        │
│ 3. Parallel Retrieval Fan-Out (20ms Hard Deadline):                                    │
│    ├── Dense Vector Search ──► Top-40 Candidates from HNSW Read Replicas               │
│    └── Sparse Text Search  ──► Top-40 Candidates from BM25 Shards                      │
│                                                                                        │
│ 4. Staged Ranking & Native uint32 Entitlement Cascade:                                 │
│    ├── Reciprocal Rank Fusion (RRF, k=60) ──► Blend 80 candidates into Top-20          │
│    └── Native In-Memory Roaring Bitmap ──► Intersect Caller Bitmap with uint32 doc_id  │
│                                            (Zero runtime UUID mapping overhead, <1ms)  │
│                                                                                        │
│ 5. Bounded Triton Cross-Encoder Reranking:                                             │
│    └── Top-10 Permitted Candidates ──► Batched Triton Cross-Encoder (18–30ms P99)      │
│        ├── Logit >= τ ───────────────► High-Confidence Chunk Accepted                  │
│        └── Logit < τ (or Degraded) ──► Fallback to Top RRF Candidates (No HyDE loop)   │
│                                                                                        │
│ 6. Parent Container Hydration:                                                         │
│    └── Hydrate Top-5 Surviving Chunks (800–1,500t) ──► Redis Cluster MGET (3–6ms)      │
└───────────────────────┬────────────────────────────────────────┬───────────────────────┘
                        │ Fire-and-Forget Ring Buffer            │ Enriched Context Chunks
                        ▼                                        ▼
┌──────────────────────────────────────┐    ┌────────────────────────────────────────────┐
│      rag-telemetry-audit-daemon      │    │     Edge Gateway / Consumer LLM Client     │
│   (Zero-alloc HIPAA/SOC 2 tracing)   │    │   (Handles prompt dialect compilation)     │
└──────────────────────────────────────┘    └────────────────────────────────────────────┘
```

---

## Strategic Objectives

1. **Zero-Mapping Entitlement-Aware Retrieval**: Native evaluation of immutable `uint32` document IDs against caller Roaring Bitmaps directly inside orchestrator heap memory, eliminating runtime string-to-integer UUID translation lookups, lock contention, and GC overhead.
2. **Predictable Staged Candidate Cascade**: Fixed deterministic candidate narrowing (`Top-40 dense + Top-40 sparse -> Top-20 RRF -> ACL filter -> Top-10 cross-encoder -> Top-5 hydrated`) to protect GPU inference pools from queue saturation.
3. **Normalized Text Query Caching**: Two-tier Redis query caching utilizing normalized semantic query strings and embedding model version hashes (`SHA256(Normalize(QueryText) + TenantID + EmbeddingModelVersion)`), combined with live Tier 2 Roaring Bitmap entitlement verification.
4. **Deterministic Circuit Breaking**: Fallback to top-ranked RRF candidates when cross-encoder logits drop below threshold $\tau$ or Triton GPU queues stall, strictly eliminating synchronous HyDE LLM expansion loops on the critical path.
5. **Focused Retrieval Boundary**: Pure focus on ranking, filtering, and parent container hydration, returning clean Protobuf v3 / REST candidate models to upstream gateways without prompt dialect markup compilation.
6. **Continuous Documentation & Contract Synchronization**: Ensure OpenAPI 3.1 specifications (`api/openapi/openapi.yaml`) and [`.ai/doc-api.md`](doc-api.md) are continuously synchronized with every code change or addition, no matter how small.

---

## Calibrated Quantitative SLOs & SLIs

- **Cold-Path Retrieval Latency**: **P99 $\le 75\text{ ms}$**
  - Dense Embedding Generation (Triton FP16): $\le 15\text{ ms}$
  - Parallel Search Fan-Out (HNSW + BM25): $\le 18\text{ ms}$ (hard `context.WithTimeout` deadline: $20\text{ ms}$)
  - RRF (`k=60`) & Native `uint32` Bitmap Intersection: $\le 2\text{ ms}$ (bitmap intersection $< 1\text{ ms}$)
  - Triton Cross-Encoder Reranking (Top-10 batch): $\le 30\text{ ms}$
  - Redis Cluster Parent Container `MGET`: $\le 5\text{ ms}$
  - Transit & Protobuf/JSON Marshaling: $\le 5\text{ ms}$
- **Fast-Path (Tier 1 & 2 Cache Hit) Latency**: **P99 $\le 8\text{ ms}$**
- **Sustained Throughput**: **5,000 queries/sec** across **14–16 baseline pods** ($\sim 320\text{–}350\text{ RPS/pod}$) distributed across 3 AZs (HPA bounds: 14 to 32 pods).
- **Per-Pod Resource Ceilings**:
  - CPU Request / Limit: `4000m` / `8000m`
  - Memory Request / Limit: `2 GiB` / `4 GiB`
  - Go Runtime Flags: `GOMEMLIMIT=3600MiB`, `GOGC=100`
- **Availability**: **99.99%** query availability with automatic single-index fallback if either the dense HNSW or sparse BM25 search tier fails or times out.

---

## Scope & Exclusions

### In-Scope
- **Ingress Query Normalization & Tier 1 Caching**: Text normalization, canonical SHA-256 query hashing, and TTL-bounded Redis 7.2 Cluster lookups.
- **Triton Embedding Dispatch**: Streaming/batched query vector generation over high-performance gRPC channels to Triton FP16 pools.
- **Parallel Multi-Index Fan-Out**: Concurrent dispatch to dense HNSW vector read replicas and distributed BM25 text shards with strict $20\text{ ms}$ hard deadline contexts.
- **Native Roaring Bitmap Filtering**: Bitwise AND intersections using pre-assigned, immutable `uint32` document IDs against compressed caller role/session entitlement bitmaps.
- **Reciprocal Rank Fusion (RRF)**: Merging dense and sparse candidates using normalized scoring constant $k=60$.
- **Triton Cross-Encoder Reranking & Deterministic Fallback**: Batched logit scoring for surviving Top-10 permitted candidates, with safe fallback to Top-10 un-reranked RRF candidates on low logit confidence ($<\tau$) or Triton circuit trip.
- **Parent Chunk Hydration**: Pipelined `MGET` batch fetching of pre-compiled parent containers (800–1,500 tokens) from Redis 7.2 Cluster.
- **Telemetry Ring Buffer Dispatch**: Zero-allocation lock-free ring buffer emitter pushing compliance trace spans to `rag-telemetry-audit-daemon`.

### Exclusions (Explicitly Out-of-Scope)
- **Dynamic Chat Dialect Compilation**: Rendering model chat markup (Anthropic XML, Llama-3 headers) is removed; returned payloads contain enriched text and metadata chunks, leaving prompt compilation to the consuming Edge Gateway / LLM client.
- **Interactive HyDE Fallback Execution**: Interactive LLM query expansion on the critical path is strictly excluded.
- **Document Parsing & Ingestion Indexing**: Chunking, layout parsing, and index mutation remain handled by ingestion workers (`rag-ingestion-coordinator`, `rag-document-ai-worker`, `rag-chunk-lifecycle-service`).
- **Upstream Document ID Generation**: Contiguous `uint32 doc_id` assignment is owned by `rag-kb-catalog-service` and ingestion workers.
