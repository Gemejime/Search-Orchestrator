# Documented APIs & Contracts - RAG Search Orchestrator (`rag-search-orchestrator`)

This document is the authoritative specification for all public and internal interfaces of the `rag-search-orchestrator` microservice.

> [!IMPORTANT]
> **Continuous Doc API & OpenAPI 3.1 Updates**: This file and `api/openapi/openapi.yaml` MUST be kept synchronized on **every code change or addition**, regardless of how small the modification is.

---

## 1. OpenAPI 3.1 REST API Specification

### Base URL
`http://localhost:8080` (Internal Service / Ingress Gateway)

### Endpoints Overview

| Method | Endpoint | Description | Auth Scope |
| :--- | :--- | :--- | :--- |
| `POST` | `/v1/search` | Execute staged retrieval cascade (Cache -> Embed -> Fan-Out -> RRF -> Roaring ACL -> Rerank -> Hydrate) | `search:read` |
| `POST` | `/v1/search/diagnostic` | Execute retrieval cascade with detailed score waterfalls and per-stage timing breakdowns | `search:debug` |
| `GET` | `/healthz` | Kubernetes Liveness Probe | Public |
| `GET` | `/ready` | Kubernetes Readiness Probe (checks Redis Cluster, Triton, HNSW/BM25 pools, drain state) | Public |
| `GET` | `/metrics` | Prometheus Metrics Scrape Endpoint | Public / Ops |
| `GET` | `/openapi.yaml` | Raw OpenAPI 3.1 YAML specification | Public |
| `GET` | `/docs` | Swagger UI documentation interface | Public |

---

### Detailed Endpoint Contracts

#### 1. `POST /v1/search`
Executes the deterministic retrieval pipeline and returns up to Top-5 enriched parent chunks (800–1,500 tokens). Prompt dialect compilation is explicitly excluded.

- **Request Headers**:
  - `Authorization: Bearer <token>` *(redacted in all logs)*
  - `Content-Type: application/json`
  - `X-Tenant-ID: <string>`
  - `X-Request-ID: <ulid|uuid>` *(optional; generated if omitted)*
- **Request Body**:
```json
{
  "tenant_id": "tenant-corp-01",
  "query_text": "What are the Q3 SOC 2 encryption rotation requirements?",
  "caller_acl_bitmap_b64": "OjAAAAEAAAAAAA==",
  "top_k": 5,
  "embedding_model_version": "bge-m3-fp16-v2"
}
```
- **Response `200 OK`**:
```json
{
  "request_id": "01JCYZ8K9MABCDEF1234567890",
  "tenant_id": "tenant-corp-01",
  "cache_hit": false,
  "reranker_fallback": false,
  "chunks": [
    {
      "doc_id": 482910,
      "chunk_id": "chk_482910_004",
      "parent_key": "parent:tenant-corp-01:482910:004",
      "text": "Pre-compiled parent container text (800-1,500 tokens)...",
      "score": 0.9142,
      "metadata": {
        "source_uri": "soc2-controls-2026.pdf",
        "page_range": "12-14"
      }
    }
  ],
  "latency_ms": 42.6
}
```
- **Response `400 Bad Request`**:
```json
{
  "error": "BAD_REQUEST",
  "message": "Invalid tenant_id, empty query_text, or malformed caller_acl_bitmap"
}
```

#### 2. `POST /v1/search/diagnostic`
Executes the same retrieval cascade while returning per-stage latency breakdowns and candidate score waterfalls (RRF scores, ACL drop counts, raw Cross-Encoder logits, and fallback flags).

- **Response `200 OK`**: Includes all fields from `POST /v1/search` plus `score_trace` (`dense_candidates`, `sparse_candidates`, `rrf_top20_count`, `acl_permitted_count`, `rerank_logits`, `stage_timings_ms`).

#### 3. `GET /healthz`, `GET /ready`, `GET /metrics`
- `GET /healthz` $\rightarrow$ `200 OK` `{"status": "alive"}`
- `GET /ready` $\rightarrow$ `200 OK` `{"status": "ready", "checks": {"redis": "ok", "triton": "ok", "hnsw": "ok", "bm25": "ok"}}` or `503 Service Unavailable` (`{"status": "draining"}` during `preStop` shutdown).
- `GET /metrics` $\rightarrow$ Prometheus exposition format (`text/plain; version=0.0.4`).

---

## 2. gRPC Protobuf v3 Contracts (`api/proto/v1/`)

### `api/proto/v1/search_service.proto`
Inbound retrieval ingress over multiplexed HTTP/2 with 30s TCP keepalives:
```protobuf
syntax = "proto3";

package rag.search.v1;

option go_package = "github.com/rag-platform/rag-search-orchestrator/api/proto/v1;searchv1";

import "api/proto/v1/types.proto";

service SearchService {
  rpc Search(SearchRequest) returns (SearchResponse);
  rpc DiagnosticSearch(SearchRequest) returns (DiagnosticSearchResponse);
}
```

### `api/proto/v1/types.proto`
Key message contracts:
- `SearchRequest`: `string request_id`, `string tenant_id`, `string query_text`, `bytes caller_acl_bitmap` *(compressed Roaring Bitmap; NEVER logged)*, `int32 top_k` *(default 5, max 5)*, `string embedding_model_version`.
- `SearchResponse`: `string request_id`, `string tenant_id`, `bool cache_hit`, `bool reranker_fallback`, `repeated EnrichedChunk chunks`, `double latency_ms`.
- `EnrichedChunk`: `uint32 doc_id`, `string chunk_id`, `string parent_key`, `string text`, `double score`, `map<string, string> metadata`.
- `DiagnosticSearchResponse`: `SearchResponse response`, `ScoreTrace score_trace`.
- `ScoreTrace`: `int32 dense_count`, `int32 sparse_count`, `int32 rrf_count`, `int32 acl_permitted_count`, `bool circuit_breaker_tripped`, `map<uint32, double> rerank_logits` *(keyed by candidate `doc_id`)*, `map<string, double> stage_timings_ms`.

---

## 3. Redis 7.2 Cluster Cache & Hydration Contracts

1. **Tier 1 Normalized Query Cache**:
   - **Key Format**: `qcache:<tenant_id>:<sha256(len_bytes(Normalize(QueryText)) + ":" + Normalize(QueryText) + ":" + len_bytes(TenantID) + ":" + TenantID + ":" + len_bytes(EmbeddingModelVersion) + ":" + EmbeddingModelVersion)>`
   - **Value**: Compact serialized payload containing the original computation's `reranker_fallback` state (`bool`) and a list of candidate references (`uint32 doc_id`, `string chunk_id`, `string parent_key`, and final `double score`) (TTL-bounded), so cache-hit responses populate `reranker_fallback` and per-chunk `score` directly from the cached entry.
   - **Tier 2 Rule**: Cache hits MUST still execute in-memory Roaring Bitmap intersection against `caller_acl_bitmap` before hydration. If permissions fail or yield an empty set, **bypass** directly to live fan-out without evicting the shared tenant cache entry (to avoid cache thrashing for other callers).
2. **Parent Container Hydration**:
   - **Operation**: Cluster-safe per-slot `MGET` batches or pipelined individual `GET` commands on pre-warmed parent keys (`parent:<tenant_id>:<doc_id>:<chunk_id>`) for Top-5 surviving chunks (800–1,500 tokens each, target $3\text{–}6\text{ ms}$).

---

## 4. Lock-Free Ring Buffer Compliance Telemetry (`rag-telemetry-audit-daemon`)

Asynchronous zero-alloc audit span emitted off the critical search path:
- **Fields**: `trace_id`, `request_id`, `tenant_id`, `query_hash` *(SHA-256, never raw PII)*, `cache_hit`, `permitted_doc_ids` *(fixed-capacity `[20]uint32` array + `uint8` count in ring buffer slots to avoid heap slice allocations)*, `reranker_fallback`, `total_latency_us`, `timestamp_unix_ns`.

---

## 5. Prometheus Metrics Contract

| Metric Name | Type | Labels | Purpose |
| :--- | :--- | :--- | :--- |
| `search_orchestrator_requests_total` | Counter | `tenant_id`, `status_code`, `cache_hit` | Total search volume & cache hit ratio |
| `search_orchestrator_latency_seconds` | Histogram | — | End-to-end latency (buckets 5ms–250ms; P50/P90/P99) |
| `search_fanout_latency_seconds` | Histogram | `component` | Isolated stage duration (`hnsw_dense`, `bm25_sparse`, `triton_embed`, `triton_rerank`, `redis_hydrate`) |
| `search_fanout_stage_errors_total` | Counter | `component`, `error` | Stage errors including `DeadlineExceeded` (20ms fan-out) |
| `search_circuit_breaker_tripped_total` | Counter | — | Triton Cross-Encoder fallback activations |
| `search_roaring_bitmap_eval_duration_seconds` | Histogram | — | Microsecond-level bitwise ACL intersection duration |
| `search_last_bitmap_sync_timestamp_seconds` | Gauge | — | Epoch timestamp of last entitlement bitmap sync *(applicable only when local Redis Pub/Sub bitmap caching is enabled; per-request `caller_acl_bitmap` is stateless)* |
