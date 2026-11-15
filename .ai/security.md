# Security Context - RAG Search Orchestrator (`rag-search-orchestrator`)

## Threat Model & Trust Boundaries

- **Untrusted Ingress Payloads**: Search requests arrive from the Edge API Gateway / downstream LLM clients. `tenant_id`, query text length, `top_k` bounds, and `caller_acl_bitmap` byte payloads must be validated before entering cache lookups or inference pools.
- **Pre-Inference Entitlement Isolation (Zero-Leakage Guarantee)**: Native `uint32` Roaring Bitmap ACL filtering MUST execute **immediately after RRF blending and strictly before Triton Cross-Encoder reranking**. Unpermitted document chunks must never reach Triton GPU inference buffers or parent container hydration.
- **Tenant Isolation**: Strict logical separation across all operations. Tier 1 Redis cache keys incorporate `TenantID` (`SHA256(Normalize(QueryText) + TenantID + EmbeddingModelVersion)`), preventing cross-tenant cache poisoning or leakage.
- **Tier 2 Cache Entitlement Verification**: A Tier 1 Redis cache hit NEVER bypasses authorization. Cached `uint32` candidate references MUST be intersected with the caller's live `caller_acl_bitmap` in Tier 2 before hydrating or returning results.
- **Orchestrator Runtime**: Isolated stateless microservice running as `nonroot:nonroot` (UID `65532`) in a `gcr.io/distroless/static:nonroot` container with zero shell utilities and a read-only root filesystem.

---

## Secured Logging Invariants

1. **Structured JSON Telemetry Only**:
   - All logs emitted via `log/slog` in JSON format containing correlation metadata (`trace_id`, `request_id`, `tenant_id`, `cache_hit`, `stage`, `duration_ms`).
2. **Credentials, Tokens & ACL Redaction**:
   - `Authorization` headers, bearer tokens, Redis cluster passwords, Triton auth tokens, and DSNs must be stripped/masked before logging.
   - Raw `caller_acl_bitmap` byte payloads must NEVER be written to stdout or logs; log only summary cardinality/timing (`permitted_count`, `bitmap_eval_us`).
3. **PII/PHI Protection**:
   - Avoid logging raw sensitive query strings in production info/error logs; log normalized query hashes (`query_hash`) and compliance audit spans asynchronously to `rag-telemetry-audit-daemon` (HIPAA / SOC 2).
4. **Error Trace Sanitization**:
   - Client-facing HTTP/gRPC error payloads must not expose internal filesystem paths, stack traces, or internal Kubernetes service hostnames.

---

## Network & Container Hardening

1. **Distroless Non-Root Container**:
   - Base image: `gcr.io/distroless/static:nonroot`
   - UID: `65532` (`runAsNonRoot: true`, `runAsUser: 65532`)
   - Read-only root filesystem (`readOnlyRootFilesystem: true`) with ephemeral `/tmp` mount only if needed.
   - Drop all Linux capabilities (`capabilities: drop: ["ALL"]`).
2. **Kubernetes Network Policies**:
   - **Ingress**: Restricted to Edge API Gateway / authorized internal consumers on `:8080` (HTTP/REST) and `:50051` (gRPC/HTTP/2), plus Prometheus scraper on `/metrics`.
   - **Egress**: Allowed only to Redis 7.2 Cluster (`:6379`), HNSW Dense Read Replicas, BM25 Sparse Shards, Triton Inference Cluster (gRPC), `rag-telemetry-audit-daemon`, and cluster DNS (`kube-dns`/`CoreDNS` `:53`). General external internet egress is strictly denied.
3. **Transport Security**:
   - TLS/mTLS for inter-service gRPC and Redis Cluster connections in production; multiplexed HTTP/2 with 30s TCP keepalives.

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
