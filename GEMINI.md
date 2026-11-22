# GEMINI.md - Project Instructions for `rag-search-orchestrator`

This repository implements **`rag-search-orchestrator`**, the mission-critical, low-latency Go retrieval orchestration microservice of the disaggregated RAG platform.

## 1. Mandatory Context & Reading Order

Before planning, editing, or implementing any code, you MUST read and follow:
1. [`.ai/agents.md`](.ai/agents.md) — **Primary Agent Persona, System Role & Execution Protocol**.
2. [`.ai.md`](.ai.md) — Top-level application building context, non-negotiable invariants, and pre-commit rules.
3. The remaining specification files in [`.ai/`](.ai/):
   - [`.ai/project-overview.md`](.ai/project-overview.md) — 6-stage retrieval pipeline, calibrated SLOs ($\le 75\text{ ms}$ cold-path P99, $\le 8\text{ ms}$ cache hit P99, $5,000\text{ RPS}$), and scope boundaries.
   - [`.ai/architecture.md`](.ai/architecture.md) — 5-layer backend architecture (`Production Grade Backend Concepts.pdf`), folder structure, Kubernetes pod sizing, and observability matrix.
   - [`.ai/doc-api.md`](.ai/doc-api.md) — Authoritative REST (OpenAPI 3.1), gRPC (Protobuf v3), Redis Cluster, ring-buffer telemetry, and Prometheus contracts.
   - [`.ai/code-standards.md`](.ai/code-standards.md) — Go 1.23+ standards, `sync.Pool` zero-alloc patterns, 20ms fan-out deadlines, and circuit breaking.
   - [`.ai/security.md`](.ai/security.md) — Native `uint32` Roaring Bitmap pre-inference ACL isolation, Tier 2 cache verification, distroless hardening, and secured logging.
   - [`.ai/ai-workflow-rules.md`](.ai/ai-workflow-rules.md) — Spec-driven workflow, human-in-the-loop rules, and pre-commit checklist.
   - [`.ai/progress-tracker.md`](.ai/progress-tracker.md) — Implementation phases, ADRs (`ADR-001`–`ADR-005`), and operational runbooks.

---

## 2. Non-Negotiable Engineering Invariants

1. **Continuous Doc API & OpenAPI 3.1 Synchronization**:
   - [`.ai/doc-api.md`](.ai/doc-api.md) and `api/openapi/openapi.yaml` MUST be updated on **every code change or addition**, regardless of how small the modification is, whenever any endpoint, request/response model, Protobuf contract, Redis key schema, or metric changes.
2. **Secured Logging**:
   - All logs MUST be structured JSON (`log/slog`). Never use `fmt.Print*` in production packages (`cmd/`, `internal/`, `pkg/`).
   - Strictly redact `Authorization` headers, bearer tokens, raw `caller_acl_bitmap` bytes, Redis/Triton credentials, internal DSNs/paths, and tenant PII/PHI before emission.
3. **Native `uint32` Roaring Bitmap Entitlements**:
   - Intersect caller Roaring Bitmaps with `uint32 doc_id` candidates in $<1\text{ ms}$ with zero heap allocations **before** candidates reach Triton Cross-Encoder GPU buffers.
4. **Fixed Staged Cascade & Deterministic Circuit Breaking**:
   - Enforce Top-40 Dense + Top-40 Sparse ($20\text{ ms}$ deadline) $\rightarrow$ Top-20 RRF (`k=60`) $\rightarrow$ Roaring ACL $\rightarrow$ Top-10 Triton Cross-Encoder $\rightarrow$ Top-5 Redis `MGET` hydrated parent chunks.
   - On Triton logit $<\tau$, latency $>35\text{ ms}$, or error rate $>5\%$, fall back to Top-10 ACL-filtered RRF candidates (never trigger synchronous HyDE LLM loops).

---

## 3. Git Commit Standards

- **Git Commit Frequency**: Commit code as you build. Every working session or phase must contain **at least two git commits**.
- **Conventional Commit Patterns**: All git commit messages must strictly adhere to the following prefixes:
  - `feat:` for new features 
  - `fix:` for bug fixes
  - `refactor:` for code refactoring
  - `docs:` for documentation updates
  - `tests:` for tests addition and modification
  - `chore:` for maintenance tasks and environment setup
