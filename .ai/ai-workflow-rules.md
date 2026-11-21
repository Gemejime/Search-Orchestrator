# AI Workflow Rules - RAG Search Orchestrator (`rag-search-orchestrator`)

## Development Approach

Build this microservice incrementally following a spec-driven workflow:
- The specification documents in `tasks/` and context files in [`.ai/`](.) define system invariants, data structures, and operational boundaries.
- Adhere strictly to the 5-layer backend architecture documented in [`.ai/architecture.md`](architecture.md) aligned with `tasks/Production Grade Backend Concepts.pdf`.
- Implement against these specs without inferring undocumented dependencies or deviating from architectural decision records (ADR-001 through ADR-005).

---

## Human-in-the-Loop Rules

- **Plan First**: Propose an implementation plan or prompt in `prompts/<task-name>.md` before making substantial code changes, and await confirmation when requirements are ambiguous.
- **Ambiguity Escalation**: If Triton model names/thresholds ($\tau$), Redis key TTLs, shard topologies, or Protobuf field numbers are ambiguous, prompt for clarification before proceeding.
- **Destructive Action Guards**: Always request human confirmation before running destructive operations, breaking published Protobuf v3 contracts, or removing production Helm/ArgoCD manifests.
- **Demonstrate & Verify**: Provide clear verification steps, unit/benchmark/race test output, and linting checks after completing any unit.

---

## Non-Negotiable Invariants

1. **Continuous Doc API & OpenAPI Maintenance**: [`.ai/doc-api.md`](doc-api.md) and `api/openapi/openapi.yaml` MUST be updated on **every code change or addition**, regardless of how small the modification is, whenever any endpoint, schema, proto, cache key, or metric contract changes.
2. **Secured Logging**: All logs emitted by `rag-search-orchestrator` must be structured JSON (`log/slog`). Strictly redact `Authorization` headers, bearer tokens, raw `caller_acl_bitmap` bytes, Redis/Triton credentials, and tenant PII/PHI before emission.
3. **Native `uint32` Roaring Bitmap Entitlements**: Execute sub-millisecond, zero-alloc bitwise AND intersections before Triton reranking; never introduce runtime UUID-to-integer translation maps.
4. **Fixed Staged Candidate Cascade**: Enforce Top-40 Dense + Top-40 Sparse $\rightarrow$ Top-20 RRF (`k=60`) $\rightarrow$ Roaring Bitmap ACL $\rightarrow$ Top-10 Triton Cross-Encoder $\rightarrow$ Top-5 Hydrated Parent Chunks. No dynamic knee pruning or synchronous HyDE loops.
5. **Deterministic Circuit Breaking**: Fall back cleanly to Top-10 ACL-filtered RRF candidates on Triton logit $<\tau$, latency $>35\text{ ms}$, or error rate $>5\%$.
6. **Git Commit Standards**:
   - **Git Commit Frequency**: Commit code as you build. Every working session or phase must contain **at least two git commits**.
   - **Conventional Commit Patterns**: All git commit messages must strictly adhere to the following prefixes:
     - `feat:` for new features 
     - `fix:` for bug fixes
     - `refactor:` for code refactoring
     - `docs:` for documentation updates
     - `tests:` for tests addition and modification
     - `chore:` for maintenance tasks and environment setup
7. **Quality Gates**: `gofmt -s`, `go vet`, `golangci-lint`, `govulncheck`, `gosec`, and `go test -race ./...` must pass cleanly before any commit.

---

## Pre-Commit Verification Checklist

Before committing code, submitting changes, or marking a task complete:

1. **Formatting**: Run `gofmt -s -w .` (zero unformatted files).
2. **Static Analysis & Linting**: Run `go vet ./...` and `golangci-lint run`.
3. **Security & Secured Logging Audit**: Run `govulncheck ./...`, `gosec ./...`, and verify no `fmt.Print*`, bearer tokens, raw `caller_acl_bitmap` bytes, credentials, or PII are logged.
4. **Automated Tests & Benchmarks**: Ensure unit/race tests pass (`go test -v -race ./...`) and Roaring Bitmap / RRF benchmarks maintain zero-alloc $<1\text{ ms}$ targets.
5. **Doc API & OpenAPI Sync**: Confirm [`.ai/doc-api.md`](doc-api.md) and `api/openapi/openapi.yaml` reflect any new or modified endpoints, schemas, protos, or cache contracts.
6. **Progress Tracker**: Update [`.ai/progress-tracker.md`](progress-tracker.md) with session notes and completed items.
