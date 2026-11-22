# Phase 2: Native `uint32` Roaring Bitmap ACL & Zero-Alloc Pools

## 1. Goals
1. **Native `uint32` Roaring Bitmap ACL (`internal/domain/acl/roaring_evaluator.go`)**:
   - Pure Go implementation (zero network dependencies) using `github.com/RoaringBitmap/roaring/v2` (standard Roaring binary format via `UnmarshalBinary`).
   - Implement `ranking.ACLEvaluator` (`IsPermitted(docID uint32) bool`) plus batch/in-place filtering helpers (`FilterDocIDs`, `FilterCandidates`) executing in `< 1 ms` with `0` heap allocations on the hot path.
   - Use a `sync.Pool` of `*roaring.Bitmap` instances (`Clear()` on `Close()`) to avoid per-request bitmap heap allocations.
   - Fail closed on `nil`, empty, or malformed `caller_acl_bitmap` payloads with sanitized error messages (never log or embed raw bitmap bytes).

2. **Zero-Alloc Candidate & Slice Pools (`pkg/pool/candidate_pool.go`)**:
   - Provide `sync.Pool`-backed pools for:
     - `[]entity.SearchCandidate` (default capacity `40` for Dense/Sparse fan-out)
     - `[]entity.FusedCandidate` (default capacity `20` for RRF / ACL-filtered slices)
     - `[]uint32` (default capacity `40` for `doc_id` batch operations)
   - Always reset slice lengths (`buf = buf[:0]`) before returning to the pool and drop oversized slices above a configured max capacity to bound memory retention.

3. **Security & Benchmark Tests (`test/unit/`)**:
   - `test/unit/roaring_evaluator_test.go`: Sparse and dense `uint32` distributions, fail-closed checks on `nil`/empty/corrupt bitmaps, `ranking.CascadeFilter` integration, `testing.AllocsPerRun` zero-alloc assertions, and `100,000+` `uint32` ID benchmarks asserting `< 1 ms` execution and `0 allocs/op`.
   - `test/unit/candidate_pool_test.go`: Slice reuse, length reset, oversized buffer eviction, and steady-state `0 allocs/op` verification.

4. **Documentation & Synchronization**:
   - Update `.ai/doc-api.md` and `api/openapi/openapi.yaml` to specify standard Roaring binary serialization for `caller_acl_bitmap` / `caller_acl_bitmap_b64`.
   - Update `.ai/progress-tracker.md` to mark Phase 2 as `Completed`.

## 2. Security & Invariants
- Zero network dependencies in `internal/domain/`.
- Zero `fmt.Print*` in production packages (`internal/`, `pkg/`); no raw `caller_acl_bitmap` bytes in logs or errors.
- Fail-closed ACL evaluation on `nil`, empty, or corrupt bitmaps and after `Close()`.
- Git commits: At least 2 conventional commits (`feat:`, `tests:`, `docs:`).

## 3. Verification
- `gofmt -s -l .` (clean)
- `go vet ./...`
- `golangci-lint run ./...`
- `govulncheck ./...`
- `gosec -exclude-generated ./...`
- `go run -C .github/scripts . ../../internal ../../pkg`
- `go test -v ./...` (and `go test -v -race ./...` when `CGO_ENABLED=1` with a C compiler is available)
- `go test -run=^$ -bench=. -benchmem ./test/unit/...`
