package unit_test

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/rag-platform/rag-search-orchestrator/internal/domain/acl"
	"github.com/rag-platform/rag-search-orchestrator/internal/domain/entity"
	"github.com/rag-platform/rag-search-orchestrator/internal/domain/ranking"
	"github.com/rag-platform/rag-search-orchestrator/pkg/pool"
)

func mustMarshalBitmap(t testing.TB, ids ...uint32) []byte {
	t.Helper()
	bm := roaring.New()
	bm.AddMany(ids)
	b, err := bm.MarshalBinary()
	if err != nil {
		t.Fatalf("failed to marshal roaring bitmap: %v", err)
	}
	return b
}

func TestRoaringEvaluator_FailClosedOnEmptyNilCorruptAndClosed(t *testing.T) {
	var nilEval *acl.RoaringEvaluator
	if nilEval.IsPermitted(1) {
		t.Fatal("nil evaluator must fail closed")
	}
	if nilEval.Cardinality() != 0 {
		t.Fatal("nil evaluator must report 0 cardinality")
	}

	if _, err := acl.NewRoaringEvaluator(nil); err != acl.ErrEmptyACLBitmap {
		t.Fatalf("expected ErrEmptyACLBitmap for nil payload, got %v", err)
	}
	if _, err := acl.NewRoaringEvaluator([]byte{}); err != acl.ErrEmptyACLBitmap {
		t.Fatalf("expected ErrEmptyACLBitmap for empty payload, got %v", err)
	}

	corrupt := []byte{0xde, 0xad, 0xbe, 0xef, 0x01, 0x02}
	_, err := acl.NewRoaringEvaluator(corrupt)
	if err != acl.ErrInvalidACLBitmap {
		t.Fatalf("expected ErrInvalidACLBitmap for corrupt payload, got %v", err)
	}
	// Secured-logging check: error message must not leak raw bytes.
	if bytes.Contains([]byte(err.Error()), []byte(hex.EncodeToString(corrupt))) {
		t.Fatal("error message must not contain raw bitmap bytes")
	}

	ep := acl.NewRoaringEvaluatorPool()
	if _, err := ep.Acquire(corrupt); err != acl.ErrInvalidACLBitmap {
		t.Fatalf("expected ErrInvalidACLBitmap from pool Acquire, got %v", err)
	}

	// Structurally invalid bitmap: valid Roaring header and length, but unsorted/duplicate array container values.
	structurallyInvalid := mustMarshalBitmap(t, 1, 2, 3)
	// Overwrite the final uint16 element (3) with 1 so the array container is no longer strictly sorted.
	structurallyInvalid[len(structurallyInvalid)-2] = 1
	structurallyInvalid[len(structurallyInvalid)-1] = 0
	if _, err := acl.NewRoaringEvaluator(structurallyInvalid); err != acl.ErrInvalidACLBitmap {
		t.Fatalf("expected ErrInvalidACLBitmap for structurally invalid bitmap, got %v", err)
	}
	if _, err := ep.Acquire(structurallyInvalid); err != acl.ErrInvalidACLBitmap {
		t.Fatalf("expected ErrInvalidACLBitmap from pool Acquire for structurally invalid bitmap, got %v", err)
	}

	validBytes := mustMarshalBitmap(t, 10, 20, 30)
	ev, err := ep.Acquire(validBytes)
	if err != nil {
		t.Fatalf("unexpected error acquiring valid bitmap: %v", err)
	}
	if !ev.IsPermitted(20) || ev.IsPermitted(99) {
		t.Fatal("unexpected ACL evaluation result")
	}
	ev.Close()
	if ev.IsPermitted(20) {
		t.Fatal("closed evaluator must fail closed")
	}
}

func TestRoaringEvaluator_SparseAndDenseDistributionsWithCascade(t *testing.T) {
	// Mix sparse high-range uint32 IDs (near 2^31) and dense low-range IDs.
	permitted := []uint32{2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 2_000_000_001, 4_000_000_002}
	raw := mustMarshalBitmap(t, permitted...)

	ep := acl.NewRoaringEvaluatorPool()
	ev, err := ep.Acquire(raw)
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}
	defer ev.Close()

	if ev.Cardinality() != uint64(len(permitted)) {
		t.Fatalf("expected cardinality %d, got %d", len(permitted), ev.Cardinality())
	}

	cf := ranking.NewCascadeFilter()
	rrf := make([]entity.FusedCandidate, 20)
	for i := 0; i < 20; i++ {
		rrf[i] = entity.FusedCandidate{
			DocID:    uint32(i + 1),
			ChunkID:  fmt.Sprintf("chk_%02d", i+1),
			RRFScore: 1.0 / float64(60+i+1),
		}
	}

	permAll, rerankIn := cf.FilterForRerank(rrf, ev)
	if len(permAll) != 10 || len(rerankIn) != 10 {
		t.Fatalf("expected 10 permitted and 10 rerank inputs, got %d/%d", len(permAll), len(rerankIn))
	}
	for _, c := range rerankIn {
		if c.DocID%2 != 0 {
			t.Fatalf("unpermitted odd DocID %d leaked into rerank input", c.DocID)
		}
	}
}

func TestRoaringEvaluator_ZeroAllocsOver100kIDs(t *testing.T) {
	// Build a 100,000-element caller bitmap (even IDs) and a 100,000-element candidate bitmap (IDs 1..100,000).
	const count = 100_000
	callerBM := roaring.New()
	candTemplate := roaring.New()
	probe := make([]uint32, count)
	for i := 0; i < count; i++ {
		callerBM.Add(uint32(i * 2))
		candTemplate.Add(uint32(i + 1))
		probe[i] = uint32(i + 1)
	}
	raw, err := callerBM.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	ev, err := acl.NewRoaringEvaluator(raw)
	if err != nil {
		t.Fatalf("NewRoaringEvaluator failed: %v", err)
	}
	defer ev.Close()

	// 1. Verify zero-alloc FilterDocIDs over 100,000 uint32 IDs.
	dst := make([]uint32, 0, count)
	allocs := testing.AllocsPerRun(20, func() {
		out := ev.FilterDocIDs(probe, dst)
		if len(out) != count/2 {
			t.Fatalf("expected %d permitted IDs, got %d", count/2, len(out))
		}
	})
	if allocs != 0 {
		t.Fatalf("expected 0 heap allocations for 100k FilterDocIDs, got %.2f", allocs)
	}

	// 2. Verify native bitwise AND intersection over 100,000 uint32 IDs executes in <1ms with 0 allocs.
	candBM := candTemplate.Clone()
	// Warm up once, then measure in-place bitwise AND.
	ev.IntersectInPlace(candBM)
	if candBM.GetCardinality() != count/2 {
		t.Fatalf("expected %d permitted IDs after intersection, got %d", count/2, candBM.GetCardinality())
	}

	// Pre-build fresh candidate bitmaps outside the measured section so every run intersects all 100,000 IDs
	// without including setup allocations or time in the measurement.
	const runs = 100
	candRuns := make([]*roaring.Bitmap, runs+1)
	for i := range candRuns {
		bm := roaring.New()
		bm.AddMany(probe)
		candRuns[i] = bm
	}
	runIdx := 0
	var totalElapsed time.Duration
	intersectAllocs := testing.AllocsPerRun(runs, func() {
		bm := candRuns[runIdx]
		runIdx++
		iterStart := time.Now()
		ev.IntersectInPlace(bm)
		totalElapsed += time.Since(iterStart)
	})
	elapsed := totalElapsed / time.Duration(runIdx)
	for i := 0; i < runIdx; i++ {
		if candRuns[i].GetCardinality() != count/2 {
			t.Fatalf("run %d: expected %d permitted IDs after intersection, got %d", i, count/2, candRuns[i].GetCardinality())
		}
	}
	if intersectAllocs != 0 {
		t.Fatalf("expected 0 heap allocations for 100k IntersectInPlace, got %.2f", intersectAllocs)
	}
	if elapsed >= time.Millisecond {
		t.Fatalf("expected <1ms bitwise intersection over 100,000 uint32 IDs, took %v", elapsed)
	}
}

func TestRoaringEvaluator_FilterCandidatesWithCandidatePool(t *testing.T) {
	raw := mustMarshalBitmap(t, 101, 103, 105)
	ev, err := acl.NewRoaringEvaluator(raw)
	if err != nil {
		t.Fatalf("NewRoaringEvaluator failed: %v", err)
	}
	defer ev.Close()

	cp := pool.NewCandidatePool()
	dst := cp.GetFusedCandidates()
	defer cp.PutFusedCandidates(dst)

	in := []entity.FusedCandidate{
		{DocID: 100, ChunkID: "c100"},
		{DocID: 101, ChunkID: "c101"},
		{DocID: 102, ChunkID: "c102"},
		{DocID: 103, ChunkID: "c103"},
		{DocID: 104, ChunkID: "c104"},
		{DocID: 105, ChunkID: "c105"},
	}

	*dst = ev.FilterCandidates(in, *dst)
	if len(*dst) != 3 {
		t.Fatalf("expected 3 permitted candidates, got %d", len(*dst))
	}
	if (*dst)[0].DocID != 101 || (*dst)[1].DocID != 103 || (*dst)[2].DocID != 105 {
		t.Fatalf("unexpected filtered candidates: %+v", *dst)
	}
}

func BenchmarkRoaringEvaluator_100kIDs(b *testing.B) {
	const count = 100_000
	bm := roaring.New()
	probe := make([]uint32, count)
	for i := 0; i < count; i++ {
		bm.Add(uint32(i * 2))
		probe[i] = uint32(i)
	}
	raw, err := bm.MarshalBinary()
	if err != nil {
		b.Fatalf("MarshalBinary failed: %v", err)
	}

	ev, err := acl.NewRoaringEvaluator(raw)
	if err != nil {
		b.Fatalf("NewRoaringEvaluator failed: %v", err)
	}
	defer ev.Close()

	dst := make([]uint32, 0, count)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out := ev.FilterDocIDs(probe, dst)
		if len(out) != count/2 {
			b.Fatalf("unexpected count: %d", len(out))
		}
	}
}

func BenchmarkRoaringEvaluator_IntersectInPlace100k(b *testing.B) {
	const count = 100_000
	callerBM := roaring.New()
	candTemplate := roaring.New()
	for i := 0; i < count; i++ {
		callerBM.Add(uint32(i * 2))
		candTemplate.Add(uint32(i + 1))
	}
	raw, err := callerBM.MarshalBinary()
	if err != nil {
		b.Fatalf("MarshalBinary failed: %v", err)
	}

	ev, err := acl.NewRoaringEvaluator(raw)
	if err != nil {
		b.Fatalf("NewRoaringEvaluator failed: %v", err)
	}
	defer ev.Close()

	candBM := roaring.New()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		candBM.Clear()
		candBM.Or(candTemplate)
		b.StartTimer()
		ev.IntersectInPlace(candBM)
		if candBM.GetCardinality() != count/2 {
			b.Fatalf("expected %d permitted IDs, got %d", count/2, candBM.GetCardinality())
		}
	}
}
