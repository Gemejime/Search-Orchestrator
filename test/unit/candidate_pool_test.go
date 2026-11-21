package unit_test

import (
	"testing"

	"github.com/rag-platform/rag-search-orchestrator/internal/domain/entity"
	"github.com/rag-platform/rag-search-orchestrator/pkg/pool"
)

func TestCandidatePool_ReuseAndReset(t *testing.T) {
	cp := pool.NewCandidatePool()

	searchBuf := cp.GetSearchCandidates()
	if len(*searchBuf) != 0 || cap(*searchBuf) < pool.DefaultSearchCandidateCap {
		t.Fatalf("unexpected initial search buffer len=%d cap=%d", len(*searchBuf), cap(*searchBuf))
	}
	*searchBuf = append(*searchBuf, entity.SearchCandidate{DocID: 42, ChunkID: "chk_42"})
	cp.PutSearchCandidates(searchBuf)

	searchBuf2 := cp.GetSearchCandidates()
	if len(*searchBuf2) != 0 {
		t.Fatalf("expected reset search buffer len=0, got %d", len(*searchBuf2))
	}
	cp.PutSearchCandidates(searchBuf2)

	fusedBuf := cp.GetFusedCandidates()
	if len(*fusedBuf) != 0 || cap(*fusedBuf) < pool.DefaultFusedCandidateCap {
		t.Fatalf("unexpected initial fused buffer len=%d cap=%d", len(*fusedBuf), cap(*fusedBuf))
	}
	*fusedBuf = append(*fusedBuf, entity.FusedCandidate{DocID: 7, ChunkID: "chk_7", RRFScore: 0.5})
	cp.PutFusedCandidates(fusedBuf)

	fusedBuf2 := cp.GetFusedCandidates()
	if len(*fusedBuf2) != 0 {
		t.Fatalf("expected reset fused buffer len=0, got %d", len(*fusedBuf2))
	}
	cp.PutFusedCandidates(fusedBuf2)

	docIDs := cp.GetDocIDs()
	if len(*docIDs) != 0 || cap(*docIDs) < pool.DefaultDocIDCap {
		t.Fatalf("unexpected initial docID buffer len=%d cap=%d", len(*docIDs), cap(*docIDs))
	}
	*docIDs = append(*docIDs, 100, 200)
	cp.PutDocIDs(docIDs)

	docIDs2 := cp.GetDocIDs()
	if len(*docIDs2) != 0 {
		t.Fatalf("expected reset docID buffer len=0, got %d", len(*docIDs2))
	}
	cp.PutDocIDs(docIDs2)

	// Verify truncated slots beyond len(*buf) up to cap(*buf) are cleared before pooling.
	truncSearch := make([]entity.SearchCandidate, 3, pool.DefaultSearchCandidateCap)
	truncSearch[1] = entity.SearchCandidate{DocID: 11, ChunkID: "truncated_slot_1"}
	truncSearch[2] = entity.SearchCandidate{DocID: 22, ChunkID: "truncated_slot_2"}
	truncSearch = truncSearch[:1]
	cp.PutSearchCandidates(&truncSearch)
	if resliced := truncSearch[:3]; resliced[1].ChunkID != "" || resliced[2].ChunkID != "" {
		t.Fatalf("expected truncated SearchCandidate slots to be cleared, got %+v", resliced[1:3])
	}

	truncFused := make([]entity.FusedCandidate, 3, pool.DefaultFusedCandidateCap)
	truncFused[1] = entity.FusedCandidate{DocID: 33, ChunkID: "truncated_fused_1"}
	truncFused[2] = entity.FusedCandidate{DocID: 44, ChunkID: "truncated_fused_2"}
	truncFused = truncFused[:1]
	cp.PutFusedCandidates(&truncFused)
	if resliced := truncFused[:3]; resliced[1].ChunkID != "" || resliced[2].ChunkID != "" {
		t.Fatalf("expected truncated FusedCandidate slots to be cleared, got %+v", resliced[1:3])
	}
}

func TestCandidatePool_DropsOversizedAndHandlesNil(t *testing.T) {
	cp := pool.NewCandidatePool()

	cp.PutSearchCandidates(nil)
	cp.PutFusedCandidates(nil)
	cp.PutDocIDs(nil)

	bigSearch := make([]entity.SearchCandidate, 0, pool.MaxSearchCandidateCap+1)
	cp.PutSearchCandidates(&bigSearch)
	gotSearch := cp.GetSearchCandidates()
	if cap(*gotSearch) > pool.MaxSearchCandidateCap {
		t.Fatalf("expected oversized search buffer to be dropped, got cap=%d", cap(*gotSearch))
	}
	cp.PutSearchCandidates(gotSearch)

	bigFused := make([]entity.FusedCandidate, 0, pool.MaxFusedCandidateCap+1)
	cp.PutFusedCandidates(&bigFused)
	gotFused := cp.GetFusedCandidates()
	if cap(*gotFused) > pool.MaxFusedCandidateCap {
		t.Fatalf("expected oversized fused buffer to be dropped, got cap=%d", cap(*gotFused))
	}
	cp.PutFusedCandidates(gotFused)

	bigDocIDs := make([]uint32, 0, pool.MaxDocIDCap+1)
	cp.PutDocIDs(&bigDocIDs)
	gotDocIDs := cp.GetDocIDs()
	if cap(*gotDocIDs) > pool.MaxDocIDCap {
		t.Fatalf("expected oversized docID buffer to be dropped, got cap=%d", cap(*gotDocIDs))
	}
	cp.PutDocIDs(gotDocIDs)
}

func TestCandidatePool_ZeroAllocsSteadyState(t *testing.T) {
	cp := pool.NewCandidatePool()

	// Warm up each pool on the current goroutine.
	cp.PutSearchCandidates(cp.GetSearchCandidates())
	cp.PutFusedCandidates(cp.GetFusedCandidates())
	cp.PutDocIDs(cp.GetDocIDs())

	searchAllocs := testing.AllocsPerRun(500, func() {
		buf := cp.GetSearchCandidates()
		*buf = append(*buf, entity.SearchCandidate{DocID: 1})
		cp.PutSearchCandidates(buf)
	})
	if searchAllocs != 0 {
		t.Fatalf("expected 0 allocs for SearchCandidate pool, got %.2f", searchAllocs)
	}

	fusedAllocs := testing.AllocsPerRun(500, func() {
		buf := cp.GetFusedCandidates()
		*buf = append(*buf, entity.FusedCandidate{DocID: 1})
		cp.PutFusedCandidates(buf)
	})
	if fusedAllocs != 0 {
		t.Fatalf("expected 0 allocs for FusedCandidate pool, got %.2f", fusedAllocs)
	}

	docIDAllocs := testing.AllocsPerRun(500, func() {
		buf := cp.GetDocIDs()
		*buf = append(*buf, 1)
		cp.PutDocIDs(buf)
	})
	if docIDAllocs != 0 {
		t.Fatalf("expected 0 allocs for DocID pool, got %.2f", docIDAllocs)
	}
}
