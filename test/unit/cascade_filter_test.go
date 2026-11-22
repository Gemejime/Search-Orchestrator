package unit_test

import (
	"fmt"
	"testing"

	"github.com/rag-platform/rag-search-orchestrator/internal/domain/entity"
	"github.com/rag-platform/rag-search-orchestrator/internal/domain/ranking"
)

func TestCascadeFilter_EnforcesBoundsAndFailClosedACL(t *testing.T) {
	cf := ranking.NewCascadeFilter()

	rrf := make([]entity.FusedCandidate, 25)
	for i := 0; i < 25; i++ {
		rrf[i] = entity.FusedCandidate{
			DocID:    uint32(i + 1),
			ChunkID:  fmt.Sprintf("chk_%02d", i+1),
			RRFScore: 1.0 / float64(60+i+1),
		}
	}

	// Nil ACL must fail closed.
	permNil, rerankNil := cf.FilterForRerank(rrf, nil)
	if len(permNil) != 0 || len(rerankNil) != 0 {
		t.Fatalf("expected fail-closed empty output on nil ACL, got %d/%d", len(permNil), len(rerankNil))
	}

	// Permit even DocIDs only. Within Top-20 (DocID 1..20), 10 are even (2,4..20); DocID 22,24 are beyond Top-20 cap.
	evenACL := ranking.ACLPredicate(func(docID uint32) bool {
		return docID%2 == 0
	})
	perm, rerank := cf.FilterForRerank(rrf, evenACL)
	if len(perm) != 10 {
		t.Fatalf("expected 10 permitted within Top-20 RRF, got %d", len(perm))
	}
	if len(rerank) != ranking.MaxRerankInputs {
		t.Fatalf("expected Top-10 rerank input, got %d", len(rerank))
	}
	for _, c := range perm {
		if c.DocID%2 != 0 || c.DocID > 20 {
			t.Fatalf("unexpected candidate in permitted output: %+v", c)
		}
	}
}

func TestCascadeFilter_ThresholdAndFallbackBackfill(t *testing.T) {
	cf := ranking.NewCascadeFilter()
	acl := ranking.ACLPredicate(func(docID uint32) bool {
		return docID != 99 // 99 is unpermitted
	})

	aclPermittedRRF := []entity.FusedCandidate{
		{DocID: 1, ChunkID: "c1", RRFScore: 0.032},
		{DocID: 2, ChunkID: "c2", RRFScore: 0.031},
		{DocID: 99, ChunkID: "c99", RRFScore: 0.030}, // unpermitted defense-in-depth check
		{DocID: 3, ChunkID: "c3", RRFScore: 0.029},
		{DocID: 4, ChunkID: "c4", RRFScore: 0.028},
		{DocID: 5, ChunkID: "c5", RRFScore: 0.027},
	}

	// Only 2 candidates pass tau=0.50: (DocID 3, logit 0.90) and (DocID 1, logit 0.60).
	// Unpermitted DocID 99 has high logit 0.99 but MUST be excluded.
	reranked := []entity.FusedCandidate{
		{DocID: 99, ChunkID: "c99", RRFScore: 0.030, RerankLogit: 0.99},
		{DocID: 1, ChunkID: "c1", RRFScore: 0.032, RerankLogit: 0.60},
		{DocID: 2, ChunkID: "c2", RRFScore: 0.031, RerankLogit: 0.10}, // below tau
		{DocID: 3, ChunkID: "c3", RRFScore: 0.029, RerankLogit: 0.90},
	}

	selected, fallbackUsed := cf.SelectTopKWithThreshold(reranked, aclPermittedRRF, acl, 5, 0.50)
	if !fallbackUsed {
		t.Fatal("expected fallbackUsed=true when fewer than topK pass tau")
	}
	if len(selected) != 5 {
		t.Fatalf("expected 5 selected chunks, got %d", len(selected))
	}

	// Order: (3, c3) [0.90], (1, c1) [0.60], then backfilled from RRF: (2, c2), (4, c4), (5, c5)
	wantOrder := []uint32{3, 1, 2, 4, 5}
	for i, wantID := range wantOrder {
		if selected[i].DocID != wantID {
			t.Fatalf("slot %d: expected DocID %d, got %d", i, wantID, selected[i].DocID)
		}
	}
	if selected[0].FinalScore != 0.90 || selected[1].FinalScore != 0.60 {
		t.Fatalf("expected rerank logits as FinalScore for passing candidates, got %+v", selected[:2])
	}
	if selected[2].FinalScore != 0.031 {
		t.Fatalf("expected RRFScore as FinalScore for backfilled candidate, got %f", selected[2].FinalScore)
	}
}

func TestCascadeFilter_CircuitBreakerEmptyRerankedPromotesRRF(t *testing.T) {
	cf := ranking.NewCascadeFilter()
	acl := ranking.ACLPredicate(func(docID uint32) bool { return true })

	aclPermittedRRF := []entity.FusedCandidate{
		{DocID: 10, ChunkID: "a", RRFScore: 0.03},
		{DocID: 20, ChunkID: "b", RRFScore: 0.02},
		{DocID: 30, ChunkID: "c", RRFScore: 0.01},
	}

	selected, fallbackUsed := cf.SelectTopKWithThreshold(nil, aclPermittedRRF, acl, 2, 0.5)
	if !fallbackUsed {
		t.Fatal("expected fallbackUsed=true when reranked is empty")
	}
	if len(selected) != 2 {
		t.Fatalf("expected 2 selected chunks, got %d", len(selected))
	}
	if selected[0].DocID != 10 || selected[1].DocID != 20 {
		t.Fatalf("unexpected fallback order: %+v", selected)
	}
}

func BenchmarkCascadeFilter(b *testing.B) {
	cf := ranking.NewCascadeFilter()
	acl := ranking.ACLPredicate(func(docID uint32) bool { return docID%2 == 0 })
	rrf := make([]entity.FusedCandidate, 20)
	for i := 0; i < 20; i++ {
		rrf[i] = entity.FusedCandidate{
			DocID:       uint32(i + 1),
			ChunkID:     fmt.Sprintf("chk_%d", i+1),
			RRFScore:    1.0 / float64(60+i+1),
			RerankLogit: float64(20-i) * 0.05,
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		perm, rerankIn := cf.FilterForRerank(rrf, acl)
		sel, _ := cf.SelectTopKWithThreshold(rerankIn, perm, acl, 5, 0.5)
		if len(sel) != 5 {
			b.Fatalf("expected 5, got %d", len(sel))
		}
	}
}
