package unit_test

import (
	"fmt"
	"math"
	"testing"

	"github.com/rag-platform/rag-search-orchestrator/internal/domain/entity"
	"github.com/rag-platform/rag-search-orchestrator/internal/domain/ranking"
)

func TestRRFEngine_FuseMathAndDeduplication(t *testing.T) {
	engine := ranking.NewRRFEngine(ranking.DefaultRRFK)

	dense := []entity.SearchCandidate{
		// Duplicate (10, "c1") at rank 1 and rank 3 -> must keep rank 1 only.
		{DocID: 10, ChunkID: "c1", ParentKey: "p10_c1", Modality: entity.ModalityDense, Rank: 1},
		{DocID: 20, ChunkID: "c1", ParentKey: "p20_c1", Modality: entity.ModalityDense, Rank: 2},
		{DocID: 10, ChunkID: "c1", ParentKey: "p10_c1", Modality: entity.ModalityDense, Rank: 3},
		// Second chunk of doc_id 10 must remain distinct from (10, "c1").
		{DocID: 10, ChunkID: "c2", ParentKey: "p10_c2", Modality: entity.ModalityDense, Rank: 4},
	}

	sparse := []entity.SearchCandidate{
		{DocID: 20, ChunkID: "c1", ParentKey: "p20_c1", Modality: entity.ModalitySparse, Rank: 1},
		{DocID: 10, ChunkID: "c1", ParentKey: "p10_c1", Modality: entity.ModalitySparse, Rank: 2},
	}

	fused := engine.Fuse(dense, sparse)
	if len(fused) != 3 {
		t.Fatalf("expected 3 deduplicated candidates, got %d", len(fused))
	}

	// (10, "c1"): 1/(60+1) + 1/(60+2) = 1/61 + 1/62
	// (20, "c1"): 1/(60+2) + 1/(60+1) = 1/62 + 1/61 (exact tie -> lower DocID 10 wins!)
	wantTopScore := 1.0/61.0 + 1.0/62.0
	if math.Abs(fused[0].RRFScore-wantTopScore) > 1e-12 {
		t.Fatalf("expected top RRF score %.12f, got %.12f", wantTopScore, fused[0].RRFScore)
	}
	if fused[0].DocID != 10 || fused[0].ChunkID != "c1" {
		t.Fatalf("expected tie-break winner (10, c1), got (%d, %s)", fused[0].DocID, fused[0].ChunkID)
	}
	if fused[1].DocID != 20 || fused[1].ChunkID != "c1" {
		t.Fatalf("expected second (20, c1), got (%d, %s)", fused[1].DocID, fused[1].ChunkID)
	}
	if fused[2].DocID != 10 || fused[2].ChunkID != "c2" {
		t.Fatalf("expected third (10, c2), got (%d, %s)", fused[2].DocID, fused[2].ChunkID)
	}
}

func TestRRFEngine_Enforces40CapAnd20OutputCap(t *testing.T) {
	engine := ranking.NewRRFEngine(60)

	dense := make([]entity.SearchCandidate, 50)
	sparse := make([]entity.SearchCandidate, 50)
	for i := 0; i < 50; i++ {
		dense[i] = entity.SearchCandidate{
			DocID:    uint32(i + 1),
			ChunkID:  fmt.Sprintf("d_%02d", i+1),
			Modality: entity.ModalityDense,
		}
		sparse[i] = entity.SearchCandidate{
			DocID:    uint32(100 + i + 1),
			ChunkID:  fmt.Sprintf("s_%02d", i+1),
			Modality: entity.ModalitySparse,
		}
	}

	fused := engine.Fuse(dense, sparse)
	if len(fused) != ranking.MaxRRFOutput {
		t.Fatalf("expected MaxRRFOutput=%d, got %d", ranking.MaxRRFOutput, len(fused))
	}

	// Verify 41st+ dense/sparse candidates (DocID 41..50, 141..150) were capped out.
	for _, c := range fused {
		if (c.DocID >= 41 && c.DocID <= 50) || c.DocID >= 141 {
			t.Fatalf("candidate beyond [:40] cap leaked into fused output: %+v", c)
		}
	}
}

func TestRRFEngine_DeterministicTieBreakingByDocIDAndChunkID(t *testing.T) {
	engine := ranking.NewRRFEngine(60)

	// Equal RRF scores: all appear only in dense or sparse at rank 1.
	dense := []entity.SearchCandidate{
		{DocID: 30, ChunkID: "b", Rank: 1, Modality: entity.ModalityDense},
	}
	sparse := []entity.SearchCandidate{
		{DocID: 10, ChunkID: "b", Rank: 1, Modality: entity.ModalitySparse},
		{DocID: 10, ChunkID: "a", Rank: 1, Modality: entity.ModalitySparse},
	}

	fused := engine.Fuse(dense, sparse)
	if len(fused) != 3 {
		t.Fatalf("expected 3 results, got %d", len(fused))
	}
	if fused[0].DocID != 10 || fused[0].ChunkID != "a" {
		t.Fatalf("expected (10, a) first, got (%d, %s)", fused[0].DocID, fused[0].ChunkID)
	}
	if fused[1].DocID != 10 || fused[1].ChunkID != "b" {
		t.Fatalf("expected (10, b) second, got (%d, %s)", fused[1].DocID, fused[1].ChunkID)
	}
	if fused[2].DocID != 30 || fused[2].ChunkID != "b" {
		t.Fatalf("expected (30, b) third, got (%d, %s)", fused[2].DocID, fused[2].ChunkID)
	}
}

func BenchmarkRRFEngineFuse(b *testing.B) {
	engine := ranking.NewRRFEngine(ranking.DefaultRRFK)
	dense := make([]entity.SearchCandidate, 40)
	sparse := make([]entity.SearchCandidate, 40)
	for i := 0; i < 40; i++ {
		dense[i] = entity.SearchCandidate{
			DocID:    uint32(i + 1),
			ChunkID:  fmt.Sprintf("chk_%d", i+1),
			Modality: entity.ModalityDense,
		}
		sparse[i] = entity.SearchCandidate{
			DocID:    uint32((i + 20) % 50),
			ChunkID:  fmt.Sprintf("chk_%d", (i+20)%50),
			Modality: entity.ModalitySparse,
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out := engine.Fuse(dense, sparse)
		if len(out) == 0 {
			b.Fatal("empty fused output")
		}
	}
}
