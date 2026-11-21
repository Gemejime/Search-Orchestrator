package ranking

import (
	"sort"

	"github.com/rag-platform/rag-search-orchestrator/internal/domain/entity"
)

const (
	// DefaultRRFK is the standard Reciprocal Rank Fusion smoothing constant.
	DefaultRRFK = 60
	// MaxModalityCandidates is the per-modality input cap (Top-40 dense, Top-40 sparse).
	MaxModalityCandidates = 40
	// MaxRRFOutput is the maximum number of fused candidates emitted by RRF (Top-20).
	MaxRRFOutput = 20
)

type candidateKey struct {
	docID   uint32
	chunkID string
}

type modalityEntry struct {
	candidate entity.SearchCandidate
	rank      int
}

// RRFEngine blends dense and sparse search candidates using Reciprocal Rank Fusion.
type RRFEngine struct {
	k int
}

// NewRRFEngine constructs an RRFEngine with the given smoothing constant k.
// If k <= 0, DefaultRRFK (60) is used.
func NewRRFEngine(k int) *RRFEngine {
	if k <= 0 {
		k = DefaultRRFK
	}
	return &RRFEngine{k: k}
}

// Fuse enforces the [:40] per-modality input cap, deduplicates intra-modality by keeping
// the lowest 1-based rank per (doc_id, chunk_id), deduplicates cross-modality by
// (doc_id, chunk_id), sums RRF scores (1 / (k + rank)), and returns up to Top-20
// candidates sorted deterministically (higher RRFScore -> lower DocID -> smaller ChunkID).
func (e *RRFEngine) Fuse(dense, sparse []entity.SearchCandidate) []entity.FusedCandidate {
	denseDedup := deduplicateModality(capModality(dense))
	sparseDedup := deduplicateModality(capModality(sparse))

	if len(denseDedup) == 0 && len(sparseDedup) == 0 {
		return nil
	}

	fusedMap := make(map[candidateKey]*entity.FusedCandidate, len(denseDedup)+len(sparseDedup))

	for key, entry := range denseDedup {
		score := 1.0 / float64(e.k+entry.rank)
		fc := &entity.FusedCandidate{
			DocID:      entry.candidate.DocID,
			ChunkID:    entry.candidate.ChunkID,
			ParentKey:  entry.candidate.ParentKey,
			RRFScore:   score,
			FinalScore: score,
			DenseRank:  entry.rank,
			Metadata:   entry.candidate.Metadata,
		}
		fusedMap[key] = fc
	}

	for key, entry := range sparseDedup {
		score := 1.0 / float64(e.k+entry.rank)
		if existing, ok := fusedMap[key]; ok {
			existing.RRFScore += score
			existing.FinalScore = existing.RRFScore
			existing.SparseRank = entry.rank
			if existing.ParentKey == "" && entry.candidate.ParentKey != "" {
				existing.ParentKey = entry.candidate.ParentKey
			}
			if len(existing.Metadata) == 0 && len(entry.candidate.Metadata) > 0 {
				existing.Metadata = entry.candidate.Metadata
			}
		} else {
			fusedMap[key] = &entity.FusedCandidate{
				DocID:      entry.candidate.DocID,
				ChunkID:    entry.candidate.ChunkID,
				ParentKey:  entry.candidate.ParentKey,
				RRFScore:   score,
				FinalScore: score,
				SparseRank: entry.rank,
				Metadata:   entry.candidate.Metadata,
			}
		}
	}

	results := make([]entity.FusedCandidate, 0, len(fusedMap))
	for _, fc := range fusedMap {
		results = append(results, *fc)
	}

	sort.Slice(results, func(i, j int) bool {
		if results[i].RRFScore != results[j].RRFScore {
			return results[i].RRFScore > results[j].RRFScore
		}
		if results[i].DocID != results[j].DocID {
			return results[i].DocID < results[j].DocID
		}
		return results[i].ChunkID < results[j].ChunkID
	})

	if len(results) > MaxRRFOutput {
		results = results[:MaxRRFOutput]
	}

	return results
}

func capModality(candidates []entity.SearchCandidate) []entity.SearchCandidate {
	if len(candidates) > MaxModalityCandidates {
		return candidates[:MaxModalityCandidates]
	}
	return candidates
}

func deduplicateModality(candidates []entity.SearchCandidate) map[candidateKey]modalityEntry {
	if len(candidates) == 0 {
		return nil
	}
	dedup := make(map[candidateKey]modalityEntry, len(candidates))
	for i, c := range candidates {
		rank := c.Rank
		if rank <= 0 {
			rank = i + 1
		}
		key := candidateKey{docID: c.DocID, chunkID: c.ChunkID}
		if existing, ok := dedup[key]; ok {
			if rank < existing.rank {
				dedup[key] = modalityEntry{candidate: c, rank: rank}
			}
		} else {
			dedup[key] = modalityEntry{candidate: c, rank: rank}
		}
	}
	return dedup
}
