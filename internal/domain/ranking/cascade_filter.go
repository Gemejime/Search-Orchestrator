package ranking

import (
	"sort"

	"github.com/rag-platform/rag-search-orchestrator/internal/domain/entity"
)

const (
	// MaxDenseCandidates is the hard cap on dense HNSW candidates entering RRF.
	MaxDenseCandidates = 40
	// MaxSparseCandidates is the hard cap on sparse BM25 candidates entering RRF.
	MaxSparseCandidates = 40
	// MaxRRFOutputs is the hard cap on RRF candidates entering ACL filtering.
	MaxRRFOutputs = 20
	// MaxRerankInputs is the hard cap on ACL-permitted candidates sent to Triton Cross-Encoder.
	MaxRerankInputs = 10
	// MaxHydratedOutputs is the hard cap on final hydrated chunks returned to the caller.
	MaxHydratedOutputs = 5
)

// ACLEvaluator checks whether a native uint32 doc_id is permitted for the caller.
type ACLEvaluator interface {
	IsPermitted(docID uint32) bool
}

// ACLPredicate adapts a function into an ACLEvaluator.
type ACLPredicate func(docID uint32) bool

// IsPermitted evaluates the predicate for the given docID.
func (p ACLPredicate) IsPermitted(docID uint32) bool {
	return p(docID)
}

// CascadeFilter enforces the fixed candidate cascade bounds:
// Top-20 RRF -> Roaring ACL -> Top-10 Cross-Encoder input -> Top-5 Hydrated output.
type CascadeFilter struct{}

// NewCascadeFilter constructs a CascadeFilter.
func NewCascadeFilter() *CascadeFilter {
	return &CascadeFilter{}
}

// FilterForRerank enforces the Top-20 RRF bound, applies the ACL evaluator (fail-closed if nil),
// and returns all ACL-permitted RRF candidates along with the Top-10 slice for reranking.
func (f *CascadeFilter) FilterForRerank(
	rrfCandidates []entity.FusedCandidate,
	acl ACLEvaluator,
) (permittedAll []entity.FusedCandidate, rerankInput []entity.FusedCandidate) {
	if acl == nil || len(rrfCandidates) == 0 {
		return nil, nil
	}

	limit := len(rrfCandidates)
	if limit > MaxRRFOutputs {
		limit = MaxRRFOutputs
	}

	permittedAll = make([]entity.FusedCandidate, 0, limit)
	for i := 0; i < limit; i++ {
		if acl.IsPermitted(rrfCandidates[i].DocID) {
			permittedAll = append(permittedAll, rrfCandidates[i])
		}
	}

	if len(permittedAll) == 0 {
		return nil, nil
	}

	rerankCap := len(permittedAll)
	if rerankCap > MaxRerankInputs {
		rerankCap = MaxRerankInputs
	}
	rerankInput = permittedAll[:rerankCap]

	return permittedAll, rerankInput
}

// SelectTopKWithThreshold selects up to topK (clamped to [1..5]) candidates passing
// RerankLogit >= tau. If fewer than topK candidates pass, it sets fallbackUsed=true and
// backfills remaining slots with deduplicated ACL-permitted RRF candidates.
func (f *CascadeFilter) SelectTopKWithThreshold(
	reranked []entity.FusedCandidate,
	aclPermittedRRF []entity.FusedCandidate,
	acl ACLEvaluator,
	topK int,
	tau float64,
) (selected []entity.FusedCandidate, fallbackUsed bool) {
	if acl == nil {
		return nil, false
	}

	if topK <= 0 || topK > MaxHydratedOutputs {
		topK = MaxHydratedOutputs
	}

	passing := make([]entity.FusedCandidate, 0, len(reranked))
	for _, c := range reranked {
		if !acl.IsPermitted(c.DocID) {
			continue
		}
		if c.RerankLogit >= tau {
			c.FinalScore = c.RerankLogit
			passing = append(passing, c)
		}
	}

	sort.Slice(passing, func(i, j int) bool {
		if passing[i].RerankLogit != passing[j].RerankLogit {
			return passing[i].RerankLogit > passing[j].RerankLogit
		}
		if passing[i].RRFScore != passing[j].RRFScore {
			return passing[i].RRFScore > passing[j].RRFScore
		}
		if passing[i].DocID != passing[j].DocID {
			return passing[i].DocID < passing[j].DocID
		}
		return passing[i].ChunkID < passing[j].ChunkID
	})

	selected = make([]entity.FusedCandidate, 0, topK)
	seen := make(map[candidateKey]struct{}, topK)

	for _, c := range passing {
		if len(selected) >= topK {
			break
		}
		key := candidateKey{docID: c.DocID, chunkID: c.ChunkID}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		selected = append(selected, c)
	}

	if len(selected) < topK {
		fallbackUsed = true
		for _, c := range aclPermittedRRF {
			if len(selected) >= topK {
				break
			}
			if !acl.IsPermitted(c.DocID) {
				continue
			}
			key := candidateKey{docID: c.DocID, chunkID: c.ChunkID}
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			c.FinalScore = c.RRFScore
			selected = append(selected, c)
		}
	}

	return selected, fallbackUsed
}
