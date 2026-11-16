package entity

// FusedCandidate represents a candidate after Reciprocal Rank Fusion and optional Cross-Encoder reranking.
type FusedCandidate struct {
	DocID       uint32
	ChunkID     string
	ParentKey   string
	RRFScore    float64
	RerankLogit float64
	FinalScore  float64
	DenseRank   int
	SparseRank  int
	Metadata    map[string]string
}

// CandidateLogit pairs a candidate's (DocID, ChunkID) identity with its raw Cross-Encoder logit.
type CandidateLogit struct {
	DocID   uint32
	ChunkID string
	Logit   float64
}

// ScoreTrace captures per-stage candidate counts, logits, and timing breakdowns for diagnostic search.
type ScoreTrace struct {
	DenseCount            int32
	SparseCount           int32
	RRFCount              int32
	ACLPermittedCount     int32
	CircuitBreakerTripped bool
	RerankLogits          []CandidateLogit
	StageTimingsMs        map[string]float64
}

// EnrichedChunk represents a hydrated parent container chunk returned to the caller.
type EnrichedChunk struct {
	DocID     uint32
	ChunkID   string
	ParentKey string
	Text      string
	Score     float64
	Metadata  map[string]string
}
