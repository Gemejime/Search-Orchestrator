package dto

// EnrichedChunk represents a hydrated parent container chunk returned to the caller.
type EnrichedChunk struct {
	DocID     uint32
	ChunkID   string
	ParentKey string
	Text      string
	Score     float64
	Metadata  map[string]string
}

// SearchResponse contains the ranked, ACL-filtered, and hydrated retrieval results.
type SearchResponse struct {
	RequestID        string
	TenantID         string
	CacheHit         bool
	RerankerFallback bool
	Chunks           []EnrichedChunk
	LatencyMs        float64
}

// CandidateLogit pairs a candidate's (DocID, ChunkID) identity with its raw Cross-Encoder logit.
type CandidateLogit struct {
	DocID   uint32
	ChunkID string
	Logit   float64
}

// ScoreTrace captures per-stage counts, reranker logits, and timing waterfalls for diagnostics.
type ScoreTrace struct {
	DenseCount            int32
	SparseCount           int32
	RRFCount              int32
	ACLPermittedCount     int32
	CircuitBreakerTripped bool
	RerankLogits          []CandidateLogit
	StageTimingsMs        map[string]float64
}

// DiagnosticSearchResponse wraps SearchResponse with per-stage diagnostic telemetry.
type DiagnosticSearchResponse struct {
	Response   SearchResponse
	ScoreTrace ScoreTrace
}
