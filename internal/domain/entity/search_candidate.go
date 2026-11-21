package entity

// Modality identifies the retrieval index that produced a candidate.
type Modality uint8

const (
	// ModalityDense indicates a candidate retrieved from the HNSW dense vector index.
	ModalityDense Modality = 1
	// ModalitySparse indicates a candidate retrieved from the BM25 sparse text index.
	ModalitySparse Modality = 2
)

// SearchCandidate represents a raw candidate returned by either dense HNSW or sparse BM25 search.
type SearchCandidate struct {
	DocID     uint32
	ChunkID   string
	ParentKey string
	Score     float64
	// Rank is the 1-based rank within the originating modality (0 implies slice index + 1).
	Rank     int
	Modality Modality
	Metadata map[string]string
}
