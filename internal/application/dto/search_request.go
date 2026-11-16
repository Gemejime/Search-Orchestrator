package dto

// SearchRequest represents a single tenant-scoped retrieval request at the application boundary.
type SearchRequest struct {
	TraceID               string
	RequestID             string
	TenantID              string
	QueryText             string
	CallerACLBitmap       []byte
	TopK                  int
	EmbeddingModelVersion string
}

// DiagnosticSearchRequest represents a single tenant-scoped diagnostic retrieval request.
type DiagnosticSearchRequest struct {
	TraceID               string
	RequestID             string
	TenantID              string
	QueryText             string
	CallerACLBitmap       []byte
	TopK                  int
	EmbeddingModelVersion string
}
