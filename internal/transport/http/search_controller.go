package http

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rag-platform/rag-search-orchestrator/internal/application/dto"
	"github.com/rag-platform/rag-search-orchestrator/internal/application/usecase"
	"github.com/rag-platform/rag-search-orchestrator/internal/domain/acl"
	"github.com/rag-platform/rag-search-orchestrator/internal/domain/entity"
	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/telemetry"
	"github.com/rag-platform/rag-search-orchestrator/internal/transport/grpc/interceptor"
)

const (
	maxTenantIDBytes    = 128
	maxQueryTextBytes   = 8192
	maxACLBitmapBytes   = 4 << 20 // 4 MiB
	maxModelVersionLen  = 128
	maxRequestBodyBytes = 6 << 20 // 6 MiB (accommodates 4 MiB base64-encoded ACL bitmap + bounded JSON fields)
)

// SearchUseCase defines the primary search execution contract for the HTTP controller.
type SearchUseCase interface {
	Execute(ctx context.Context, req dto.SearchRequest) (dto.SearchResponse, error)
}

// DiagnosticSearchUseCase defines the diagnostic search execution contract for the HTTP controller.
type DiagnosticSearchUseCase interface {
	Execute(ctx context.Context, req dto.DiagnosticSearchRequest) (dto.DiagnosticSearchResponse, error)
}

type httpSearchRequest struct {
	RequestID             string `json:"request_id"`
	TenantID              string `json:"tenant_id"`
	QueryText             string `json:"query_text"`
	CallerACLBitmapB64    string `json:"caller_acl_bitmap_b64"`
	TopK                  int    `json:"top_k"`
	EmbeddingModelVersion string `json:"embedding_model_version"`
}

type httpEnrichedChunk struct {
	DocID     uint32            `json:"doc_id"`
	ChunkID   string            `json:"chunk_id"`
	ParentKey string            `json:"parent_key"`
	Text      string            `json:"text"`
	Score     float64           `json:"score"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

type httpSearchResponse struct {
	RequestID        string              `json:"request_id"`
	TenantID         string              `json:"tenant_id"`
	CacheHit         bool                `json:"cache_hit"`
	RerankerFallback bool                `json:"reranker_fallback"`
	Chunks           []httpEnrichedChunk `json:"chunks"`
	LatencyMs        float64             `json:"latency_ms"`
}

type httpCandidateLogit struct {
	DocID   uint32  `json:"doc_id"`
	ChunkID string  `json:"chunk_id"`
	Logit   float64 `json:"logit"`
}

type httpScoreTrace struct {
	DenseCount            int32                `json:"dense_count"`
	SparseCount           int32                `json:"sparse_count"`
	RRFCount              int32                `json:"rrf_count"`
	ACLPermittedCount     int32                `json:"acl_permitted_count"`
	CircuitBreakerTripped bool                 `json:"circuit_breaker_tripped"`
	RerankLogits          []httpCandidateLogit `json:"rerank_logits,omitempty"`
	StageTimingsMs        map[string]float64   `json:"stage_timings_ms,omitempty"`
}

type httpDiagnosticSearchResponse struct {
	httpSearchResponse
	ScoreTrace httpScoreTrace `json:"score_trace"`
}

type httpErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

// SearchController handles REST retrieval requests aligned with OpenAPI 3.1.
type SearchController struct {
	search     SearchUseCase
	diagnostic DiagnosticSearchUseCase
	metrics    *telemetry.PrometheusMetrics
	logger     *slog.Logger
}

// NewSearchController constructs a SearchController.
func NewSearchController(
	search SearchUseCase,
	diagnostic DiagnosticSearchUseCase,
	metrics *telemetry.PrometheusMetrics,
	logger *slog.Logger,
) *SearchController {
	if logger == nil {
		logger = slog.Default()
	}
	return &SearchController{
		search:     search,
		diagnostic: diagnostic,
		metrics:    metrics,
		logger:     logger,
	}
}

// HandleSearch handles POST /v1/search.
func (c *SearchController) HandleSearch(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	req, traceID, errMsg := parseHTTPSearchRequest(r)
	if errMsg != "" {
		c.recordAndRespondError(r.Context(), w, traceID, req.RequestID, req.TenantID, http.StatusBadRequest, "BAD_REQUEST", errMsg, start)
		return
	}

	resp, err := c.search.Execute(r.Context(), req)
	if err != nil {
		statusCode, errCode, msg := mapUseCaseError(err)
		c.recordAndRespondError(r.Context(), w, traceID, req.RequestID, req.TenantID, statusCode, errCode, msg, start)
		return
	}

	dur := time.Since(start)
	c.metrics.RecordRequest(resp.TenantID, strconv.Itoa(http.StatusOK), resp.CacheHit, dur)
	c.logger.InfoContext(r.Context(), "http search completed",
		slog.String("trace_id", traceID),
		slog.String("request_id", resp.RequestID),
		slog.String("tenant_id", resp.TenantID),
		slog.Bool("cache_hit", resp.CacheHit),
		slog.Int("status_code", http.StatusOK),
		slog.Float64("duration_ms", float64(dur.Microseconds())/1000.0),
	)

	writeJSON(w, http.StatusOK, toHTTPSearchResponse(resp))
}

// HandleDiagnosticSearch handles POST /v1/search/diagnostic.
func (c *SearchController) HandleDiagnosticSearch(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	req, traceID, errMsg := parseHTTPSearchRequest(r)
	if errMsg != "" {
		c.recordAndRespondError(r.Context(), w, traceID, req.RequestID, req.TenantID, http.StatusBadRequest, "BAD_REQUEST", errMsg, start)
		return
	}

	resp, err := c.diagnostic.Execute(r.Context(), dto.DiagnosticSearchRequest(req))
	if err != nil {
		statusCode, errCode, msg := mapUseCaseError(err)
		c.recordAndRespondError(r.Context(), w, traceID, req.RequestID, req.TenantID, statusCode, errCode, msg, start)
		return
	}

	dur := time.Since(start)
	c.metrics.RecordRequest(resp.Response.TenantID, strconv.Itoa(http.StatusOK), resp.Response.CacheHit, dur)
	c.logger.InfoContext(r.Context(), "http diagnostic search completed",
		slog.String("trace_id", traceID),
		slog.String("request_id", resp.Response.RequestID),
		slog.String("tenant_id", resp.Response.TenantID),
		slog.Bool("cache_hit", resp.Response.CacheHit),
		slog.Int("status_code", http.StatusOK),
		slog.Float64("duration_ms", float64(dur.Microseconds())/1000.0),
	)

	logits := make([]httpCandidateLogit, len(resp.ScoreTrace.RerankLogits))
	for i, l := range resp.ScoreTrace.RerankLogits {
		logits[i] = httpCandidateLogit{
			DocID:   l.DocID,
			ChunkID: l.ChunkID,
			Logit:   l.Logit,
		}
	}

	writeJSON(w, http.StatusOK, httpDiagnosticSearchResponse{
		httpSearchResponse: toHTTPSearchResponse(resp.Response),
		ScoreTrace: httpScoreTrace{
			DenseCount:            resp.ScoreTrace.DenseCount,
			SparseCount:           resp.ScoreTrace.SparseCount,
			RRFCount:              resp.ScoreTrace.RRFCount,
			ACLPermittedCount:     resp.ScoreTrace.ACLPermittedCount,
			CircuitBreakerTripped: resp.ScoreTrace.CircuitBreakerTripped,
			RerankLogits:          logits,
			StageTimingsMs:        resp.ScoreTrace.StageTimingsMs,
		},
	})
}

func (c *SearchController) recordAndRespondError(
	ctx context.Context,
	w http.ResponseWriter,
	traceID string,
	requestID string,
	tenantID string,
	statusCode int,
	errCode string,
	msg string,
	start time.Time,
) {
	dur := time.Since(start)
	c.metrics.RecordRequest(tenantID, strconv.Itoa(statusCode), false, dur)
	c.logger.WarnContext(ctx, "http search failed",
		slog.String("trace_id", traceID),
		slog.String("request_id", requestID),
		slog.String("tenant_id", tenantID),
		slog.Int("status_code", statusCode),
		slog.String("error_code", errCode),
		slog.Float64("duration_ms", float64(dur.Microseconds())/1000.0),
	)
	writeJSON(w, statusCode, httpErrorResponse{
		Error:   errCode,
		Message: msg,
	})
}

func parseHTTPSearchRequest(r *http.Request) (dto.SearchRequest, string, string) {
	traceID := strings.TrimSpace(r.Header.Get("X-Trace-ID"))
	if traceID == "" {
		traceID = interceptor.NewCorrelationID()
	}

	headerReqID := strings.TrimSpace(r.Header.Get("X-Request-ID"))
	headerTenantID := strings.TrimSpace(r.Header.Get("X-Tenant-ID"))

	limited := io.LimitReader(r.Body, maxRequestBodyBytes+1)
	bodyBytes, err := io.ReadAll(limited)
	if err != nil {
		return dto.SearchRequest{RequestID: headerReqID}, traceID, "Failed to read request body"
	}
	if len(bodyBytes) > maxRequestBodyBytes {
		return dto.SearchRequest{RequestID: headerReqID}, traceID, "Request body exceeds maximum allowed size"
	}

	var raw httpSearchRequest
	if err := json.Unmarshal(bodyBytes, &raw); err != nil {
		return dto.SearchRequest{RequestID: headerReqID}, traceID, "Malformed JSON request payload"
	}

	reqID := strings.TrimSpace(raw.RequestID)
	if reqID == "" {
		reqID = headerReqID
	}
	if reqID == "" {
		reqID = interceptor.NewCorrelationID()
	}

	tenantID := strings.TrimSpace(raw.TenantID)
	if tenantID == "" {
		tenantID = headerTenantID
	} else if headerTenantID != "" && tenantID != headerTenantID {
		return dto.SearchRequest{RequestID: reqID}, traceID, "X-Tenant-ID header does not match body tenant_id"
	}

	if tenantID == "" || len(tenantID) > maxTenantIDBytes {
		return dto.SearchRequest{RequestID: reqID}, traceID, "Invalid tenant_id"
	}
	if strings.TrimSpace(raw.QueryText) == "" || len(raw.QueryText) > maxQueryTextBytes {
		return dto.SearchRequest{RequestID: reqID, TenantID: tenantID}, traceID, "Invalid query_text"
	}
	if strings.TrimSpace(raw.EmbeddingModelVersion) == "" || len(raw.EmbeddingModelVersion) > maxModelVersionLen {
		return dto.SearchRequest{RequestID: reqID, TenantID: tenantID}, traceID, "Invalid embedding_model_version"
	}
	if raw.TopK < 0 || raw.TopK > entity.MaxTopK {
		return dto.SearchRequest{RequestID: reqID, TenantID: tenantID}, traceID, "top_k must be between 0 and 5"
	}
	if strings.TrimSpace(raw.CallerACLBitmapB64) == "" {
		return dto.SearchRequest{RequestID: reqID, TenantID: tenantID}, traceID, "caller_acl_bitmap_b64 must not be empty"
	}

	aclBytes, err := base64.StdEncoding.DecodeString(raw.CallerACLBitmapB64)
	if err != nil || len(aclBytes) == 0 || len(aclBytes) > maxACLBitmapBytes {
		return dto.SearchRequest{RequestID: reqID, TenantID: tenantID}, traceID, "Malformed caller_acl_bitmap_b64"
	}

	return dto.SearchRequest{
		TraceID:               traceID,
		RequestID:             reqID,
		TenantID:              tenantID,
		QueryText:             raw.QueryText,
		CallerACLBitmap:       aclBytes,
		TopK:                  raw.TopK,
		EmbeddingModelVersion: raw.EmbeddingModelVersion,
	}, traceID, ""
}

func mapUseCaseError(err error) (int, string, string) {
	switch {
	case errors.Is(err, entity.ErrEmptyTenantID),
		errors.Is(err, entity.ErrEmptyQueryText),
		errors.Is(err, entity.ErrEmptyEmbeddingModelVersion),
		errors.Is(err, entity.ErrInvalidTopK),
		errors.Is(err, acl.ErrEmptyACLBitmap),
		errors.Is(err, acl.ErrInvalidACLBitmap):
		return http.StatusBadRequest, "BAD_REQUEST", "Invalid tenant_id, empty query_text, or malformed caller_acl_bitmap"
	case errors.Is(err, usecase.ErrBothModalitiesFailed):
		return http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", "Retrieval backends are temporarily unavailable"
	default:
		return http.StatusInternalServerError, "INTERNAL_ERROR", "Internal retrieval error"
	}
}

func toHTTPSearchResponse(resp dto.SearchResponse) httpSearchResponse {
	chunks := make([]httpEnrichedChunk, len(resp.Chunks))
	for i, ch := range resp.Chunks {
		chunks[i] = httpEnrichedChunk{
			DocID:     ch.DocID,
			ChunkID:   ch.ChunkID,
			ParentKey: ch.ParentKey,
			Text:      ch.Text,
			Score:     ch.Score,
			Metadata:  ch.Metadata,
		}
	}
	return httpSearchResponse{
		RequestID:        resp.RequestID,
		TenantID:         resp.TenantID,
		CacheHit:         resp.CacheHit,
		RerankerFallback: resp.RerankerFallback,
		Chunks:           chunks,
		LatencyMs:        resp.LatencyMs,
	}
}

func writeJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
}
