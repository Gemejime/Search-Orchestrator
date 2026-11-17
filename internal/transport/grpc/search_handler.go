package grpc

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	searchv1 "github.com/rag-platform/rag-search-orchestrator/api/proto/v1"
	"github.com/rag-platform/rag-search-orchestrator/internal/application/dto"
	"github.com/rag-platform/rag-search-orchestrator/internal/application/usecase"
	"github.com/rag-platform/rag-search-orchestrator/internal/domain/acl"
	"github.com/rag-platform/rag-search-orchestrator/internal/domain/entity"
	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/telemetry"
	"github.com/rag-platform/rag-search-orchestrator/internal/transport/grpc/interceptor"
)

const (
	maxTenantIDBytes   = 128
	maxQueryTextBytes  = 8192
	maxACLBitmapBytes  = 4 << 20 // 4 MiB
	maxModelVersionLen = 128

	// MaxRecvMsgSizeBytes bounds incoming gRPC SearchRequest payloads so a max-size
	// caller_acl_bitmap plus bounded metadata fields and protobuf overhead can reach the handler.
	MaxRecvMsgSizeBytes = maxACLBitmapBytes + maxQueryTextBytes + maxTenantIDBytes + maxModelVersionLen + (64 << 10)
)

// SearchUseCase defines the primary search execution contract for the gRPC handler.
type SearchUseCase interface {
	Execute(ctx context.Context, req dto.SearchRequest) (dto.SearchResponse, error)
}

// DiagnosticSearchUseCase defines the diagnostic search execution contract for the gRPC handler.
type DiagnosticSearchUseCase interface {
	Execute(ctx context.Context, req dto.DiagnosticSearchRequest) (dto.DiagnosticSearchResponse, error)
}

// SearchHandler implements searchv1.SearchServiceServer.
type SearchHandler struct {
	searchv1.UnimplementedSearchServiceServer
	search     SearchUseCase
	diagnostic DiagnosticSearchUseCase
	metrics    *telemetry.PrometheusMetrics
}

// NewSearchHandler constructs a SearchHandler.
func NewSearchHandler(
	search SearchUseCase,
	diagnostic DiagnosticSearchUseCase,
	metrics *telemetry.PrometheusMetrics,
) *SearchHandler {
	return &SearchHandler{
		search:     search,
		diagnostic: diagnostic,
		metrics:    metrics,
	}
}

// Search executes the staged retrieval cascade and returns up to Top-5 hydrated parent chunks.
func (h *SearchHandler) Search(ctx context.Context, req *searchv1.SearchRequest) (*searchv1.SearchResponse, error) {
	start := time.Now()
	if req == nil {
		h.metrics.RecordRequest("", codes.InvalidArgument.String(), false, time.Since(start))
		return nil, status.Error(codes.InvalidArgument, "request must not be nil")
	}

	appReq, err := validateAndMapRequest(
		ctx,
		req.GetRequestId(),
		req.GetTenantId(),
		req.GetQueryText(),
		req.GetCallerAclBitmap(),
		int(req.GetTopK()),
		req.GetEmbeddingModelVersion(),
	)
	if err != nil {
		h.metrics.RecordRequest("", codes.InvalidArgument.String(), false, time.Since(start))
		return nil, err
	}

	resp, execErr := h.search.Execute(ctx, appReq)
	if execErr != nil {
		grpcErr := mapDomainError(execErr)
		h.metrics.RecordRequest(appReq.TenantID, status.Code(grpcErr).String(), false, time.Since(start))
		return nil, grpcErr
	}

	h.metrics.RecordRequest(resp.TenantID, codes.OK.String(), resp.CacheHit, time.Since(start))
	return toProtoSearchResponse(resp), nil
}

// DiagnosticSearch executes the retrieval cascade and returns per-stage score and timing waterfalls.
func (h *SearchHandler) DiagnosticSearch(
	ctx context.Context,
	req *searchv1.DiagnosticSearchRequest,
) (*searchv1.DiagnosticSearchResponse, error) {
	start := time.Now()
	if req == nil {
		h.metrics.RecordRequest("", codes.InvalidArgument.String(), false, time.Since(start))
		return nil, status.Error(codes.InvalidArgument, "request must not be nil")
	}

	appReq, err := validateAndMapRequest(
		ctx,
		req.GetRequestId(),
		req.GetTenantId(),
		req.GetQueryText(),
		req.GetCallerAclBitmap(),
		int(req.GetTopK()),
		req.GetEmbeddingModelVersion(),
	)
	if err != nil {
		h.metrics.RecordRequest("", codes.InvalidArgument.String(), false, time.Since(start))
		return nil, err
	}

	resp, execErr := h.diagnostic.Execute(ctx, dto.DiagnosticSearchRequest(appReq))
	if execErr != nil {
		grpcErr := mapDomainError(execErr)
		h.metrics.RecordRequest(appReq.TenantID, status.Code(grpcErr).String(), false, time.Since(start))
		return nil, grpcErr
	}

	h.metrics.RecordRequest(resp.Response.TenantID, codes.OK.String(), resp.Response.CacheHit, time.Since(start))

	logits := make([]*searchv1.CandidateLogit, len(resp.ScoreTrace.RerankLogits))
	for i, l := range resp.ScoreTrace.RerankLogits {
		logits[i] = &searchv1.CandidateLogit{
			DocId:   l.DocID,
			ChunkId: l.ChunkID,
			Logit:   l.Logit,
		}
	}

	return &searchv1.DiagnosticSearchResponse{
		Response: toProtoSearchResponse(resp.Response),
		ScoreTrace: &searchv1.ScoreTrace{
			DenseCount:            resp.ScoreTrace.DenseCount,
			SparseCount:           resp.ScoreTrace.SparseCount,
			RrfCount:              resp.ScoreTrace.RRFCount,
			AclPermittedCount:     resp.ScoreTrace.ACLPermittedCount,
			CircuitBreakerTripped: resp.ScoreTrace.CircuitBreakerTripped,
			RerankLogits:          logits,
			StageTimingsMs:        resp.ScoreTrace.StageTimingsMs,
		},
	}, nil
}

func validateAndMapRequest(
	ctx context.Context,
	requestID string,
	tenantID string,
	queryText string,
	aclBitmap []byte,
	topK int,
	modelVersion string,
) (dto.SearchRequest, error) {
	traceID := interceptor.TraceIDFromContext(ctx)
	if traceID == "" {
		traceID = interceptor.NewCorrelationID()
	}

	reqID := strings.TrimSpace(requestID)
	if reqID == "" {
		reqID = interceptor.RequestIDFromContext(ctx)
	}
	if reqID == "" {
		reqID = interceptor.NewCorrelationID()
	}

	tid := strings.TrimSpace(tenantID)
	if tid == "" || len(tid) > maxTenantIDBytes {
		return dto.SearchRequest{}, status.Error(codes.InvalidArgument, "invalid tenant_id")
	}
	if strings.TrimSpace(queryText) == "" || len(queryText) > maxQueryTextBytes {
		return dto.SearchRequest{}, status.Error(codes.InvalidArgument, "invalid query_text")
	}
	if strings.TrimSpace(modelVersion) == "" || len(modelVersion) > maxModelVersionLen {
		return dto.SearchRequest{}, status.Error(codes.InvalidArgument, "invalid embedding_model_version")
	}
	if topK < 0 || topK > entity.MaxTopK {
		return dto.SearchRequest{}, status.Error(codes.InvalidArgument, "top_k must be between 0 and "+strconv.Itoa(entity.MaxTopK))
	}
	if len(aclBitmap) == 0 || len(aclBitmap) > maxACLBitmapBytes {
		return dto.SearchRequest{}, status.Error(codes.InvalidArgument, "invalid caller_acl_bitmap")
	}

	return dto.SearchRequest{
		TraceID:               traceID,
		RequestID:             reqID,
		TenantID:              tid,
		QueryText:             queryText,
		CallerACLBitmap:       aclBitmap,
		TopK:                  topK,
		EmbeddingModelVersion: modelVersion,
	}, nil
}

func mapDomainError(err error) error {
	switch {
	case errors.Is(err, entity.ErrEmptyTenantID),
		errors.Is(err, entity.ErrEmptyQueryText),
		errors.Is(err, entity.ErrEmptyEmbeddingModelVersion),
		errors.Is(err, entity.ErrInvalidTopK),
		errors.Is(err, acl.ErrEmptyACLBitmap),
		errors.Is(err, acl.ErrInvalidACLBitmap):
		return status.Error(codes.InvalidArgument, "invalid search request parameters")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "retrieval deadline exceeded")
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "request canceled")
	case errors.Is(err, usecase.ErrBothModalitiesFailed):
		return status.Error(codes.Unavailable, "retrieval backends temporarily unavailable")
	default:
		return status.Error(codes.Internal, "internal retrieval error")
	}
}

func toProtoSearchResponse(resp dto.SearchResponse) *searchv1.SearchResponse {
	chunks := make([]*searchv1.EnrichedChunk, len(resp.Chunks))
	for i, ch := range resp.Chunks {
		chunks[i] = &searchv1.EnrichedChunk{
			DocId:     ch.DocID,
			ChunkId:   ch.ChunkID,
			ParentKey: ch.ParentKey,
			Text:      ch.Text,
			Score:     ch.Score,
			Metadata:  ch.Metadata,
		}
	}
	return &searchv1.SearchResponse{
		RequestId:        resp.RequestID,
		TenantId:         resp.TenantID,
		CacheHit:         resp.CacheHit,
		RerankerFallback: resp.RerankerFallback,
		Chunks:           chunks,
		LatencyMs:        resp.LatencyMs,
	}
}
