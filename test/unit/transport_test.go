package unit

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	searchv1 "github.com/rag-platform/rag-search-orchestrator/api/proto/v1"
	"github.com/rag-platform/rag-search-orchestrator/internal/application/dto"
	"github.com/rag-platform/rag-search-orchestrator/internal/application/usecase"
	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/telemetry"
	grpctransport "github.com/rag-platform/rag-search-orchestrator/internal/transport/grpc"
	"github.com/rag-platform/rag-search-orchestrator/internal/transport/grpc/interceptor"
	httptransport "github.com/rag-platform/rag-search-orchestrator/internal/transport/http"
)

type stubSearchUC struct {
	resp  dto.SearchResponse
	err   error
	panic bool
}

func (s stubSearchUC) Execute(_ context.Context, req dto.SearchRequest) (dto.SearchResponse, error) {
	if s.panic {
		panic("simulated panic with /internal/secret/path")
	}
	if s.err != nil {
		return dto.SearchResponse{}, s.err
	}
	out := s.resp
	out.RequestID = req.RequestID
	out.TenantID = req.TenantID
	return out, nil
}

type stubDiagnosticUC struct {
	resp dto.DiagnosticSearchResponse
	err  error
}

func (s stubDiagnosticUC) Execute(_ context.Context, req dto.DiagnosticSearchRequest) (dto.DiagnosticSearchResponse, error) {
	if s.err != nil {
		return dto.DiagnosticSearchResponse{}, s.err
	}
	out := s.resp
	out.Response.RequestID = req.RequestID
	out.Response.TenantID = req.TenantID
	return out, nil
}

func mustValidBitmapB64(t *testing.T, ids ...uint32) ([]byte, string) {
	t.Helper()
	bm := roaring.BitmapOf(ids...)
	raw, err := bm.ToBytes()
	if err != nil {
		t.Fatalf("roaring ToBytes: %v", err)
	}
	return raw, base64.StdEncoding.EncodeToString(raw)
}

func TestHTTPTransport_EndpointsValidationAndRedaction(t *testing.T) {
	t.Parallel()

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, nil))
	metrics := telemetry.NewPrometheusMetrics()

	_, bitmapB64 := mustValidBitmapB64(t, 10, 20)

	ctrl := httptransport.NewSearchController(
		stubSearchUC{
			resp: dto.SearchResponse{
				CacheHit: true,
				Chunks: []dto.EnrichedChunk{
					{DocID: 10, ChunkID: "c1", ParentKey: "parent:t1:10:c1", Text: "ok", Score: 0.9},
				},
				LatencyMs: 2.5,
			},
		},
		stubDiagnosticUC{
			resp: dto.DiagnosticSearchResponse{
				Response: dto.SearchResponse{
					Chunks:    []dto.EnrichedChunk{},
					LatencyMs: 3.1,
				},
				ScoreTrace: dto.ScoreTrace{
					DenseCount:        40,
					SparseCount:       40,
					RRFCount:          20,
					ACLPermittedCount: 1,
				},
			},
		},
		metrics,
		logger,
	)

	var draining atomic.Bool
	var redisErr atomic.Bool
	router := httptransport.NewRouter(httptransport.RouterConfig{
		Controller: ctrl,
		Metrics:    metrics,
		Draining:   &draining,
		Checks: map[string]httptransport.ReadinessCheck{
			"redis": func(context.Context) error {
				if redisErr.Load() {
					return errors.New("redis down")
				}
				return nil
			},
		},
	})

	// 1. Valid POST /v1/search with sensitive Authorization header & query PII.
	secretToken := "super-secret-jwt-token-xyz"
	sensitiveQuery := "CONFIDENTIAL_SSN_123_45_6789"
	body := `{"tenant_id":"tenant-1","query_text":"` + sensitiveQuery + `","caller_acl_bitmap_b64":"` + bitmapB64 + `","top_k":5,"embedding_model_version":"bge-m3-fp16-v2"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/search", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+secretToken)
	req.Header.Set("X-Request-ID", "req-http-1")
	req.Header.Set("X-Trace-ID", "trace-http-1")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rec.Code, rec.Body.String())
	}

	// Verify secured logging redaction: neither token, bitmap, nor raw query text appears in logs.
	logs := logBuf.String()
	if strings.Contains(logs, secretToken) || strings.Contains(logs, bitmapB64) || strings.Contains(logs, sensitiveQuery) {
		t.Fatalf("secured logging violation: sensitive data leaked into logs: %s", logs)
	}
	if !strings.Contains(logs, "trace-http-1") || !strings.Contains(logs, "req-http-1") {
		t.Fatalf("expected trace_id and request_id in logs: %s", logs)
	}

	// 2. Invalid POST /v1/search (top_k > 5).
	badReq := httptest.NewRequest(http.MethodPost, "/v1/search", strings.NewReader(`{"tenant_id":"tenant-1","query_text":"hi","caller_acl_bitmap_b64":"`+bitmapB64+`","top_k":9,"embedding_model_version":"v1"}`))
	badRec := httptest.NewRecorder()
	router.ServeHTTP(badRec, badReq)
	if badRec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request, got %d", badRec.Code)
	}

	// 3. POST /v1/search/diagnostic.
	diagReq := httptest.NewRequest(http.MethodPost, "/v1/search/diagnostic", strings.NewReader(body))
	diagRec := httptest.NewRecorder()
	router.ServeHTTP(diagRec, diagReq)
	if diagRec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for diagnostic, got %d", diagRec.Code)
	}
	var diagOut map[string]any
	if err := json.Unmarshal(diagRec.Body.Bytes(), &diagOut); err != nil {
		t.Fatalf("unmarshal diagnostic response: %v", err)
	}
	if _, ok := diagOut["score_trace"]; !ok {
		t.Fatalf("expected score_trace in diagnostic response")
	}

	// 4. /healthz, /ready, draining, /openapi.yaml, /docs, /metrics.
	hRec := httptest.NewRecorder()
	router.ServeHTTP(hRec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if hRec.Code != http.StatusOK {
		t.Fatalf("/healthz expected 200, got %d", hRec.Code)
	}

	rRec := httptest.NewRecorder()
	router.ServeHTTP(rRec, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if rRec.Code != http.StatusOK {
		t.Fatalf("/ready expected 200, got %d", rRec.Code)
	}

	redisErr.Store(true)
	rFailRec := httptest.NewRecorder()
	router.ServeHTTP(rFailRec, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if rFailRec.Code != http.StatusServiceUnavailable {
		t.Fatalf("/ready expected 503 when redis down, got %d", rFailRec.Code)
	}

	draining.Store(true)
	drainRec := httptest.NewRecorder()
	router.ServeHTTP(drainRec, httptest.NewRequest(http.MethodGet, "/ready", nil))
	if drainRec.Code != http.StatusServiceUnavailable || !strings.Contains(drainRec.Body.String(), `"draining"`) {
		t.Fatalf("/ready expected 503 draining, got %d: %s", drainRec.Code, drainRec.Body.String())
	}

	specRec := httptest.NewRecorder()
	router.ServeHTTP(specRec, httptest.NewRequest(http.MethodGet, "/openapi.yaml", nil))
	if specRec.Code != http.StatusOK || !strings.Contains(specRec.Body.String(), "openapi: 3.1.0") {
		t.Fatalf("/openapi.yaml unexpected response: %d", specRec.Code)
	}

	docsRec := httptest.NewRecorder()
	router.ServeHTTP(docsRec, httptest.NewRequest(http.MethodGet, "/docs", nil))
	if docsRec.Code != http.StatusOK || !strings.Contains(docsRec.Body.String(), "swagger-ui") {
		t.Fatalf("/docs unexpected response: %d", docsRec.Code)
	}

	mRec := httptest.NewRecorder()
	router.ServeHTTP(mRec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if mRec.Code != http.StatusOK || !strings.Contains(mRec.Body.String(), "search_orchestrator_requests_total") {
		t.Fatalf("/metrics missing expected metric: %d", mRec.Code)
	}
}

func TestHTTPTransport_SanitizedErrorResponses(t *testing.T) {
	t.Parallel()

	metrics := telemetry.NewPrometheusMetrics()
	_, bitmapB64 := mustValidBitmapB64(t, 1)
	body := `{"tenant_id":"t1","query_text":"hello","caller_acl_bitmap_b64":"` + bitmapB64 + `","top_k":5,"embedding_model_version":"v1"}`

	ctrl := httptransport.NewSearchController(
		stubSearchUC{err: errors.New("dial tcp 10.244.3.19:6379: /var/run/secrets/redis")},
		stubDiagnosticUC{},
		metrics,
		slog.Default(),
	)
	router := httptransport.NewRouter(httptransport.RouterConfig{Controller: ctrl, Metrics: metrics})

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/search", strings.NewReader(body)))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "10.244.3.19") || strings.Contains(rec.Body.String(), "/var/run") {
		t.Fatalf("internal error leaked host/path to client: %s", rec.Body.String())
	}

	ctrlUnavail := httptransport.NewSearchController(
		stubSearchUC{err: usecase.ErrBothModalitiesFailed},
		stubDiagnosticUC{},
		metrics,
		slog.Default(),
	)
	routerUnavail := httptransport.NewRouter(httptransport.RouterConfig{Controller: ctrlUnavail, Metrics: metrics})
	recUnavail := httptest.NewRecorder()
	routerUnavail.ServeHTTP(recUnavail, httptest.NewRequest(http.MethodPost, "/v1/search", strings.NewReader(body)))
	if recUnavail.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", recUnavail.Code)
	}
}

func TestGRPCTransport_SearchDiagnosticValidationAndPanicRecovery(t *testing.T) {
	t.Parallel()

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, nil))
	metrics := telemetry.NewPrometheusMetrics()

	rawBitmap, _ := mustValidBitmapB64(t, 10, 20)

	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer(grpc.UnaryInterceptor(interceptor.UnaryServerInterceptor(logger)))
	searchv1.RegisterSearchServiceServer(srv, grpctransport.NewSearchHandler(
		stubSearchUC{
			resp: dto.SearchResponse{
				Chunks: []dto.EnrichedChunk{
					{DocID: 10, ChunkID: "c1", ParentKey: "parent:t1:10:c1", Text: "hello", Score: 0.95},
				},
			},
		},
		stubDiagnosticUC{
			resp: dto.DiagnosticSearchResponse{
				Response: dto.SearchResponse{Chunks: []dto.EnrichedChunk{}},
				ScoreTrace: dto.ScoreTrace{
					DenseCount:        40,
					SparseCount:       40,
					RRFCount:          20,
					ACLPermittedCount: 1,
				},
			},
		},
		metrics,
	))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() {
		srv.Stop()
		_ = lis.Close()
	})

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	client := searchv1.NewSearchServiceClient(conn)
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("x-trace-id", "trace-grpc-1"))

	resp, err := client.Search(ctx, &searchv1.SearchRequest{
		TenantId:              "tenant-1",
		QueryText:             "CONFIDENTIAL_QUERY_PII",
		CallerAclBitmap:       rawBitmap,
		TopK:                  5,
		EmbeddingModelVersion: "bge-m3-fp16-v2",
	})
	if err != nil {
		t.Fatalf("gRPC Search failed: %v", err)
	}
	if len(resp.GetChunks()) != 1 || resp.GetRequestId() == "" {
		t.Fatalf("unexpected gRPC response: %+v", resp)
	}

	if strings.Contains(logBuf.String(), "CONFIDENTIAL_QUERY_PII") {
		t.Fatalf("query PII leaked into gRPC logs: %s", logBuf.String())
	}

	// Validation error -> InvalidArgument.
	_, err = client.Search(ctx, &searchv1.SearchRequest{TenantId: "", QueryText: "x"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", status.Code(err))
	}

	// DiagnosticSearch.
	diagResp, err := client.DiagnosticSearch(ctx, &searchv1.DiagnosticSearchRequest{
		TenantId:              "tenant-1",
		QueryText:             "soc2",
		CallerAclBitmap:       rawBitmap,
		TopK:                  5,
		EmbeddingModelVersion: "bge-m3-fp16-v2",
	})
	if err != nil || diagResp.GetScoreTrace().GetRrfCount() != 20 {
		t.Fatalf("DiagnosticSearch unexpected result: resp=%+v err=%v", diagResp, err)
	}

	// Panic recovery interceptor test.
	panicLis := bufconn.Listen(1024 * 1024)
	panicSrv := grpc.NewServer(grpc.UnaryInterceptor(interceptor.UnaryServerInterceptor(logger)))
	searchv1.RegisterSearchServiceServer(panicSrv, grpctransport.NewSearchHandler(
		stubSearchUC{panic: true},
		stubDiagnosticUC{},
		metrics,
	))
	go func() { _ = panicSrv.Serve(panicLis) }()
	t.Cleanup(func() {
		panicSrv.Stop()
		_ = panicLis.Close()
	})
	pConn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return panicLis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient panic: %v", err)
	}
	t.Cleanup(func() { _ = pConn.Close() })

	_, err = searchv1.NewSearchServiceClient(pConn).Search(ctx, &searchv1.SearchRequest{
		TenantId:              "tenant-1",
		QueryText:             "hello",
		CallerAclBitmap:       rawBitmap,
		TopK:                  5,
		EmbeddingModelVersion: "v1",
	})
	if status.Code(err) != codes.Internal || strings.Contains(err.Error(), "/internal/secret/path") {
		t.Fatalf("expected sanitized Internal error on panic, got %v", err)
	}
}
