//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	goredis "github.com/redis/go-redis/v9"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/rag-platform/rag-search-orchestrator/internal/application/dto"
	"github.com/rag-platform/rag-search-orchestrator/internal/application/usecase"
	"github.com/rag-platform/rag-search-orchestrator/internal/domain/acl"
	rediscache "github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/cache/redis"
	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/grpcutil"
	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/pb/downstreamv1"
	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/pb/tritonv2"
	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/search/dense"
	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/search/sparse"
	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/telemetry"
	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/triton"
	httptransport "github.com/rag-platform/rag-search-orchestrator/internal/transport/http"
	"github.com/rag-platform/rag-search-orchestrator/pkg/pool"
)

type integrationDenseServer struct {
	downstreamv1.UnimplementedDenseSearchServiceServer
}

func (integrationDenseServer) Search(context.Context, *downstreamv1.DenseSearchServiceSearchRequest) (*downstreamv1.DenseSearchServiceSearchResponse, error) {
	cands := make([]*downstreamv1.DownstreamCandidate, 0, 10)
	for i := uint32(1); i <= 10; i++ {
		cands = append(cands, &downstreamv1.DownstreamCandidate{
			DocId:     i,
			ChunkId:   fmt.Sprintf("chk_%d", i),
			ParentKey: fmt.Sprintf("parent:tenant-int:%d:chk_%d", i, i),
			Score:     float64(100 - i),
		})
	}
	return &downstreamv1.DenseSearchServiceSearchResponse{Candidates: cands}, nil
}

type integrationSparseServer struct {
	downstreamv1.UnimplementedSparseSearchServiceServer
}

func (integrationSparseServer) Search(context.Context, *downstreamv1.SparseSearchServiceSearchRequest) (*downstreamv1.SparseSearchServiceSearchResponse, error) {
	cands := make([]*downstreamv1.DownstreamCandidate, 0, 10)
	for i := uint32(1); i <= 10; i++ {
		cands = append(cands, &downstreamv1.DownstreamCandidate{
			DocId:     i,
			ChunkId:   fmt.Sprintf("chk_%d", i),
			ParentKey: fmt.Sprintf("parent:tenant-int:%d:chk_%d", i, i),
			Score:     float64(90 - i),
		})
	}
	return &downstreamv1.SparseSearchServiceSearchResponse{Candidates: cands}, nil
}

type integrationTritonServer struct {
	tritonv2.UnimplementedGRPCInferenceServiceServer
}

func (integrationTritonServer) ModelStreamInfer(stream grpc.BidiStreamingServer[tritonv2.ModelInferRequest, tritonv2.ModelStreamInferResponse]) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return nil
		}
		resp := &tritonv2.ModelInferResponse{
			ModelName: req.GetModelName(),
			Outputs: []*tritonv2.ModelInferResponse_InferOutputTensor{
				{
					Name:     "EMBEDDING",
					Datatype: "FP32",
					Shape:    []int64{1, 4},
					Contents: &tritonv2.InferTensorContents{Fp32Contents: []float32{0.1, 0.2, 0.3, 0.4}},
				},
			},
		}
		if err := stream.Send(&tritonv2.ModelStreamInferResponse{InferResponse: resp}); err != nil {
			return err
		}
	}
}

func (integrationTritonServer) ModelInfer(_ context.Context, req *tritonv2.ModelInferRequest) (*tritonv2.ModelInferResponse, error) {
	n := int(req.GetInputs()[0].GetShape()[0])
	logits := make([]float32, n)
	for i := 0; i < n; i++ {
		logits[i] = float32(10 - i)
	}
	return &tritonv2.ModelInferResponse{
		ModelName: req.GetModelName(),
		Outputs: []*tritonv2.ModelInferResponse_InferOutputTensor{
			{
				Name:     "LOGITS",
				Datatype: "FP32",
				Shape:    []int64{int64(n), 1},
				Contents: &tritonv2.InferTensorContents{Fp32Contents: logits},
			},
		},
	}, nil
}

func startLocalGRPCServer(t *testing.T, register func(*grpc.Server)) string {
	t.Helper()
	var lc net.ListenConfig
	lis, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	register(srv)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() {
		srv.Stop()
		_ = lis.Close()
	})
	return lis.Addr().String()
}

func TestSearchPipeline_WithTestcontainersRedis(t *testing.T) {
	ctx := context.Background()
	redisContainer, err := tcredis.Run(ctx, "redis:7.2-alpine")
	if err != nil {
		t.Skipf("skipping Testcontainers integration test (docker unavailable): %v", err)
	}
	t.Cleanup(func() { _ = redisContainer.Terminate(ctx) })

	connStr, err := redisContainer.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("redis ConnectionString: %v", err)
	}
	opts, err := goredis.ParseURL(connStr)
	if err != nil {
		t.Fatalf("ParseURL: %v", err)
	}
	redisClient := goredis.NewUniversalClient(&goredis.UniversalOptions{
		Addrs: []string{opts.Addr},
	})
	t.Cleanup(func() { _ = redisClient.Close() })

	// Seed parent chunks in Redis.
	for i := 1; i <= 10; i++ {
		key := fmt.Sprintf("parent:tenant-int:%d:chk_%d", i, i)
		val := fmt.Sprintf(`{"text":"parent content for doc %d","metadata":{"doc":"%d"}}`, i, i)
		if err := redisClient.Set(ctx, key, val, time.Minute).Err(); err != nil {
			t.Fatalf("seed redis %s: %v", key, err)
		}
	}

	denseAddr := startLocalGRPCServer(t, func(s *grpc.Server) {
		downstreamv1.RegisterDenseSearchServiceServer(s, integrationDenseServer{})
	})
	sparseAddr := startLocalGRPCServer(t, func(s *grpc.Server) {
		downstreamv1.RegisterSparseSearchServiceServer(s, integrationSparseServer{})
	})
	tritonAddr := startLocalGRPCServer(t, func(s *grpc.Server) {
		tritonv2.RegisterGRPCInferenceServiceServer(s, integrationTritonServer{})
	})

	dialOpts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	densePool, _ := grpcutil.NewConnPool(grpcutil.Config{Targets: []string{denseAddr}, PoolSize: 1, DialOptions: dialOpts})
	sparsePool, _ := grpcutil.NewConnPool(grpcutil.Config{Targets: []string{sparseAddr}, PoolSize: 1, DialOptions: dialOpts})
	tritonPool, _ := grpcutil.NewConnPool(grpcutil.Config{Targets: []string{tritonAddr}, PoolSize: 1, DialOptions: dialOpts})
	t.Cleanup(func() {
		_ = densePool.Close()
		_ = sparsePool.Close()
		_ = tritonPool.Close()
	})
	warmCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_ = densePool.Warmup(warmCtx)
	_ = sparsePool.Warmup(warmCtx)
	_ = tritonPool.Warmup(warmCtx)

	logger := slog.Default()
	metrics := telemetry.NewPrometheusMetrics()
	ringEmitter := telemetry.NewRingBufferEmitter(1024, telemetry.NoopAuditSink{}, logger)
	t.Cleanup(func() { _ = ringEmitter.Close(ctx) })

	uc := usecase.NewExecuteSearchUseCase(
		usecase.SearchConfig{RerankTau: 0},
		usecase.SearchDeps{
			DenseSearch:   dense.NewHNSWClient(densePool, dense.DefaultDenseTimeout, logger),
			SparseSearch:  sparse.NewBM25Client([]*grpcutil.ConnPool{sparsePool}, sparse.DefaultSparseTimeout, logger),
			Embedder:      triton.NewEmbeddingClient(tritonPool, "bge-m3", triton.DefaultEmbedTimeout, logger),
			Reranker:      triton.NewRerankerClient(tritonPool, "bge-rerank", "", triton.DefaultRerankTimeout, logger),
			ParentChunks:  rediscache.NewParentChunkReader(redisClient, logger),
			QueryCache:    rediscache.NewQueryTextCache(redisClient, rediscache.QueryCacheConfig{TTL: time.Minute}),
			Telemetry:     ringEmitter,
			Breaker:       triton.NewCircuitBreaker(triton.CircuitBreakerConfig{}),
			CandidatePool: pool.NewCandidatePool(),
			EvaluatorPool: acl.NewRoaringEvaluatorPool(),
			Metrics:       metrics,
			Logger:        logger,
		},
	)
	diagUC := usecase.NewExecuteDiagnosticSearchUseCase(uc)

	router := httptransport.NewRouter(httptransport.RouterConfig{
		Controller: httptransport.NewSearchController(uc, diagUC, metrics, logger),
		Metrics:    metrics,
	})

	bmAll, _ := roaring.BitmapOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).ToBytes()
	// Cold path (cache miss -> populates Tier 1 cache).
	resp1, err := uc.Execute(ctx, dto.SearchRequest{
		RequestID:             "int-req-1",
		TenantID:              "tenant-int",
		QueryText:             "soc2 encryption rotation",
		CallerACLBitmap:       bmAll,
		TopK:                  5,
		EmbeddingModelVersion: "bge-m3-v2",
	})
	if err != nil {
		t.Fatalf("cold search failed: %v", err)
	}
	if resp1.CacheHit || len(resp1.Chunks) != 5 {
		t.Fatalf("expected cold miss with 5 chunks, got cache_hit=%v len=%d", resp1.CacheHit, len(resp1.Chunks))
	}

	// Warm path (Tier 1 cache hit + Tier 2 ACL verification).
	resp2, err := uc.Execute(ctx, dto.SearchRequest{
		RequestID:             "int-req-2",
		TenantID:              "tenant-int",
		QueryText:             "  SOC2   Encryption Rotation ",
		CallerACLBitmap:       bmAll,
		TopK:                  5,
		EmbeddingModelVersion: "bge-m3-v2",
	})
	if err != nil {
		t.Fatalf("warm search failed: %v", err)
	}
	if !resp2.CacheHit || len(resp2.Chunks) != 5 {
		t.Fatalf("expected cache hit with 5 chunks, got cache_hit=%v len=%d", resp2.CacheHit, len(resp2.Chunks))
	}

	// Tier 2 bypass-without-eviction: caller with only 1 permitted doc (< top_k=5) bypasses cache.
	bmRestricted, _ := roaring.BitmapOf(1).ToBytes()
	resp3, err := uc.Execute(ctx, dto.SearchRequest{
		RequestID:             "int-req-3",
		TenantID:              "tenant-int",
		QueryText:             "soc2 encryption rotation",
		CallerACLBitmap:       bmRestricted,
		TopK:                  5,
		EmbeddingModelVersion: "bge-m3-v2",
	})
	if err != nil || resp3.CacheHit || len(resp3.Chunks) != 1 {
		t.Fatalf("expected Tier 2 bypass with 1 chunk, got cache_hit=%v len=%d err=%v", resp3.CacheHit, len(resp3.Chunks), err)
	}

	// Verify /ready HTTP endpoint returns 200.
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))
	var readyBody map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &readyBody)
	if rec.Code != http.StatusOK || !strings.EqualFold(fmt.Sprint(readyBody["status"]), "ready") {
		t.Fatalf("unexpected /ready response: %d %s", rec.Code, rec.Body.String())
	}
}
