//go:build chaos

package chaos

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/rag-platform/rag-search-orchestrator/internal/application/dto"
	"github.com/rag-platform/rag-search-orchestrator/internal/application/usecase"
	"github.com/rag-platform/rag-search-orchestrator/internal/domain/acl"
	"github.com/rag-platform/rag-search-orchestrator/internal/domain/entity"
	"github.com/rag-platform/rag-search-orchestrator/internal/domain/repository"
	rediscache "github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/cache/redis"
	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/grpcutil"
	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/pb/downstreamv1"
	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/pb/tritonv2"
	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/search/dense"
	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/search/sparse"
	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/telemetry"
	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/triton"
	"github.com/rag-platform/rag-search-orchestrator/pkg/pool"
)

type chaosDenseServer struct {
	downstreamv1.UnimplementedDenseSearchServiceServer
	delay time.Duration
	err   error
}

func (s *chaosDenseServer) Search(ctx context.Context, _ *downstreamv1.DenseSearchServiceSearchRequest) (*downstreamv1.DenseSearchServiceSearchResponse, error) {
	if s.delay > 0 {
		select {
		case <-time.After(s.delay):
		case <-ctx.Done():
			return nil, status.Error(codes.DeadlineExceeded, "dense shard deadline exceeded")
		}
	}
	if s.err != nil {
		return nil, s.err
	}
	cands := make([]*downstreamv1.DownstreamCandidate, 0, 10)
	for i := uint32(1); i <= 10; i++ {
		cands = append(cands, &downstreamv1.DownstreamCandidate{
			DocId:     i,
			ChunkId:   fmt.Sprintf("chk_%d", i),
			ParentKey: fmt.Sprintf("parent:tenant-chaos:%d:chk_%d", i, i),
			Score:     float64(100 - i),
		})
	}
	return &downstreamv1.DenseSearchServiceSearchResponse{Candidates: cands}, nil
}

type chaosSparseServer struct {
	downstreamv1.UnimplementedSparseSearchServiceServer
	delay time.Duration
	err   error
}

func (s *chaosSparseServer) Search(ctx context.Context, _ *downstreamv1.SparseSearchServiceSearchRequest) (*downstreamv1.SparseSearchServiceSearchResponse, error) {
	if s.delay > 0 {
		select {
		case <-time.After(s.delay):
		case <-ctx.Done():
			return nil, status.Error(codes.DeadlineExceeded, "sparse shard deadline exceeded")
		}
	}
	if s.err != nil {
		return nil, s.err
	}
	cands := make([]*downstreamv1.DownstreamCandidate, 0, 10)
	for i := uint32(1); i <= 10; i++ {
		cands = append(cands, &downstreamv1.DownstreamCandidate{
			DocId:     i,
			ChunkId:   fmt.Sprintf("chk_%d", i),
			ParentKey: fmt.Sprintf("parent:tenant-chaos:%d:chk_%d", i, i),
			Score:     float64(90 - i),
		})
	}
	return &downstreamv1.SparseSearchServiceSearchResponse{Candidates: cands}, nil
}

type chaosTritonServer struct {
	tritonv2.UnimplementedGRPCInferenceServiceServer
	rerankDelay atomic.Int64
	rerankCalls atomic.Int64
}

func (s *chaosTritonServer) ModelStreamInfer(stream grpc.BidiStreamingServer[tritonv2.ModelInferRequest, tritonv2.ModelStreamInferResponse]) error {
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

func (s *chaosTritonServer) ModelInfer(ctx context.Context, req *tritonv2.ModelInferRequest) (*tritonv2.ModelInferResponse, error) {
	s.rerankCalls.Add(1)
	if d := time.Duration(s.rerankDelay.Load()); d > 0 {
		select {
		case <-time.After(d):
		case <-ctx.Done():
			return nil, status.Error(codes.DeadlineExceeded, "triton rerank deadline exceeded")
		}
	}
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

type chaosParentStub struct{}

func (chaosParentStub) HydrateParents(_ context.Context, _ string, candidates []entity.FusedCandidate) ([]entity.EnrichedChunk, error) {
	out := make([]entity.EnrichedChunk, len(candidates))
	for i, c := range candidates {
		out[i] = entity.EnrichedChunk{
			DocID:     c.DocID,
			ChunkID:   c.ChunkID,
			ParentKey: c.ParentKey,
			Text:      "chaos parent text",
			Score:     c.FinalScore,
		}
	}
	return out, nil
}

func startChaosPool(t *testing.T, register func(*grpc.Server)) *grpcutil.ConnPool {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	register(srv)
	go func() { _ = srv.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		srv.Stop()
		_ = lis.Close()
		t.Fatalf("grpc.NewClient bufconn: %v", err)
	}

	cp, err := grpcutil.NewConnPoolFromConns([]*grpc.ClientConn{conn})
	if err != nil {
		_ = conn.Close()
		srv.Stop()
		_ = lis.Close()
		t.Fatalf("NewConnPoolFromConns: %v", err)
	}

	t.Cleanup(func() {
		_ = cp.Close()
		srv.Stop()
		_ = lis.Close()
	})

	warmCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := cp.Warmup(warmCtx); err != nil {
		t.Fatalf("bufconn warmup: %v", err)
	}

	return cp
}

func counterValue(t *testing.T, m *telemetry.PrometheusMetrics, metricName string, labels map[string]string) float64 {
	t.Helper()
	mfs, err := m.Gatherer().Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != metricName {
			continue
		}
		for _, metric := range mf.GetMetric() {
			if len(labels) == 0 {
				return metric.GetCounter().GetValue()
			}
			matched := true
			for k, v := range labels {
				found := false
				for _, lp := range metric.GetLabel() {
					if lp.GetName() == k && lp.GetValue() == v {
						found = true
						break
					}
				}
				if !found {
					matched = false
					break
				}
			}
			if matched {
				return metric.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func TestChaos_Triton50msLatencyInjection_TripsCircuitBreaker(t *testing.T) {
	tritonSrv := &chaosTritonServer{}
	tritonSrv.rerankDelay.Store(int64(55 * time.Millisecond))

	densePool := startChaosPool(t, func(s *grpc.Server) {
		downstreamv1.RegisterDenseSearchServiceServer(s, &chaosDenseServer{})
	})
	sparsePool := startChaosPool(t, func(s *grpc.Server) {
		downstreamv1.RegisterSparseSearchServiceServer(s, &chaosSparseServer{})
	})
	tritonPool := startChaosPool(t, func(s *grpc.Server) {
		tritonv2.RegisterGRPCInferenceServiceServer(s, tritonSrv)
	})

	logger := slog.New(slog.DiscardHandler)
	metrics := telemetry.NewPrometheusMetrics()
	ring := telemetry.NewRingBufferEmitter(1024, telemetry.NoopAuditSink{}, logger)
	t.Cleanup(func() { _ = ring.Close(context.Background()) })

	breaker := triton.NewCircuitBreaker(triton.CircuitBreakerConfig{
		OnTrip: metrics.IncCircuitBreakerTripped,
	})

	uc := usecase.NewExecuteSearchUseCase(
		usecase.SearchConfig{RerankTau: 0},
		usecase.SearchDeps{
			DenseSearch:   dense.NewHNSWClient(densePool, dense.DefaultDenseTimeout, logger),
			SparseSearch:  sparse.NewBM25Client([]*grpcutil.ConnPool{sparsePool}, sparse.DefaultSparseTimeout, logger),
			Embedder:      triton.NewEmbeddingClient(tritonPool, "bge-m3", triton.DefaultEmbedTimeout, logger),
			Reranker:      triton.NewRerankerClient(tritonPool, "bge-rerank", "", triton.DefaultRerankTimeout, logger),
			ParentChunks:  chaosParentStub{},
			Telemetry:     ring,
			Breaker:       breaker,
			CandidatePool: pool.NewCandidatePool(),
			EvaluatorPool: acl.NewRoaringEvaluatorPool(),
			Metrics:       metrics,
			Logger:        logger,
		},
	)
	diagUC := usecase.NewExecuteDiagnosticSearchUseCase(uc)

	aclBytes, _ := roaring.BitmapOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).ToBytes()

	// Drive >= DefaultMinRequests (20) requests under 55ms Triton latency injection.
	for i := 0; i < 20; i++ {
		resp, err := uc.Execute(context.Background(), dto.SearchRequest{
			RequestID:             fmt.Sprintf("chaos-triton-%d", i),
			TenantID:              "tenant-chaos",
			QueryText:             "soc2 encryption rotation",
			CallerACLBitmap:       aclBytes,
			TopK:                  5,
			EmbeddingModelVersion: "bge-m3-v2",
		})
		if err != nil {
			t.Fatalf("request %d failed unexpectedly: %v", i, err)
		}
		if !resp.RerankerFallback || len(resp.Chunks) != 5 {
			t.Fatalf("request %d expected reranker_fallback=true and 5 chunks, got fallback=%v len=%d", i, resp.RerankerFallback, len(resp.Chunks))
		}
	}

	if breaker.State() != triton.StateOpen {
		t.Fatalf("expected circuit breaker StateOpen, got %v", breaker.State())
	}
	if trips := counterValue(t, metrics, "search_circuit_breaker_tripped_total", nil); trips < 1 {
		t.Fatalf("expected search_circuit_breaker_tripped_total >= 1, got %v", trips)
	}

	// Once tripped, requests must bypass Triton reranker and complete well within 75ms SLO.
	callsBefore := tritonSrv.rerankCalls.Load()
	start := time.Now()
	diagResp, err := diagUC.Execute(context.Background(), dto.DiagnosticSearchRequest{
		RequestID:             "chaos-triton-tripped",
		TenantID:              "tenant-chaos",
		QueryText:             "soc2 encryption rotation",
		CallerACLBitmap:       aclBytes,
		TopK:                  5,
		EmbeddingModelVersion: "bge-m3-v2",
	})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("tripped fallback request failed: %v", err)
	}
	if !diagResp.Response.RerankerFallback || !diagResp.ScoreTrace.CircuitBreakerTripped {
		t.Fatalf("expected reranker_fallback=true and circuit_breaker_tripped=true, got %+v", diagResp)
	}
	if tritonSrv.rerankCalls.Load() != callsBefore {
		t.Fatalf("expected Triton reranker to be bypassed when StateOpen")
	}
	if elapsed > 75*time.Millisecond {
		t.Fatalf("post-trip fallback latency %v exceeded 75ms SLO", elapsed)
	}
}

func TestChaos_DenseAndSparseShardDropouts_SingleIndexFallback(t *testing.T) {
	tritonPool := startChaosPool(t, func(s *grpc.Server) {
		tritonv2.RegisterGRPCInferenceServiceServer(s, &chaosTritonServer{})
	})

	aclBytes, _ := roaring.BitmapOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).ToBytes()

	cases := []struct {
		name           string
		denseSrv       *chaosDenseServer
		sparseSrv      *chaosSparseServer
		errComponent   string
		expectedErrTag string
	}{
		{
			name:           "SparseShard_DeadlineExceeded_20ms",
			denseSrv:       &chaosDenseServer{},
			sparseSrv:      &chaosSparseServer{delay: 40 * time.Millisecond},
			errComponent:   usecase.ComponentBM25Sparse,
			expectedErrTag: "DeadlineExceeded",
		},
		{
			name:           "DenseShard_Unavailable",
			denseSrv:       &chaosDenseServer{err: status.Error(codes.Unavailable, "hnsw replica down")},
			sparseSrv:      &chaosSparseServer{},
			errComponent:   usecase.ComponentHNSWDense,
			expectedErrTag: "Internal",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			densePool := startChaosPool(t, func(s *grpc.Server) {
				downstreamv1.RegisterDenseSearchServiceServer(s, tc.denseSrv)
			})
			sparsePool := startChaosPool(t, func(s *grpc.Server) {
				downstreamv1.RegisterSparseSearchServiceServer(s, tc.sparseSrv)
			})

			logger := slog.New(slog.DiscardHandler)
			metrics := telemetry.NewPrometheusMetrics()
			ring := telemetry.NewRingBufferEmitter(128, telemetry.NoopAuditSink{}, logger)
			t.Cleanup(func() { _ = ring.Close(context.Background()) })

			uc := usecase.NewExecuteSearchUseCase(
				usecase.SearchConfig{RerankTau: 0},
				usecase.SearchDeps{
					DenseSearch:   dense.NewHNSWClient(densePool, dense.DefaultDenseTimeout, logger),
					SparseSearch:  sparse.NewBM25Client([]*grpcutil.ConnPool{sparsePool}, sparse.DefaultSparseTimeout, logger),
					Embedder:      triton.NewEmbeddingClient(tritonPool, "bge-m3", triton.DefaultEmbedTimeout, logger),
					Reranker:      triton.NewRerankerClient(tritonPool, "bge-rerank", "", triton.DefaultRerankTimeout, logger),
					ParentChunks:  chaosParentStub{},
					Telemetry:     ring,
					Breaker:       triton.NewCircuitBreaker(triton.CircuitBreakerConfig{}),
					CandidatePool: pool.NewCandidatePool(),
					EvaluatorPool: acl.NewRoaringEvaluatorPool(),
					Metrics:       metrics,
					Logger:        logger,
				},
			)

			start := time.Now()
			resp, err := uc.Execute(context.Background(), dto.SearchRequest{
				RequestID:             "chaos-shard-" + tc.name,
				TenantID:              "tenant-chaos",
				QueryText:             "soc2 encryption rotation",
				CallerACLBitmap:       aclBytes,
				TopK:                  5,
				EmbeddingModelVersion: "bge-m3-v2",
			})
			elapsed := time.Since(start)
			if err != nil {
				t.Fatalf("expected single-index fallback to succeed, got err=%v", err)
			}
			if len(resp.Chunks) != 5 {
				t.Fatalf("expected 5 chunks from surviving modality, got %d", len(resp.Chunks))
			}
			if elapsed > 75*time.Millisecond {
				t.Fatalf("single-index fallback took %v, expected <= 75ms", elapsed)
			}
			if count := counterValue(t, metrics, "search_fanout_stage_errors_total", map[string]string{
				"component": tc.errComponent,
				"error":     tc.expectedErrTag,
			}); count < 1 {
				t.Fatalf("expected stage error counter for %s/%s >= 1, got %v", tc.errComponent, tc.expectedErrTag, count)
			}
		})
	}
}

func TestChaos_RedisPartitionAndTimeout_GracefulFallbackAndNoSecretLeak(t *testing.T) {
	// Blackhole TCP listener that accepts connections and never replies, forcing Redis timeouts.
	var lc net.ListenConfig
	blackholeLis, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen blackhole: %v", err)
	}
	t.Cleanup(func() { _ = blackholeLis.Close() })
	go func() {
		for {
			conn, acceptErr := blackholeLis.Accept()
			if acceptErr != nil {
				return
			}
			go func(c net.Conn) {
				time.Sleep(5 * time.Second)
				_ = c.Close()
			}(conn)
		}
	}()

	const secretRedisPassword = "super-secret-redis-password-999"
	redisClient := goredis.NewUniversalClient(&goredis.UniversalOptions{
		Addrs:        []string{blackholeLis.Addr().String()},
		Password:     secretRedisPassword,
		DialTimeout:  10 * time.Millisecond,
		ReadTimeout:  10 * time.Millisecond,
		WriteTimeout: 10 * time.Millisecond,
		MaxRetries:   0,
	})
	t.Cleanup(func() { _ = redisClient.Close() })

	var logBuf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	metrics := telemetry.NewPrometheusMetrics()
	ring := telemetry.NewRingBufferEmitter(128, telemetry.NoopAuditSink{}, logger)
	t.Cleanup(func() { _ = ring.Close(context.Background()) })

	densePool := startChaosPool(t, func(s *grpc.Server) {
		downstreamv1.RegisterDenseSearchServiceServer(s, &chaosDenseServer{})
	})
	sparsePool := startChaosPool(t, func(s *grpc.Server) {
		downstreamv1.RegisterSparseSearchServiceServer(s, &chaosSparseServer{})
	})
	tritonPool := startChaosPool(t, func(s *grpc.Server) {
		tritonv2.RegisterGRPCInferenceServiceServer(s, &chaosTritonServer{})
	})

	aclBytes, _ := roaring.BitmapOf(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).ToBytes()
	queryCache := rediscache.NewQueryTextCache(redisClient, rediscache.QueryCacheConfig{
		ReadTimeout:  5 * time.Millisecond,
		WriteTimeout: 5 * time.Millisecond,
	})
	parentReader := rediscache.NewParentChunkReader(redisClient, logger)

	// 1. Tier 1 cache timeout/partition must fall back to live fan-out and succeed when hydration is healthy.
	ucCachePartition := usecase.NewExecuteSearchUseCase(
		usecase.SearchConfig{RerankTau: 0},
		usecase.SearchDeps{
			DenseSearch:   dense.NewHNSWClient(densePool, dense.DefaultDenseTimeout, logger),
			SparseSearch:  sparse.NewBM25Client([]*grpcutil.ConnPool{sparsePool}, sparse.DefaultSparseTimeout, logger),
			Embedder:      triton.NewEmbeddingClient(tritonPool, "bge-m3", triton.DefaultEmbedTimeout, logger),
			Reranker:      triton.NewRerankerClient(tritonPool, "bge-rerank", "", triton.DefaultRerankTimeout, logger),
			ParentChunks:  chaosParentStub{},
			QueryCache:    queryCache,
			Telemetry:     ring,
			Breaker:       triton.NewCircuitBreaker(triton.CircuitBreakerConfig{}),
			CandidatePool: pool.NewCandidatePool(),
			EvaluatorPool: acl.NewRoaringEvaluatorPool(),
			Metrics:       metrics,
			Logger:        logger,
		},
	)

	resp, err := ucCachePartition.Execute(context.Background(), dto.SearchRequest{
		RequestID:             "chaos-redis-cache",
		TenantID:              "tenant-chaos",
		QueryText:             "soc2 encryption rotation",
		CallerACLBitmap:       aclBytes,
		TopK:                  5,
		EmbeddingModelVersion: "bge-m3-v2",
	})
	if err != nil {
		t.Fatalf("expected live fan-out fallback on Tier 1 Redis failure, got err=%v", err)
	}
	if resp.CacheHit || len(resp.Chunks) != 5 {
		t.Fatalf("expected cache_hit=false and 5 chunks, got cache_hit=%v len=%d", resp.CacheHit, len(resp.Chunks))
	}

	// 2. Parent hydration Redis partition must fail cleanly within bounded timeout without panics or secret leakage.
	ucHydratePartition := usecase.NewExecuteSearchUseCase(
		usecase.SearchConfig{RerankTau: 0},
		usecase.SearchDeps{
			DenseSearch:   dense.NewHNSWClient(densePool, dense.DefaultDenseTimeout, logger),
			SparseSearch:  sparse.NewBM25Client([]*grpcutil.ConnPool{sparsePool}, sparse.DefaultSparseTimeout, logger),
			Embedder:      triton.NewEmbeddingClient(tritonPool, "bge-m3", triton.DefaultEmbedTimeout, logger),
			Reranker:      triton.NewRerankerClient(tritonPool, "bge-rerank", "", triton.DefaultRerankTimeout, logger),
			ParentChunks:  parentReader,
			QueryCache:    repository.QueryCachePort(nil),
			Telemetry:     ring,
			Breaker:       triton.NewCircuitBreaker(triton.CircuitBreakerConfig{}),
			CandidatePool: pool.NewCandidatePool(),
			EvaluatorPool: acl.NewRoaringEvaluatorPool(),
			Metrics:       metrics,
			Logger:        logger,
		},
	)

	start := time.Now()
	_, hydrateErr := ucHydratePartition.Execute(context.Background(), dto.SearchRequest{
		RequestID:             "chaos-redis-hydrate",
		TenantID:              "tenant-chaos",
		QueryText:             "soc2 encryption rotation",
		CallerACLBitmap:       aclBytes,
		TopK:                  5,
		EmbeddingModelVersion: "bge-m3-v2",
	})
	hydrateDur := time.Since(start)
	if hydrateErr == nil {
		t.Fatal("expected bounded hydration error under Redis partition, got nil")
	}
	if hydrateDur > 100*time.Millisecond {
		t.Fatalf("hydration timeout exceeded bound: %v", hydrateDur)
	}

	logs := logBuf.String()
	if strings.Contains(logs, secretRedisPassword) || strings.Contains(hydrateErr.Error(), secretRedisPassword) {
		t.Fatal("redis password leaked in logs or error")
	}
}
