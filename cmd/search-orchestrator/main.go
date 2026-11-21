package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	searchv1 "github.com/rag-platform/rag-search-orchestrator/api/proto/v1"
	"github.com/rag-platform/rag-search-orchestrator/internal/application/usecase"
	"github.com/rag-platform/rag-search-orchestrator/internal/config"
	"github.com/rag-platform/rag-search-orchestrator/internal/domain/acl"
	rediscache "github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/cache/redis"
	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/grpcutil"
	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/search/dense"
	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/search/sparse"
	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/telemetry"
	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/triton"
	grpctransport "github.com/rag-platform/rag-search-orchestrator/internal/transport/grpc"
	"github.com/rag-platform/rag-search-orchestrator/internal/transport/grpc/interceptor"
	httptransport "github.com/rag-platform/rag-search-orchestrator/internal/transport/http"
	"github.com/rag-platform/rag-search-orchestrator/pkg/pool"
)

const (
	defaultWarmupTimeout   = 5 * time.Second
	defaultShutdownTimeout = 15 * time.Second
)

func main() {
	if err := config.LoadDotEnv(os.Getenv("ENV_FILE")); err != nil {
		bootstrapLogger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelWarn,
		}))
		bootstrapLogger.Warn("failed to load .env file", slog.String("error_class", "dotenv_load_failed"))
	}
	config.ApplyRuntimeEnv()

	cfg := config.Load()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: cfg.LogLevel,
	}))
	slog.SetDefault(logger)

	os.Exit(run(cfg, logger))
}

func run(cfg config.Config, logger *slog.Logger) int {
	metrics := telemetry.NewPrometheusMetrics()
	auditSink := telemetry.LogAuditSink{Logger: logger}
	ringEmitter := telemetry.NewRingBufferEmitter(cfg.RingBufferCapacity, auditSink, logger)

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	hnswPool, err := grpcutil.NewConnPool(grpcutil.Config{
		Targets:     cfg.HNSWTargets,
		PoolSize:    cfg.GRPCPoolSize,
		DialOptions: dialOpts,
	})
	if err != nil {
		logger.Error("failed to initialize connection pool",
			slog.String("component", "hnsw_pool"),
			slog.String("error_class", classifyPoolInitError(err)),
		)
		return 1
	}
	defer func() { _ = hnswPool.Close() }()

	bm25Pools := make([]*grpcutil.ConnPool, 0, len(cfg.BM25Targets))
	for i, target := range cfg.BM25Targets {
		p, shardErr := grpcutil.NewConnPool(grpcutil.Config{
			Targets:     []string{target},
			PoolSize:    cfg.GRPCPoolSize,
			DialOptions: dialOpts,
		})
		if shardErr != nil {
			logger.Error("failed to initialize connection pool",
				slog.String("component", "bm25_shard_pool"),
				slog.Int("shard", i),
				slog.String("error_class", classifyPoolInitError(shardErr)),
			)
			return 1
		}
		bm25Pools = append(bm25Pools, p)
	}
	defer func() {
		for _, p := range bm25Pools {
			_ = p.Close()
		}
	}()

	embedPool, err := grpcutil.NewConnPool(grpcutil.Config{
		Targets:     cfg.TritonEmbedTargets,
		PoolSize:    cfg.GRPCPoolSize,
		DialOptions: dialOpts,
	})
	if err != nil {
		logger.Error("failed to initialize connection pool",
			slog.String("component", "triton_embed_pool"),
			slog.String("error_class", classifyPoolInitError(err)),
		)
		return 1
	}
	defer func() { _ = embedPool.Close() }()

	rerankPool, err := grpcutil.NewConnPool(grpcutil.Config{
		Targets:     cfg.TritonRerankTargets,
		PoolSize:    cfg.GRPCPoolSize,
		DialOptions: dialOpts,
	})
	if err != nil {
		logger.Error("failed to initialize connection pool",
			slog.String("component", "triton_rerank_pool"),
			slog.String("error_class", classifyPoolInitError(err)),
		)
		return 1
	}
	defer func() { _ = rerankPool.Close() }()

	warmupCtx, warmupCancel := context.WithTimeout(context.Background(), defaultWarmupTimeout)
	warmupPools(warmupCtx, logger, hnswPool, bm25Pools, embedPool, rerankPool)
	warmupCancel()

	redisClient := goredis.NewUniversalClient(&goredis.UniversalOptions{
		Addrs:    cfg.RedisAddrs,
		Password: cfg.RedisPassword,
	})
	defer func() { _ = redisClient.Close() }()

	pingCtx, pingCancel := context.WithTimeout(context.Background(), defaultWarmupTimeout)
	if pingErr := redisClient.Ping(pingCtx).Err(); pingErr != nil {
		logger.Warn("redis startup ping failed", slog.String("error", pingErr.Error()))
	}
	pingCancel()

	queryCache := rediscache.NewQueryTextCache(redisClient, rediscache.QueryCacheConfig{
		TTL: cfg.CacheTTL,
	})
	parentReader := rediscache.NewParentChunkReader(redisClient, logger)

	breaker := triton.NewCircuitBreaker(triton.CircuitBreakerConfig{
		OnTrip: metrics.IncCircuitBreakerTripped,
	})

	searchUC := usecase.NewExecuteSearchUseCase(
		usecase.SearchConfig{RerankTau: cfg.RerankTau},
		usecase.SearchDeps{
			DenseSearch:   dense.NewHNSWClient(hnswPool, dense.DefaultDenseTimeout, logger),
			SparseSearch:  sparse.NewBM25Client(bm25Pools, sparse.DefaultSparseTimeout, logger),
			Embedder:      triton.NewEmbeddingClient(embedPool, cfg.TritonEmbedModel, triton.DefaultEmbedTimeout, logger),
			Reranker:      triton.NewRerankerClient(rerankPool, cfg.TritonRerankModel, cfg.TritonRerankVer, triton.DefaultRerankTimeout, logger),
			ParentChunks:  parentReader,
			QueryCache:    queryCache,
			Telemetry:     ringEmitter,
			Breaker:       breaker,
			CandidatePool: pool.NewCandidatePool(),
			EvaluatorPool: acl.NewRoaringEvaluatorPool(),
			Metrics:       metrics,
			Logger:        logger,
		},
	)
	diagnosticUC := usecase.NewExecuteDiagnosticSearchUseCase(searchUC)

	var draining atomic.Bool

	grpcHandler := grpctransport.NewSearchHandler(searchUC, diagnosticUC, metrics)
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(grpctransport.MaxRecvMsgSizeBytes),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    grpcutil.DefaultKeepaliveTime,
			Timeout: grpcutil.DefaultKeepaliveTimeout,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.UnaryInterceptor(interceptor.UnaryServerInterceptor(logger)),
	)
	searchv1.RegisterSearchServiceServer(grpcServer, grpcHandler)

	var lc net.ListenConfig
	grpcLis, err := lc.Listen(context.Background(), "tcp", cfg.GRPCAddr)
	if err != nil {
		logger.Error("failed to bind grpc listener", slog.String("addr", cfg.GRPCAddr), slog.String("error", err.Error()))
		return 1
	}

	httpController := httptransport.NewSearchController(searchUC, diagnosticUC, metrics, logger)
	httpRouter := httptransport.NewRouter(httptransport.RouterConfig{
		Controller: httpController,
		Metrics:    metrics,
		Draining:   &draining,
		Checks: map[string]httptransport.ReadinessCheck{
			"redis": parentReader.Ping,
		},
	})
	httpServer := httptransport.NewHTTPServer(cfg.HTTPAddr, httpRouter)

	errCh := make(chan error, 2)
	go func() {
		logger.Info("starting gRPC server", slog.String("addr", cfg.GRPCAddr))
		if srvErr := grpcServer.Serve(grpcLis); srvErr != nil {
			errCh <- srvErr
		}
	}()
	go func() {
		logger.Info("starting HTTP server", slog.String("addr", cfg.HTTPAddr))
		if srvErr := httpServer.ListenAndServe(); srvErr != nil && !errors.Is(srvErr, http.ErrServerClosed) {
			errCh <- srvErr
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		logger.Info("shutdown signal received, entering drain state", slog.String("signal", sig.String()))
	case srvErr := <-errCh:
		logger.Error("server exited unexpectedly", slog.String("error", srvErr.Error()))
		return 1
	}

	draining.Store(true)
	if cfg.ShutdownDrainDelay > 0 {
		time.Sleep(cfg.ShutdownDrainDelay)
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
	defer shutdownCancel()

	grpcDone := make(chan struct{})
	go func() {
		defer close(grpcDone)
		grpcServer.GracefulStop()
	}()

	httpDone := make(chan error, 1)
	go func() {
		httpDone <- httpServer.Shutdown(shutdownCtx)
	}()

	grpcStopped := false
	httpStopped := false
	for !grpcStopped || !httpStopped {
		select {
		case <-grpcDone:
			grpcStopped = true
			grpcDone = nil
		case httpErr := <-httpDone:
			httpStopped = true
			httpDone = nil
			if httpErr != nil {
				logger.Warn("http server shutdown error", slog.String("error", httpErr.Error()))
			}
		case <-shutdownCtx.Done():
			if !grpcStopped {
				logger.Warn("grpc graceful stop timed out, forcing stop")
				grpcServer.Stop()
				<-grpcDone
				grpcStopped = true
			}
			if !httpStopped {
				httpErr := <-httpDone
				httpStopped = true
				if httpErr != nil {
					logger.Warn("http server shutdown error", slog.String("error", httpErr.Error()))
				}
			}
		}
	}

	if err := ringEmitter.Close(shutdownCtx); err != nil {
		logger.Warn("ring buffer flush error", slog.String("error", err.Error()))
	}

	logger.Info("rag-search-orchestrator stopped cleanly")
	return 0
}

func warmupPools(
	ctx context.Context,
	logger *slog.Logger,
	hnswPool *grpcutil.ConnPool,
	bm25Pools []*grpcutil.ConnPool,
	embedPool *grpcutil.ConnPool,
	rerankPool *grpcutil.ConnPool,
) {
	if err := hnswPool.Warmup(ctx); err != nil {
		logger.Warn("hnsw pool warmup incomplete", slog.String("error", err.Error()))
	}
	for i, p := range bm25Pools {
		if err := p.Warmup(ctx); err != nil {
			logger.Warn("bm25 shard pool warmup incomplete", slog.Int("shard", i), slog.String("error", err.Error()))
		}
	}
	if err := embedPool.Warmup(ctx); err != nil {
		logger.Warn("triton embed pool warmup incomplete", slog.String("error", err.Error()))
	}
	if err := rerankPool.Warmup(ctx); err != nil {
		logger.Warn("triton rerank pool warmup incomplete", slog.String("error", err.Error()))
	}
}

func classifyPoolInitError(err error) string {
	switch {
	case errors.Is(err, grpcutil.ErrNoTargets):
		return "no_targets"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		return "dial_failed"
	}
}
