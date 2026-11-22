package usecase

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rag-platform/rag-search-orchestrator/internal/application/dto"
	"github.com/rag-platform/rag-search-orchestrator/internal/domain/acl"
	"github.com/rag-platform/rag-search-orchestrator/internal/domain/entity"
	"github.com/rag-platform/rag-search-orchestrator/internal/domain/ranking"
	"github.com/rag-platform/rag-search-orchestrator/internal/domain/repository"
	"github.com/rag-platform/rag-search-orchestrator/pkg/pool"
)

const (
	// DefaultEmbedTimeout is the hard deadline for Triton FP16 query embedding (15ms SLO).
	DefaultEmbedTimeout = 15 * time.Millisecond
	// DefaultFanoutTimeout is the hard deadline for parallel HNSW + BM25 fan-out (20ms).
	DefaultFanoutTimeout = 20 * time.Millisecond
	// DefaultRerankTimeout is the hard deadline for Triton Cross-Encoder reranking (35ms).
	DefaultRerankTimeout = 35 * time.Millisecond
	// DefaultHydrateTimeout is the deadline for Redis parent chunk hydration.
	DefaultHydrateTimeout = 10 * time.Millisecond
)

var (
	// ErrBothModalitiesFailed is returned when both dense HNSW and sparse BM25 search fail.
	ErrBothModalitiesFailed = errors.New("all retrieval modalities failed")
	// ErrNoSearchPortsConfigured is returned when neither dense nor sparse search ports are wired.
	ErrNoSearchPortsConfigured = errors.New("no retrieval search ports configured")
)

// RerankCircuitBreaker guards Triton Cross-Encoder reranking against GPU stalls and error spikes.
type RerankCircuitBreaker interface {
	Allow(ctx context.Context) (allowed bool, isProbe bool, reason string)
	Record(duration time.Duration, err error, isProbe bool)
}

// SearchConfig configures timeouts, RRF smoothing, and Cross-Encoder logit threshold tau.
type SearchConfig struct {
	RRFK           int
	RerankTau      float64
	EmbedTimeout   time.Duration
	FanoutTimeout  time.Duration
	RerankTimeout  time.Duration
	HydrateTimeout time.Duration
}

// SearchDeps bundles the domain ports, pools, circuit breaker, and observers for search use cases.
type SearchDeps struct {
	DenseSearch   repository.DenseSearchPort
	SparseSearch  repository.SparseSearchPort
	Embedder      repository.TritonEmbeddingPort
	Reranker      repository.TritonRerankerPort
	ParentChunks  repository.ParentChunkPort
	QueryCache    repository.QueryCachePort
	Telemetry     repository.TelemetryEmitterPort
	Breaker       RerankCircuitBreaker
	CandidatePool *pool.CandidatePool
	EvaluatorPool *acl.RoaringEvaluatorPool
	Metrics       MetricsObserver
	Logger        *slog.Logger
}

// ExecuteSearchUseCase orchestrates the 6-stage deterministic retrieval cascade.
type ExecuteSearchUseCase struct {
	cfg     SearchConfig
	deps    SearchDeps
	rrf     *ranking.RRFEngine
	cascade *ranking.CascadeFilter
}

// NewExecuteSearchUseCase constructs an ExecuteSearchUseCase with production defaults.
func NewExecuteSearchUseCase(cfg SearchConfig, deps SearchDeps) *ExecuteSearchUseCase {
	if cfg.RRFK <= 0 {
		cfg.RRFK = ranking.DefaultRRFK
	}
	if cfg.EmbedTimeout <= 0 {
		cfg.EmbedTimeout = DefaultEmbedTimeout
	}
	if cfg.FanoutTimeout <= 0 {
		cfg.FanoutTimeout = DefaultFanoutTimeout
	}
	if cfg.RerankTimeout <= 0 {
		cfg.RerankTimeout = DefaultRerankTimeout
	}
	if cfg.HydrateTimeout <= 0 {
		cfg.HydrateTimeout = DefaultHydrateTimeout
	}
	if deps.CandidatePool == nil {
		deps.CandidatePool = pool.NewCandidatePool()
	}
	if deps.EvaluatorPool == nil {
		deps.EvaluatorPool = acl.NewRoaringEvaluatorPool()
	}
	if deps.Metrics == nil {
		deps.Metrics = noopMetricsObserver{}
	}
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}

	return &ExecuteSearchUseCase{
		cfg:     cfg,
		deps:    deps,
		rrf:     ranking.NewRRFEngine(cfg.RRFK),
		cascade: ranking.NewCascadeFilter(),
	}
}

// Execute runs the primary staged retrieval cascade and returns enriched parent chunks.
func (uc *ExecuteSearchUseCase) Execute(ctx context.Context, req dto.SearchRequest) (dto.SearchResponse, error) {
	diag, err := uc.runCascade(ctx, req, false)
	if err != nil {
		return dto.SearchResponse{}, err
	}
	return diag.Response, nil
}

func (uc *ExecuteSearchUseCase) runCascade(
	ctx context.Context,
	req dto.SearchRequest,
	captureTrace bool,
) (dto.DiagnosticSearchResponse, error) {
	totalStart := time.Now()

	// 1. Validate QueryContext and decode caller Roaring Bitmap (fail-closed).
	qc, err := entity.NewQueryContext(
		req.RequestID,
		req.TenantID,
		req.QueryText,
		req.EmbeddingModelVersion,
		req.CallerACLBitmap,
		req.TopK,
	)
	if err != nil {
		return dto.DiagnosticSearchResponse{}, err
	}

	eval, err := uc.deps.EvaluatorPool.Acquire(qc.CallerACLBitmap)
	if err != nil {
		return dto.DiagnosticSearchResponse{}, err
	}
	defer eval.Close()

	var timings map[string]float64
	if captureTrace {
		timings = make(map[string]float64, 6)
	}

	// 2. Optional Tier 1/2 cache lookup with mandatory live Roaring Bitmap verification.
	if uc.deps.QueryCache != nil {
		if diagResp, hit, cacheErr := uc.tryTier1Cache(ctx, req.TraceID, qc, eval, captureTrace, timings, totalStart); cacheErr != nil {
			uc.deps.Logger.WarnContext(ctx, "tier1 cache lookup failed, proceeding to live fan-out",
				slog.String("trace_id", req.TraceID),
				slog.String("request_id", qc.RequestID),
				slog.String("tenant_id", qc.TenantID),
				slog.String("query_hash", qc.QueryHash),
				slog.String("error", cacheErr.Error()),
			)
		} else if hit {
			return diagResp, nil
		}
	}

	// 3. Dense query embedding inference (15ms cap; graceful sparse-only fallback on error).
	queryVec, embedOK := uc.embedQuery(ctx, req.TraceID, qc, timings)

	// 4. Concurrent Dense (Top-40) + Sparse (Top-40) fan-out (20ms hard deadline).
	denseBuf := uc.deps.CandidatePool.GetSearchCandidates()
	defer uc.deps.CandidatePool.PutSearchCandidates(denseBuf)
	sparseBuf := uc.deps.CandidatePool.GetSearchCandidates()
	defer uc.deps.CandidatePool.PutSearchCandidates(sparseBuf)

	denseCount, sparseCount, fanoutErr := uc.fanout(
		ctx, req.TraceID, qc, queryVec, embedOK, denseBuf, sparseBuf, timings,
	)
	if fanoutErr != nil {
		return dto.DiagnosticSearchResponse{}, fanoutErr
	}

	// 5. RRF (k=60, 80 -> 20) + zero-alloc Roaring Bitmap ACL filtering (<1ms).
	rrfStart := time.Now()
	rrfResults := uc.rrf.Fuse(*denseBuf, *sparseBuf)

	aclBuf := uc.deps.CandidatePool.GetFusedCandidates()
	defer uc.deps.CandidatePool.PutFusedCandidates(aclBuf)

	aclStart := time.Now()
	rrfCap := len(rrfResults)
	if rrfCap > ranking.MaxRRFOutputs {
		rrfCap = ranking.MaxRRFOutputs
	}
	permitted := eval.FilterCandidates(rrfResults[:rrfCap], *aclBuf)
	uc.deps.Metrics.ObserveRoaringEval(time.Since(aclStart))
	if captureTrace {
		timings[ComponentRRFACL] = float64(time.Since(rrfStart).Microseconds()) / 1000.0
	}

	// Zero ACL-permitted results return a valid empty SearchResponse.
	if len(permitted) == 0 {
		uc.maybeStoreTier1Cache(ctx, qc, rrfResults, false)
		totalDur := time.Since(totalStart)
		uc.emitAuditSpan(req.TraceID, qc, false, nil, false, totalDur)
		return dto.DiagnosticSearchResponse{
			Response: dto.SearchResponse{
				RequestID: qc.RequestID,
				TenantID:  qc.TenantID,
				CacheHit:  false,
				Chunks:    []dto.EnrichedChunk{},
				LatencyMs: float64(totalDur.Microseconds()) / 1000.0,
			},
			ScoreTrace: dto.ScoreTrace{
				DenseCount:        safeInt32(denseCount),
				SparseCount:       safeInt32(sparseCount),
				RRFCount:          safeInt32(len(rrfResults)),
				ACLPermittedCount: 0,
				StageTimingsMs:    timings,
			},
		}, nil
	}

	// 6. Bounded Triton Cross-Encoder reranking (Top-10 permitted) + circuit breaker + tau threshold.
	rerankCap := len(permitted)
	if rerankCap > ranking.MaxRerankInputs {
		rerankCap = ranking.MaxRerankInputs
	}
	rerankInput := permitted[:rerankCap]

	selected, rerankerFallback, cbTripped, rerankLogits := uc.rerankWithGuard(
		ctx, req.TraceID, qc, rerankInput, permitted, eval, captureTrace, timings,
	)

	// 7. Parent chunk hydration (Top-K <= 5), optional Tier 1 cache write, and audit telemetry.
	chunks, err := uc.hydrate(ctx, qc, selected, timings)
	if err != nil {
		return dto.DiagnosticSearchResponse{}, err
	}

	uc.maybeStoreTier1Cache(ctx, qc, rrfResults, rerankerFallback)

	totalDur := time.Since(totalStart)
	uc.emitAuditSpan(req.TraceID, qc, false, permitted, rerankerFallback, totalDur)

	return dto.DiagnosticSearchResponse{
		Response: dto.SearchResponse{
			RequestID:        qc.RequestID,
			TenantID:         qc.TenantID,
			CacheHit:         false,
			RerankerFallback: rerankerFallback,
			Chunks:           chunks,
			LatencyMs:        float64(totalDur.Microseconds()) / 1000.0,
		},
		ScoreTrace: dto.ScoreTrace{
			DenseCount:            safeInt32(denseCount),
			SparseCount:           safeInt32(sparseCount),
			RRFCount:              safeInt32(len(rrfResults)),
			ACLPermittedCount:     safeInt32(len(permitted)),
			CircuitBreakerTripped: cbTripped,
			RerankLogits:          rerankLogits,
			StageTimingsMs:        timings,
		},
	}, nil
}

func (uc *ExecuteSearchUseCase) tryTier1Cache(
	ctx context.Context,
	traceID string,
	qc entity.QueryContext,
	eval *acl.RoaringEvaluator,
	captureTrace bool,
	timings map[string]float64,
	totalStart time.Time,
) (dto.DiagnosticSearchResponse, bool, error) {
	entry, hit, err := uc.deps.QueryCache.Get(ctx, qc.Tier1CacheKey())
	if err != nil || !hit {
		return dto.DiagnosticSearchResponse{}, false, err
	}

	fusedBuf := uc.deps.CandidatePool.GetFusedCandidates()
	defer uc.deps.CandidatePool.PutFusedCandidates(fusedBuf)
	aclBuf := uc.deps.CandidatePool.GetFusedCandidates()
	defer uc.deps.CandidatePool.PutFusedCandidates(aclBuf)

	for _, c := range entry.Candidates {
		var metadata map[string]string
		if c.Text != "" {
			metadata = map[string]string{"text": c.Text}
		}
		*fusedBuf = append(*fusedBuf, entity.FusedCandidate{
			DocID:      c.DocID,
			ChunkID:    c.ChunkID,
			ParentKey:  c.ParentKey,
			Metadata:   metadata,
			FinalScore: c.Score,
			RRFScore:   c.Score,
		})
	}

	aclStart := time.Now()
	permitted := eval.FilterCandidates(*fusedBuf, *aclBuf)
	aclDur := time.Since(aclStart)
	uc.deps.Metrics.ObserveRoaringEval(aclDur)
	if captureTrace && timings != nil {
		timings[ComponentRRFACL] = float64(aclDur.Microseconds()) / 1000.0
	}

	// Tier 2 rule: if fewer than TopK survive ACL, bypass to live fan-out without evicting cache.
	if len(permitted) < qc.TopK {
		return dto.DiagnosticSearchResponse{}, false, nil
	}

	rerankCap := len(permitted)
	if rerankCap > ranking.MaxRerankInputs {
		rerankCap = ranking.MaxRerankInputs
	}
	rerankInput := permitted[:rerankCap]

	selected, rerankerFallback, cbTripped, rerankLogits := uc.rerankWithGuard(
		ctx, traceID, qc, rerankInput, permitted, eval, captureTrace, timings,
	)

	chunks, err := uc.hydrate(ctx, qc, selected, timings)
	if err != nil {
		return dto.DiagnosticSearchResponse{}, false, err
	}

	totalDur := time.Since(totalStart)
	uc.emitAuditSpan(traceID, qc, true, permitted, rerankerFallback, totalDur)

	return dto.DiagnosticSearchResponse{
		Response: dto.SearchResponse{
			RequestID:        qc.RequestID,
			TenantID:         qc.TenantID,
			CacheHit:         true,
			RerankerFallback: rerankerFallback,
			Chunks:           chunks,
			LatencyMs:        float64(totalDur.Microseconds()) / 1000.0,
		},
		ScoreTrace: dto.ScoreTrace{
			RRFCount:              safeInt32(len(entry.Candidates)),
			ACLPermittedCount:     safeInt32(len(permitted)),
			CircuitBreakerTripped: cbTripped,
			RerankLogits:          rerankLogits,
			StageTimingsMs:        timings,
		},
	}, true, nil
}

func (uc *ExecuteSearchUseCase) embedQuery(
	ctx context.Context,
	traceID string,
	qc entity.QueryContext,
	timings map[string]float64,
) ([]float32, bool) {
	if uc.deps.Embedder == nil {
		return nil, false
	}
	embedCtx, cancel := context.WithTimeout(ctx, uc.cfg.EmbedTimeout)
	defer cancel()

	start := time.Now()
	vec, err := uc.deps.Embedder.EmbedQuery(embedCtx, qc.NormalizedQuery, qc.EmbeddingModelVersion)
	dur := time.Since(start)
	uc.deps.Metrics.ObserveStageLatency(ComponentTritonEmbed, dur)
	if timings != nil {
		timings[ComponentTritonEmbed] = float64(dur.Microseconds()) / 1000.0
	}
	if err != nil {
		uc.deps.Metrics.IncStageError(ComponentTritonEmbed, classifyError(err))
		uc.deps.Logger.WarnContext(ctx, "triton embedding failed, degrading to sparse-only",
			slog.String("trace_id", traceID),
			slog.String("request_id", qc.RequestID),
			slog.String("tenant_id", qc.TenantID),
			slog.String("query_hash", qc.QueryHash),
			slog.String("stage", ComponentTritonEmbed),
			slog.Float64("duration_ms", float64(dur.Microseconds())/1000.0),
			slog.String("error", err.Error()),
		)
		return nil, false
	}
	return vec, true
}

func (uc *ExecuteSearchUseCase) fanout(
	ctx context.Context,
	traceID string,
	qc entity.QueryContext,
	queryVec []float32,
	embedOK bool,
	denseBuf *[]entity.SearchCandidate,
	sparseBuf *[]entity.SearchCandidate,
	timings map[string]float64,
) (denseCount int, sparseCount int, err error) {
	runDense := embedOK && uc.deps.DenseSearch != nil
	runSparse := uc.deps.SparseSearch != nil
	if !runDense && !runSparse {
		if uc.deps.DenseSearch == nil && uc.deps.SparseSearch == nil {
			return 0, 0, ErrNoSearchPortsConfigured
		}
		return 0, 0, ErrBothModalitiesFailed
	}

	fanoutCtx, cancel := context.WithTimeout(ctx, uc.cfg.FanoutTimeout)
	defer cancel()

	var (
		wg        sync.WaitGroup
		denseErr  error
		sparseErr error
		denseDur  time.Duration
		sparseDur time.Duration
	)

	if runDense {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := time.Now()
			res, dErr := uc.deps.DenseSearch.SearchDense(fanoutCtx, qc.TenantID, queryVec, ranking.MaxDenseCandidates)
			denseDur = time.Since(start)
			if dErr != nil {
				denseErr = dErr
				return
			}
			if len(res) > ranking.MaxDenseCandidates {
				res = res[:ranking.MaxDenseCandidates]
			}
			*denseBuf = append(*denseBuf, res...)
		}()
	}

	if runSparse {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := time.Now()
			res, sErr := uc.deps.SparseSearch.SearchSparse(fanoutCtx, qc.TenantID, qc.NormalizedQuery, ranking.MaxSparseCandidates)
			sparseDur = time.Since(start)
			if sErr != nil {
				sparseErr = sErr
				return
			}
			if len(res) > ranking.MaxSparseCandidates {
				res = res[:ranking.MaxSparseCandidates]
			}
			*sparseBuf = append(*sparseBuf, res...)
		}()
	}

	wg.Wait()

	if runDense {
		uc.deps.Metrics.ObserveStageLatency(ComponentHNSWDense, denseDur)
		if timings != nil {
			timings[ComponentHNSWDense] = float64(denseDur.Microseconds()) / 1000.0
		}
		if denseErr != nil {
			uc.deps.Metrics.IncStageError(ComponentHNSWDense, classifyError(denseErr))
			uc.deps.Logger.WarnContext(ctx, "dense search modality failed",
				slog.String("trace_id", traceID),
				slog.String("request_id", qc.RequestID),
				slog.String("tenant_id", qc.TenantID),
				slog.String("stage", ComponentHNSWDense),
				slog.Float64("duration_ms", float64(denseDur.Microseconds())/1000.0),
				slog.String("error", denseErr.Error()),
			)
		}
	}

	if runSparse {
		uc.deps.Metrics.ObserveStageLatency(ComponentBM25Sparse, sparseDur)
		if timings != nil {
			timings[ComponentBM25Sparse] = float64(sparseDur.Microseconds()) / 1000.0
		}
		if sparseErr != nil {
			uc.deps.Metrics.IncStageError(ComponentBM25Sparse, classifyError(sparseErr))
			uc.deps.Logger.WarnContext(ctx, "sparse search modality failed",
				slog.String("trace_id", traceID),
				slog.String("request_id", qc.RequestID),
				slog.String("tenant_id", qc.TenantID),
				slog.String("stage", ComponentBM25Sparse),
				slog.Float64("duration_ms", float64(sparseDur.Microseconds())/1000.0),
				slog.String("error", sparseErr.Error()),
			)
		}
	}

	denseOK := runDense && denseErr == nil
	sparseOK := runSparse && sparseErr == nil
	if !denseOK && !sparseOK {
		return 0, 0, errors.Join(ErrBothModalitiesFailed, denseErr, sparseErr)
	}

	return len(*denseBuf), len(*sparseBuf), nil
}

func (uc *ExecuteSearchUseCase) rerankWithGuard(
	ctx context.Context,
	traceID string,
	qc entity.QueryContext,
	rerankInput []entity.FusedCandidate,
	permittedAll []entity.FusedCandidate,
	eval *acl.RoaringEvaluator,
	captureTrace bool,
	timings map[string]float64,
) (selected []entity.FusedCandidate, rerankerFallback bool, cbTripped bool, logits []dto.CandidateLogit) {
	if uc.deps.Reranker == nil {
		return promoteRRF(permittedAll, qc.TopK), true, false, nil
	}

	var isProbe bool
	if uc.deps.Breaker != nil {
		allowed, probe, reason := uc.deps.Breaker.Allow(ctx)
		if !allowed {
			cbTripped = reason != "budget_exhausted"
			uc.deps.Logger.WarnContext(ctx, "triton reranker bypassed by circuit breaker",
				slog.String("trace_id", traceID),
				slog.String("request_id", qc.RequestID),
				slog.String("tenant_id", qc.TenantID),
				slog.String("stage", ComponentTritonRerank),
				slog.String("reason", reason),
			)
			return promoteRRF(permittedAll, qc.TopK), true, cbTripped, nil
		}
		isProbe = probe
	}

	rerankCtx, cancel := context.WithTimeout(ctx, uc.cfg.RerankTimeout)
	defer cancel()

	start := time.Now()
	reranked, err := uc.deps.Reranker.Rerank(rerankCtx, qc.NormalizedQuery, rerankInput)
	dur := time.Since(start)
	uc.deps.Metrics.ObserveStageLatency(ComponentTritonRerank, dur)
	if timings != nil {
		timings[ComponentTritonRerank] = float64(dur.Microseconds()) / 1000.0
	}
	if uc.deps.Breaker != nil {
		uc.deps.Breaker.Record(dur, err, isProbe)
	}

	if err != nil {
		uc.deps.Metrics.IncStageError(ComponentTritonRerank, classifyError(err))
		uc.deps.Logger.WarnContext(ctx, "triton reranker failed, promoting RRF fallback",
			slog.String("trace_id", traceID),
			slog.String("request_id", qc.RequestID),
			slog.String("tenant_id", qc.TenantID),
			slog.String("stage", ComponentTritonRerank),
			slog.Float64("duration_ms", float64(dur.Microseconds())/1000.0),
			slog.String("error", err.Error()),
		)
		return promoteRRF(permittedAll, qc.TopK), true, false, nil
	}

	if captureTrace {
		logits = make([]dto.CandidateLogit, len(reranked))
		for i, c := range reranked {
			logits[i] = dto.CandidateLogit{
				DocID:   c.DocID,
				ChunkID: c.ChunkID,
				Logit:   c.RerankLogit,
			}
		}
	}

	selected, fallbackUsed := uc.cascade.SelectTopKWithThreshold(reranked, permittedAll, eval, qc.TopK, uc.cfg.RerankTau)
	return selected, fallbackUsed, false, logits
}

func (uc *ExecuteSearchUseCase) hydrate(
	ctx context.Context,
	qc entity.QueryContext,
	selected []entity.FusedCandidate,
	timings map[string]float64,
) ([]dto.EnrichedChunk, error) {
	if len(selected) == 0 {
		return []dto.EnrichedChunk{}, nil
	}

	start := time.Now()
	// Metadata-only fallback stub allowed ONLY when ParentChunkPort == nil (pre-Phase 4 wiring).
	if uc.deps.ParentChunks == nil {
		out := make([]dto.EnrichedChunk, len(selected))
		for i, c := range selected {
			out[i] = dto.EnrichedChunk{
				DocID:     c.DocID,
				ChunkID:   c.ChunkID,
				ParentKey: c.ParentKey,
				Score:     c.FinalScore,
				Metadata:  c.Metadata,
			}
		}
		if timings != nil {
			timings[ComponentRedisHydrate] = float64(time.Since(start).Microseconds()) / 1000.0
		}
		return out, nil
	}

	hydrateCtx, cancel := context.WithTimeout(ctx, uc.cfg.HydrateTimeout)
	defer cancel()

	hydrated, err := uc.deps.ParentChunks.HydrateParents(hydrateCtx, qc.TenantID, selected)
	dur := time.Since(start)
	uc.deps.Metrics.ObserveStageLatency(ComponentRedisHydrate, dur)
	if timings != nil {
		timings[ComponentRedisHydrate] = float64(dur.Microseconds()) / 1000.0
	}
	if err != nil {
		uc.deps.Metrics.IncStageError(ComponentRedisHydrate, classifyError(err))
		return nil, err
	}

	out := make([]dto.EnrichedChunk, len(hydrated))
	for i, h := range hydrated {
		out[i] = dto.EnrichedChunk{
			DocID:     h.DocID,
			ChunkID:   h.ChunkID,
			ParentKey: h.ParentKey,
			Text:      h.Text,
			Score:     h.Score,
			Metadata:  h.Metadata,
		}
	}
	return out, nil
}

func (uc *ExecuteSearchUseCase) maybeStoreTier1Cache(
	ctx context.Context,
	qc entity.QueryContext,
	preACLRRF []entity.FusedCandidate,
	rerankerFallback bool,
) {
	if uc.deps.QueryCache == nil || len(preACLRRF) < entity.MaxTopK {
		return
	}
	cached := make([]repository.CachedCandidate, len(preACLRRF))
	for i, c := range preACLRRF {
		cached[i] = repository.CachedCandidate{
			DocID:     c.DocID,
			ChunkID:   c.ChunkID,
			ParentKey: c.ParentKey,
			Text:      c.Metadata["text"],
			Score:     c.RRFScore,
		}
	}
	if err := uc.deps.QueryCache.Set(ctx, qc.Tier1CacheKey(), repository.Tier1CacheEntry{
		RerankerFallback: rerankerFallback,
		Candidates:       cached,
	}); err != nil {
		uc.deps.Logger.WarnContext(ctx, "tier1 cache set failed",
			slog.String("request_id", qc.RequestID),
			slog.String("tenant_id", qc.TenantID),
			slog.String("query_hash", qc.QueryHash),
			slog.String("error", err.Error()),
		)
	}
}

func safeInt32(v int) int32 {
	if v < 0 {
		return 0
	}
	if v > math.MaxInt32 {
		return math.MaxInt32
	}
	return int32(v)
}

func (uc *ExecuteSearchUseCase) emitAuditSpan(
	traceID string,
	qc entity.QueryContext,
	cacheHit bool,
	permitted []entity.FusedCandidate,
	rerankerFallback bool,
	totalDur time.Duration,
) {
	if uc.deps.Telemetry == nil {
		return
	}
	var span repository.SearchAuditSpan
	span.TraceID = traceID
	span.RequestID = qc.RequestID
	span.TenantID = qc.TenantID
	span.QueryHash = qc.QueryHash
	span.CacheHit = cacheHit
	span.RerankerFallback = rerankerFallback
	span.TotalLatencyUs = totalDur.Microseconds()
	span.TimestampUnixNs = time.Now().UnixNano()

	var permittedCount uint8
	for i := 0; i < len(permitted) && i < len(span.PermittedDocIDs); i++ {
		span.PermittedDocIDs[i] = permitted[i].DocID
		permittedCount++
	}
	span.PermittedCount = permittedCount

	uc.deps.Telemetry.Emit(span)
}

func promoteRRF(permittedAll []entity.FusedCandidate, topK int) []entity.FusedCandidate {
	if topK > len(permittedAll) {
		topK = len(permittedAll)
	}
	out := make([]entity.FusedCandidate, topK)
	for i := 0; i < topK; i++ {
		out[i] = permittedAll[i]
		out[i].FinalScore = out[i].RRFScore
	}
	return out
}

func classifyError(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "DeadlineExceeded"
	}
	if errors.Is(err, context.Canceled) {
		return "Canceled"
	}
	var grpcStatus interface{ GRPCStatus() *status.Status }
	if errors.As(err, &grpcStatus) {
		switch grpcStatus.GRPCStatus().Code() {
		case codes.DeadlineExceeded:
			return "DeadlineExceeded"
		case codes.Canceled:
			return "Canceled"
		}
	}
	return "Internal"
}
