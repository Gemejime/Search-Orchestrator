package unit

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"math"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/rag-platform/rag-search-orchestrator/internal/domain/entity"
	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/grpcutil"
	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/pb/downstreamv1"
	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/pb/tritonv2"
	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/search/dense"
	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/search/sparse"
	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/triton"
)

type mockDenseServer struct {
	downstreamv1.UnimplementedDenseSearchServiceServer
	delay      time.Duration
	candidates []*downstreamv1.DownstreamCandidate
	err        error
}

func (m *mockDenseServer) Search(ctx context.Context, req *downstreamv1.DenseSearchRequest) (*downstreamv1.DenseSearchResponse, error) {
	if m.delay > 0 {
		select {
		case <-time.After(m.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if m.err != nil {
		return nil, m.err
	}
	return &downstreamv1.DenseSearchResponse{Candidates: m.candidates}, nil
}

type mockSparseServer struct {
	downstreamv1.UnimplementedSparseSearchServiceServer
	delay      time.Duration
	candidates []*downstreamv1.DownstreamCandidate
	err        error
}

func (m *mockSparseServer) Search(ctx context.Context, req *downstreamv1.SparseSearchRequest) (*downstreamv1.SparseSearchResponse, error) {
	if m.delay > 0 {
		select {
		case <-time.After(m.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if m.err != nil {
		return nil, m.err
	}
	return &downstreamv1.SparseSearchResponse{Candidates: m.candidates}, nil
}

type mockTritonServer struct {
	tritonv2.UnimplementedGRPCInferenceServiceServer
	embedFP16  bool
	embedVec   []float32
	embedErr   string
	rerankResp []float32
	delay      time.Duration
}

func (m *mockTritonServer) ModelStreamInfer(stream grpc.BidiStreamingServer[tritonv2.ModelInferRequest, tritonv2.ModelStreamInferResponse]) error {
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if m.delay > 0 {
			time.Sleep(m.delay)
		}
		if m.embedErr != "" {
			_ = stream.Send(&tritonv2.ModelStreamInferResponse{ErrorMessage: m.embedErr})
			continue
		}
		resp := &tritonv2.ModelInferResponse{
			ModelName:    req.GetModelName(),
			ModelVersion: req.GetModelVersion(),
		}
		if m.embedFP16 {
			// Encode 1.0 (0x3C00) and 0.5 (0x3800) as FP16 raw bytes.
			raw := make([]byte, 4)
			binary.LittleEndian.PutUint16(raw[0:2], 0x3C00)
			binary.LittleEndian.PutUint16(raw[2:4], 0x3800)
			resp.Outputs = []*tritonv2.ModelInferResponse_InferOutputTensor{
				{Name: "EMBEDDING", Datatype: "FP16", Shape: []int64{1, 2}},
			}
			resp.RawOutputContents = [][]byte{raw}
		} else {
			resp.Outputs = []*tritonv2.ModelInferResponse_InferOutputTensor{
				{
					Name:     "EMBEDDING",
					Datatype: "FP32",
					Shape:    []int64{1, int64(len(m.embedVec))},
					Contents: &tritonv2.InferTensorContents{Fp32Contents: m.embedVec},
				},
			}
		}
		if err := stream.Send(&tritonv2.ModelStreamInferResponse{InferResponse: resp}); err != nil {
			return err
		}
	}
}

func (m *mockTritonServer) ModelInfer(ctx context.Context, req *tritonv2.ModelInferRequest) (*tritonv2.ModelInferResponse, error) {
	if m.delay > 0 {
		select {
		case <-time.After(m.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return &tritonv2.ModelInferResponse{
		ModelName: req.GetModelName(),
		Outputs: []*tritonv2.ModelInferResponse_InferOutputTensor{
			{
				Name:     "LOGITS",
				Datatype: "FP32",
				Shape:    []int64{int64(len(m.rerankResp)), 1},
				Contents: &tritonv2.InferTensorContents{Fp32Contents: m.rerankResp},
			},
		},
	}, nil
}

func startBufconnPool(t *testing.T, register func(*grpc.Server)) *grpcutil.ConnPool {
	t.Helper()
	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	register(srv)
	go func() {
		_ = srv.Serve(lis)
	}()
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
	pool, err := grpcutil.NewConnPoolFromConns([]*grpc.ClientConn{conn})
	if err != nil {
		t.Fatalf("NewConnPoolFromConns: %v", err)
	}
	warmCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := pool.Warmup(warmCtx); err != nil {
		t.Fatalf("pool.Warmup: %v", err)
	}
	t.Cleanup(func() { _ = pool.Close() })
	return pool
}

func TestHNSWClient_BufconnAndTimeout(t *testing.T) {
	t.Parallel()

	cands := make([]*downstreamv1.DownstreamCandidate, 50)
	for i := range cands {
		cands[i] = &downstreamv1.DownstreamCandidate{
			DocId:   uint32(100 + i),
			ChunkId: "chk",
			Score:   float64(50 - i),
		}
	}

	pool := startBufconnPool(t, func(s *grpc.Server) {
		downstreamv1.RegisterDenseSearchServiceServer(s, &mockDenseServer{candidates: cands})
	})

	client := dense.NewHNSWClient(pool, 20*time.Millisecond, slog.Default())
	got, err := client.SearchDense(context.Background(), "tenant-1", []float32{0.1, 0.2}, 40)
	if err != nil {
		t.Fatalf("SearchDense failed: %v", err)
	}
	if len(got) != 40 {
		t.Fatalf("expected Top-40 cap, got %d", len(got))
	}
	if got[0].Modality != entity.ModalityDense || got[0].Rank != 1 {
		t.Fatalf("unexpected candidate metadata: %+v", got[0])
	}

	// Timeout enforcement
	slowPool := startBufconnPool(t, func(s *grpc.Server) {
		downstreamv1.RegisterDenseSearchServiceServer(s, &mockDenseServer{delay: 60 * time.Millisecond})
	})
	slowClient := dense.NewHNSWClient(slowPool, 15*time.Millisecond, slog.Default())
	if _, err := slowClient.SearchDense(context.Background(), "tenant-1", []float32{0.1}, 40); err == nil {
		t.Fatal("expected deadline exceeded error from slow HNSW server")
	}
}

func TestBM25Client_PartialShardFailureAndDeterministicMerge(t *testing.T) {
	t.Parallel()

	shard1 := startBufconnPool(t, func(s *grpc.Server) {
		downstreamv1.RegisterSparseSearchServiceServer(s, &mockSparseServer{
			candidates: []*downstreamv1.DownstreamCandidate{
				{DocId: 2, ChunkId: "c1", Score: 5.0},
				{DocId: 1, ChunkId: "c1", Score: 5.0},
			},
		})
	})
	shard2 := startBufconnPool(t, func(s *grpc.Server) {
		downstreamv1.RegisterSparseSearchServiceServer(s, &mockSparseServer{
			err: status.Error(codes.Unavailable, "shard 2 down"),
		})
	})
	shard3 := startBufconnPool(t, func(s *grpc.Server) {
		downstreamv1.RegisterSparseSearchServiceServer(s, &mockSparseServer{
			candidates: []*downstreamv1.DownstreamCandidate{
				{DocId: 3, ChunkId: "c1", Score: 9.0},
				{DocId: 1, ChunkId: "c1", Score: 7.0}, // higher score for same (1, c1)
			},
		})
	})

	client := sparse.NewBM25Client([]*grpcutil.ConnPool{shard1, shard2, shard3}, 20*time.Millisecond, slog.Default())
	got, err := client.SearchSparse(context.Background(), "tenant-1", "soc2 rotation", 40)
	if err != nil {
		t.Fatalf("expected partial shard failure tolerance, got err: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 deduplicated candidates, got %d", len(got))
	}
	if got[0].DocID != 3 || got[1].DocID != 1 || got[2].DocID != 2 {
		t.Fatalf("unexpected deterministic order: %+v", got)
	}
	if got[1].Score != 7.0 {
		t.Fatalf("expected deduplication to keep higher score 7.0, got %v", got[1].Score)
	}

	// All shards failing must return error.
	allFail := sparse.NewBM25Client([]*grpcutil.ConnPool{shard2}, 20*time.Millisecond, slog.Default())
	if _, err := allFail.SearchSparse(context.Background(), "tenant-1", "soc2", 40); err == nil {
		t.Fatal("expected error when all BM25 shards fail")
	}
}

func TestTritonEmbeddingAndRerankerClients(t *testing.T) {
	t.Parallel()

	fp16Pool := startBufconnPool(t, func(s *grpc.Server) {
		tritonv2.RegisterGRPCInferenceServiceServer(s, &mockTritonServer{
			embedFP16:  true,
			rerankResp: []float32{2.5, -1.0},
		})
	})

	embClient := triton.NewEmbeddingClient(fp16Pool, "bge-m3", 100*time.Millisecond, slog.Default())
	vec, err := embClient.EmbedQuery(context.Background(), "encryption rotation", "v2")
	if err != nil {
		t.Fatalf("EmbedQuery FP16 failed: %v", err)
	}
	if len(vec) != 2 || math.Abs(float64(vec[0]-1.0)) > 1e-3 || math.Abs(float64(vec[1]-0.5)) > 1e-3 {
		t.Fatalf("unexpected FP16 decoded vector: %v", vec)
	}

	rerankClient := triton.NewRerankerClient(fp16Pool, "bge-reranker", "1", 100*time.Millisecond, slog.Default())
	reranked, err := rerankClient.Rerank(context.Background(), "encryption rotation", []entity.FusedCandidate{
		{DocID: 10, ChunkID: "c1", Metadata: map[string]string{"text": "passage 1"}},
		{DocID: 11, ChunkID: "c2", Metadata: map[string]string{"text": "passage 2"}},
	})
	if err != nil {
		t.Fatalf("Rerank failed: %v", err)
	}
	if len(reranked) != 2 || reranked[0].RerankLogit != 2.5 || reranked[1].RerankLogit != -1.0 {
		t.Fatalf("unexpected reranked logits: %+v", reranked)
	}
}
