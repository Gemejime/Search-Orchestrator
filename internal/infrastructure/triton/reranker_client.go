package triton

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/rag-platform/rag-search-orchestrator/internal/domain/entity"
	"github.com/rag-platform/rag-search-orchestrator/internal/domain/ranking"
	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/grpcutil"
	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/pb/tritonv2"
)

const (
	// DefaultRerankTimeout is the hard deadline for Triton Cross-Encoder reranking (35ms).
	DefaultRerankTimeout = 35 * time.Millisecond
	// DefaultRerankModelName is the default Triton Cross-Encoder model name.
	DefaultRerankModelName = "bge-reranker-cross-encoder"
)

// RerankerClient dispatches batched Top-10 Cross-Encoder scoring to Triton ModelInfer.
type RerankerClient struct {
	pool         *grpcutil.ConnPool
	modelName    string
	modelVersion string
	timeout      time.Duration
	logger       *slog.Logger
}

// NewRerankerClient constructs a RerankerClient with a 35ms hard deadline.
func NewRerankerClient(
	pool *grpcutil.ConnPool,
	modelName string,
	modelVersion string,
	timeout time.Duration,
	logger *slog.Logger,
) *RerankerClient {
	if modelName == "" {
		modelName = DefaultRerankModelName
	}
	if timeout <= 0 {
		timeout = DefaultRerankTimeout
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &RerankerClient{
		pool:         pool,
		modelName:    modelName,
		modelVersion: modelVersion,
		timeout:      timeout,
		logger:       logger,
	}
}

// Rerank scores up to Top-10 ACL-permitted candidates against queryText via Triton ModelInfer.
func (c *RerankerClient) Rerank(
	ctx context.Context,
	queryText string,
	candidates []entity.FusedCandidate,
) ([]entity.FusedCandidate, error) {
	if len(candidates) == 0 {
		return nil, nil
	}
	if len(candidates) > ranking.MaxRerankInputs {
		candidates = candidates[:ranking.MaxRerankInputs]
	}

	callCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	conn, err := c.pool.Get()
	if err != nil {
		return nil, err
	}

	batchSize := len(candidates)
	queries := make([][]byte, batchSize)
	passages := make([][]byte, batchSize)
	queryBytes := []byte(queryText)

	for i, cand := range candidates {
		queries[i] = queryBytes
		if txt, ok := cand.Metadata["text"]; ok && txt != "" {
			passages[i] = []byte(txt)
		} else {
			passages[i] = []byte(cand.ChunkID)
		}
	}

	req := &tritonv2.ModelInferRequest{
		ModelName:    c.modelName,
		ModelVersion: c.modelVersion,
		Inputs: []*tritonv2.ModelInferRequest_InferInputTensor{
			{
				Name:     "QUERY",
				Datatype: "BYTES",
				Shape:    []int64{int64(batchSize), 1},
				Contents: &tritonv2.InferTensorContents{BytesContents: queries},
			},
			{
				Name:     "PASSAGE",
				Datatype: "BYTES",
				Shape:    []int64{int64(batchSize), 1},
				Contents: &tritonv2.InferTensorContents{BytesContents: passages},
			},
		},
		Outputs: []*tritonv2.ModelInferRequest_InferRequestedOutputTensor{
			{Name: "LOGITS"},
		},
	}

	client := tritonv2.NewGRPCInferenceServiceClient(conn)
	resp, err := client.ModelInfer(callCtx, req)
	if err != nil {
		return nil, fmt.Errorf("triton rerank rpc: %w", err)
	}

	logits, err := extractFloat32Tensor(resp, 0)
	if err != nil {
		return nil, err
	}
	if len(logits) != batchSize {
		return nil, fmt.Errorf("triton rerank: expected %d logits, got %d", batchSize, len(logits))
	}

	out := make([]entity.FusedCandidate, batchSize)
	copy(out, candidates)
	for i := range out {
		out[i].RerankLogit = float64(logits[i])
	}
	return out, nil
}
