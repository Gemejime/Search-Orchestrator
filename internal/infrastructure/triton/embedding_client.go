package triton

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/grpcutil"
	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/pb/tritonv2"
)

const (
	// DefaultEmbedTimeout is the hard deadline for Triton FP16 query embedding (15ms).
	DefaultEmbedTimeout = 15 * time.Millisecond
	// DefaultEmbedModelName is the default Triton embedding model name.
	DefaultEmbedModelName = "bge-m3-embedding"
)

var (
	// ErrEmptyQueryText is returned when queryText is empty.
	ErrEmptyQueryText = errors.New("triton: queryText must not be empty")
	// ErrEmptyTritonResponse is returned when Triton returns no output tensor data.
	ErrEmptyTritonResponse = errors.New("triton: empty inference response")
)

// EmbeddingClient dispatches FP16 query embedding inference to Triton over ModelStreamInfer.
type EmbeddingClient struct {
	pool      *grpcutil.ConnPool
	modelName string
	timeout   time.Duration
	logger    *slog.Logger
}

// NewEmbeddingClient constructs an EmbeddingClient with a 15ms default deadline.
func NewEmbeddingClient(
	pool *grpcutil.ConnPool,
	modelName string,
	timeout time.Duration,
	logger *slog.Logger,
) *EmbeddingClient {
	if modelName == "" {
		modelName = DefaultEmbedModelName
	}
	if timeout <= 0 {
		timeout = DefaultEmbedTimeout
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &EmbeddingClient{
		pool:      pool,
		modelName: modelName,
		timeout:   timeout,
		logger:    logger,
	}
}

// EmbedQuery generates a dense float32 query vector via Triton GRPCInferenceService/ModelStreamInfer.
func (c *EmbeddingClient) EmbedQuery(
	ctx context.Context,
	queryText string,
	modelVersion string,
) ([]float32, error) {
	if queryText == "" {
		return nil, ErrEmptyQueryText
	}

	callCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	conn, err := c.pool.Get()
	if err != nil {
		return nil, err
	}

	client := tritonv2.NewGRPCInferenceServiceClient(conn)
	stream, err := client.ModelStreamInfer(callCtx)
	if err != nil {
		return nil, fmt.Errorf("triton embed stream open: %w", err)
	}

	req := &tritonv2.ModelInferRequest{
		ModelName:    c.modelName,
		ModelVersion: modelVersion,
		Inputs: []*tritonv2.ModelInferRequest_InferInputTensor{
			{
				Name:     "TEXT",
				Datatype: "BYTES",
				Shape:    []int64{1, 1},
				Contents: &tritonv2.InferTensorContents{
					BytesContents: [][]byte{[]byte(queryText)},
				},
			},
		},
		Outputs: []*tritonv2.ModelInferRequest_InferRequestedOutputTensor{
			{Name: "EMBEDDING"},
		},
	}

	if err := stream.Send(req); err != nil {
		return nil, fmt.Errorf("triton embed stream send: %w", err)
	}
	if err := stream.CloseSend(); err != nil {
		return nil, fmt.Errorf("triton embed stream close send: %w", err)
	}

	streamResp, err := stream.Recv()
	if err != nil {
		return nil, fmt.Errorf("triton embed stream recv: %w", err)
	}
	if msg := streamResp.GetErrorMessage(); msg != "" {
		return nil, fmt.Errorf("triton embed inference error: %s", msg)
	}

	inferResp := streamResp.GetInferResponse()
	if inferResp == nil || len(inferResp.GetOutputs()) == 0 {
		return nil, ErrEmptyTritonResponse
	}

	vec, err := extractFloat32Tensor(inferResp, 0)
	if err != nil {
		return nil, err
	}
	return vec, nil
}

func extractFloat32Tensor(resp *tritonv2.ModelInferResponse, idx int) ([]float32, error) {
	outputs := resp.GetOutputs()
	if idx >= len(outputs) {
		return nil, ErrEmptyTritonResponse
	}
	out := outputs[idx]
	if contents := out.GetContents(); contents != nil && len(contents.GetFp32Contents()) > 0 {
		fp32 := contents.GetFp32Contents()
		res := make([]float32, len(fp32))
		copy(res, fp32)
		return res, nil
	}

	rawOutputs := resp.GetRawOutputContents()
	if idx >= len(rawOutputs) || len(rawOutputs[idx]) == 0 {
		return nil, ErrEmptyTritonResponse
	}
	raw := rawOutputs[idx]

	switch out.GetDatatype() {
	case "FP16":
		if len(raw)%2 != 0 {
			return nil, fmt.Errorf("triton: invalid FP16 byte length %d", len(raw))
		}
		res := make([]float32, len(raw)/2)
		for i := range res {
			bits := binary.LittleEndian.Uint16(raw[i*2 : i*2+2])
			res[i] = float16ToFloat32(bits)
		}
		return res, nil
	default: // FP32
		if len(raw)%4 != 0 {
			return nil, fmt.Errorf("triton: invalid FP32 byte length %d", len(raw))
		}
		res := make([]float32, len(raw)/4)
		for i := range res {
			bits := binary.LittleEndian.Uint32(raw[i*4 : i*4+4])
			res[i] = math.Float32frombits(bits)
		}
		return res, nil
	}
}

func float16ToFloat32(h uint16) float32 {
	sign := uint32(h&0x8000) << 16
	exp := uint32(h&0x7C00) >> 10
	mant := uint32(h & 0x03FF)

	switch exp {
	case 0:
		if mant == 0 {
			return math.Float32frombits(sign)
		}
		// Subnormal FP16 -> normalized FP32
		exp = 127 - 15 + 1
		for (mant & 0x0400) == 0 {
			mant <<= 1
			exp--
		}
		mant &= 0x03FF
		return math.Float32frombits(sign | (exp << 23) | (mant << 13))
	case 0x1F:
		return math.Float32frombits(sign | 0x7F800000 | (mant << 13))
	default:
		return math.Float32frombits(sign | ((exp + 127 - 15) << 23) | (mant << 13))
	}
}
