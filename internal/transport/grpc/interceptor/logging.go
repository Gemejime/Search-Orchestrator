package interceptor

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"runtime/debug"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type ctxKey uint8

const (
	traceIDKey ctxKey = iota + 1
	requestIDKey
)

const (
	// MetadataTraceID is the incoming gRPC metadata key for trace correlation.
	MetadataTraceID = "x-trace-id"
	// MetadataRequestID is the incoming gRPC metadata key for request correlation.
	MetadataRequestID = "x-request-id"
)

// tenantCarrier allows interceptors to extract tenant_id from request messages without importing protobuf stubs directly.
type tenantCarrier interface {
	GetTenantId() string
}

// requestIDCarrier allows interceptors to extract request_id from request messages.
type requestIDCarrier interface {
	GetRequestId() string
}

// TraceIDFromContext returns the correlated trace_id stored in ctx, or empty string if absent.
func TraceIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(traceIDKey).(string); ok {
		return v
	}
	return ""
}

// RequestIDFromContext returns the correlated request_id stored in ctx, or empty string if absent.
func RequestIDFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(requestIDKey).(string); ok {
		return v
	}
	return ""
}

// WithCorrelationIDs attaches trace_id and request_id to ctx.
func WithCorrelationIDs(ctx context.Context, traceID, requestID string) context.Context {
	ctx = context.WithValue(ctx, traceIDKey, traceID)
	ctx = context.WithValue(ctx, requestIDKey, requestID)
	return ctx
}

// NewCorrelationID generates a 128-bit random hex identifier.
func NewCorrelationID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "00000000000000000000000000000000"
	}
	return hex.EncodeToString(b[:])
}

// UnaryServerInterceptor provides panic recovery, trace_id/request_id correlation, and secured
// structured JSON logging for all inbound gRPC unary RPCs.
// Strictly redacts Authorization, bearer tokens, raw caller_acl_bitmap bytes, credentials, and query PII.
func UnaryServerInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	if logger == nil {
		logger = slog.Default()
	}
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp any, err error) {
		start := time.Now()

		traceID, reqID := extractGRPCIDs(ctx, req)
		ctx = WithCorrelationIDs(ctx, traceID, reqID)

		var tenantID string
		if tc, ok := req.(tenantCarrier); ok {
			tenantID = tc.GetTenantId()
		}

		defer func() {
			if r := recover(); r != nil {
				logger.ErrorContext(ctx, "grpc handler panic recovered",
					slog.String("trace_id", traceID),
					slog.String("request_id", reqID),
					slog.String("tenant_id", tenantID),
					slog.String("grpc_method", info.FullMethod),
					slog.Int("stack_bytes", len(debug.Stack())),
				)
				err = status.Error(codes.Internal, "internal server error")
			}
		}()

		resp, err = handler(ctx, req)

		code := status.Code(err)
		durMs := float64(time.Since(start).Microseconds()) / 1000.0

		logAttrs := []any{
			slog.String("trace_id", traceID),
			slog.String("request_id", reqID),
			slog.String("tenant_id", tenantID),
			slog.String("grpc_method", info.FullMethod),
			slog.String("grpc_code", code.String()),
			slog.Float64("duration_ms", durMs),
		}
		if err != nil {
			logger.WarnContext(ctx, "grpc request completed with error", logAttrs...)
		} else {
			logger.InfoContext(ctx, "grpc request completed", logAttrs...)
		}

		return resp, err
	}
}

func extractGRPCIDs(ctx context.Context, req any) (traceID, requestID string) {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if vals := md.Get(MetadataTraceID); len(vals) > 0 {
			traceID = strings.TrimSpace(vals[0])
		}
		if vals := md.Get(MetadataRequestID); len(vals) > 0 {
			requestID = strings.TrimSpace(vals[0])
		}
	}
	if rc, ok := req.(requestIDCarrier); ok {
		if id := strings.TrimSpace(rc.GetRequestId()); id != "" {
			requestID = id
		}
	}
	if traceID == "" {
		traceID = NewCorrelationID()
	}
	if requestID == "" {
		requestID = NewCorrelationID()
	}
	return traceID, requestID
}
