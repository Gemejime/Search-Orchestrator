package unit

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rag-platform/rag-search-orchestrator/internal/domain/repository"
	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/telemetry"
)

type recordingSink struct {
	mu      sync.Mutex
	spans   []repository.SearchAuditSpan
	blockCh chan struct{}
}

func (s *recordingSink) WriteSpan(ctx context.Context, span repository.SearchAuditSpan) error {
	if s.blockCh != nil {
		select {
		case <-s.blockCh:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s.mu.Lock()
	s.spans = append(s.spans, span)
	s.mu.Unlock()
	return nil
}

func (s *recordingSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.spans)
}

func TestRingBufferEmitter_ZeroAllocEmit(t *testing.T) {
	emitter := telemetry.NewRingBufferEmitter(1024, telemetry.NoopAuditSink{}, nil)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = emitter.Close(ctx)
	}()

	span := repository.SearchAuditSpan{
		TraceID:          "trace-123",
		RequestID:        "req-123",
		TenantID:         "tenant-1",
		QueryHash:        "hash-123",
		CacheHit:         true,
		PermittedCount:   3,
		RerankerFallback: false,
		TotalLatencyUs:   1200,
		TimestampUnixNs:  1700000000,
	}
	span.PermittedDocIDs[0] = 10
	span.PermittedDocIDs[1] = 20
	span.PermittedDocIDs[2] = 30

	allocs := testing.AllocsPerRun(200, func() {
		emitter.Emit(span)
	})
	if allocs != 0 {
		t.Fatalf("expected 0 heap allocations on Emit(), got %.2f", allocs)
	}
}

func TestRingBufferEmitter_ConcurrentMPSCAndFlush(t *testing.T) {
	t.Parallel()

	sink := &recordingSink{}
	emitter := telemetry.NewRingBufferEmitter(4096, sink, nil)

	const producers = 8
	const perProducer = 200

	var wg sync.WaitGroup
	wg.Add(producers)
	for p := 0; p < producers; p++ {
		go func(pid int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				emitter.Emit(repository.SearchAuditSpan{
					TraceID:        "trace",
					RequestID:      "req",
					TenantID:       "tenant-1",
					PermittedCount: uint8(pid),
					TotalLatencyUs: int64(i),
				})
			}
		}(p)
	}
	wg.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := emitter.Close(ctx); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	if emitter.DroppedCount() != 0 {
		t.Fatalf("expected 0 dropped spans, got %d", emitter.DroppedCount())
	}
	if got := sink.count(); got != producers*perProducer {
		t.Fatalf("expected %d delivered spans, got %d", producers*perProducer, got)
	}
}

func TestRingBufferEmitter_DropOnFullNonBlocking(t *testing.T) {
	t.Parallel()

	blockCh := make(chan struct{})
	sink := &recordingSink{blockCh: blockCh}
	emitter := telemetry.NewRingBufferEmitter(4, sink, nil)

	// Emit more than capacity while consumer is blocked on the first span.
	for i := 0; i < 32; i++ {
		emitter.Emit(repository.SearchAuditSpan{
			TenantID:       "tenant-1",
			TotalLatencyUs: int64(i),
		})
	}

	if emitter.DroppedCount() == 0 {
		t.Fatalf("expected DroppedCount > 0 when ring buffer is saturated")
	}

	close(blockCh)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := emitter.Close(ctx); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Emitting after Close must also be non-blocking and increment DroppedCount.
	before := emitter.DroppedCount()
	emitter.Emit(repository.SearchAuditSpan{TenantID: "tenant-1"})
	if emitter.DroppedCount() <= before {
		t.Fatalf("expected DroppedCount to increment after Close")
	}
}

func BenchmarkRingBufferEmitter_Emit(b *testing.B) {
	var consumed atomic.Uint64
	emitter := telemetry.NewRingBufferEmitter(16384, sinkFunc(func(context.Context, repository.SearchAuditSpan) error {
		consumed.Add(1)
		return nil
	}), nil)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = emitter.Close(ctx)
	}()

	span := repository.SearchAuditSpan{
		TraceID:        "trace-bench",
		RequestID:      "req-bench",
		TenantID:       "tenant-1",
		QueryHash:      "hash-bench",
		PermittedCount: 5,
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			emitter.Emit(span)
		}
	})
}

type sinkFunc func(ctx context.Context, span repository.SearchAuditSpan) error

func (f sinkFunc) WriteSpan(ctx context.Context, span repository.SearchAuditSpan) error {
	return f(ctx, span)
}
