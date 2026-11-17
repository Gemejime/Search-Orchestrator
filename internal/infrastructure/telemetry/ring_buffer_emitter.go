package telemetry

import (
	"context"
	"errors"
	"log/slog"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/rag-platform/rag-search-orchestrator/internal/domain/repository"
)

const (
	// DefaultRingBufferCapacity is the default power-of-2 slot count for the MPSC audit ring buffer.
	DefaultRingBufferCapacity = 8192
	// defaultSinkWriteTimeout bounds individual AuditSink writes during background draining.
	defaultSinkWriteTimeout = 2 * time.Second
)

// AuditSink receives drained compliance audit spans asynchronously off the critical search path.
type AuditSink interface {
	WriteSpan(ctx context.Context, span repository.SearchAuditSpan) error
}

// NoopAuditSink discards audit spans when no external telemetry daemon sink is configured.
type NoopAuditSink struct{}

// WriteSpan implements AuditSink as a no-op.
func (NoopAuditSink) WriteSpan(context.Context, repository.SearchAuditSpan) error {
	return nil
}

// LogAuditSink emits zero-PII audit spans as structured JSON via log/slog.
type LogAuditSink struct {
	Logger *slog.Logger
}

// WriteSpan implements AuditSink by logging non-PII provenance metadata.
func (s LogAuditSink) WriteSpan(ctx context.Context, span repository.SearchAuditSpan) error {
	if s.Logger == nil {
		return nil
	}
	s.Logger.InfoContext(ctx, "audit_span",
		slog.String("trace_id", span.TraceID),
		slog.String("request_id", span.RequestID),
		slog.String("tenant_id", span.TenantID),
		slog.String("query_hash", span.QueryHash),
		slog.Bool("cache_hit", span.CacheHit),
		slog.Int("permitted_count", int(span.PermittedCount)),
		slog.Bool("reranker_fallback", span.RerankerFallback),
		slog.Int64("total_latency_us", span.TotalLatencyUs),
		slog.Int64("timestamp_unix_ns", span.TimestampUnixNs),
	)
	return nil
}

type ringSlot struct {
	seq  atomic.Uint64
	span repository.SearchAuditSpan
}

// RingBufferEmitter implements repository.TelemetryEmitterPort using a bounded, lock-free
// Vyukov MPSC ring buffer with zero heap allocations on Emit() and non-blocking drop-on-full.
type RingBufferEmitter struct {
	slots   []ringSlot
	mask    uint64
	head    atomic.Uint64
	tail    uint64
	dropped atomic.Uint64
	closed  atomic.Bool

	sink   AuditSink
	logger *slog.Logger
	notify chan struct{}
	stopCh chan struct{}
	doneCh chan struct{}
}

// NewRingBufferEmitter constructs a lock-free MPSC RingBufferEmitter and starts its background consumer.
// Capacity is rounded up to the next power of 2 (minimum 2; 0 defaults to DefaultRingBufferCapacity).
func NewRingBufferEmitter(capacity int, sink AuditSink, logger *slog.Logger) *RingBufferEmitter {
	if capacity <= 0 {
		capacity = DefaultRingBufferCapacity
	}
	pow2 := nextPowerOfTwo(capacity)
	if pow2 < 2 {
		pow2 = 2
	}
	if sink == nil {
		sink = NoopAuditSink{}
	}
	if logger == nil {
		logger = slog.Default()
	}

	slots := make([]ringSlot, pow2)
	for i := range slots {
		slots[i].seq.Store(uint64(i))
	}

	e := &RingBufferEmitter{
		slots:  slots,
		mask:   uint64(pow2 - 1),
		sink:   sink,
		logger: logger,
		notify: make(chan struct{}, 1),
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
	go e.consumeLoop()
	return e
}

// Emit enqueues a SearchAuditSpan without blocking or allocating on the heap.
// If the ring buffer is full or closed, the span is dropped and DroppedCount is incremented.
func (e *RingBufferEmitter) Emit(span repository.SearchAuditSpan) {
	if e == nil || e.closed.Load() {
		if e != nil {
			e.dropped.Add(1)
		}
		return
	}

	for {
		pos := e.head.Load()
		slot := &e.slots[pos&e.mask]
		seq := slot.seq.Load()
		dif := int64(seq) - int64(pos)
		if dif == 0 {
			if e.head.CompareAndSwap(pos, pos+1) {
				slot.span = span
				slot.seq.Store(pos + 1)
				select {
				case e.notify <- struct{}{}:
				default:
				}
				return
			}
		} else if dif < 0 {
			e.dropped.Add(1)
			return
		} else {
			runtime.Gosched()
		}
	}
}

// DroppedCount returns the total number of spans dropped due to buffer saturation or shutdown.
func (e *RingBufferEmitter) DroppedCount() uint64 {
	if e == nil {
		return 0
	}
	return e.dropped.Load()
}

// Capacity returns the total slot capacity of the ring buffer.
func (e *RingBufferEmitter) Capacity() int {
	if e == nil {
		return 0
	}
	return len(e.slots)
}

// Close stops accepting new spans, signals the background consumer to drain remaining items,
// and waits until draining completes or ctx expires.
func (e *RingBufferEmitter) Close(ctx context.Context) error {
	if e == nil || !e.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(e.stopCh)

	select {
	case <-e.doneCh:
		return nil
	case <-ctx.Done():
		return errors.Join(errors.New("ring buffer flush timed out"), ctx.Err())
	}
}

func (e *RingBufferEmitter) consumeLoop() {
	defer close(e.doneCh)

	for {
		drained := e.drainAvailable()
		if e.closed.Load() && !drained {
			// Perform one final drain pass in case a producer finished writing right before closed was observed.
			if !e.drainAvailable() {
				return
			}
			continue
		}
		if drained {
			continue
		}

		select {
		case <-e.notify:
		case <-e.stopCh:
		}
	}
}

func (e *RingBufferEmitter) drainAvailable() bool {
	processed := false
	for {
		slot := &e.slots[e.tail&e.mask]
		seq := slot.seq.Load()
		dif := int64(seq) - int64(e.tail+1)
		if dif != 0 {
			return processed
		}

		span := slot.span
		slot.span = repository.SearchAuditSpan{}
		slot.seq.Store(e.tail + e.mask + 1)
		e.tail++
		processed = true

		writeCtx, cancel := context.WithTimeout(context.Background(), defaultSinkWriteTimeout)
		if err := e.sink.WriteSpan(writeCtx, span); err != nil {
			e.logger.Warn("audit sink write failed",
				slog.String("trace_id", span.TraceID),
				slog.String("request_id", span.RequestID),
				slog.String("tenant_id", span.TenantID),
				slog.String("error", err.Error()),
			)
		}
		cancel()
	}
}

func nextPowerOfTwo(n int) int {
	if n <= 1 {
		return 1
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n++
	return n
}
