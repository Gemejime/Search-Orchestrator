package unit

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/triton"
)

func TestCircuitBreaker_MinRequestsAndErrorRateTrip(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_700_000_000, 0)
	var trips atomic.Int32
	cb := triton.NewCircuitBreaker(triton.CircuitBreakerConfig{
		WindowSize:         20,
		MinRequests:        20,
		ErrorRateThreshold: 0.05,
		SlowCallDuration:   35 * time.Millisecond,
		OpenTimeout:        10 * time.Second,
		Now:                func() time.Time { return now },
		OnTrip:             func() { trips.Add(1) },
	})

	// 2 errors in first 10 requests should NOT trip because minRequests=20 is not yet met.
	for i := 0; i < 10; i++ {
		allowed, isProbe, _ := cb.Allow(context.Background())
		if !allowed || isProbe {
			t.Fatalf("req %d: expected allowed=true, isProbe=false", i)
		}
		var err error
		if i < 2 {
			err = errors.New("transient error")
		}
		cb.Record(5*time.Millisecond, err, false)
	}
	if cb.State() != triton.StateClosed {
		t.Fatalf("expected StateClosed before minRequests, got %v", cb.State())
	}

	// Fill remaining 10 requests with 1 more error -> 3/20 = 15% > 5%, should trip Open.
	for i := 0; i < 10; i++ {
		allowed, _, _ := cb.Allow(context.Background())
		if !allowed {
			t.Fatalf("expected allowed=true before trip")
		}
		var err error
		if i == 9 {
			err = errors.New("trip error")
		}
		cb.Record(5*time.Millisecond, err, false)
	}

	if cb.State() != triton.StateOpen {
		t.Fatalf("expected StateOpen after >5%% error rate, got %v", cb.State())
	}
	if trips.Load() != 1 {
		t.Fatalf("expected 1 trip callback, got %d", trips.Load())
	}

	// While open, Allow must return false.
	if allowed, _, reason := cb.Allow(context.Background()); allowed || reason != "circuit_open" {
		t.Fatalf("expected circuit_open rejection, got allowed=%v reason=%q", allowed, reason)
	}
}

func TestCircuitBreaker_ConsecutiveSlowCallsAndHalfOpenSingleFlight(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_700_000_000, 0)
	cb := triton.NewCircuitBreaker(triton.CircuitBreakerConfig{
		WindowSize:               100,
		MinRequests:              20,
		SlowCallDuration:         35 * time.Millisecond,
		ConsecutiveSlowThreshold: 3,
		OpenTimeout:              5 * time.Second,
		Now:                      func() time.Time { return now },
	})

	for i := 0; i < 3; i++ {
		allowed, _, _ := cb.Allow(context.Background())
		if !allowed {
			t.Fatalf("expected allowed=true at %d", i)
		}
		cb.Record(36*time.Millisecond, nil, false)
	}
	if cb.State() != triton.StateOpen {
		t.Fatalf("expected StateOpen after consecutive >35ms stalls, got %v", cb.State())
	}

	// Advance past OpenTimeout -> HalfOpen single-flight probe.
	now = now.Add(6 * time.Second)
	allowed, isProbe, _ := cb.Allow(context.Background())
	if !allowed || !isProbe {
		t.Fatalf("expected single-flight HalfOpen probe, got allowed=%v isProbe=%v", allowed, isProbe)
	}

	// Second concurrent caller must be rejected while probe is in flight.
	allowed2, isProbe2, reason2 := cb.Allow(context.Background())
	if allowed2 || isProbe2 || reason2 != "half_open_probe_in_flight" {
		t.Fatalf("expected half_open_probe_in_flight, got allowed=%v isProbe=%v reason=%q", allowed2, isProbe2, reason2)
	}

	// Probe succeeds within 35ms -> closes circuit.
	cb.Record(12*time.Millisecond, nil, true)
	if cb.State() != triton.StateClosed {
		t.Fatalf("expected StateClosed after successful probe, got %v", cb.State())
	}
}

func TestCircuitBreaker_DeadlineBudgetBypass(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_700_000_000, 0)
	cb := triton.NewCircuitBreaker(triton.CircuitBreakerConfig{
		MinRemainingBudget: 25 * time.Millisecond,
		Now:                func() time.Time { return now },
	})

	ctx, cancel := context.WithDeadline(context.Background(), now.Add(20*time.Millisecond))
	defer cancel()

	allowed, isProbe, reason := cb.Allow(ctx)
	if allowed || isProbe || reason != "budget_exhausted" {
		t.Fatalf("expected budget_exhausted bypass, got allowed=%v isProbe=%v reason=%q", allowed, isProbe, reason)
	}
	if cb.State() != triton.StateClosed {
		t.Fatalf("budget bypass must not trip global circuit breaker, got %v", cb.State())
	}
}
