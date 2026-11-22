package triton

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// CircuitState represents the current state of the Triton reranker circuit breaker.
type CircuitState int32

const (
	// StateClosed allows all requests through while monitoring rolling errors and slow calls.
	StateClosed CircuitState = 0
	// StateOpen short-circuits reranker requests directly to the RRF fallback.
	StateOpen CircuitState = 1
	// StateHalfOpen permits a single-flight probe to test Triton recovery.
	StateHalfOpen CircuitState = 2
)

const (
	// DefaultWindowSize is the sliding window size for tracking reranker outcomes.
	DefaultWindowSize = 100
	// DefaultMinRequests is the minimum sample count before error/slow rate tripping applies.
	DefaultMinRequests = 20
	// DefaultErrorRateThreshold is the combined error + slow-call ratio threshold (5%).
	DefaultErrorRateThreshold = 0.05
	// DefaultSlowCallDuration is the reranker latency threshold (35ms).
	DefaultSlowCallDuration = 35 * time.Millisecond
	// DefaultConsecutiveSlowThreshold trips the breaker after consecutive >35ms stalls.
	DefaultConsecutiveSlowThreshold = 5
	// DefaultMinRemainingBudget bypasses reranking when remaining ctx.Deadline budget is < 25ms.
	DefaultMinRemainingBudget = 25 * time.Millisecond
	// DefaultOpenTimeout is how long the breaker stays open before allowing a HalfOpen probe.
	DefaultOpenTimeout = 10 * time.Second
)

// CircuitBreakerConfig configures deterministic Triton Cross-Encoder circuit breaking.
type CircuitBreakerConfig struct {
	WindowSize               int
	MinRequests              int
	ErrorRateThreshold       float64
	SlowCallDuration         time.Duration
	ConsecutiveSlowThreshold int
	MinRemainingBudget       time.Duration
	OpenTimeout              time.Duration
	Now                      func() time.Time
	OnTrip                   func()
}

// CircuitBreaker guards Triton Cross-Encoder reranking against GPU queue stalls and error spikes.
type CircuitBreaker struct {
	cfg CircuitBreakerConfig

	mu              sync.Mutex
	state           CircuitState
	stateAtomic     atomic.Int32
	openedAt        time.Time
	probeInFlight   bool
	window          []bool
	head            int
	count           int
	badCount        int
	consecutiveSlow int
}

// NewCircuitBreaker constructs a deterministic CircuitBreaker with production defaults.
func NewCircuitBreaker(cfg CircuitBreakerConfig) *CircuitBreaker {
	if cfg.WindowSize <= 0 {
		cfg.WindowSize = DefaultWindowSize
	}
	if cfg.MinRequests <= 0 {
		cfg.MinRequests = DefaultMinRequests
	}
	if cfg.ErrorRateThreshold <= 0 {
		cfg.ErrorRateThreshold = DefaultErrorRateThreshold
	}
	if cfg.SlowCallDuration <= 0 {
		cfg.SlowCallDuration = DefaultSlowCallDuration
	}
	if cfg.ConsecutiveSlowThreshold <= 0 {
		cfg.ConsecutiveSlowThreshold = DefaultConsecutiveSlowThreshold
	}
	if cfg.MinRemainingBudget <= 0 {
		cfg.MinRemainingBudget = DefaultMinRemainingBudget
	}
	if cfg.OpenTimeout <= 0 {
		cfg.OpenTimeout = DefaultOpenTimeout
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}

	return &CircuitBreaker{
		cfg:    cfg,
		state:  StateClosed,
		window: make([]bool, cfg.WindowSize),
	}
}

// State returns the current CircuitState without blocking.
func (cb *CircuitBreaker) State() CircuitState {
	return CircuitState(cb.stateAtomic.Load())
}

// Allow checks whether a rerank call should proceed, or bypass to ACL-filtered RRF fallback.
// When allowed is true and isProbe is true, the caller holds the single-flight HalfOpen probe slot
// and must invoke Record with isProbe=true.
func (cb *CircuitBreaker) Allow(ctx context.Context) (allowed bool, isProbe bool, reason string) {
	now := cb.cfg.Now()
	if deadline, ok := ctx.Deadline(); ok {
		if deadline.Sub(now) < cb.cfg.MinRemainingBudget {
			return false, false, "budget_exhausted"
		}
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		return true, false, ""
	case StateOpen:
		if now.Sub(cb.openedAt) >= cb.cfg.OpenTimeout {
			cb.setStateLocked(StateHalfOpen)
			cb.probeInFlight = true
			return true, true, ""
		}
		return false, false, "circuit_open"
	case StateHalfOpen:
		if !cb.probeInFlight {
			cb.probeInFlight = true
			return true, true, ""
		}
		return false, false, "half_open_probe_in_flight"
	default:
		return false, false, "circuit_open"
	}
}

// Record updates the sliding window and state machine after a rerank attempt completes.
func (cb *CircuitBreaker) Record(duration time.Duration, err error, isProbe bool) {
	isSlow := duration > cb.cfg.SlowCallDuration
	isBad := err != nil || isSlow

	var tripped bool

	cb.mu.Lock()
	if isProbe && cb.state == StateHalfOpen {
		cb.probeInFlight = false
		if isBad {
			cb.setStateLocked(StateOpen)
			cb.openedAt = cb.cfg.Now()
			tripped = true
		} else {
			cb.resetWindowLocked()
			cb.setStateLocked(StateClosed)
		}
		cb.mu.Unlock()
		if tripped && cb.cfg.OnTrip != nil {
			cb.cfg.OnTrip()
		}
		return
	}

	if cb.state == StateClosed {
		if cb.count == len(cb.window) {
			if cb.window[cb.head] {
				cb.badCount--
			}
		} else {
			cb.count++
		}
		cb.window[cb.head] = isBad
		if isBad {
			cb.badCount++
		}
		cb.head = (cb.head + 1) % len(cb.window)

		if isSlow {
			cb.consecutiveSlow++
		} else {
			cb.consecutiveSlow = 0
		}

		rateExceeded := cb.count >= cb.cfg.MinRequests &&
			float64(cb.badCount)/float64(cb.count) > cb.cfg.ErrorRateThreshold
		stalled := cb.consecutiveSlow >= cb.cfg.ConsecutiveSlowThreshold

		if rateExceeded || stalled {
			cb.setStateLocked(StateOpen)
			cb.openedAt = cb.cfg.Now()
			cb.probeInFlight = false
			tripped = true
		}
	}
	cb.mu.Unlock()

	if tripped && cb.cfg.OnTrip != nil {
		cb.cfg.OnTrip()
	}
}

func (cb *CircuitBreaker) setStateLocked(next CircuitState) {
	cb.state = next
	cb.stateAtomic.Store(int32(next))
}

func (cb *CircuitBreaker) resetWindowLocked() {
	clear(cb.window)
	cb.head = 0
	cb.count = 0
	cb.badCount = 0
	cb.consecutiveSlow = 0
}
