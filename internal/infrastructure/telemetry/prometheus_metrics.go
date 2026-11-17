package telemetry

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// PrometheusMetrics implements usecase.MetricsObserver and exposes transport-level
// Prometheus collectors aligned with .ai/doc-api.md.
type PrometheusMetrics struct {
	registry *prometheus.Registry

	requestsTotal       *prometheus.CounterVec
	requestLatency      prometheus.Histogram
	fanoutLatency       *prometheus.HistogramVec
	fanoutStageErrors   *prometheus.CounterVec
	circuitBreakerTrips prometheus.Counter
	roaringEvalDuration prometheus.Histogram
	lastBitmapSyncTs    prometheus.Gauge
}

// NewPrometheusMetrics registers all Search Orchestrator Prometheus metrics on a dedicated registry.
func NewPrometheusMetrics() *PrometheusMetrics {
	reg := prometheus.NewRegistry()

	m := &PrometheusMetrics{
		registry: reg,
		requestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "search_orchestrator_requests_total",
			Help: "Total number of search requests partitioned by tenant, status code, and cache hit.",
		}, []string{"tenant_id", "status_code", "cache_hit"}),
		requestLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "search_orchestrator_latency_seconds",
			Help:    "End-to-end search request latency in seconds (5ms to 250ms).",
			Buckets: prometheus.ExponentialBuckets(0.005, 1.48, 11),
		}),
		fanoutLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "search_fanout_latency_seconds",
			Help:    "Isolated retrieval stage latency in seconds by component.",
			Buckets: prometheus.ExponentialBuckets(0.001, 1.5, 11),
		}, []string{"component"}),
		fanoutStageErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "search_fanout_stage_errors_total",
			Help: "Total number of stage errors by component and error classification.",
		}, []string{"component", "error"}),
		circuitBreakerTrips: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "search_circuit_breaker_tripped_total",
			Help: "Total number of Triton Cross-Encoder circuit breaker trips.",
		}),
		roaringEvalDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "search_roaring_bitmap_eval_duration_seconds",
			Help:    "Microsecond-level Roaring Bitmap ACL intersection duration in seconds.",
			Buckets: prometheus.ExponentialBuckets(0.00001, 2, 10),
		}),
		lastBitmapSyncTs: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "search_last_bitmap_sync_timestamp_seconds",
			Help: "Epoch timestamp of the last entitlement bitmap synchronization.",
		}),
	}

	reg.MustRegister(
		m.requestsTotal,
		m.requestLatency,
		m.fanoutLatency,
		m.fanoutStageErrors,
		m.circuitBreakerTrips,
		m.roaringEvalDuration,
		m.lastBitmapSyncTs,
	)

	return m
}

// Registry returns the underlying Prometheus registry for scraping via promhttp.HandlerFor.
func (m *PrometheusMetrics) Registry() *prometheus.Registry {
	return m.registry
}

// Gatherer returns the prometheus.Gatherer for /metrics exposition.
func (m *PrometheusMetrics) Gatherer() prometheus.Gatherer {
	return m.registry
}

// ObserveStageLatency implements usecase.MetricsObserver.
func (m *PrometheusMetrics) ObserveStageLatency(component string, duration time.Duration) {
	if m == nil {
		return
	}
	m.fanoutLatency.WithLabelValues(component).Observe(duration.Seconds())
}

// IncStageError implements usecase.MetricsObserver.
func (m *PrometheusMetrics) IncStageError(component string, errCode string) {
	if m == nil {
		return
	}
	m.fanoutStageErrors.WithLabelValues(component, errCode).Inc()
}

// ObserveRoaringEval implements usecase.MetricsObserver.
func (m *PrometheusMetrics) ObserveRoaringEval(duration time.Duration) {
	if m == nil {
		return
	}
	m.roaringEvalDuration.Observe(duration.Seconds())
}

// IncCircuitBreakerTripped implements usecase.MetricsObserver.
func (m *PrometheusMetrics) IncCircuitBreakerTripped() {
	if m == nil {
		return
	}
	m.circuitBreakerTrips.Inc()
}

// RecordRequest records end-to-end request count and latency at the transport boundary.
func (m *PrometheusMetrics) RecordRequest(tenantID, statusCode string, cacheHit bool, duration time.Duration) {
	if m == nil {
		return
	}
	if tenantID == "" {
		tenantID = "unknown"
	}
	m.requestsTotal.WithLabelValues(tenantID, statusCode, strconv.FormatBool(cacheHit)).Inc()
	m.requestLatency.Observe(duration.Seconds())
}

// SetLastBitmapSyncTimestamp updates the entitlement bitmap sync gauge.
func (m *PrometheusMetrics) SetLastBitmapSyncTimestamp(epochSeconds float64) {
	if m == nil {
		return
	}
	m.lastBitmapSyncTs.Set(epochSeconds)
}
