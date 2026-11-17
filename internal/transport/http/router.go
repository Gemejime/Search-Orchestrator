package http

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/rag-platform/rag-search-orchestrator/api/openapi"
	"github.com/rag-platform/rag-search-orchestrator/internal/infrastructure/telemetry"
)

const (
	defaultReadHeaderTimeout = 5 * time.Second
	defaultReadTimeout       = 10 * time.Second
	defaultWriteTimeout      = 10 * time.Second
	defaultIdleTimeout       = 60 * time.Second
	defaultReadyCheckTimeout = 2 * time.Second
)

// ReadinessCheck probes a downstream dependency for /ready.
type ReadinessCheck func(ctx context.Context) error

// RouterConfig configures the HTTP transport router and readiness probes.
type RouterConfig struct {
	Controller *SearchController
	Metrics    *telemetry.PrometheusMetrics
	Checks     map[string]ReadinessCheck
	Draining   *atomic.Bool
}

type healthResponse struct {
	Status string `json:"status"`
}

type readinessResponse struct {
	Status string            `json:"status"`
	Checks map[string]string `json:"checks,omitempty"`
}

const swaggerUIHTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>RAG Search Orchestrator API</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css" />
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>
    window.onload = function() {
      SwaggerUIBundle({
        url: "/openapi.yaml",
        dom_id: "#swagger-ui"
      });
    };
  </script>
</body>
</html>`

// NewRouter builds the HTTP ServeMux exposing /v1/search, /v1/search/diagnostic,
// /healthz, /ready, /metrics, /openapi.yaml, and /docs.
func NewRouter(cfg RouterConfig) http.Handler {
	mux := http.NewServeMux()

	if cfg.Controller != nil {
		mux.HandleFunc("POST /v1/search", cfg.Controller.HandleSearch)
		mux.HandleFunc("POST /v1/search/diagnostic", cfg.Controller.HandleDiagnosticSearch)
	}

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, healthResponse{Status: "alive"})
	})

	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, r *http.Request) {
		if cfg.Draining != nil && cfg.Draining.Load() {
			writeJSON(w, http.StatusServiceUnavailable, readinessResponse{Status: "draining"})
			return
		}

		checkCtx, cancel := context.WithTimeout(r.Context(), defaultReadyCheckTimeout)
		defer cancel()

		results := make(map[string]string, len(cfg.Checks))
		allOK := true
		for name, fn := range cfg.Checks {
			if fn == nil {
				results[name] = "ok"
				continue
			}
			if err := fn(checkCtx); err != nil {
				results[name] = "unavailable"
				allOK = false
			} else {
				results[name] = "ok"
			}
		}

		if !allOK {
			writeJSON(w, http.StatusServiceUnavailable, readinessResponse{
				Status: "not_ready",
				Checks: results,
			})
			return
		}
		writeJSON(w, http.StatusOK, readinessResponse{
			Status: "ready",
			Checks: results,
		})
	})

	if cfg.Metrics != nil {
		mux.Handle("GET /metrics", promhttp.HandlerFor(cfg.Metrics.Gatherer(), promhttp.HandlerOpts{}))
	}

	mux.HandleFunc("GET /openapi.yaml", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(openapi.SpecYAML)
	})

	mux.HandleFunc("GET /docs", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(swaggerUIHTML))
	})

	return mux
}

// NewHTTPServer constructs an http.Server with production timeout hardening.
func NewHTTPServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: defaultReadHeaderTimeout,
		ReadTimeout:       defaultReadTimeout,
		WriteTimeout:      defaultWriteTimeout,
		IdleTimeout:       defaultIdleTimeout,
	}
}
