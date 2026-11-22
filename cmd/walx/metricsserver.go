package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/benipranata/walx/internal/metrics"
)

const metricsNamespace = "walx"

// newMetrics registers the walx metrics on reg.
func newMetrics(reg prometheus.Registerer) *metrics.Prometheus {
	return metrics.NewPrometheus(reg, metricsNamespace)
}

// serveMetrics serves /metrics from reg on ln until ctx ends, and returns a
// channel closed once the server has stopped. A nil log discards.
func serveMetrics(ctx context.Context, ln net.Listener, reg prometheus.Gatherer, log *slog.Logger) <-chan struct{} {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler(reg))
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	done := make(chan struct{})
	go func() {
		defer close(done)
		go func() {
			<-ctx.Done()
			sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			_ = srv.Shutdown(sctx)
		}()
		if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Error("metrics server stopped", "err", err)
		}
	}()
	return done
}
