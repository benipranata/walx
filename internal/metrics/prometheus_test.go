package metrics_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/event"
	"github.com/benipranata/walx/internal/metrics"
)

var _ event.Metrics = (*metrics.Prometheus)(nil)

func TestPrometheus(t *testing.T) {
	tests := []struct {
		name string
		do   func(m event.Metrics)
		want string
	}{
		{"counter", func(m event.Metrics) {
			m.Count("events_total", 2, "database", "a")
			m.Count("events_total", 3, "database", "a")
		}, `walx_events_total{database="a"} 5`},
		{"counter with extra labels in any order", func(m event.Metrics) {
			m.Count("events_dropped_total", 1, "table", "public.t", "database", "a", "op", "insert")
		}, `walx_events_dropped_total{database="a",op="insert",table="public.t"} 1`},
		{"skipped", func(m event.Metrics) { m.Count("events_skipped_total", 4, "database", "a") },
			`walx_events_skipped_total{database="a"} 4`},
		{"gauge", func(m event.Metrics) {
			m.Gauge("slot_lag_bytes", 10, "database", "a")
			m.Gauge("slot_lag_bytes", 7, "database", "a")
		}, `walx_slot_lag_bytes{database="a"} 7`},
		{"wal status", func(m event.Metrics) { m.Gauge("wal_status", 2, "database", "a") },
			`walx_wal_status{database="a"} 2`},
		{"histogram", func(m event.Metrics) { m.Observe("sink_latency_seconds", 0.2, "database", "a") },
			`walx_sink_latency_seconds_count{database="a"} 1`},
		{"missing label is empty", func(m event.Metrics) { m.Count("events_total", 1) },
			`walx_events_total{database=""} 1`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			m := metrics.NewPrometheus(reg, "walx")
			tt.do(m)
			srv := httptest.NewServer(metrics.Handler(reg))
			defer srv.Close()
			resp, err := http.Get(srv.URL)
			require.NoError(t, err)
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Contains(t, string(body), tt.want)
		})
	}
}

func TestPrometheusIgnoresUnknown(t *testing.T) {
	tests := []struct {
		name string
		do   func(m event.Metrics)
	}{
		{"unknown count", func(m event.Metrics) { m.Count("nope", 1, "database", "a") }},
		{"unknown gauge", func(m event.Metrics) { m.Gauge("nope", 1) }},
		{"unknown observe", func(m event.Metrics) { m.Observe("nope", 1) }},
		{"wrong kind", func(m event.Metrics) { m.Count("slot_lag_bytes", 1, "database", "a") }},
		{"stray and odd labels", func(m event.Metrics) { m.Count("events_total", 1, "database", "a", "stray", "x", "odd") }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			m := metrics.NewPrometheus(reg, "")
			require.NotPanics(t, func() { tt.do(m) })
		})
	}
}
