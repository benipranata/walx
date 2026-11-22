package event_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/event"
)

func TestNopMetrics(t *testing.T) {
	tests := []struct {
		name string
		call func(m event.Metrics)
	}{
		{"count", func(m event.Metrics) { m.Count("events_total", 1, "database", "main") }},
		{"gauge", func(m event.Metrics) { m.Gauge("slot_lag_bytes", 42, "database", "main") }},
		{"observe", func(m event.Metrics) { m.Observe("sink_latency_seconds", 0.5, "database", "main") }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var m event.Metrics = event.NopMetrics{}
			require.NotPanics(t, func() { tt.call(m) })
		})
	}
}
