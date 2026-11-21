package event_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/event"
)

func TestNopMetrics(t *testing.T) {
	var m event.Metrics = event.NopMetrics{}
	require.NotPanics(t, func() {
		m.Count("events_total", 1, "database", "main")
		m.Gauge("slot_lag_bytes", 42, "database", "main")
	})
}
