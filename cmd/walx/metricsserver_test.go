package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestServeMetrics(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		wantCode int
		wantBody string
	}{
		{"metrics", "/metrics", http.StatusOK, "walx_events_total"},
		{"other path", "/", http.StatusNotFound, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			m := newMetrics(reg)
			m.Count("events_total", 3, "database", "a")
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(context.Background())
			done := serveMetrics(ctx, ln, reg, nil)

			resp, err := http.Get("http://" + ln.Addr().String() + tt.path)
			require.NoError(t, err)
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			require.Equal(t, tt.wantCode, resp.StatusCode)
			require.Contains(t, string(body), tt.wantBody)

			cancel()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("metrics server did not stop")
			}
		})
	}
}
