package main

import (
	"context"
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/config"
)

func TestAppRunErrors(t *testing.T) {
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer busy.Close()
	tests := []struct {
		name    string
		cfg     config.Config
		wantErr string
	}{
		{"metrics port in use", config.Config{Metrics: config.MetricsConfig{Listen: busy.Addr().String()}}, "metrics listener"},
		{"unknown sink type", config.Config{Databases: []config.DatabaseConfig{{Name: "a", Sink: config.SinkConfig{Type: "kafka"}}}}, "unknown sink type"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := newApp(&tt.cfg, newRegistry(), newLogger(io.Discard)).run(context.Background())
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}
