package main

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/config"
	"github.com/benipranata/walx/internal/sink"
)

func TestNewRegistry(t *testing.T) {
	tests := []struct {
		name    string
		cfg     config.SinkConfig
		wantErr string
	}{
		{"stdout", config.SinkConfig{Type: "stdout"}, ""},
		{"file", config.SinkConfig{Type: "file", Path: filepath.Join(t.TempDir(), "out.jsonl")}, ""},
		{"file in a missing directory", config.SinkConfig{Type: "file", Path: filepath.Join(t.TempDir(), "no", "out.jsonl")}, "not writable"},
		{"nats without url", config.SinkConfig{Type: "nats"}, "sink.url"},
		{"unknown", config.SinkConfig{Type: "kafka"}, "unknown sink type"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := newRegistry().Build("a", tt.cfg)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.NoError(t, s.Close())
		})
	}
}

func TestNewRegistryUnknownIsConfigError(t *testing.T) {
	_, err := sink.NewRegistry().Build("a", config.SinkConfig{Type: "stdout"})
	require.ErrorIs(t, err, sink.ErrUnknownType)
}
