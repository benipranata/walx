package nats_test

import (
	"encoding/json"
	"testing"

	"github.com/benipranata/walx/internal/config"
	"github.com/benipranata/walx/internal/event"
	natssink "github.com/benipranata/walx/internal/sink/nats"
	"github.com/stretchr/testify/require"
)

func mustJSON(t *testing.T, e *event.Event) []byte {
	t.Helper()
	b, err := json.Marshal(e)
	require.NoError(t, err)
	return b
}

func TestFactoryErrors(t *testing.T) {
	tests := []struct {
		name string
		cfg  config.SinkConfig
		want string
	}{
		{"no url", config.SinkConfig{Type: "nats"}, "needs sink.url"},
		{"bad subject", config.SinkConfig{Type: "nats", URL: "nats://127.0.0.1:1", Subject: "a.*"}, "reserved"},
		{"unreachable", config.SinkConfig{Type: "nats", URL: "nats://127.0.0.1:1"}, "connect"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := natssink.Factory("db", tt.cfg)
			require.ErrorContains(t, err, tt.want)
			require.Nil(t, s)
		})
	}
}
