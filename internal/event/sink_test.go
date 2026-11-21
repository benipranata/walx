package event_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/event"
)

func TestSinkFunc(t *testing.T) {
	tests := []struct {
		name  string
		batch event.Batch
	}{
		{"one event", event.Batch{CommitLSN: 1, EndLSN: 2, Events: []*event.Event{{ID: "a"}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got event.Batch
			var s event.Sink = event.SinkFunc(func(_ context.Context, b event.Batch) error {
				got = b
				return nil
			})
			require.NoError(t, s.Send(context.Background(), tt.batch))
			require.Equal(t, tt.batch, got)
			require.NoError(t, s.Close())
		})
	}
}
