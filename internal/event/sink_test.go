package event_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/event"
)

func TestSinkFunc(t *testing.T) {
	var got event.Batch
	var s event.Sink = event.SinkFunc(func(_ context.Context, b event.Batch) error {
		got = b
		return nil
	})
	want := event.Batch{CommitLSN: 1, EndLSN: 2, Events: []event.Event{{ID: "a"}}}
	require.NoError(t, s.Send(context.Background(), want))
	require.Equal(t, want, got)
	require.NoError(t, s.Close())
}
