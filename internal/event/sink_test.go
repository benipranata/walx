package event_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/event"
)

type permErr struct{ code int }

func (e *permErr) Error() string { return fmt.Sprint("code ", e.code) }

func TestPermanent(t *testing.T) {
	base := errors.New("boom")
	err := event.Permanent(base)

	require.True(t, event.IsPermanent(err))
	require.ErrorIs(t, err, base)
	require.Equal(t, "boom", err.Error())
}

func TestPermanentAs(t *testing.T) {
	err := event.Permanent(&permErr{code: 7})

	var pe *permErr
	require.ErrorAs(t, err, &pe)
	require.Equal(t, 7, pe.code)
}

func TestPermanentNil(t *testing.T) {
	require.NoError(t, event.Permanent(nil))
	require.False(t, event.IsPermanent(nil))
}

func TestIsPermanentWrapped(t *testing.T) {
	err := fmt.Errorf("send: %w", event.Permanent(errors.New("bad")))
	require.True(t, event.IsPermanent(err))
	require.False(t, event.IsPermanent(errors.New("transient")))
}

func TestNopMetrics(t *testing.T) {
	var m event.Metrics = event.NopMetrics{}
	require.NotPanics(t, func() {
		m.Count("events_total", 1, "database", "main")
		m.Gauge("slot_lag_bytes", 42, "database", "main")
	})
}

func TestSinkFunc(t *testing.T) {
	var got event.Batch
	var s event.Sink = event.SinkFunc(func(_ context.Context, b event.Batch) error {
		got = b
		return nil
	})
	want := event.Batch{CommitLSN: 1, EndLSN: 2, Events: []event.Event{{ID: "a"}}}
	require.NoError(t, s.Send(context.Background(), want))
	require.Equal(t, want, got)
}
