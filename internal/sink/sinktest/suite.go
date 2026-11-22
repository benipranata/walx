// Package sinktest holds the contract suite every sink must pass.
package sinktest

import (
	"context"
	"fmt"
	"testing"

	"github.com/benipranata/walx/internal/event"
	"github.com/stretchr/testify/require"
)

// Target is one sink under test plus the means to inspect its destination.
type Target struct {
	// Sink is the sink under test. The suite closes it.
	Sink event.Sink
	// Written returns the Event.IDs durably at the destination, in order.
	Written func() []string
	// Break makes the destination fail writes until restore is called.
	// Optional: without it the failure case is skipped.
	Break func() (restore func())
	// Unwritable is a batch the sink can never write (so it must return a
	// Permanent error). Optional: without it that case is skipped.
	Unwritable *event.Batch
}

// Run runs the contract suite. newTarget must return a fresh sink with an
// empty destination on every call.
func Run(t *testing.T, newTarget func(t *testing.T) Target) {
	t.Helper()
	tests := []struct {
		name string
		run  func(t *testing.T, tg Target)
	}{
		{"ordered delivery", testOrdered},
		{"nil only after durable write", testDurable},
		{"failed write is not nil and not written", testFailure},
		{"permanent for an unwritable batch", testPermanent},
		{"close twice", testCloseTwice},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tg := newTarget(t)
			t.Cleanup(func() { _ = tg.Sink.Close() })
			tt.run(t, tg)
		})
	}
}

// Batch returns a batch of n insert events with IDs <prefix>-<i>.
func Batch(prefix string, n int) event.Batch {
	b := event.Batch{CommitLSN: 1, EndLSN: 2}
	for i := 1; i <= n; i++ {
		b.Events = append(b.Events, &event.Event{
			ID:     fmt.Sprintf("%s-%d", prefix, i),
			Source: "db",
			Op:     event.OpInsert,
			Schema: "public",
			Table:  "t",
			After:  map[string]any{"id": i},
			Key:    map[string]any{"id": i},
		})
	}
	return b
}

func ids(bs ...event.Batch) []string {
	var out []string
	for _, b := range bs {
		for _, e := range b.Events {
			out = append(out, e.ID)
		}
	}
	return out
}

func testOrdered(t *testing.T, tg Target) {
	var all []event.Batch
	for _, p := range []string{"a", "b", "c"} {
		b := Batch(p, 3)
		require.NoError(t, tg.Sink.Send(context.Background(), b))
		all = append(all, b)
	}
	require.Equal(t, ids(all...), tg.Written())
}

func testDurable(t *testing.T, tg Target) {
	for _, p := range []string{"a", "b"} {
		b := Batch(p, 2)
		require.NoError(t, tg.Sink.Send(context.Background(), b))
		require.Subset(t, tg.Written(), ids(b), "Send returned nil before the batch was readable")
	}
}

func testFailure(t *testing.T, tg Target) {
	if tg.Break == nil {
		t.Skip("target cannot simulate a failing destination")
	}
	b := Batch("a", 2)
	restore := tg.Break()
	err := tg.Sink.Send(context.Background(), b)
	require.Error(t, err)
	require.False(t, event.IsPermanent(err), "a destination outage must be retryable")
	require.Empty(t, tg.Written())

	restore()
	require.NoError(t, tg.Sink.Send(context.Background(), b))
	require.Equal(t, ids(b), tg.Written())
}

func testPermanent(t *testing.T, tg Target) {
	if tg.Unwritable == nil {
		t.Skip("target has no batch it can never write")
	}
	err := tg.Sink.Send(context.Background(), *tg.Unwritable)
	require.Error(t, err)
	require.True(t, event.IsPermanent(err))
	require.Empty(t, tg.Written())
}

func testCloseTwice(t *testing.T, tg Target) {
	require.NoError(t, tg.Sink.Send(context.Background(), Batch("a", 1)))
	require.NoError(t, tg.Sink.Close())
	require.NotPanics(t, func() { require.NoError(t, tg.Sink.Close()) })
}
