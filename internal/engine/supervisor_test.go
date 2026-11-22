package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/event"
)

func TestSupervisorRun(t *testing.T) {
	boom := errors.New("boom")
	other := errors.New("other")
	// stopOnCancel blocks until ctx ends, then returns ctx.Err().
	stopOnCancel := func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }
	tests := []struct {
		name      string
		run       []func(context.Context) error
		cancelPre bool
		wantErr   error
		wantText  string
	}{
		{name: "first error wins and cancels the rest",
			run:     []func(context.Context) error{stopOnCancel, func(context.Context) error { return boom }, stopOnCancel},
			wantErr: boom},
		{name: "cancellation induced by the supervisor is not reported",
			run: []func(context.Context) error{
				func(ctx context.Context) error { <-ctx.Done(); return other },
				func(context.Context) error { return boom }},
			wantErr: boom},
		{name: "parent cancellation returns the parent error",
			run:       []func(context.Context) error{stopOnCancel, stopOnCancel},
			cancelPre: true, wantErr: context.Canceled},
		{name: "a return of nil without cancellation is an error and stops the rest",
			run:      []func(context.Context) error{stopOnCancel, func(context.Context) error { return nil }},
			wantText: "stopped unexpectedly"},
		{name: "a Canceled return without cancellation is an error and stops the rest",
			run:      []func(context.Context) error{stopOnCancel, func(context.Context) error { return context.Canceled }},
			wantText: "stopped unexpectedly"},
		{name: "a genuine error wrapping Canceled is reported",
			run:     []func(context.Context) error{stopOnCancel, func(context.Context) error { return fmt.Errorf("sink: %w", context.Canceled) }},
			wantErr: context.Canceled, wantText: "sink:"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var s Supervisor
			for i, r := range tt.run {
				s.Engines = append(s.Engines, Named{Name: string(rune('a' + i)), Run: r})
			}
			done := make(chan error, 1)
			go func() { done <- s.Run(ctx) }()
			if tt.cancelPre {
				time.Sleep(20 * time.Millisecond)
				cancel()
			}
			err := wait(t, done)
			require.Error(t, err)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			}
			require.Contains(t, err.Error(), tt.wantText)
		})
	}
}

func TestSupervisorNoEngines(t *testing.T) {
	tests := []struct{ name string }{{name: "no engines is an error"}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Error(t, (&Supervisor{}).Run(context.Background()))
		})
	}
}

func TestSupervisorLogsFailure(t *testing.T) {
	tests := []struct{ name string }{{name: "failing engine is logged with its name"}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out syncBuffer
			s := Supervisor{Logger: slog.New(slog.NewTextHandler(&out, nil)), Engines: []Named{
				{Name: "orders", Run: func(context.Context) error { return errors.New("boom") }},
				{Name: "users", Run: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }},
			}}
			require.Error(t, s.Run(context.Background()))
			require.True(t, strings.Contains(out.String(), "db=orders") && strings.Contains(out.String(), "err=boom"), out.String())
		})
	}
}

// TestSupervisorEngines runs two real engines over fake sources.
func TestSupervisorEngines(t *testing.T) {
	boom := errors.New("bad row")
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "databases stream independently and in order", run: func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			srcA, srcB := newFakeSource(), newFakeSource()
			sinkA, sinkB := newMemSink(t), newMemSink(t)
			a := New(Config{Name: "a", Source: srcA, Sink: sinkA})
			b := New(Config{Name: "b", Source: srcB, Sink: sinkB})
			stubDecode(a)
			stubDecode(b)
			feed(ctx, srcA, 5)
			feed(ctx, srcB, 3)
			s := Supervisor{Engines: []Named{{"a", a.Run}, {"b", b.Run}}}
			done := make(chan error, 1)
			go func() { done <- s.Run(ctx) }()
			require.Eventually(t, func() bool { return len(sinkA.Batches()) == 5 && len(sinkB.Batches()) == 3 }, 5*time.Second, 5*time.Millisecond)
			for i, bt := range sinkA.Batches() {
				require.EqualValues(t, (i+1)*10, bt.EndLSN)
			}
			for i, bt := range sinkB.Batches() {
				require.EqualValues(t, (i+1)*10, bt.EndLSN)
			}
			cancel()
			require.ErrorIs(t, wait(t, done), context.Canceled)
		}},
		{name: "a permanent error in one stops the other gracefully", run: func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			srcA, srcB := newFinalSource(), newFakeSource()
			entered, release := make(chan struct{}), make(chan struct{})
			var finished atomic.Bool
			sinkA := newCloseSink(t, func(context.Context, event.Batch) error {
				close(entered)
				<-release
				finished.Store(true)
				return nil
			})
			sinkB := newCloseSink(t, func(context.Context, event.Batch) error { return event.Permanent(boom) })
			a := New(Config{Name: "a", Source: srcA, Sink: sinkA})
			b := New(Config{Name: "b", Source: srcB, Sink: sinkB})
			stubDecode(a)
			stubDecode(b)
			feed(ctx, srcA, 1)
			s := Supervisor{Engines: []Named{{"a", a.Run}, {"b", b.Run}}}
			done := make(chan error, 1)
			go func() { done <- s.Run(ctx) }()
			<-entered
			feed(ctx, srcB, 1)
			require.Eventually(t, func() bool { return sinkB.closed.Load() == 1 }, 5*time.Second, 5*time.Millisecond)
			select {
			case <-done:
				t.Fatal("supervisor returned before the in-flight call finished")
			case <-time.After(50 * time.Millisecond):
			}
			close(release)
			err := wait(t, done)
			require.ErrorIs(t, err, boom)
			require.True(t, finished.Load())
			require.EqualValues(t, 1, sinkA.closed.Load())
			require.EqualValues(t, 1, sinkB.closed.Load())
			require.Equal(t, 1, <-srcA.atFinal)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}
