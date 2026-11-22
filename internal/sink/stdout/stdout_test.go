package stdout_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benipranata/walx/internal/config"
	"github.com/benipranata/walx/internal/event"
	"github.com/benipranata/walx/internal/sink/sinktest"
	"github.com/benipranata/walx/internal/sink/stdout"
	"github.com/sebdah/goldie/v2"
	"github.com/stretchr/testify/require"
)

// buffer is a goroutine-safe writer that can be told to fail.
type buffer struct {
	mu     sync.Mutex
	buf    bytes.Buffer
	broken bool
	closed bool
}

func (b *buffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.broken {
		return 0, errors.New("broken pipe")
	}
	return b.buf.Write(p)
}

func (b *buffer) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	return nil
}

func (b *buffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *buffer) ids() []string {
	var out []string
	for _, l := range strings.Split(strings.TrimSuffix(b.String(), "\n"), "\n") {
		if l == "" {
			continue
		}
		var e struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal([]byte(l), &e); err != nil {
			panic(err)
		}
		out = append(out, e.ID)
	}
	return out
}

func TestContract(t *testing.T) {
	tests := []struct {
		name string
	}{
		{"writer sink"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sinktest.Run(t, func(t *testing.T) sinktest.Target {
				w := &buffer{}
				return sinktest.Target{
					Sink:    stdout.New(w),
					Written: w.ids,
					Break: func() func() {
						w.mu.Lock()
						w.broken = true
						w.mu.Unlock()
						return func() {
							w.mu.Lock()
							w.broken = false
							w.mu.Unlock()
						}
					},
					Unwritable: &event.Batch{Events: []*event.Event{
						{ID: "a-1", After: map[string]any{"v": 1}},
						{ID: "bad", After: map[string]any{"v": math.NaN()}},
					}},
				}
			})
		})
	}
}

func TestGolden(t *testing.T) {
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	tests := []struct {
		name  string
		batch event.Batch
	}{
		{
			name: "batch",
			batch: event.Batch{Events: []*event.Event{
				{
					ID: "orders-0/16B3748-1", Source: "orders", Op: event.OpInsert, Schema: "public", Table: "t",
					After: map[string]any{"id": 1, "name": "a<b>&"}, Key: map[string]any{"id": 1},
					LSN: 0x16B3748, TxID: 7, CommitTS: ts,
				},
				{
					ID: "orders-0/16B3748-2", Source: "orders", Op: event.OpUpdate, Schema: "public", Table: "t",
					Before: map[string]any{"id": 1, "doc": "x"},
					After:  map[string]any{"id": 1, "doc": event.Unchanged},
					Key:    map[string]any{"id": 1}, LSN: 0x16B3748, TxID: 7, CommitTS: ts,
				},
				{
					ID: "orders-0/16B3748-3", Source: "orders", Op: event.OpDelete, Schema: "public", Table: "t",
					Before: map[string]any{"id": 1}, Key: map[string]any{"id": 1},
					LSN: 0x16B3748, TxID: 7, CommitTS: ts,
				},
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &buffer{}
			require.NoError(t, stdout.New(w).Send(context.Background(), tt.batch))
			goldie.New(t).Assert(t, tt.name, []byte(w.String()))
		})
	}
}

func TestSendErrors(t *testing.T) {
	tests := []struct {
		name string
		do   func(s event.Sink, w *buffer) error
		want string
	}{
		{
			name: "send after close",
			do: func(s event.Sink, w *buffer) error {
				_ = s.Close()
				return s.Send(context.Background(), sinktest.Batch("a", 1))
			},
			want: "closed",
		},
		{
			name: "cancelled context",
			do: func(s event.Sink, w *buffer) error {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return s.Send(ctx, sinktest.Batch("a", 1))
			},
			want: "canceled",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &buffer{}
			err := tt.do(stdout.New(w), w)
			require.ErrorContains(t, err, tt.want)
			require.False(t, event.IsPermanent(err))
			require.Empty(t, w.String())
		})
	}
}

type shortWriter struct{}

func (shortWriter) Write(p []byte) (int, error) { return len(p) / 2, nil }

func TestShortWrite(t *testing.T) {
	tests := []struct {
		name string
	}{
		{"writer reports fewer bytes without error"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := stdout.New(shortWriter{}).Send(context.Background(), sinktest.Batch("a", 2))
			require.ErrorIs(t, err, io.ErrShortWrite)
			require.False(t, event.IsPermanent(err))
		})
	}
}

func TestCloseDoesNotCloseWriter(t *testing.T) {
	tests := []struct {
		name string
	}{
		{"closer writer"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &buffer{}
			require.NoError(t, stdout.New(w).Close())
			require.False(t, w.closed)
		})
	}
}

func TestFactory(t *testing.T) {
	tests := []struct {
		name string
		cfg  config.SinkConfig
	}{
		{"stdout", config.SinkConfig{Type: "stdout"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := stdout.Factory("orders", tt.cfg)
			require.NoError(t, err)
			require.NotNil(t, s)
			require.NoError(t, s.Close())
		})
	}
}
