package dlq_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sebdah/goldie/v2"
	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/event"
	"github.com/benipranata/walx/internal/sink/appendlog"
	"github.com/benipranata/walx/internal/sink/dlq"
	"github.com/benipranata/walx/internal/sink/jsonl"
	"github.com/benipranata/walx/internal/sink/sinktest"
)

var failedAt = time.Date(2026, 3, 4, 5, 6, 7, 89, time.FixedZone("x", 3600))

func open(t *testing.T, path string) *dlq.Sink {
	t.Helper()
	s, err := dlq.Open(path, "orders", "nats", dlq.WithClock(func() time.Time { return failedAt }))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func goldenBatch() event.Batch {
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	return event.Batch{Events: []*event.Event{
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
	}}
}

func TestEnvelopeGolden(t *testing.T) {
	tests := []struct {
		name  string
		batch event.Batch
		cause error
	}{
		{name: "envelope", batch: goldenBatch(), cause: event.Permanent(errors.New("message too big: <1MiB> & more"))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "d.jsonl")
			require.NoError(t, open(t, path).SendFailed(context.Background(), tt.batch, tt.cause))
			got, err := os.ReadFile(path)
			require.NoError(t, err)
			goldie.New(t).Assert(t, tt.name, got)
		})
	}
}

func TestEventIsByteIdenticalToTheSinkEncoding(t *testing.T) {
	tests := []struct {
		name  string
		batch event.Batch
	}{
		{name: "golden batch", batch: goldenBatch()},
		{name: "single event", batch: sinktest.Batch("a", 1)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "d.jsonl")
			require.NoError(t, open(t, path).SendFailed(context.Background(), tt.batch, errors.New("boom")))
			want, err := jsonl.Encode(tt.batch.Events)
			require.NoError(t, err)
			wantLines := strings.Split(strings.TrimSuffix(string(want), "\n"), "\n")
			raw, err := os.ReadFile(path)
			require.NoError(t, err)
			lines := strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n")
			require.Len(t, lines, len(wantLines))
			for i, l := range lines {
				var env struct {
					FailedAt string          `json:"failed_at"`
					Event    json.RawMessage `json:"event"`
				}
				require.NoError(t, json.Unmarshal([]byte(l), &env))
				require.Equal(t, wantLines[i], string(env.Event))
				require.Contains(t, l, `"event":`+wantLines[i]+"}")
				require.Equal(t, "2026-03-04T04:06:07.000000089Z", env.FailedAt)
			}
		})
	}
}

func TestSendFailedErrors(t *testing.T) {
	bad := sinktest.Batch("a", 2)
	bad.Events[1].After = map[string]any{"v": math.NaN()}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	tests := []struct {
		name      string
		ctx       context.Context
		batch     event.Batch
		permanent bool
		want      string
	}{
		{name: "unencodable event is permanent and writes nothing", ctx: context.Background(), batch: bad, permanent: true, want: "a-2"},
		{name: "cancelled context", ctx: cancelled, batch: sinktest.Batch("a", 1), want: "canceled"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "d.jsonl")
			err := open(t, path).SendFailed(tt.ctx, tt.batch, errors.New("boom"))
			require.ErrorContains(t, err, tt.want)
			require.Equal(t, tt.permanent, event.IsPermanent(err))
			got, _ := os.ReadFile(path)
			require.Empty(t, got)
		})
	}
}

func TestSendFailedAfterClose(t *testing.T) {
	tests := []struct{ name string }{{"send after close"}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := open(t, filepath.Join(t.TempDir(), "d.jsonl"))
			require.NoError(t, s.Close())
			err := s.SendFailed(context.Background(), sinktest.Batch("a", 1), nil)
			require.ErrorIs(t, err, appendlog.ErrClosed)
			require.False(t, event.IsPermanent(err))
		})
	}
}

func TestNilCause(t *testing.T) {
	tests := []struct{ name string }{{"nil cause writes an empty error"}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "d.jsonl")
			require.NoError(t, open(t, path).SendFailed(context.Background(), sinktest.Batch("a", 1), nil))
			got, _ := os.ReadFile(path)
			require.Contains(t, string(got), `"error":""`)
		})
	}
}

func TestOpen(t *testing.T) {
	dir := t.TempDir()
	existing := filepath.Join(dir, "existing.jsonl")
	require.NoError(t, os.WriteFile(existing, []byte("old\n"), 0o644))
	torn := filepath.Join(dir, "torn.jsonl")
	require.NoError(t, os.WriteFile(torn, []byte("{\"failed_at\":"), 0o644))
	tests := []struct {
		name    string
		path    string
		wantErr string
		want    int
	}{
		{name: "creates a missing file", path: filepath.Join(dir, "new.jsonl"), want: 1},
		{name: "appends to an existing file", path: existing, want: 2},
		{name: "ends a torn last line", path: torn, want: 2},
		{name: "missing directory", path: filepath.Join(dir, "nodir", "x"), wantErr: "nodir"},
		{name: "empty path", path: "", wantErr: "dlq.path"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := dlq.Open(tt.path, "orders", "file")
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				require.Nil(t, s)
				return
			}
			require.NoError(t, err)
			require.NoError(t, s.SendFailed(context.Background(), sinktest.Batch("a", 1), errors.New("x")))
			require.NoError(t, s.Close())
			got, err := os.ReadFile(tt.path)
			require.NoError(t, err)
			require.Equal(t, tt.want, bytes.Count(got, []byte("\n")))
		})
	}
}

// asSink adapts a dead-letter sink to event.Sink for the contract suite.
type asSink struct{ *dlq.Sink }

func (a asSink) Send(ctx context.Context, b event.Batch) error {
	return a.SendFailed(ctx, b, errors.New("rejected"))
}

func TestContract(t *testing.T) {
	sinktest.Run(t, func(t *testing.T) sinktest.Target {
		path := filepath.Join(t.TempDir(), "d.jsonl")
		unwritable := sinktest.Batch("u", 1)
		unwritable.Events[0].After = map[string]any{"v": math.NaN()}
		return sinktest.Target{
			Sink: asSink{open(t, path)},
			Written: func() []string {
				data, err := os.ReadFile(path)
				require.NoError(t, err)
				var out []string
				sc := bufio.NewScanner(bytes.NewReader(data))
				for sc.Scan() {
					var env struct {
						Event struct {
							ID string `json:"id"`
						} `json:"event"`
					}
					require.NoError(t, json.Unmarshal(sc.Bytes(), &env))
					out = append(out, env.Event.ID)
				}
				return out
			},
			Unwritable: &unwritable,
		}
	})
}
