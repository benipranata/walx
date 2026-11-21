package engine

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/benipranata/walx/internal/event"
	"github.com/benipranata/walx/internal/postgres"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

// fakeSource is a Source fed from the test.
type fakeSource struct {
	out       chan pglogrepl.XLogData
	runErr    chan error
	mu        sync.Mutex
	confirmed []pglogrepl.LSN
}

func newFakeSource() *fakeSource {
	return &fakeSource{out: make(chan pglogrepl.XLogData), runErr: make(chan error, 1)}
}

func (f *fakeSource) Run(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-f.runErr:
		return err
	}
}

func (f *fakeSource) Messages() <-chan pglogrepl.XLogData { return f.out }

func (f *fakeSource) SetConfirmed(l pglogrepl.LSN) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.confirmed = append(f.confirmed, l)
}

func (f *fakeSource) Confirmed() []pglogrepl.LSN {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]pglogrepl.LSN(nil), f.confirmed...)
}

// feed queues n messages whose decode (stubbed) yields one batch each; batch i
// has EndLSN 10*i and one event.
func feed(ctx context.Context, src *fakeSource, n int) {
	go func() {
		for i := 1; i <= n; i++ {
			select {
			case src.out <- pglogrepl.XLogData{WALStart: pglogrepl.LSN(i)}:
			case <-ctx.Done():
				return
			}
		}
	}()
}

func stubDecode(e *Engine) {
	e.decode = func(x pglogrepl.XLogData) (*event.Batch, error) {
		return &event.Batch{
			Events:    []*event.Event{{ID: "d-" + event.LSN(x.WALStart).String(), Op: event.OpInsert}},
			CommitLSN: event.LSN(x.WALStart*10 - 1),
			EndLSN:    event.LSN(x.WALStart * 10),
		}, nil
	}
}

func runEngine(ctx context.Context, e *Engine) <-chan error {
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()
	return done
}

func wait(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("engine did not stop")
		return nil
	}
}

func TestEngineRun(t *testing.T) {
	boom := errors.New("boom")
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "batches are sent in order and confirmed after the sink returns", run: func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			src := newFakeSource()
			var sent []event.LSN
			var violations int
			sink := event.SinkFunc(func(_ context.Context, b event.Batch) error {
				for _, c := range src.Confirmed() {
					if event.LSN(c) >= b.EndLSN {
						violations++
					}
				}
				sent = append(sent, b.EndLSN)
				return nil
			})
			e := New(Config{Name: "a", Source: src, Sink: sink, QueueSize: 2})
			stubDecode(e)
			done := runEngine(ctx, e)
			feed(ctx, src, 5)
			require.Eventually(t, func() bool { return len(src.Confirmed()) == 5 }, 5*time.Second, 5*time.Millisecond)
			cancel()
			require.ErrorIs(t, wait(t, done), context.Canceled)
			require.Equal(t, []event.LSN{10, 20, 30, 40, 50}, sent)
			require.Equal(t, []pglogrepl.LSN{10, 20, 30, 40, 50}, src.Confirmed())
			require.Zero(t, violations)
		}},
		{name: "a sink error stops the run without confirming", run: func(t *testing.T) {
			src := newFakeSource()
			sink := event.SinkFunc(func(context.Context, event.Batch) error { return boom })
			e := New(Config{Name: "a", Source: src, Sink: sink})
			stubDecode(e)
			done := runEngine(context.Background(), e)
			feed(context.Background(), src, 1)
			err := wait(t, done)
			require.ErrorIs(t, err, boom)
			require.Empty(t, src.Confirmed())
		}},
		{name: "a source error stops the run", run: func(t *testing.T) {
			src := newFakeSource()
			e := New(Config{Name: "a", Source: src, Sink: event.SinkFunc(nil)})
			done := runEngine(context.Background(), e)
			src.runErr <- postgres.ErrStreamEnded
			require.ErrorIs(t, wait(t, done), postgres.ErrStreamEnded)
		}},
		{name: "a decode error stops the run", run: func(t *testing.T) {
			src := newFakeSource()
			e := New(Config{Name: "a", Source: src, Sink: event.SinkFunc(nil)})
			e.decode = func(pglogrepl.XLogData) (*event.Batch, error) { return nil, boom }
			done := runEngine(context.Background(), e)
			feed(context.Background(), src, 1)
			require.ErrorIs(t, wait(t, done), boom)
		}},
		{name: "messages without a commit produce no batch", run: func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			src := newFakeSource()
			var calls, seen atomic.Int32
			e := New(Config{Name: "a", Source: src, Sink: event.SinkFunc(func(context.Context, event.Batch) error { calls.Add(1); return nil })})
			e.decode = func(pglogrepl.XLogData) (*event.Batch, error) { seen.Add(1); return nil, nil }
			done := runEngine(ctx, e)
			feed(ctx, src, 3)
			require.Eventually(t, func() bool { return seen.Load() == 3 }, 5*time.Second, 5*time.Millisecond)
			cancel()
			require.ErrorIs(t, wait(t, done), context.Canceled)
			require.Zero(t, calls.Load())
		}},
		{name: "batches the filter empties never reach the sink or the confirm", run: func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			src := newFakeSource()
			var sent []event.LSN
			sink := event.SinkFunc(func(_ context.Context, b event.Batch) error { sent = append(sent, b.EndLSN); return nil })
			drop := func(b event.Batch) event.Batch {
				if b.EndLSN == 20 {
					b.Events = nil
				}
				return b
			}
			e := New(Config{Name: "a", Source: src, Sink: sink, Filter: drop})
			stubDecode(e)
			var empties []event.LSN
			e.empty = func(b event.Batch) { empties = append(empties, b.EndLSN) }
			done := runEngine(ctx, e)
			feed(ctx, src, 3)
			require.Eventually(t, func() bool { return len(src.Confirmed()) == 2 }, 5*time.Second, 5*time.Millisecond)
			cancel()
			require.ErrorIs(t, wait(t, done), context.Canceled)
			require.Equal(t, []event.LSN{10, 30}, sent)
			require.Equal(t, []event.LSN{20}, empties)
			require.Equal(t, []pglogrepl.LSN{10, 30}, src.Confirmed())
		}},
		{name: "a blocked sink bounds the queue and applies backpressure", run: func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			src := newFakeSource()
			release := make(chan struct{})
			entered := make(chan struct{}, 1)
			sink := event.SinkFunc(func(ctx context.Context, b event.Batch) error {
				select {
				case entered <- struct{}{}:
				default:
				}
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
			const queue = 3
			e := New(Config{Name: "a", Source: src, Sink: sink, QueueSize: queue})
			var mu sync.Mutex
			var decoded int
			e.decode = func(x pglogrepl.XLogData) (*event.Batch, error) {
				mu.Lock()
				decoded++
				mu.Unlock()
				return &event.Batch{Events: []*event.Event{{}}, EndLSN: event.LSN(x.WALStart)}, nil
			}
			done := runEngine(ctx, e)
			feed(ctx, src, 50)
			<-entered
			time.Sleep(200 * time.Millisecond)
			mu.Lock()
			// one in the sink, queue slots, and one the reader holds while blocked
			require.LessOrEqual(t, decoded, queue+2)
			require.Greater(t, decoded, queue)
			mu.Unlock()
			close(release)
			require.Eventually(t, func() bool { return len(src.Confirmed()) == 50 }, 5*time.Second, 5*time.Millisecond)
			cancel()
			require.ErrorIs(t, wait(t, done), context.Canceled)
		}},
		{name: "cancel during a blocked send returns the context error", run: func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			src := newFakeSource()
			entered := make(chan struct{})
			sink := event.SinkFunc(func(ctx context.Context, b event.Batch) error {
				close(entered)
				<-ctx.Done()
				return ctx.Err()
			})
			e := New(Config{Name: "a", Source: src, Sink: sink})
			stubDecode(e)
			done := runEngine(ctx, e)
			feed(ctx, src, 1)
			<-entered
			cancel()
			require.ErrorIs(t, wait(t, done), context.Canceled)
			require.Empty(t, src.Confirmed())
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

func beginMsg(final event.LSN, xid uint32) []byte {
	b := []byte{'B'}
	b = binary.BigEndian.AppendUint64(b, uint64(final))
	b = binary.BigEndian.AppendUint64(b, 0)
	return binary.BigEndian.AppendUint32(b, xid)
}

func commitMsg(commit, end event.LSN) []byte {
	b := []byte{'C', 0}
	b = binary.BigEndian.AppendUint64(b, uint64(commit))
	b = binary.BigEndian.AppendUint64(b, uint64(end))
	return binary.BigEndian.AppendUint64(b, 0)
}

func TestDecodeMessage(t *testing.T) {
	tests := []struct {
		name    string
		msgs    [][]byte
		want    *event.Batch
		wantErr string
	}{
		{name: "begin gives no batch", msgs: [][]byte{beginMsg(100, 7)}},
		{name: "commit gives an empty batch", msgs: [][]byte{beginMsg(100, 7), commitMsg(100, 120)},
			want: &event.Batch{CommitLSN: 100, EndLSN: 120}},
		{name: "garbage fails to parse", msgs: [][]byte{{'?'}}, wantErr: "parse pgoutput message"},
		{name: "empty payload fails to parse", msgs: [][]byte{{}}, wantErr: "parse pgoutput message"},
		{name: "commit without begin fails", msgs: [][]byte{commitMsg(100, 120)}, wantErr: "commit without begin"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := postgres.NewTxBuffer("a", postgres.NewRelations(), nil)
			e := New(Config{Name: "a", Source: newFakeSource(), Buffer: buf, Sink: event.SinkFunc(nil)})
			var got *event.Batch
			var err error
			for _, m := range tt.msgs {
				got, err = e.decode(pglogrepl.XLogData{WALStart: 1, WALData: m})
				if err != nil {
					break
				}
			}
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestNewDefaults(t *testing.T) {
	tests := []struct {
		name string
		size int
		want int
	}{
		{name: "zero uses the default", size: 0, want: defaultQueueSize},
		{name: "negative uses the default", size: -1, want: defaultQueueSize},
		{name: "explicit size kept", size: 4, want: 4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := New(Config{Name: "a", Source: newFakeSource(), Sink: event.SinkFunc(nil), QueueSize: tt.size})
			require.Equal(t, tt.want, e.cfg.QueueSize)
			require.NotNil(t, e.log)
		})
	}
}

func TestLogFields(t *testing.T) {
	boom := errors.New("boom")
	tests := []struct {
		name     string
		sinkErr  error
		wantMsg  string
		wantKeys map[string]any
	}{
		{name: "delivered batch", wantMsg: "batch delivered", wantKeys: map[string]any{
			"db": "orders", "commit_lsn": "0/13", "end_lsn": "0/14", "events": float64(1), "level": "DEBUG"}},
		{name: "sink failure", sinkErr: boom, wantMsg: "sink send failed", wantKeys: map[string]any{
			"db": "orders", "commit_lsn": "0/13", "end_lsn": "0/14", "events": float64(1), "err": "boom", "level": "ERROR"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var out syncBuffer
			log := slog.New(slog.NewJSONHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug}))
			src := newFakeSource()
			sink := event.SinkFunc(func(context.Context, event.Batch) error { return tt.sinkErr })
			e := New(Config{Name: "orders", Source: src, Sink: sink, Logger: log})
			e.decode = func(pglogrepl.XLogData) (*event.Batch, error) {
				return &event.Batch{Events: []*event.Event{{}}, CommitLSN: 0x13, EndLSN: 0x14}, nil
			}
			done := runEngine(ctx, e)
			feed(ctx, src, 1)
			if tt.sinkErr != nil {
				require.ErrorIs(t, wait(t, done), tt.sinkErr)
			} else {
				require.Eventually(t, func() bool { return len(src.Confirmed()) == 1 }, 5*time.Second, 5*time.Millisecond)
				cancel()
				require.ErrorIs(t, wait(t, done), context.Canceled)
			}
			var found bool
			for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
				var rec map[string]any
				require.NoError(t, json.Unmarshal([]byte(line), &rec))
				if rec["msg"] != tt.wantMsg {
					continue
				}
				found = true
				for k, v := range tt.wantKeys {
					require.Equal(t, v, rec[k], k)
				}
			}
			require.True(t, found, "no %q line in %s", tt.wantMsg, out.String())
		})
	}
}

func TestEmptyBatchLog(t *testing.T) {
	tests := []struct{ name string }{{name: "default hook logs at debug"}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out syncBuffer
			log := slog.New(slog.NewJSONHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug}))
			e := New(Config{Name: "a", Source: newFakeSource(), Sink: event.SinkFunc(nil), Logger: log})
			e.empty(event.Batch{CommitLSN: 1, EndLSN: 2})
			require.Contains(t, out.String(), `"msg":"empty batch not sent"`)
			require.Contains(t, out.String(), `"end_lsn":"0/2"`)
		})
	}
}

func TestStoppedLog(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "failure logs the error", err: errors.New("lost"), want: `"level":"ERROR","msg":"engine stopped"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out syncBuffer
			log := slog.New(slog.NewJSONHandler(&out, nil))
			src := newFakeSource()
			e := New(Config{Name: "a", Source: src, Sink: event.SinkFunc(nil), Logger: log})
			done := runEngine(context.Background(), e)
			src.runErr <- tt.err
			require.ErrorIs(t, wait(t, done), tt.err)
			require.Contains(t, out.String(), tt.want)
			require.Contains(t, out.String(), `"err":"lost"`)
		})
	}
}

// syncBuffer is a bytes.Buffer safe for the logger's concurrent writers.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
