package engine

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/config"
	"github.com/benipranata/walx/internal/event"
	"github.com/benipranata/walx/internal/postgres"
)

var errDropped = errors.New("connection reset")

// feedRange sends messages from..to (WALStart i, stubbed to batch EndLSN 10*i).
func feedRange(ctx context.Context, src *fakeSource, from, to int) {
	go func() {
		for i := from; i <= to; i++ {
			select {
			case src.out <- pglogrepl.XLogData{WALStart: pglogrepl.LSN(i)}:
			case <-ctx.Done():
				return
			}
		}
	}()
}

// factory hands out the given sources in order and records each resume LSN.
type factory struct {
	mu      sync.Mutex
	srcs    []*fakeSource
	errs    []error // returned before the next source, in order
	resumes []pglogrepl.LSN
	calls   int
	onCall  func()
}

func (f *factory) New(_ context.Context, resume pglogrepl.LSN) (Source, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.resumes = append(f.resumes, resume)
	if f.onCall != nil {
		f.onCall()
	}
	if len(f.errs) > 0 {
		err := f.errs[0]
		f.errs = f.errs[1:]
		return nil, err
	}
	s := f.srcs[0]
	f.srcs = f.srcs[1:]
	return s, nil
}

func (f *factory) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *factory) Resumes() []pglogrepl.LSN {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]pglogrepl.LSN(nil), f.resumes...)
}

// sleeps records backoff waits and returns at once.
type sleeps struct {
	mu sync.Mutex
	ds []time.Duration
}

func (s *sleeps) sleep(_ context.Context, d time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ds = append(s.ds, d)
	return nil
}

func (s *sleeps) List() []time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]time.Duration(nil), s.ds...)
}

func reconnectEngine(cfg Config, f *factory, sl *sleeps) *Engine {
	cfg.NewSource = f.New
	e := New(cfg)
	stubDecode(e)
	e.jitter = func(d time.Duration) time.Duration { return d }
	e.sleep = sl.sleep
	return e
}

func TestReconnect(t *testing.T) {
	retry := config.RetryConfig{Initial: 100 * time.Millisecond, Max: 400 * time.Millisecond}
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "a dropped source reconnects from the confirmed LSN and the stream continues", run: func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			src1, src2 := newFakeSource(), newFakeSource()
			f, sl := &factory{srcs: []*fakeSource{src2}}, &sleeps{}
			var logs bytes.Buffer
			var mu sync.Mutex
			var sent []event.LSN
			sink := event.SinkFunc(func(_ context.Context, b event.Batch) error {
				mu.Lock()
				defer mu.Unlock()
				sent = append(sent, b.EndLSN)
				return nil
			})
			e := reconnectEngine(Config{Name: "a", Source: src1, Sink: sink, Retry: retry,
				Logger: slog.New(slog.NewTextHandler(&logs, nil))}, f, sl)
			done := runEngine(ctx, e)
			feedRange(ctx, src1, 1, 2)
			require.Eventually(t, func() bool { return len(src1.Confirmed()) == 2 }, 5*time.Second, 5*time.Millisecond)
			src1.runErr <- errDropped
			require.Eventually(t, func() bool { return f.Calls() == 1 }, 5*time.Second, 5*time.Millisecond)
			feedRange(ctx, src2, 3, 4)
			require.Eventually(t, func() bool { return len(src2.Confirmed()) == 2 }, 5*time.Second, 5*time.Millisecond)
			cancel()
			require.ErrorIs(t, wait(t, done), context.Canceled)

			require.Equal(t, []pglogrepl.LSN{20}, f.Resumes())
			require.Equal(t, []time.Duration{100 * time.Millisecond}, sl.List())
			require.Equal(t, []pglogrepl.LSN{10, 20}, src1.Confirmed(), "the old source gets no confirm after the reconnect")
			require.Equal(t, []pglogrepl.LSN{30, 40}, src2.Confirmed())
			mu.Lock()
			require.Equal(t, []event.LSN{10, 20, 30, 40}, sent)
			mu.Unlock()
			require.Contains(t, logs.String(), "reconnecting")
			require.Contains(t, logs.String(), "attempt=1")
			require.Contains(t, logs.String(), "resume_lsn=0/14")
			require.Contains(t, logs.String(), "connection reset")
		}},
		{name: "queued batches are delivered before the source restarts", run: func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			src1, src2 := newFakeSource(), newFakeSource()
			f, sl := &factory{srcs: []*fakeSource{src2}}, &sleeps{}
			gate := make(chan struct{})
			sink := event.SinkFunc(func(ctx context.Context, b event.Batch) error {
				select {
				case <-gate:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			})
			e := reconnectEngine(Config{Name: "a", Source: src1, Sink: sink, QueueSize: 4}, f, sl)
			done := runEngine(ctx, e)
			feedRange(ctx, src1, 1, 3)
			require.Eventually(t, func() bool { return e.pending.Load() == 3 }, 5*time.Second, 5*time.Millisecond)
			src1.runErr <- errDropped
			time.Sleep(50 * time.Millisecond)
			require.Zero(t, f.Calls(), "no reconnect while batches are queued")
			close(gate)
			require.Eventually(t, func() bool { return f.Calls() == 1 }, 5*time.Second, 5*time.Millisecond)
			require.Equal(t, []pglogrepl.LSN{30}, f.Resumes())
			cancel()
			require.ErrorIs(t, wait(t, done), context.Canceled)
		}},
		{name: "a partial transaction is discarded on reconnect", run: func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			src1, src2 := newFakeSource(), newFakeSource()
			buf := postgres.NewTxBuffer("a", postgres.NewRelations(), nil)
			f, sl := &factory{srcs: []*fakeSource{src2}}, &sleeps{}
			var inTx bool
			f.onCall = func() { inTx = buf.InTx() } // runs on the reader, like Push
			sink := event.SinkFunc(func(context.Context, event.Batch) error { return nil })
			cfg := Config{Name: "a", Source: src1, Sink: sink, Buffer: buf}
			cfg.NewSource = f.New
			e := New(cfg)
			e.jitter = func(d time.Duration) time.Duration { return d }
			e.sleep = sl.sleep
			pushed := make(chan bool, 1)
			inner := e.decode
			e.decode = func(x pglogrepl.XLogData) (*event.Batch, error) {
				b, err := inner(x)
				pushed <- buf.InTx()
				return b, err
			}
			done := runEngine(ctx, e)
			src1.out <- pglogrepl.XLogData{WALStart: 1, WALData: beginMessage(100)}
			require.True(t, <-pushed)
			src1.runErr <- errDropped
			require.Eventually(t, func() bool { return f.Calls() == 1 }, 5*time.Second, 5*time.Millisecond)
			require.False(t, inTx)
			cancel()
			require.ErrorIs(t, wait(t, done), context.Canceled)
		}},
		{name: "ErrSlotActive is retried with a doubling capped backoff", run: func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			src1, src2 := newFakeSource(), newFakeSource()
			active := fmt.Errorf("slot: %w", postgres.ErrSlotActive)
			f, sl := &factory{srcs: []*fakeSource{src2}, errs: []error{active, active, active}}, &sleeps{}
			e := reconnectEngine(Config{Name: "a", Source: src1, Sink: event.SinkFunc(nil), Retry: retry}, f, sl)
			done := runEngine(ctx, e)
			src1.runErr <- errDropped
			require.Eventually(t, func() bool { return f.Calls() == 4 }, 5*time.Second, 5*time.Millisecond)
			require.Equal(t, []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 400 * time.Millisecond}, sl.List())
			cancel()
			require.ErrorIs(t, wait(t, done), context.Canceled)
		}},
		{name: "the backoff restarts after the new stream made progress", run: func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			src1, src2, src3 := newFakeSource(), newFakeSource(), newFakeSource()
			f, sl := &factory{srcs: []*fakeSource{src2, src3}, errs: []error{errDropped}}, &sleeps{}
			e := reconnectEngine(Config{Name: "a", Source: src1, Sink: event.SinkFunc(func(context.Context, event.Batch) error { return nil }), Retry: retry}, f, sl)
			done := runEngine(ctx, e)
			src1.runErr <- errDropped // no progress; first attempt fails; second yields src2
			require.Eventually(t, func() bool { return f.Calls() == 2 }, 5*time.Second, 5*time.Millisecond)
			feedRange(ctx, src2, 1, 1)
			require.Eventually(t, func() bool { return len(src2.Confirmed()) == 1 }, 5*time.Second, 5*time.Millisecond)
			src2.runErr <- errDropped
			require.Eventually(t, func() bool { return f.Calls() == 3 }, 5*time.Second, 5*time.Millisecond)
			require.Equal(t, []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 100 * time.Millisecond}, sl.List())
			cancel()
			require.ErrorIs(t, wait(t, done), context.Canceled)
		}},
		{name: "a stream that keeps failing without progress backs off further", run: func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			src1, src2, src3 := newFakeSource(), newFakeSource(), newFakeSource()
			f, sl := &factory{srcs: []*fakeSource{src2, src3}}, &sleeps{}
			e := reconnectEngine(Config{Name: "a", Source: src1, Sink: event.SinkFunc(nil), Retry: retry}, f, sl)
			done := runEngine(ctx, e)
			src1.runErr <- errDropped
			require.Eventually(t, func() bool { return f.Calls() == 1 }, 5*time.Second, 5*time.Millisecond)
			src2.runErr <- errDropped
			require.Eventually(t, func() bool { return f.Calls() == 2 }, 5*time.Second, 5*time.Millisecond)
			require.Equal(t, []time.Duration{100 * time.Millisecond, 200 * time.Millisecond}, sl.List())
			cancel()
			require.ErrorIs(t, wait(t, done), context.Canceled)
		}},
		{name: "cancel during the reconnect backoff stops the run", run: func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			src1 := newFakeSource()
			f := &factory{}
			e := reconnectEngine(Config{Name: "a", Source: src1, Sink: event.SinkFunc(nil)}, f, &sleeps{})
			waiting := make(chan struct{})
			e.sleep = func(ctx context.Context, _ time.Duration) error {
				close(waiting)
				<-ctx.Done()
				return ctx.Err()
			}
			done := runEngine(ctx, e)
			src1.runErr <- errDropped
			<-waiting
			cancel()
			require.ErrorIs(t, wait(t, done), context.Canceled)
			require.Zero(t, f.Calls())
		}},
		{name: "cancel while the factory is connecting stops the run", run: func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			src1 := newFakeSource()
			f, sl := &factory{}, &sleeps{}
			e := reconnectEngine(Config{Name: "a", Source: src1, Sink: event.SinkFunc(nil)}, f, sl)
			entered := make(chan struct{})
			e.cfg.NewSource = func(ctx context.Context, _ pglogrepl.LSN) (Source, error) {
				close(entered)
				<-ctx.Done()
				return nil, fmt.Errorf("reconnect: %w", ctx.Err())
			}
			done := runEngine(ctx, e)
			src1.runErr <- errDropped
			<-entered
			cancel()
			require.ErrorIs(t, wait(t, done), context.Canceled)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

func TestReconnectFatal(t *testing.T) {
	auth := fmt.Errorf("reconnect: connect: %w", &pgconn.PgError{Code: "28P01"})
	tests := []struct {
		name      string
		srcErr    error // the first source fails with it
		factErr   error // the factory fails with it
		decodeErr error // decoding the first message fails with it
		want      error
		calls     int
	}{
		{name: "publication missing on the stream", srcErr: fmt.Errorf("x: %w", postgres.ErrPublicationMissing), want: postgres.ErrPublicationMissing},
		{name: "slot lost on the stream", srcErr: fmt.Errorf("x: %w", postgres.ErrSlotLost), want: postgres.ErrSlotLost},
		{name: "unsupported version from the factory", srcErr: errDropped, factErr: fmt.Errorf("reconnect: %w", postgres.ErrUnsupportedVersion), want: postgres.ErrUnsupportedVersion, calls: 1},
		{name: "auth failure from the factory", srcErr: errDropped, factErr: auth, want: auth, calls: 1},
		{name: "slot lost from the factory", srcErr: errDropped, factErr: fmt.Errorf("x: %w", postgres.ErrSlotLost), want: postgres.ErrSlotLost, calls: 1},
		{name: "transaction too large", decodeErr: &postgres.TxTooLargeError{Limit: postgres.LimitBytes}, want: &postgres.TxTooLargeError{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src1 := newFakeSource()
			var errs []error
			if tt.factErr != nil {
				errs = []error{tt.factErr}
			}
			f, sl := &factory{errs: errs}, &sleeps{}
			e := reconnectEngine(Config{Name: "a", Source: src1, Sink: event.SinkFunc(nil)}, f, sl)
			if tt.decodeErr != nil {
				e.decode = func(pglogrepl.XLogData) (*event.Batch, error) { return nil, tt.decodeErr }
			}
			done := runEngine(context.Background(), e)
			if tt.decodeErr != nil {
				feedRange(context.Background(), src1, 1, 1)
			} else {
				src1.runErr <- tt.srcErr
			}
			err := wait(t, done)
			if tt.name == "transaction too large" {
				var tooLarge *postgres.TxTooLargeError
				require.ErrorAs(t, err, &tooLarge)
			} else {
				require.ErrorIs(t, err, tt.want)
			}
			require.Equal(t, tt.calls, f.Calls())
		})
	}
}

// beginMessage returns a pgoutput Begin message for xid.
func beginMessage(xid uint32) []byte {
	b := []byte{'B'}
	b = binary.BigEndian.AppendUint64(b, 100) // final LSN
	b = binary.BigEndian.AppendUint64(b, 0)   // commit time
	return binary.BigEndian.AppendUint32(b, xid)
}
