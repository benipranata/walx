package nats_test

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/benipranata/walx/internal/event"
	natssink "github.com/benipranata/walx/internal/sink/nats"
	"github.com/benipranata/walx/internal/sink/sinktest"
	"github.com/benipranata/walx/internal/sink/subject"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func TestContract(t *testing.T) {
	tests := []struct {
		name   string
		break_ func(f *fakePub)
		fix    func(f *fakePub)
	}{
		{"ack error", func(f *fakePub) { f.ackErr = errBroken }, func(f *fakePub) { f.ackErr = nil }},
		{"publish error", func(f *fakePub) { f.publishErr = errBroken }, func(f *fakePub) { f.publishErr = nil }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sinktest.Run(t, func(t *testing.T) sinktest.Target {
				f := &fakePub{}
				return sinktest.Target{
					Sink:    natssink.New(f, natssink.Options{Database: "db"}),
					Written: f.ids,
					Break: func() func() {
						f.set(tt.break_)
						return func() { f.set(tt.fix) }
					},
					Unwritable: &event.Batch{Events: []*event.Event{
						{ID: "a-1", Op: event.OpInsert, Schema: "public", Table: "t", After: map[string]any{"v": 1}},
						{ID: "bad", Op: event.OpInsert, Schema: "public", Table: "t", After: map[string]any{"v": math.NaN()}},
					}},
				}
			})
		})
	}
}

func TestSubjectAndHeaders(t *testing.T) {
	status := func(from, to any) *event.Event {
		e := &event.Event{ID: "x-1", Source: "db", Op: event.OpUpdate, Schema: "public", Table: "orders"}
		if from != nil {
			e.Before = map[string]any{"status": from}
		}
		e.After = map[string]any{"status": to}
		return e
	}
	tests := []struct {
		name     string
		e        *event.Event
		tmpl     string
		wantSubj string
		wantHdr  map[string]string
		noHdr    []string
	}{
		{
			name:     "non-fsm table uses the op",
			e:        &event.Event{ID: "x-1", Source: "db", Op: event.OpDelete, Schema: "public", Table: "users", Before: map[string]any{"id": 1}},
			wantSubj: "cdc.public.users.delete",
			wantHdr: map[string]string{
				"Nats-Msg-Id": "x-1", "Walx-Op": "delete", "Walx-Source": "db", "Walx-Schema": "public",
				"Walx-Table": "users", "Walx-Commit-Lsn": "0/1",
			},
			noHdr: []string{"Walx-State-From", "Walx-State-To"},
		},
		{
			name:     "fsm table uses the new state",
			e:        status("new", "paid"),
			wantSubj: "cdc.public.orders.paid",
			wantHdr:  map[string]string{"Walx-State-From": "new", "Walx-State-To": "paid", "Walx-Op": "update"},
		},
		{
			name:     "insert has a state but no from state",
			e:        &event.Event{ID: "x-1", Source: "db", Op: event.OpInsert, Schema: "public", Table: "orders", After: map[string]any{"status": "new"}},
			wantSubj: "cdc.public.orders.new",
			wantHdr:  map[string]string{"Walx-State-To": "new"},
			noHdr:    []string{"Walx-State-From"},
		},
		{
			name:     "reference only event has no from state",
			e:        status(nil, "paid"),
			wantSubj: "cdc.public.orders.paid",
			wantHdr:  map[string]string{"Walx-State-To": "paid"},
			noHdr:    []string{"Walx-State-From"},
		},
		{
			name:     "state colliding with an op is escaped",
			e:        status("new", "delete"),
			wantSubj: "cdc.public.orders.state_delete",
			wantHdr:  map[string]string{"Walx-State-To": "delete"},
		},
		{
			name:     "state with reserved characters",
			e:        status("new", "in.progress *"),
			wantSubj: "cdc.public.orders.in_progress__",
			wantHdr:  map[string]string{"Walx-State-To": "in.progress *"},
		},
		{
			name: "unchanged state reads Before",
			e: &event.Event{ID: "x-1", Source: "db", Op: event.OpUpdate, Schema: "public", Table: "orders",
				Before: map[string]any{"status": "paid"}, After: map[string]any{"status": event.Unchanged}},
			wantSubj: "cdc.public.orders.paid",
			wantHdr:  map[string]string{"Walx-State-From": "paid", "Walx-State-To": "paid"},
		},
		{
			name:     "delete on an fsm table uses the op",
			e:        &event.Event{ID: "x-1", Source: "db", Op: event.OpDelete, Schema: "public", Table: "orders", Before: map[string]any{"status": "paid"}},
			wantSubj: "cdc.public.orders.delete",
			noHdr:    []string{"Walx-State-To", "Walx-State-From"},
		},
		{
			name:     "null state falls back to the op",
			e:        status("new", nil),
			wantSubj: "cdc.public.orders.update",
			noHdr:    []string{"Walx-State-To"},
		},
		{
			name:     "empty state falls back to the op",
			e:        status("new", ""),
			wantSubj: "cdc.public.orders.update",
			noHdr:    []string{"Walx-State-To"},
		},
		{
			name:     "numeric state",
			e:        status(1, 2),
			wantSubj: "cdc.public.orders.2",
			wantHdr:  map[string]string{"Walx-State-From": "1", "Walx-State-To": "2"},
		},
		{
			name:     "line break in a state is kept out of the header",
			e:        status("new", "a\nb"),
			wantSubj: "cdc.public.orders.a_b",
			wantHdr:  map[string]string{"Walx-State-To": "a b"},
		},
		{
			name:     "long state is cut in the header",
			e:        status("new", strings.Repeat("s", 2000)),
			wantSubj: "cdc.public.orders." + strings.Repeat("s", 2000),
			wantHdr:  map[string]string{"Walx-State-To": strings.Repeat("s", 1024)},
		},
		{
			name:     "custom template with database",
			e:        status("new", "paid"),
			tmpl:     "{database}.{table}.{token}",
			wantSubj: "my_db.orders.paid",
		},
		{
			name:     "reserved characters in names",
			e:        &event.Event{ID: "x-1", Op: event.OpInsert, Schema: "my.schema", Table: "a b", After: map[string]any{"id": 1}},
			wantSubj: "cdc.my_schema.a_b.insert",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakePub{}
			opts := natssink.Options{Database: "my.db", FSMColumns: map[string]string{"public.orders": "status"}}
			if tt.tmpl != "" {
				opts.Database = "my_db"
				tm, err := subject.Parse(tt.tmpl)
				require.NoError(t, err)
				opts.Template = tm
			}
			s := natssink.New(f, opts)
			t.Cleanup(func() { _ = s.Close() })
			require.NoError(t, s.Send(context.Background(), event.Batch{Events: []*event.Event{tt.e}, CommitLSN: 1}))
			msgs := f.accepted()
			require.Len(t, msgs, 1)
			m := msgs[0]
			require.Equal(t, tt.wantSubj, m.Subject)
			for k, v := range tt.wantHdr {
				require.Equal(t, v, m.Header.Get(k), k)
			}
			for _, k := range tt.noHdr {
				require.Empty(t, m.Header.Values(k), k)
			}
			require.JSONEq(t, string(mustJSON(t, tt.e)), string(m.Data))
			require.NotContains(t, string(m.Data), "\n")
		})
	}
}

func TestPermanent(t *testing.T) {
	tests := []struct {
		name  string
		e     *event.Event
		setup func(f *fakePub)
		want  string
	}{
		{"empty table", &event.Event{ID: "x-1", Op: event.OpInsert, Schema: "public"}, nil, "{table} is empty"},
		{"empty schema", &event.Event{ID: "x-1", Op: event.OpInsert, Table: "orders"}, nil, "{schema} is empty"},
		{"payload too large", &event.Event{ID: "x-1", Op: event.OpInsert, Schema: "public", Table: "t"}, func(f *fakePub) { f.publishErr = nats.ErrMaxPayload }, "maximum payload"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakePub{}
			if tt.setup != nil {
				tt.setup(f)
			}
			s := natssink.New(f, natssink.Options{FSMColumns: map[string]string{"public.orders": "status"}})
			err := s.Send(context.Background(), event.Batch{Events: []*event.Event{tt.e}})
			require.ErrorContains(t, err, tt.want)
			require.True(t, event.IsPermanent(err))
			require.Zero(t, f.publishedN)
		})
	}
}

func TestPermanentBeforeAnyPublish(t *testing.T) {
	tests := []struct{ name string }{{"a bad second event publishes nothing"}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakePub{}
			s := natssink.New(f, natssink.Options{})
			b := sinktest.Batch("a", 1)
			b.Events = append(b.Events, &event.Event{ID: "bad", Op: event.OpInsert, Schema: "public"})
			require.True(t, event.IsPermanent(s.Send(context.Background(), b)))
			require.Zero(t, f.publishedN)
		})
	}
}

func TestAckWait(t *testing.T) {
	tests := []struct {
		name    string
		ctx     func() (context.Context, context.CancelFunc)
		ackWait time.Duration
		want    error
	}{
		{"default wait without a ctx deadline", func() (context.Context, context.CancelFunc) {
			return context.WithCancel(context.Background())
		}, 20 * time.Millisecond, context.DeadlineExceeded},
		{"ctx deadline wins", func() (context.Context, context.CancelFunc) {
			return context.WithTimeout(context.Background(), 20*time.Millisecond)
		}, time.Hour, context.DeadlineExceeded},
		{"ctx cancel", func() (context.Context, context.CancelFunc) {
			ctx, cancel := context.WithCancel(context.Background())
			time.AfterFunc(20*time.Millisecond, cancel)
			return ctx, cancel
		}, time.Hour, context.Canceled},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakePub{hang: true}
			s := natssink.New(f, natssink.Options{AckWait: tt.ackWait})
			t.Cleanup(func() { _ = s.Close() })
			ctx, cancel := tt.ctx()
			defer cancel()
			err := s.Send(ctx, sinktest.Batch("a", 2))
			require.ErrorIs(t, err, tt.want)
			require.False(t, event.IsPermanent(err))
			require.Empty(t, f.ids())
		})
	}
}

func TestAlreadyCancelled(t *testing.T) {
	tests := []struct{ name string }{{"cancelled before send"}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakePub{}
			s := natssink.New(f, natssink.Options{})
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			require.ErrorIs(t, s.Send(ctx, sinktest.Batch("a", 1)), context.Canceled)
			require.Zero(t, f.publishedN)
		})
	}
}

func TestReturnsAfterAllAcks(t *testing.T) {
	tests := []struct {
		name string
		n    int
	}{
		{"slow acks", 5},
		{"more than one chunk", 2500},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakePub{ackDelay: 30 * time.Millisecond}
			s := natssink.New(f, natssink.Options{})
			t.Cleanup(func() { _ = s.Close() })
			b := sinktest.Batch("a", tt.n)
			require.NoError(t, s.Send(context.Background(), b))
			require.Equal(t, tt.n, f.ackedN(), "Send returned before every ack")
			var want []string
			for _, e := range b.Events {
				want = append(want, e.ID)
			}
			require.Equal(t, want, f.ids())
		})
	}
}

func TestPartialFailureIsRetryable(t *testing.T) {
	tests := []struct{ name string }{{"second publish fails, retry succeeds"}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &fakePub{failPublish: map[string]error{"a-2": errBroken}}
			s := natssink.New(f, natssink.Options{})
			t.Cleanup(func() { _ = s.Close() })
			b := sinktest.Batch("a", 3)
			err := s.Send(context.Background(), b)
			require.ErrorIs(t, err, errBroken)
			require.False(t, event.IsPermanent(err))

			f.set(func(f *fakePub) { f.failPublish = nil })
			require.NoError(t, s.Send(context.Background(), b))
			require.Contains(t, f.ids(), "a-3")
		})
	}
}

func TestClose(t *testing.T) {
	tests := []struct {
		name    string
		onClose error
	}{
		{"runs OnClose once", nil},
		{"returns the OnClose error", errBroken},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			s := natssink.New(&fakePub{}, natssink.Options{OnClose: func() error { calls++; return tt.onClose }})
			require.Equal(t, tt.onClose, s.Close())
			require.NoError(t, s.Close())
			require.Equal(t, 1, calls)
			err := s.Send(context.Background(), sinktest.Batch("a", 1))
			require.ErrorContains(t, err, "closed")
			require.False(t, errors.Is(err, context.Canceled))
		})
	}
}
