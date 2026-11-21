package postgres

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

type recv struct {
	msg pgproto3.BackendMessage
	err error
}

type fakeConn struct {
	in      chan recv
	sendErr error

	mu   sync.Mutex
	sent []pglogrepl.StandbyStatusUpdate
}

func newFakeConn() *fakeConn { return &fakeConn{in: make(chan recv, 16)} }

func (f *fakeConn) ReceiveMessage(ctx context.Context) (pgproto3.BackendMessage, error) {
	select {
	case r := <-f.in:
		return r.msg, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (f *fakeConn) SendStatus(_ context.Context, ssu pglogrepl.StandbyStatusUpdate) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, ssu)
	return f.sendErr
}

func (f *fakeConn) statuses() []pglogrepl.StandbyStatusUpdate {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]pglogrepl.StandbyStatusUpdate(nil), f.sent...)
}

func keepalive(replyRequested bool) *pgproto3.CopyData {
	b := make([]byte, 18)
	b[0] = pglogrepl.PrimaryKeepaliveMessageByteID
	if replyRequested {
		b[17] = 1
	}
	return &pgproto3.CopyData{Data: b}
}

func xlog(start pglogrepl.LSN, payload string) *pgproto3.CopyData {
	b := make([]byte, 25, 25+len(payload))
	b[0] = pglogrepl.XLogDataByteID
	binary.BigEndian.PutUint64(b[1:], uint64(start))
	return &pgproto3.CopyData{Data: append(b[:25], payload...)}
}

func TestStandbyRun(t *testing.T) {
	defer goleak.VerifyNone(t)
	pgErr := &pgproto3.ErrorResponse{Code: "XX000", Message: "boom"}
	tests := []struct {
		name     string
		in       []recv
		confirm  pglogrepl.LSN
		interval time.Duration
		sendErr  error
		wait     func(t *testing.T, s *Standby, f *fakeConn)
		wantErr  func(t *testing.T, err error)
		check    func(t *testing.T, f *fakeConn)
	}{
		{
			name:    "idle timer sends status with confirmed flush",
			confirm: 42,
			wait: func(t *testing.T, s *Standby, f *fakeConn) {
				require.Eventually(t, func() bool { return len(f.statuses()) >= 2 }, 2*time.Second, time.Millisecond)
				s.SetConfirmed(99)
				n := len(f.statuses())
				require.Eventually(t, func() bool { return len(f.statuses()) > n+1 }, 2*time.Second, time.Millisecond)
				require.Equal(t, pglogrepl.LSN(99), s.Confirmed())
			},
			wantErr: func(t *testing.T, err error) { require.ErrorIs(t, err, context.Canceled) },
			check: func(t *testing.T, f *fakeConn) {
				st := f.statuses()
				require.Equal(t, pglogrepl.LSN(42), st[0].WALFlushPosition)
				require.Equal(t, pglogrepl.LSN(42), st[0].WALApplyPosition)
				require.Equal(t, pglogrepl.LSN(99), st[len(st)-1].WALFlushPosition, "final status carries latest confirm")
			},
		},
		{
			name:     "keepalive without reply request sends nothing",
			in:       []recv{{msg: keepalive(false)}},
			interval: time.Hour,
			wait: func(t *testing.T, s *Standby, f *fakeConn) {
				time.Sleep(20 * time.Millisecond)
				require.Empty(t, f.statuses())
			},
			wantErr: func(t *testing.T, err error) { require.ErrorIs(t, err, context.Canceled) },
		},
		{
			name:     "keepalive with reply request answers immediately",
			in:       []recv{{msg: keepalive(true)}},
			interval: time.Hour,
			wait: func(t *testing.T, s *Standby, f *fakeConn) {
				require.Eventually(t, func() bool { return len(f.statuses()) == 1 }, time.Second, time.Millisecond)
			},
			wantErr: func(t *testing.T, err error) { require.ErrorIs(t, err, context.Canceled) },
		},
		{
			name:    "xlogdata is forwarded and advances written position",
			in:      []recv{{msg: xlog(100, "abc")}, {msg: keepalive(true)}},
			confirm: 50,
			wait: func(t *testing.T, s *Standby, f *fakeConn) {
				x := <-s.Messages()
				require.Equal(t, pglogrepl.LSN(100), x.WALStart)
				require.Equal(t, "abc", string(x.WALData))
				require.Eventually(t, func() bool { return len(f.statuses()) >= 1 }, time.Second, time.Millisecond)
			},
			wantErr: func(t *testing.T, err error) { require.ErrorIs(t, err, context.Canceled) },
			check: func(t *testing.T, f *fakeConn) {
				require.Equal(t, pglogrepl.LSN(103), f.statuses()[0].WALWritePosition)
				require.Equal(t, pglogrepl.LSN(50), f.statuses()[0].WALFlushPosition)
			},
		},
		{
			name: "unread consumer does not starve status updates",
			in:   []recv{{msg: xlog(1, "x")}},
			wait: func(t *testing.T, s *Standby, f *fakeConn) {
				require.Eventually(t, func() bool { return len(f.statuses()) >= 2 }, 2*time.Second, time.Millisecond)
				<-s.Messages()
			},
			wantErr: func(t *testing.T, err error) { require.ErrorIs(t, err, context.Canceled) },
		},
		{
			name:     "cancel while consumer is blocked sends final status",
			in:       []recv{{msg: xlog(1, "x")}},
			interval: time.Hour,
			wait:     func(t *testing.T, s *Standby, f *fakeConn) {},
			wantErr:  func(t *testing.T, err error) { require.ErrorIs(t, err, context.Canceled) },
			check:    func(t *testing.T, f *fakeConn) { require.Len(t, f.statuses(), 1) },
		},
		{
			name:    "send failure while forwarding stops",
			in:      []recv{{msg: xlog(1, "x")}},
			sendErr: errors.New("write failed"),
			wait:    func(t *testing.T, s *Standby, f *fakeConn) {},
			wantErr: func(t *testing.T, err error) { require.ErrorContains(t, err, "write failed") },
		},
		{
			name:    "send failure on idle timer stops",
			sendErr: errors.New("write failed"),
			wait:    func(t *testing.T, s *Standby, f *fakeConn) {},
			wantErr: func(t *testing.T, err error) { require.ErrorContains(t, err, "send standby status") },
		},
		{
			name:    "send failure on keepalive reply stops",
			in:      []recv{{msg: keepalive(true)}},
			sendErr: errors.New("write failed"),
			wait:    func(t *testing.T, s *Standby, f *fakeConn) {},
			wantErr: func(t *testing.T, err error) { require.ErrorContains(t, err, "write failed") },
		},
		{
			name:    "receive error stops",
			in:      []recv{{err: errors.New("conn reset")}},
			wait:    func(t *testing.T, s *Standby, f *fakeConn) {},
			wantErr: func(t *testing.T, err error) { require.ErrorContains(t, err, "conn reset") },
		},
		{
			name: "error response becomes pg error",
			in:   []recv{{msg: pgErr}},
			wait: func(t *testing.T, s *Standby, f *fakeConn) {},
			wantErr: func(t *testing.T, err error) {
				var pe *pgconn.PgError
				require.ErrorAs(t, err, &pe)
			},
		},
		{
			name:    "copy done ends stream",
			in:      []recv{{msg: &pgproto3.CopyDone{}}},
			wait:    func(t *testing.T, s *Standby, f *fakeConn) {},
			wantErr: func(t *testing.T, err error) { require.ErrorIs(t, err, ErrStreamEnded) },
		},
		{
			name:    "malformed keepalive",
			in:      []recv{{msg: &pgproto3.CopyData{Data: []byte{'k', 1}}}},
			wait:    func(t *testing.T, s *Standby, f *fakeConn) {},
			wantErr: func(t *testing.T, err error) { require.ErrorContains(t, err, "parse keepalive") },
		},
		{
			name:    "malformed xlogdata",
			in:      []recv{{msg: &pgproto3.CopyData{Data: []byte{'w', 1}}}},
			wait:    func(t *testing.T, s *Standby, f *fakeConn) {},
			wantErr: func(t *testing.T, err error) { require.ErrorContains(t, err, "parse xlogdata") },
		},
		{
			name: "publication warning ends the stream",
			in: []recv{{msg: &pgproto3.NoticeResponse{
				SeverityUnlocalized: "WARNING", Code: "55000", Message: `skipped loading publication "nopub"`,
			}}},
			wait:    func(t *testing.T, s *Standby, f *fakeConn) {},
			wantErr: func(t *testing.T, err error) { require.ErrorIs(t, err, ErrPublicationMissing) },
		},
		{
			name: "other warnings and notices are ignored",
			in: []recv{
				{msg: &pgproto3.NoticeResponse{SeverityUnlocalized: "WARNING", Code: "01000", Message: "other"}},
				{msg: &pgproto3.NoticeResponse{SeverityUnlocalized: "NOTICE", Code: "55000", Message: "info"}},
				{msg: keepalive(true)},
			},
			interval: time.Hour,
			wait: func(t *testing.T, s *Standby, f *fakeConn) {
				require.Eventually(t, func() bool { return len(f.statuses()) >= 1 }, time.Second, time.Millisecond)
			},
			wantErr: func(t *testing.T, err error) { require.ErrorIs(t, err, context.Canceled) },
		},
		{
			name: "empty, unknown and other messages are ignored",
			in: []recv{
				{msg: &pgproto3.CopyData{}},
				{msg: &pgproto3.CopyData{Data: []byte{'z'}}},
				{msg: &pgproto3.NoticeResponse{}},
				{msg: keepalive(true)},
			},
			interval: time.Hour,
			wait: func(t *testing.T, s *Standby, f *fakeConn) {
				require.Eventually(t, func() bool { return len(f.statuses()) == 1 }, time.Second, time.Millisecond)
			},
			wantErr: func(t *testing.T, err error) { require.ErrorIs(t, err, context.Canceled) },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFakeConn()
			f.sendErr = tt.sendErr
			for _, r := range tt.in {
				f.in <- r
			}
			if tt.interval == 0 {
				tt.interval = 10 * time.Millisecond
			}
			if tt.confirm == 0 {
				tt.confirm = 1 // a zero start LSN is rejected
			}
			s, err := newStandby(f, tt.confirm, tt.interval)
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- s.Run(ctx) }()

			tt.wait(t, s, f)
			select {
			case err := <-done:
				tt.wantErr(t, err)
			case <-time.After(50 * time.Millisecond):
				cancel()
				select {
				case err := <-done:
					tt.wantErr(t, err)
				case <-time.After(2 * time.Second):
					t.Fatal("Run did not return")
				}
			}
			_, open := <-s.Messages()
			require.False(t, open)
			if tt.check != nil {
				tt.check(t, f)
			}
		})
	}
}

func TestStandbyDefaults(t *testing.T) {
	tests := []struct {
		name     string
		start    pglogrepl.LSN
		interval time.Duration
		want     time.Duration
		wantErr  error
	}{
		{name: "zero", start: 7, want: defaultStatusInterval},
		{name: "negative", start: 7, interval: -1, want: defaultStatusInterval},
		{name: "set", start: 7, interval: time.Second, want: time.Second},
		{name: "zero start would confirm unhandled WAL", interval: time.Second, wantErr: ErrZeroStartLSN},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := NewStandby(nil, tt.start, tt.interval)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				require.Nil(t, s)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, s.interval)
			require.Equal(t, tt.start, s.Confirmed())
		})
	}
}
