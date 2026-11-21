package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

const (
	defaultStatusInterval = 10 * time.Second
	finalStatusTimeout    = 5 * time.Second
	defaultSendTimeout    = 10 * time.Second
)

// ErrStreamEnded is returned by Standby.Run when the server ends the COPY
// stream.
var ErrStreamEnded = errors.New("replication stream ended")

// ErrZeroStartLSN is returned by NewStandby for a start LSN of 0. pglogrepl
// sends a flush LSN of 0 as the write position, which would confirm WAL the
// sink has not made durable.
var ErrZeroStartLSN = errors.New("standby start LSN must not be 0: use the slot's confirmed_flush_lsn")

// walConn is the part of the replication connection Standby uses.
type walConn interface {
	ReceiveMessage(ctx context.Context) (pgproto3.BackendMessage, error)
	SendStatus(ctx context.Context, ssu pglogrepl.StandbyStatusUpdate) error
}

// Standby owns a started replication stream. Its Run goroutine is the only
// user of the connection: it receives with a deadline, so the status timer can
// fire while idle, and writes every standby status update itself, so pgconn is
// never used concurrently.
type Standby struct {
	conn      walConn
	interval  time.Duration
	out       chan pglogrepl.XLogData
	confirmed atomic.Uint64
	received  pglogrepl.LSN
	next      time.Time
}

// NewStandby returns a Standby for a connection on which StartStream has
// succeeded. start is the slot's confirmed_flush_lsn (SlotInfo), never 0, and
// interval is the status update period, 10s when zero or negative; keep it well
// below wal_sender_timeout.
func NewStandby(pg *pgconn.PgConn, start pglogrepl.LSN, interval time.Duration) (*Standby, error) {
	return newStandby(pgConn{pg}, start, interval)
}

func newStandby(conn walConn, start pglogrepl.LSN, interval time.Duration) (*Standby, error) {
	if start == 0 {
		return nil, ErrZeroStartLSN
	}
	if interval <= 0 {
		interval = defaultStatusInterval
	}
	s := &Standby{conn: conn, interval: interval, out: make(chan pglogrepl.XLogData), received: start}
	s.confirmed.Store(uint64(start))
	return s, nil
}

// Messages returns the channel of XLogData messages in WAL order. Run closes it
// on return. WALData is a private copy.
func (s *Standby) Messages() <-chan pglogrepl.XLogData {
	return s.out
}

// SetConfirmed records the LSN that is durable at the destination. It is the
// flushed position of the next status update and is safe for concurrent use.
// The confirmed LSN never moves backwards, so a stale or zero value is ignored.
func (s *Standby) SetConfirmed(lsn pglogrepl.LSN) {
	for {
		cur := s.confirmed.Load()
		if uint64(lsn) <= cur || s.confirmed.CompareAndSwap(cur, uint64(lsn)) {
			return
		}
	}
}

// Confirmed returns the LSN last set with SetConfirmed.
func (s *Standby) Confirmed() pglogrepl.LSN {
	return pglogrepl.LSN(s.confirmed.Load())
}

// Run receives until ctx is done or the connection fails, forwarding XLogData
// to Messages and answering keepalives. A status update goes out every
// interval, also while idle or while the consumer is slow, and at once for a
// keepalive that requests a reply. On cancellation it sends a final status
// update, best effort, and returns ctx.Err(). It closes Messages on return.
func (s *Standby) Run(ctx context.Context) error {
	defer close(s.out)
	s.next = time.Now().Add(s.interval)
	for {
		rctx, cancel := context.WithDeadline(ctx, s.next)
		msg, err := s.conn.ReceiveMessage(rctx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				s.sendFinal(ctx)
				return ctx.Err()
			}
			if isTimeout(err) {
				if err := s.sendStatus(ctx); err != nil {
					return err
				}
				continue
			}
			return fmt.Errorf("receive: %w", err)
		}
		if err := s.handle(ctx, msg); err != nil {
			if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
				s.sendFinal(ctx)
			}
			return err
		}
	}
}

func (s *Standby) handle(ctx context.Context, msg pgproto3.BackendMessage) error {
	switch m := msg.(type) {
	case *pgproto3.CopyData:
		return s.handleCopyData(ctx, m.Data)
	case *pgproto3.ErrorResponse:
		return fmt.Errorf("receive: %w", pgconn.ErrorResponseToPgError(m))
	case *pgproto3.NoticeResponse:
		// pgoutput only warns, and then streams nothing, when a named
		// publication does not exist.
		if m.SeverityUnlocalized == "WARNING" && m.Code == "55000" {
			return fmt.Errorf("%w: %s", ErrPublicationMissing, m.Message)
		}
	case *pgproto3.CopyDone:
		return ErrStreamEnded
	}
	return nil
}

func (s *Standby) handleCopyData(ctx context.Context, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	switch data[0] {
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(data[1:])
		if err != nil {
			return fmt.Errorf("parse keepalive: %w", err)
		}
		if pkm.ReplyRequested {
			return s.sendStatus(ctx)
		}
	case pglogrepl.XLogDataByteID:
		xld, err := pglogrepl.ParseXLogData(data[1:])
		if err != nil {
			return fmt.Errorf("parse xlogdata: %w", err)
		}
		xld.WALData = bytes.Clone(xld.WALData)
		if end := xld.WALStart + pglogrepl.LSN(len(xld.WALData)); end > s.received {
			s.received = end
		}
		return s.forward(ctx, xld)
	}
	return nil
}

// forward hands xld to the consumer while still sending status updates on
// schedule, so a full queue never starves the server of replies.
func (s *Standby) forward(ctx context.Context, xld pglogrepl.XLogData) error {
	for {
		t := time.NewTimer(time.Until(s.next))
		select {
		case s.out <- xld:
			t.Stop()
			return nil
		case <-t.C:
			if err := s.sendStatus(ctx); err != nil {
				return err
			}
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		}
	}
}

func (s *Standby) sendStatus(ctx context.Context) error {
	flushed := s.Confirmed()
	err := s.conn.SendStatus(ctx, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: max(s.received, flushed),
		WALFlushPosition: flushed,
		WALApplyPosition: flushed,
		ClientTime:       time.Now(),
	})
	if err != nil {
		return fmt.Errorf("send standby status: %w", err)
	}
	s.next = time.Now().Add(s.interval)
	return nil
}

func (s *Standby) sendFinal(ctx context.Context) {
	fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), finalStatusTimeout)
	defer cancel()
	_ = s.sendStatus(fctx)
}

func isTimeout(err error) bool {
	return pgconn.Timeout(err) || errors.Is(err, context.DeadlineExceeded)
}

// pgConn adapts *pgconn.PgConn to walConn.
type pgConn struct{ pg *pgconn.PgConn }

func (c pgConn) ReceiveMessage(ctx context.Context) (pgproto3.BackendMessage, error) {
	return c.pg.ReceiveMessage(ctx)
}

func (c pgConn) SendStatus(ctx context.Context, ssu pglogrepl.StandbyStatusUpdate) error {
	// pglogrepl ignores ctx, so bound the write with a connection deadline.
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(defaultSendTimeout)
	}
	nc := c.pg.Conn()
	if err := nc.SetWriteDeadline(deadline); err != nil {
		return err
	}
	defer func() { _ = nc.SetWriteDeadline(time.Time{}) }()
	return pglogrepl.SendStandbyStatusUpdate(ctx, c.pg, ssu)
}
