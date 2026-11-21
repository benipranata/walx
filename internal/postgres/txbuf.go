package postgres

import (
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/benipranata/walx/internal/event"
)

// TxBuffer collects the events of one pgoutput stream from Begin to Commit
// and emits each transaction as a Batch, in commit order. It is not safe for
// concurrent use: one decoding goroutine owns it.
type TxBuffer struct {
	name string
	rels *Relations
	keys map[string][]string
	tx   *txState
}

type txState struct {
	finalLSN event.LSN
	xid      uint32
	commitTS time.Time
	events   []*event.Event
}

// NewTxBuffer returns a buffer for the databases entry called name. rels
// receives every Relation message; keys is passed to DecodeDML.
func NewTxBuffer(name string, rels *Relations, keys map[string][]string) *TxBuffer {
	return &TxBuffer{name: name, rels: rels, keys: keys}
}

// Push feeds one pgoutput message. walStart is the WAL position of the
// message, stored as Event.LSN. It returns a Batch when msg is a Commit, even
// for a transaction without events, and nil otherwise. Events get
// ID "<name>-<commitLSN>-<seq>" with seq counted from 1 over every DML event
// of the raw transaction (a truncate counts once per relation), so the ID
// depends on neither filters nor table config. CommitTS is in UTC. An error
// drops the open transaction, so the buffer can be reused. Origin, Type and logical
// decoding messages are ignored.
func (b *TxBuffer) Push(msg pglogrepl.Message, walStart event.LSN) (*event.Batch, error) {
	batch, err := b.push(msg, walStart)
	if err != nil {
		b.tx = nil
	}
	return batch, err
}

func (b *TxBuffer) push(msg pglogrepl.Message, walStart event.LSN) (*event.Batch, error) {
	switch m := msg.(type) {
	case *pglogrepl.RelationMessage:
		b.rels.Set(m)
	case *pglogrepl.BeginMessage:
		if b.tx != nil {
			return nil, errors.New("begin inside a transaction")
		}
		b.tx = &txState{finalLSN: event.LSN(m.FinalLSN), xid: m.Xid, commitTS: m.CommitTime.UTC()}
	case *pglogrepl.CommitMessage:
		return b.commit(m)
	case *pglogrepl.InsertMessage, *pglogrepl.UpdateMessage, *pglogrepl.DeleteMessage, *pglogrepl.TruncateMessage:
		if b.tx == nil {
			return nil, fmt.Errorf("%T outside a transaction", msg)
		}
		evs, err := DecodeDML(b.rels, b.keys, msg)
		if err != nil {
			return nil, err
		}
		for i := range evs {
			evs[i].LSN = walStart
			evs[i].TxID = b.tx.xid
			evs[i].CommitTS = b.tx.commitTS
			evs[i].Source = b.name
		}
		b.add(evs)
	}
	return nil, nil
}

// add is the one place events enter the buffer; the size cap (T2.5) hooks in
// here.
func (b *TxBuffer) add(evs []event.Event) {
	for i := range evs {
		b.tx.events = append(b.tx.events, &evs[i])
	}
}

func (b *TxBuffer) commit(m *pglogrepl.CommitMessage) (*event.Batch, error) {
	tx := b.tx
	if tx == nil {
		return nil, errors.New("commit without begin")
	}
	commit := event.LSN(m.CommitLSN)
	if commit != tx.finalLSN {
		return nil, fmt.Errorf("commit LSN %s does not match begin final LSN %s", commit, tx.finalLSN)
	}
	b.tx = nil
	for i, e := range tx.events {
		e.ID = fmt.Sprintf("%s-%s-%d", b.name, commit, i+1)
	}
	return &event.Batch{Events: tx.events, CommitLSN: commit, EndLSN: event.LSN(m.TransactionEndLSN)}, nil
}
