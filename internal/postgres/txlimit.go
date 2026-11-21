package postgres

import "github.com/jackc/pglogrepl"

// TxLimits caps one transaction in a TxBuffer. A zero field means no cap;
// config rejects a zero max_tx_bytes, so the engine always sets MaxBytes.
type TxLimits struct {
	// MaxBytes caps the raw tuple bytes of the DML messages (every column
	// value of the new and old tuples). Row framing and metadata are not
	// counted, which keeps the measure deterministic and cheap.
	MaxBytes int64
	// MaxEvents caps the events of the raw transaction, before filters.
	MaxEvents int64
}

// TxOption configures a TxBuffer.
type TxOption func(*TxBuffer)

// WithTxLimits sets the per-transaction limits of a TxBuffer.
func WithTxLimits(l TxLimits) TxOption {
	return func(b *TxBuffer) { b.lim = l }
}

// tupleBytes returns the raw column bytes carried by a DML message.
func tupleBytes(msg pglogrepl.Message) int64 {
	switch m := msg.(type) {
	case *pglogrepl.InsertMessage:
		return columnBytes(m.Tuple)
	case *pglogrepl.UpdateMessage:
		return columnBytes(m.OldTuple) + columnBytes(m.NewTuple)
	case *pglogrepl.DeleteMessage:
		return columnBytes(m.OldTuple)
	}
	return 0
}

func columnBytes(t *pglogrepl.TupleData) (n int64) {
	if t == nil {
		return 0
	}
	for _, c := range t.Columns {
		if c != nil {
			n += int64(len(c.Data))
		}
	}
	return n
}
