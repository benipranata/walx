package snapshot

import (
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/benipranata/walx/internal/event"
	"github.com/benipranata/walx/internal/postgres"
)

// builder turns cursor rows into snapshot events. seq runs over every row of
// the whole snapshot, before filters, so the IDs depend only on the data and
// the consistent point.
type builder struct {
	name string
	lsn  event.LSN
	ts   time.Time
	seq  int
}

// build decodes rows (text values, in the order of fields) with the decoder
// of the live stream, so a value renders exactly as in an insert event, and
// returns one Op=snapshot event per row. Key holds the plan's key columns
// from After; a NULL key column is an error, since it cannot identify the row.
func (b *builder) build(p tablePlan, fields []pgconn.FieldDescription, rows [][][]byte) ([]*event.Event, error) {
	rel := &pglogrepl.RelationMessage{Namespace: p.Schema, RelationName: p.Table, Columns: make([]*pglogrepl.RelationMessageColumn, len(fields))}
	for i, f := range fields {
		rel.Columns[i] = &pglogrepl.RelationMessageColumn{Name: f.Name, DataType: f.DataTypeOID}
	}
	events := make([]*event.Event, 0, len(rows))
	for _, raw := range rows {
		after, err := postgres.DecodeTuple(rel, tuple(raw))
		if err != nil {
			return nil, err
		}
		key, err := pick(p, after)
		if err != nil {
			return nil, err
		}
		b.seq++
		events = append(events, &event.Event{
			ID:       fmt.Sprintf("%s-%s-%d", b.name, b.lsn, b.seq),
			Source:   b.name,
			Op:       event.OpSnapshot,
			Schema:   p.Schema,
			Table:    p.Table,
			After:    after,
			Key:      key,
			LSN:      b.lsn,
			CommitTS: b.ts,
		})
	}
	return events, nil
}

func tuple(raw [][]byte) *pglogrepl.TupleData {
	t := &pglogrepl.TupleData{ColumnNum: uint16(len(raw)), Columns: make([]*pglogrepl.TupleDataColumn, len(raw))}
	for i, v := range raw {
		if v == nil {
			t.Columns[i] = &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeNull}
			continue
		}
		t.Columns[i] = &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeText, Length: uint32(len(v)), Data: v}
	}
	return t
}

func pick(p tablePlan, after map[string]any) (map[string]any, error) {
	if len(p.Key) == 0 {
		return nil, nil
	}
	key := make(map[string]any, len(p.Key))
	for _, c := range p.Key {
		v, ok := after[c]
		if !ok {
			return nil, fmt.Errorf("key column %q is not a column of %s.%s", c, p.Schema, p.Table)
		}
		if v == nil {
			return nil, fmt.Errorf("key column %q of %s.%s is NULL, so the row has no key", c, p.Schema, p.Table)
		}
		key[c] = v
	}
	return key, nil
}
