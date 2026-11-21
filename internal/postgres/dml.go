package postgres

import (
	"fmt"

	"github.com/jackc/pglogrepl"

	"github.com/benipranata/walx/internal/event"
)

// DecodeDML turns an Insert, Update, Delete or Truncate message into events.
// It sets Op, Schema, Table, Before, After and Key; ID, Source, LSN, TxID and
// CommitTS are left for the transaction buffer. Any other message returns nil.
//
// Before is set only when the old tuple is a full row ('O', REPLICA IDENTITY
// FULL); a 'K' tuple only feeds Key. keys maps "schema.table" to the
// configured key columns; a table not in it uses the columns the Relation
// flags as key: the primary key, or the replica identity index. Under
// REPLICA IDENTITY FULL Postgres flags every column, so without a configured
// key the Key is then the whole row, not the primary key; the Relation does
// not say which columns are the primary key, so the caller (startup identity
// check, T4.5) must put a FULL table's primary key into keys. Key is read
// from After (a column that is Unchanged there comes from Before, else it is
// an error), or from the old tuple for a delete, where a K tuple only holds
// the replica identity columns, so a configured key outside them is an error.
// Key is nil when there are no key columns. Truncate gives one
// event per relation with nil Before, After and Key; its cascade and
// restart-identity flags are ignored.
func DecodeDML(rels *Relations, keys map[string][]string, msg pglogrepl.Message) ([]event.Event, error) {
	switch m := msg.(type) {
	case *pglogrepl.InsertMessage:
		return decodeInsert(rels, keys, m)
	case *pglogrepl.UpdateMessage:
		return decodeUpdate(rels, keys, m)
	case *pglogrepl.DeleteMessage:
		return decodeDelete(rels, keys, m)
	case *pglogrepl.TruncateMessage:
		return decodeTruncate(rels, m)
	}
	return nil, nil
}

func decodeInsert(rels *Relations, keys map[string][]string, m *pglogrepl.InsertMessage) ([]event.Event, error) {
	rel, err := relation(rels, m.RelationID)
	if err != nil {
		return nil, err
	}
	after, err := DecodeTuple(rel, m.Tuple)
	if err != nil {
		return nil, err
	}
	key, err := rowKey(rel, keys, after, nil, false)
	if err != nil {
		return nil, err
	}
	return []event.Event{{Op: event.OpInsert, Schema: rel.Namespace, Table: rel.RelationName, After: after, Key: key}}, nil
}

func decodeUpdate(rels *Relations, keys map[string][]string, m *pglogrepl.UpdateMessage) ([]event.Event, error) {
	rel, err := relation(rels, m.RelationID)
	if err != nil {
		return nil, err
	}
	var before map[string]any
	if m.OldTupleType == pglogrepl.UpdateMessageTupleTypeOld && m.OldTuple != nil {
		if before, err = DecodeTuple(rel, m.OldTuple); err != nil {
			return nil, err
		}
	}
	after, err := DecodeTuple(rel, m.NewTuple)
	if err != nil {
		return nil, err
	}
	key, err := rowKey(rel, keys, after, before, false)
	if err != nil {
		return nil, err
	}
	return []event.Event{{Op: event.OpUpdate, Schema: rel.Namespace, Table: rel.RelationName,
		Before: before, After: after, Key: key}}, nil
}

func decodeDelete(rels *Relations, keys map[string][]string, m *pglogrepl.DeleteMessage) ([]event.Event, error) {
	rel, err := relation(rels, m.RelationID)
	if err != nil {
		return nil, err
	}
	if m.OldTuple == nil {
		return nil, fmt.Errorf("decode delete of %s.%s: no old tuple", rel.Namespace, rel.RelationName)
	}
	old, err := DecodeTuple(rel, m.OldTuple)
	if err != nil {
		return nil, err
	}
	key, err := rowKey(rel, keys, old, nil, m.OldTupleType == pglogrepl.DeleteMessageTupleTypeKey)
	if err != nil {
		return nil, err
	}
	ev := event.Event{Op: event.OpDelete, Schema: rel.Namespace, Table: rel.RelationName, Key: key}
	if m.OldTupleType == pglogrepl.DeleteMessageTupleTypeOld {
		ev.Before = old
	}
	return []event.Event{ev}, nil
}

func decodeTruncate(rels *Relations, m *pglogrepl.TruncateMessage) ([]event.Event, error) {
	events := make([]event.Event, 0, len(m.RelationIDs))
	for _, id := range m.RelationIDs {
		rel, err := relation(rels, id)
		if err != nil {
			return nil, err
		}
		events = append(events, event.Event{Op: event.OpTruncate, Schema: rel.Namespace, Table: rel.RelationName})
	}
	return events, nil
}

func relation(rels *Relations, id uint32) (*pglogrepl.RelationMessage, error) {
	rel, ok := rels.Get(id)
	if !ok {
		return nil, fmt.Errorf("no Relation message for relation %d", id)
	}
	return rel, nil
}

// rowKey picks the key columns of rel out of row. fallback, when not nil, is
// the old row image and supplies a column that is Unchanged in row. With
// identityOnly the row is a K tuple, which holds only the replica identity
// columns, so a configured key column outside it is an error rather than a
// silent null. A missing column, a value that stays Unchanged, or a key
// column outside the identity is an error; no key columns gives a nil key.
func rowKey(rel *pglogrepl.RelationMessage, keys map[string][]string, row, fallback map[string]any, identityOnly bool) (map[string]any, error) {
	cols := keys[rel.Namespace+"."+rel.RelationName]
	configured := len(cols) > 0
	flagged := map[string]bool{}
	for _, c := range rel.Columns {
		if c.Flags&1 != 0 {
			flagged[c.Name] = true
			if !configured {
				cols = append(cols, c.Name)
			}
		}
	}
	if len(cols) == 0 {
		return nil, nil
	}
	key := make(map[string]any, len(cols))
	for _, c := range cols {
		v, ok := row[c]
		if !ok {
			return nil, fmt.Errorf("key column %q is not a column of %s.%s", c, rel.Namespace, rel.RelationName)
		}
		if identityOnly && !flagged[c] {
			return nil, fmt.Errorf("key column %q of %s.%s is not in the replica identity", c, rel.Namespace, rel.RelationName)
		}
		if v == event.Unchanged && fallback != nil {
			v = fallback[c]
		}
		if v == event.Unchanged {
			return nil, fmt.Errorf("key column %q of %s.%s is unchanged TOAST, so its value is unknown", c, rel.Namespace, rel.RelationName)
		}
		key[c] = v
	}
	return key, nil
}
