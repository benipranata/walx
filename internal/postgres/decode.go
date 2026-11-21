package postgres

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/benipranata/walx/internal/event"
)

// Type OIDs from pg_type that the decoder maps; any other OID decodes as a
// string.
const (
	oidBool        = 16
	oidBytea       = 17
	oidName        = 19
	oidInt8        = 20
	oidInt2        = 21
	oidInt4        = 23
	oidText        = 25
	oidJSON        = 114
	oidFloat4      = 700
	oidFloat8      = 701
	oidBpchar      = 1042
	oidVarchar     = 1043
	oidDate        = 1082
	oidTimestamp   = 1114
	oidTimestamptz = 1184
	oidNumeric     = 1700
	oidUUID        = 2950
	oidJSONB       = 3802
)

// Relations caches the Relation messages of one stream by relation ID. It is
// not safe for concurrent use: one decoding goroutine owns it.
type Relations struct {
	byID map[uint32]*pglogrepl.RelationMessage
}

// NewRelations returns an empty cache.
func NewRelations() *Relations {
	return &Relations{byID: map[uint32]*pglogrepl.RelationMessage{}}
}

// Set stores rel, replacing any earlier entry with the same ID, since
// Postgres re-sends a Relation after a schema change.
func (r *Relations) Set(rel *pglogrepl.RelationMessage) {
	r.byID[rel.RelationID] = rel
}

// Reset drops every cached Relation, for use when the stream restarts and
// Postgres re-sends them.
func (r *Relations) Reset() {
	clear(r.byID)
}

// Get returns the cached Relation for id.
func (r *Relations) Get(id uint32) (*pglogrepl.RelationMessage, bool) {
	rel, ok := r.byID[id]
	return rel, ok
}

// DecodeTuple turns a tuple of rel into column values keyed by column name.
// Types map as follows: bool to bool, int2/int4/int8 to int64, float4 to
// float32, float8 to float64, numeric and uuid to string, text types to
// string, bytea to []byte, json and jsonb to the parsed value (numbers as
// json.Number), date, timestamp and timestamptz to time.Time (timestamp
// without zone is read as UTC), NULL to nil, and an unchanged TOAST column
// ('u'), whatever its type, to event.Unchanged. Other types, and the values
// time.Time or float64 cannot hold (infinity, NaN, BC dates), stay strings.
func DecodeTuple(rel *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData) (map[string]any, error) {
	if len(tuple.Columns) != len(rel.Columns) {
		return nil, fmt.Errorf("decode %s.%s: tuple has %d columns, relation %d",
			rel.Namespace, rel.RelationName, len(tuple.Columns), len(rel.Columns))
	}
	row := make(map[string]any, len(tuple.Columns))
	for i, col := range tuple.Columns {
		rc := rel.Columns[i]
		switch col.DataType {
		case pglogrepl.TupleDataTypeNull:
			row[rc.Name] = nil
		case pglogrepl.TupleDataTypeToast:
			row[rc.Name] = event.Unchanged
		case pglogrepl.TupleDataTypeText:
			v, err := decodeValue(rc.DataType, string(col.Data))
			if err != nil {
				return nil, fmt.Errorf("decode column %q of %s.%s: %w", rc.Name, rel.Namespace, rel.RelationName, err)
			}
			row[rc.Name] = v
		default:
			return nil, fmt.Errorf("decode column %q of %s.%s: unsupported tuple data type %q",
				rc.Name, rel.Namespace, rel.RelationName, col.DataType)
		}
	}
	return row, nil
}

func decodeValue(oid uint32, s string) (any, error) {
	switch oid {
	case oidBool:
		switch s {
		case "t":
			return true, nil
		case "f":
			return false, nil
		}
		return nil, fmt.Errorf("invalid bool %q", s)
	case oidInt2, oidInt4, oidInt8:
		return strconv.ParseInt(s, 10, 64)
	case oidFloat4:
		return decodeFloat(s, 32)
	case oidFloat8:
		return decodeFloat(s, 64)
	case oidBytea:
		h, ok := strings.CutPrefix(s, `\x`)
		if !ok {
			return nil, errors.New("bytea is not in hex format")
		}
		return hex.DecodeString(h)
	case oidJSON, oidJSONB:
		dec := json.NewDecoder(strings.NewReader(s))
		dec.UseNumber()
		var v any
		if err := dec.Decode(&v); err != nil {
			return nil, fmt.Errorf("invalid json: %w", err)
		}
		return v, nil
	case oidDate:
		return decodeTime("2006-01-02", s), nil
	case oidTimestamp:
		return decodeTime("2006-01-02 15:04:05.999999", s), nil
	case oidTimestamptz:
		return decodeTimestamptz(s), nil
	}
	return s, nil
}

func decodeFloat(s string, bits int) (any, error) {
	f, err := strconv.ParseFloat(s, bits)
	if err != nil {
		return nil, err
	}
	if math.IsInf(f, 0) || math.IsNaN(f) {
		return s, nil
	}
	if bits == 32 {
		return float32(f), nil
	}
	return f, nil
}

func decodeTime(layout, s string) any {
	t, err := time.Parse(layout, s)
	if err != nil {
		return s
	}
	return t
}

// decodeTimestamptz accepts the offset forms Postgres prints: +00, +05:30 and
// +05:30:15.
func decodeTimestamptz(s string) any {
	for _, layout := range []string{
		"2006-01-02 15:04:05.999999Z07",
		"2006-01-02 15:04:05.999999Z07:00",
		"2006-01-02 15:04:05.999999Z07:00:00",
	} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC()
		}
	}
	return s
}
