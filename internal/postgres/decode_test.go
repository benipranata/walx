package postgres

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/benipranata/walx/internal/event"
	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/require"
)

const typesSetup = `CREATE TABLE types (
	id int PRIMARY KEY,
	b bool, i2 smallint, i4 int, i8 bigint, f4 real, f8 double precision,
	n numeric, t text, vc varchar(10), bp char(5), nm name,
	j json, jb jsonb, u uuid, by bytea,
	d date, ts timestamp, tstz timestamptz, tm time, arr int[],
	inf timestamptz, nan double precision)`

const typesDML = `
INSERT INTO types VALUES (1,
	true, -7, 42, 9007199254740993, 1.5, 2.25,
	12345678901234567890.123456789, 'héllo', 'vc', 'ab', 'nm',
	'{"a": [1, 2.50, null]}', '{"a": [1, 2.50, null]}', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '\x00ff10',
	'2024-02-29', '2024-02-29 13:14:15.123456', '2024-02-29 13:14:15.5+02', '13:14:15', '{1,2}',
	'infinity', 'NaN');
INSERT INTO types (id) VALUES (2)`

// typesRow1 is the decoded first row of the types fixture.
var typesRow1 = map[string]any{
	"id": int64(1), "b": true, "i2": int64(-7), "i4": int64(42), "i8": int64(9007199254740993),
	"f4": float32(1.5), "f8": 2.25,
	"n": "12345678901234567890.123456789", "t": "héllo", "vc": "vc", "bp": "ab   ", "nm": "nm",
	"j":    map[string]any{"a": []any{json.Number("1"), json.Number("2.50"), nil}},
	"jb":   map[string]any{"a": []any{json.Number("1"), json.Number("2.50"), nil}},
	"u":    "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
	"by":   []byte{0x00, 0xff, 0x10},
	"d":    time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC),
	"ts":   time.Date(2024, 2, 29, 13, 14, 15, 123456000, time.UTC),
	"tstz": time.Date(2024, 2, 29, 11, 14, 15, 500000000, time.UTC),
	"tm":   "13:14:15", "arr": "{1,2}", "inf": "infinity", "nan": "NaN",
}

func typesRow2() map[string]any {
	row := map[string]any{}
	for k := range typesRow1 {
		row[k] = nil
	}
	row["id"] = int64(2)
	return row
}

const toastSetup = `CREATE TABLE toasty (id int PRIMARY KEY, big text, note text)`

// toastBigSQL builds about 9.6 kB of text that stays above the TOAST threshold
// even compressed, so Postgres stores it out of line. toastBig is its Go twin.
const toastBigSQL = `(SELECT string_agg(md5(g::text), '') FROM generate_series(1, 300) g)`

const toastDML = `
INSERT INTO toasty VALUES (1, ` + toastBigSQL + `, 'a');
UPDATE toasty SET note = 'b' WHERE id = 1`

// toastBig returns the value toastBigSQL produces.
func toastBig() string {
	var sb strings.Builder
	for g := 1; g <= 300; g++ {
		sum := md5.Sum([]byte(strconv.Itoa(g)))
		sb.WriteString(hex.EncodeToString(sum[:]))
	}
	return sb.String()
}

func toastRows() []map[string]any {
	return []map[string]any{
		{"id": int64(1), "big": toastBig(), "note": "a"},
		{"id": int64(1), "big": event.Unchanged, "note": "b"},
	}
}

// decodeRows decodes the new tuple of every insert and update in msgs.
func decodeRows(t *testing.T, msgs []pglogrepl.Message) []map[string]any {
	t.Helper()
	rels := NewRelations()
	var got []map[string]any
	for _, m := range msgs {
		var id uint32
		var tuple *pglogrepl.TupleData
		switch m := m.(type) {
		case *pglogrepl.RelationMessage:
			rels.Set(m)
			continue
		case *pglogrepl.InsertMessage:
			id, tuple = m.RelationID, m.Tuple
		case *pglogrepl.UpdateMessage:
			id, tuple = m.RelationID, m.NewTuple
		default:
			continue
		}
		rel, ok := rels.Get(id)
		require.True(t, ok)
		row, err := DecodeTuple(rel, tuple)
		require.NoError(t, err)
		got = append(got, row)
	}
	return got
}

func TestDecodeFixture(t *testing.T) {
	tests := []struct {
		name    string
		fixture string
		want    []map[string]any
	}{
		{name: "every mapped type and NULLs", fixture: "types", want: []map[string]any{typesRow1, typesRow2()}},
		{name: "unchanged toast column", fixture: "toast", want: toastRows()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, decodeRows(t, loadFixture(t, tt.fixture)))
		})
	}
}

func TestRelationsResendOverwrites(t *testing.T) {
	tests := []struct {
		name string
		cols []string
	}{
		{name: "second Relation replaces the first", cols: []string{"id", "added"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rels := NewRelations()
			_, ok := rels.Get(7)
			require.False(t, ok)
			rels.Set(&pglogrepl.RelationMessage{RelationID: 7, Columns: []*pglogrepl.RelationMessageColumn{{Name: "id"}}})
			var cols []*pglogrepl.RelationMessageColumn
			for _, c := range tt.cols {
				cols = append(cols, &pglogrepl.RelationMessageColumn{Name: c})
			}
			rels.Set(&pglogrepl.RelationMessage{RelationID: 7, Columns: cols})
			rel, ok := rels.Get(7)
			require.True(t, ok)
			require.Len(t, rel.Columns, 2)
		})
	}
}

func TestDecodeTupleErrors(t *testing.T) {
	col := func(oid uint32) *pglogrepl.RelationMessage {
		return &pglogrepl.RelationMessage{Columns: []*pglogrepl.RelationMessageColumn{{Name: "c", DataType: oid}}}
	}
	text := func(s string) *pglogrepl.TupleData {
		return &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{{DataType: 't', Data: []byte(s)}}}
	}
	tests := []struct {
		name  string
		rel   *pglogrepl.RelationMessage
		tuple *pglogrepl.TupleData
	}{
		{name: "column count mismatch", rel: col(oidText), tuple: &pglogrepl.TupleData{}},
		{name: "binary tuple data", rel: col(oidText),
			tuple: &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{{DataType: 'b'}}}},
		{name: "bad bool", rel: col(oidBool), tuple: text("x")},
		{name: "bad int", rel: col(oidInt4), tuple: text("x")},
		{name: "bad float", rel: col(oidFloat8), tuple: text("x")},
		{name: "bytea in escape format", rel: col(oidBytea), tuple: text("abc")},
		{name: "bytea bad hex", rel: col(oidBytea), tuple: text(`\xzz`)},
		{name: "bad json", rel: col(oidJSONB), tuple: text("{")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodeTuple(tt.rel, tt.tuple)
			require.Error(t, err)
		})
	}
}

func TestDecodeTupleUnchanged(t *testing.T) {
	tests := []struct {
		name string
		oid  uint32
	}{
		{name: "text", oid: oidText},
		{name: "jsonb", oid: oidJSONB},
		{name: "bytea", oid: oidBytea},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rel := &pglogrepl.RelationMessage{Columns: []*pglogrepl.RelationMessageColumn{{Name: "c", DataType: tt.oid}}}
			tuple := &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{{DataType: pglogrepl.TupleDataTypeToast}}}
			row, err := DecodeTuple(rel, tuple)
			require.NoError(t, err)
			require.Equal(t, map[string]any{"c": event.Unchanged}, row)
		})
	}
}

func TestDecodeValueEdges(t *testing.T) {
	tests := []struct {
		name string
		oid  uint32
		in   string
		want any
	}{
		{name: "false", oid: oidBool, in: "f", want: false},
		{name: "float4 infinity stays a string", oid: oidFloat4, in: "Infinity", want: "Infinity"},
		{name: "timestamptz with minutes offset", oid: oidTimestamptz, in: "2024-01-02 03:04:05+05:30",
			want: time.Date(2024, 1, 1, 21, 34, 5, 0, time.UTC)},
		{name: "timestamptz with seconds offset", oid: oidTimestamptz, in: "1883-11-18 12:00:00-04:56:02",
			want: time.Date(1883, 11, 18, 16, 56, 2, 0, time.UTC)},
		{name: "BC date stays a string", oid: oidDate, in: "0044-03-15 BC", want: "0044-03-15 BC"},
		{name: "unknown type is a string", oid: 9999, in: "x", want: "x"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := decodeValue(tt.oid, tt.in)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestRecordFixtureIntegration(t *testing.T) {
	url := startPostgres(t)
	tests := []struct {
		name    string
		fixture string
		setup   string
		dml     string
		want    []map[string]any
	}{
		{name: "types", fixture: "types", setup: typesSetup, dml: typesDML,
			want: []map[string]any{typesRow1, typesRow2()}},
		{name: "large text column", fixture: "toast", setup: toastSetup, dml: toastDML, want: toastRows()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := recordFixture(t, isolatedDatabase(t, url), tt.setup, tt.dml)
			if os.Getenv("WALX_RECORD_FIXTURES") != "" {
				writeFixture(t, tt.fixture, raw)
			}
			var msgs []pglogrepl.Message
			for _, b := range raw {
				m, err := pglogrepl.Parse(b)
				require.NoError(t, err)
				msgs = append(msgs, m)
			}
			require.Equal(t, tt.want, decodeRows(t, msgs))
		})
	}
}

// TestReplicaIdentityFullToastIntegration records what REPLICA IDENTITY FULL
// puts in the old tuple when an update leaves a TOAST column untouched.
func TestReplicaIdentityFullToastIntegration(t *testing.T) {
	url := startPostgres(t)
	tests := []struct {
		name    string
		wantOld map[string]any
	}{
		{name: "full", wantOld: map[string]any{"id": int64(1), "big": toastBig(), "note": "a"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw := recordFixture(t, isolatedDatabase(t, url),
				toastSetup+`; ALTER TABLE toasty REPLICA IDENTITY FULL`, toastDML)
			rels := NewRelations()
			var found bool
			for _, b := range raw {
				m, err := pglogrepl.Parse(b)
				require.NoError(t, err)
				switch m := m.(type) {
				case *pglogrepl.RelationMessage:
					rels.Set(m)
				case *pglogrepl.UpdateMessage:
					rel, _ := rels.Get(m.RelationID)
					require.NotNil(t, m.OldTuple)
					old, err := DecodeTuple(rel, m.OldTuple)
					require.NoError(t, err)
					newRow, err := DecodeTuple(rel, m.NewTuple)
					require.NoError(t, err)
					require.Equal(t, event.Unchanged, newRow["big"])
					require.Equal(t, tt.wantOld, old)
					found = true
				}
			}
			require.True(t, found)
		})
	}
}
