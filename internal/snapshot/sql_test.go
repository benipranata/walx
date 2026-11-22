package snapshot

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDeclareSQL(t *testing.T) {
	tests := []struct {
		name string
		plan tablePlan
		want string
	}{
		{
			name: "ordered by key",
			plan: tablePlan{Schema: "public", Table: "orders", Columns: []string{"id", "status"}, Order: []string{"id"}},
			want: `DECLARE walx_snapshot NO SCROLL CURSOR FOR SELECT "id", "status" FROM "public"."orders" ORDER BY "id"`,
		},
		{
			name: "composite key and where",
			plan: tablePlan{Schema: "s", Table: "t", Columns: []string{"a", "b", "c"}, Order: []string{"a", "b"}, Where: "c > 1"},
			want: `DECLARE walx_snapshot NO SCROLL CURSOR FOR SELECT "a", "b", "c" FROM "s"."t" WHERE (c > 1) ORDER BY "a", "b"`,
		},
		{
			name: "no key, quoted identifiers",
			plan: tablePlan{Schema: "My Schema", Table: `we"ird`, Columns: []string{`x"y`}},
			want: `DECLARE walx_snapshot NO SCROLL CURSOR FOR SELECT "x""y" FROM "My Schema"."we""ird"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, declareSQL(tt.plan))
		})
	}
}

func TestFetchSQL(t *testing.T) {
	tests := []struct {
		name string
		n    int
		want string
	}{
		{"chunk", 1000, "FETCH FORWARD 1000 FROM walx_snapshot"},
		{"one", 1, "FETCH FORWARD 1 FROM walx_snapshot"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, fetchSQL(tt.n))
		})
	}
}

func TestSlotAndSnapshotSQL(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"create slot", createSlotSQL("walx_snap_ab12"), `CREATE_REPLICATION_SLOT walx_snap_ab12 TEMPORARY LOGICAL pgoutput (SNAPSHOT 'export')`},
		{"begin", beginSQL("00000003-0000002A-1"), `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET TRANSACTION SNAPSHOT '00000003-0000002A-1'`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.got)
		})
	}
}

func TestParseTableName(t *testing.T) {
	tests := []struct {
		name       string
		in         string
		wantSchema string
		wantTable  string
		wantErr    string
	}{
		{"ok", "public.orders", "public", "orders", ""},
		{"no schema", "orders", "", "", "schema.table"},
		{"empty part", "public.", "", "", "schema.table"},
		{"extra dot", "a.b.c", "", "", "schema.table"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, tb, err := splitTable(tt.in)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantSchema, s)
			require.Equal(t, tt.wantTable, tb)
		})
	}
}
