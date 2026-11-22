package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormExpr(t *testing.T) {
	tests := []struct {
		name, a, b string
		same       bool
	}{
		{name: "outer parens and spacing", a: "(region = 'eu')", b: "region='eu'", same: true},
		{name: "nested outer parens", a: "((a > 1))", b: "a > 1", same: true},
		{name: "case outside quotes", a: "Region = 'eu' AND X", b: "region = 'eu' and x", same: true},
		{name: "case inside a literal differs", a: "r = 'EU'", b: "r = 'eu'"},
		{name: "space inside a literal differs", a: "r = 'a b'", b: "r = 'ab'"},
		{name: "doubled quote in literal", a: "r = 'it''s'", b: "(r = 'it''s')", same: true},
		{name: "double-quoted identifier keeps case", a: `"A" = 1`, b: `"a" = 1`},
		{name: "parens not wrapping the whole", a: "(a) and (b)", b: "a and b"},
		{name: "different expressions", a: "a > 1", b: "a > 2"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.same, normExpr(tt.a) == normExpr(tt.b))
		})
	}
}

func TestParseFilters(t *testing.T) {
	tests := []struct {
		name    string
		rows    [][][]byte
		want    map[string]tableFilter
		wantErr bool
	}{
		{name: "none"},
		{
			name: "filter and columns",
			rows: [][][]byte{slotRow("public.a", "(x > 1)", "id\x1fx"), slotRow("public.b", "", "")},
			want: map[string]tableFilter{
				"public.a": {Where: "(x > 1)", Columns: []string{"id", "x"}},
				"public.b": {},
			},
		},
		{name: "short row", rows: [][][]byte{slotRow("public.a")}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseFilters(tt.rows)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestProbeSQL(t *testing.T) {
	tests := []struct {
		name  string
		probe string
		want  string
	}{
		{
			name:  "creates a throwaway publication and rolls back",
			probe: probeSQL("walx_probe_1", "public.a", "id > 0"),
			want: `BEGIN; CREATE PUBLICATION "walx_probe_1" FOR TABLE "public"."a" WHERE (id > 0) WITH (publish = 'insert'); ` +
				`SELECT rowfilter FROM pg_publication_tables WHERE pubname = 'walx_probe_1'; ROLLBACK`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.probe)
		})
	}
}
