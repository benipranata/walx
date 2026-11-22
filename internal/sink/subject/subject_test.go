package subject_test

import (
	"testing"

	"github.com/benipranata/walx/internal/sink/subject"
	"github.com/stretchr/testify/require"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name    string
		tmpl    string
		wantErr string
	}{
		{"default", subject.DefaultTemplate, ""},
		{"all placeholders", "{database}.{schema}.{table}.{token}", ""},
		{"literal mixed with placeholder", "app.{database}_{table}.{token}", ""},
		{"no placeholder", "cdc", ""},
		{"empty", "", "empty"},
		{"empty token", "cdc..{token}", "empty"},
		{"leading dot", ".cdc", "empty"},
		{"trailing dot", "cdc.", "empty"},
		{"unknown placeholder", "cdc.{tabel}", `unknown placeholder "{tabel}"`},
		{"unclosed brace", "cdc.{table", "unclosed"},
		{"stray close brace", "cdc.table}", "unexpected"},
		{"empty placeholder", "cdc.{}", "unknown placeholder"},
		{"wildcard star", "cdc.*", "reserved"},
		{"wildcard full", "cdc.>", "reserved"},
		{"whitespace", "cdc. x", "whitespace"},
		{"tab", "cdc.a\tb", "whitespace"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := subject.Parse(tt.tmpl)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestRender(t *testing.T) {
	tests := []struct {
		name    string
		tmpl    string
		v       subject.Values
		want    string
		wantErr error
	}{
		{"default", subject.DefaultTemplate, subject.Values{Database: "db", Schema: "public", Table: "orders", Token: "insert"}, "cdc.public.orders.insert", nil},
		{"database", "{database}.{token}", subject.Values{Database: "db", Token: "x"}, "db.x", nil},
		{"mixed segment", "{database}_{table}", subject.Values{Database: "db", Table: "t"}, "db_t", nil},
		{"dots in names are replaced", subject.DefaultTemplate, subject.Values{Schema: "a.b", Table: "c*d", Token: "e>f"}, "cdc.a_b.c_d.e_f", nil},
		{"whitespace is replaced", subject.DefaultTemplate, subject.Values{Schema: "a b", Table: "c\td", Token: "e\nf"}, "cdc.a_b.c_d.e_f", nil},
		{"empty value", subject.DefaultTemplate, subject.Values{Schema: "public", Table: "", Token: "x"}, "", subject.ErrEmptyToken},
		{"unused placeholder may be empty", "cdc.{table}", subject.Values{Table: "t"}, "cdc.t", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpl, err := subject.Parse(tt.tmpl)
			require.NoError(t, err)
			got, err := tmpl.Render(tt.v)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestSanitize(t *testing.T) {
	tests := []struct{ name, in, want string }{
		{"plain", "orders", "orders"},
		{"dot", "a.b", "a_b"},
		{"star", "a*b", "a_b"},
		{"gt", "a>b", "a_b"},
		{"space", "a b", "a_b"},
		{"control", "a\x00b", "a_b"},
		{"unicode kept", "café", "café"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, subject.Sanitize(tt.in))
		})
	}
}

func TestStateToken(t *testing.T) {
	tests := []struct{ name, in, want string }{
		{"plain state", "paid", "paid"},
		{"insert", "insert", "state_insert"},
		{"update", "update", "state_update"},
		{"delete", "delete", "state_delete"},
		{"truncate", "truncate", "state_truncate"},
		{"snapshot", "snapshot", "state_snapshot"},
		{"escape prefix itself", "state_insert", "state_state_insert"},
		{"prefix on other word", "state_x", "state_state_x"},
		{"sanitized first", "a.b", "a_b"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, subject.StateToken(tt.in))
		})
	}
}
