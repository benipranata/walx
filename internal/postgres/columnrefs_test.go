package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/config"
)

func TestColumnRefs(t *testing.T) {
	tests := []struct {
		name string
		t    config.TableConfig
		want []columnRef
	}{
		{"nothing configured", config.TableConfig{Name: "public.a"}, nil},
		{"every field in a stable order", config.TableConfig{
			Name:        "public.a",
			Key:         []string{"id"},
			FSMColumn:   "status",
			DropColumns: []string{"secret", "note"},
			Where:       map[string]any{"z": 1, "kind": "x"},
			OnChange:    []string{"status"},
		}, []columnRef{
			{"key", "id"}, {"where", "kind"}, {"where", "z"}, {"on_change", "status"},
			{"fsm_column", "status"}, {"drop_columns", "secret"}, {"drop_columns", "note"},
		}},
		{"empty fsm_column is skipped", config.TableConfig{Name: "public.a", OnChange: []string{"s"}}, []columnRef{{"on_change", "s"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, columnRefs(tt.t))
		})
	}
}
