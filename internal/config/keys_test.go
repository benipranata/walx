package config_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/config"
)

func TestUnknownKeys(t *testing.T) {
	tests := []struct {
		name, doc, want string
	}{
		{"top level", "shutdown_timeouts: 5s\n" + mk("", ""), "shutdown_timeouts"},
		{"database", mk("", "    on_permanent: stop\n"), "databases[0].on_permanent"},
		{"postgres", mk("      urll: x\n", ""), "databases[0].postgres.urll"},
		{"retry", mk("", "    retry:\n      attempts: 3\n"), "databases[0].retry.attempts"},
		{"sink", "databases:\n  - name: a\n    postgres: {url: \"postgres://u@h/db\"}\n    sink: {type: stdout, file: x}\n", "databases[0].sink.file"},
		{"table", mk("", "    tables:\n      - name: public.t\n        op: [insert]\n"), "databases[0].tables[0].op"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := config.Parse([]byte(tt.doc))
			require.ErrorContains(t, err, tt.want)
			require.ErrorContains(t, err, "unknown key")
			require.ErrorContains(t, err, "line ")
		})
	}
}

func TestKeyErrorReporting(t *testing.T) {
	tests := []struct {
		name        string
		doc         string
		contains    []string
		notContains []string
	}{
		{
			name:     "every unknown key is reported",
			doc:      mk("      a: 1\n", "    b: 2\n"),
			contains: []string{"postgres.a", "databases[0].b"},
		},
		{
			name: "a decode error on a shared line does not guess the key",
			doc: "databases:\n" +
				"  - {name: a, name: b, postgres: {url: \"postgres://u@h/d\"}, sink: {type: stdout}}\n",
			contains:    []string{"line 2"},
			notContains: []string{"databases[0].sink.type"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := config.Parse([]byte(tt.doc))
			require.Error(t, err)
			for _, want := range tt.contains {
				require.ErrorContains(t, err, want)
			}
			for _, not := range tt.notContains {
				require.NotContains(t, err.Error(), not)
			}
		})
	}
}

func TestFreeFormWhereKeysAreNotChecked(t *testing.T) {
	tests := []struct {
		name string
		doc  string
		want map[string]any
	}{
		{
			"arbitrary keys",
			mk("", "    tables:\n      - name: public.t\n        where: {anything: 1}\n"),
			map[string]any{"anything": 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := mustParse(t, tt.doc)
			require.Equal(t, tt.want, cfg.Databases[0].Tables[0].Where)
		})
	}
}
