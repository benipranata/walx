package config_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/config"
)

func TestValidationErrors(t *testing.T) {
	file := func(name, path string) string {
		return "  - name: " + name + "\n    postgres: {url: \"postgres://u@h/" + name + "\"}\n    sink: {type: file, path: " + path + "}\n"
	}
	abs, err := filepath.Abs("x.jsonl")
	require.NoError(t, err)
	tests := []struct {
		name, doc, want string
	}{
		{"no databases", "databases: []\n", "databases: at least one"},
		{"missing databases key", "shutdown_timeout: 5s\n", "databases: at least one"},
		{"missing name", "databases:\n  - postgres: {url: \"postgres://u@h/d\"}\n    sink: {type: stdout}\n", "databases[0].name"},
		{"duplicate names", "databases:\n" + file("a", "./a") + file("a", "./b"), "databases[1].name"},
		{"duplicate file paths", "databases:\n" + file("a", "./x.jsonl") + file("b", "x.jsonl"), "databases[1].sink.path"},
		{"relative and absolute file paths", "databases:\n" + file("a", abs) + file("b", "x.jsonl"), "databases[1].sink.path"},
		{"same url and slot", "databases:\n" +
			"  - {name: a, postgres: {url: \"postgres://u@h/d\", slot: s}, sink: {type: stdout}}\n" +
			"  - {name: b, postgres: {url: \"postgres://u@h/d\", slot: s}, sink: {type: stdout}}\n", "databases[1].postgres.slot"},
		{"missing url", "databases:\n  - name: a\n    sink: {type: stdout}\n", "databases[0].postgres.url"},
		{"url without scheme", "databases:\n  - {name: a, postgres: {url: \"localhost/db\"}, sink: {type: stdout}}\n", "databases[0].postgres.url"},
		{"url wrong scheme", "databases:\n  - {name: a, postgres: {url: \"mysql://h/db\"}, sink: {type: stdout}}\n", "databases[0].postgres.url"},
		{"url unparseable", "databases:\n  - {name: a, postgres: {url: \"postgres://h:port/db\"}, sink: {type: stdout}}\n", "databases[0].postgres.url"},
		{"unknown sink type", "databases:\n  - {name: a, postgres: {url: \"postgres://u@h/d\"}, sink: {type: kafka}}\n", "databases[0].sink.type"},
		{"missing sink", "databases:\n  - {name: a, postgres: {url: \"postgres://u@h/d\"}}\n", "databases[0].sink.type"},
		{"file sink without path", "databases:\n  - {name: a, postgres: {url: \"postgres://u@h/d\"}, sink: {type: file}}\n", "databases[0].sink.path"},
		{"zero max_tx_bytes", mk("      max_tx_bytes: 0\n", ""), "databases[0].postgres.max_tx_bytes"},
		{"table without schema", mk("", "    tables:\n      - name: orders\n"), "databases[0].tables[0].name"},
		{"table too many parts", mk("", "    tables:\n      - name: a.b.c\n"), "databases[0].tables[0].name"},
		{"table empty part", mk("", "    tables:\n      - name: .t\n"), "databases[0].tables[0].name"},
		{"table quoted", mk("", "    tables:\n      - name: '\"my.schema\".t'\n"), "databases[0].tables[0].name"},
		{"table missing name", mk("", "    tables:\n      - ops: [insert]\n"), "databases[0].tables[0].name"},
		{"unknown op", mk("", "    tables:\n      - name: public.t\n        ops: [insert, upsert]\n"), "databases[0].tables[0].ops[1]"},
		{"empty ops", mk("", "    tables:\n      - {name: public.t, ops: []}\n"), "databases[0].tables[0].ops"},
		{"snapshot is not a table op", mk("", "    tables:\n      - name: public.t\n        ops: [insert, snapshot]\n"), "databases[0].tables[0].ops[1]"},
		{"unknown retry policy", mk("", "    retry:\n      policy: sometimes\n"), "databases[0].retry.policy"},
		{"zero retry initial", mk("", "    retry:\n      initial: 0s\n"), "databases[0].retry.initial"},
		{"negative retry max", mk("", "    retry:\n      max: -1s\n"), "databases[0].retry.max"},
		{"retry max below initial", mk("", "    retry:\n      initial: 10s\n      max: 1s\n"), "databases[0].retry.max"},
		{"negative max_attempts", mk("", "    retry:\n      max_attempts: -1\n"), "databases[0].retry.max_attempts"},
		{"unknown on_permanent_error", mk("", "    on_permanent_error: ignore\n"), "databases[0].on_permanent_error"},
		{"zero shutdown_timeout", "shutdown_timeout: 0s\n" + mk("", ""), "shutdown_timeout"},
		{"negative shutdown_timeout", "shutdown_timeout: -5s\n" + mk("", ""), "shutdown_timeout"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := config.Parse([]byte(tt.doc))
			require.ErrorContains(t, err, tt.want)
		})
	}
}

func TestValidationReportsEveryProblem(t *testing.T) {
	_, err := config.Parse([]byte(mk("", "    on_permanent_error: x\n    retry: {policy: y}\n")))
	require.ErrorContains(t, err, "on_permanent_error")
	require.ErrorContains(t, err, "retry.policy")
}

func TestValidationAccepts(t *testing.T) {
	tests := []struct{ name, doc string }{
		{"postgresql scheme", "databases:\n  - {name: a, postgres: {url: \"postgresql://u@h/d\"}, sink: {type: stdout}}\n"},
		{"same url, different slot", "databases:\n" +
			"  - {name: a, postgres: {url: \"postgres://u@h/d\"}, sink: {type: stdout}}\n" +
			"  - {name: b, postgres: {url: \"postgres://u@h/d\"}, sink: {type: stdout}}\n"},
		{"stdout sinks may repeat", "databases:\n" +
			"  - {name: a, postgres: {url: \"postgres://u@h/a\"}, sink: {type: stdout}}\n" +
			"  - {name: b, postgres: {url: \"postgres://u@h/b\"}, sink: {type: stdout}}\n"},
		{"max_attempts policy with 0", mk("", "    retry: {policy: max_attempts}\n")},
		{"retry max equals initial", mk("", "    retry: {initial: 1s, max: 1s}\n")},
		{"all ops", mk("", "    tables:\n      - {name: public.t, ops: [insert, update, delete, truncate]}\n")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := config.Parse([]byte(tt.doc))
			require.NoError(t, err)
		})
	}
}
