package config_test

import (
	"path/filepath"
	"strings"
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
		{"null database entry", "databases:\n  - null\n", "databases[0] (line 2): must not be null"},
		{"null entry before a valid one", "databases:\n  - null\n  - {name: a, postgres: {url: \"postgres://u@h/d\"}, sink: {type: stdout}}\n", "databases[0] (line 2): must not be null"},
		{"null table entry", mk("", "    tables:\n      - null\n"), "databases[0].tables[0] (line 8): must not be null"},
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
		{"duplicate table", mk("", "    tables:\n      - name: public.t\n      - name: public.t\n"), "databases[0].tables[1].name"},
		{"same slot in another database of one server", "databases:\n" +
			"  - {name: a, postgres: {url: \"postgres://u@h/d1\", slot: s}, sink: {type: stdout}}\n" +
			"  - {name: b, postgres: {url: \"postgres://u@h/d2\", slot: s}, sink: {type: stdout}}\n", "databases[1].postgres.slot"},
		{"same slot through postgresql scheme and explicit port", "databases:\n" +
			"  - {name: a, postgres: {url: \"postgres://u@H/d\", slot: s}, sink: {type: stdout}}\n" +
			"  - {name: b, postgres: {url: \"postgresql://v@h:5432/d\", slot: s}, sink: {type: stdout}}\n", "databases[1].postgres.slot"},
		{"same publication on one database", "databases:\n" +
			"  - {name: a, postgres: {url: \"postgres://u@h/d\", slot: s1, publication: p}, sink: {type: stdout}}\n" +
			"  - {name: b, postgres: {url: \"postgres://u@h/d\", slot: s2, publication: p}, sink: {type: stdout}}\n", "databases[1].postgres.publication"},
		{"unknown sink type", "databases:\n  - {name: a, postgres: {url: \"postgres://u@h/d\"}, sink: {type: kafka}}\n", "databases[0].sink.type"},
		{"missing sink", "databases:\n  - {name: a, postgres: {url: \"postgres://u@h/d\"}}\n", "databases[0].sink.type"},
		{"file sink without path", "databases:\n  - {name: a, postgres: {url: \"postgres://u@h/d\"}, sink: {type: file}}\n", "databases[0].sink.path"},
		{"name with a dash", "databases:\n  - {name: orders-db, postgres: {url: \"postgres://u@h/d\"}, sink: {type: stdout}}\n", "databases[0].name"},
		{"name with upper case", "databases:\n  - {name: Orders, postgres: {url: \"postgres://u@h/d\"}, sink: {type: stdout}}\n", "databases[0].name"},
		{"name starting with a digit", "databases:\n  - {name: 1db, postgres: {url: \"postgres://u@h/d\"}, sink: {type: stdout}}\n", "databases[0].name"},
		{"name too long for the default slot", "databases:\n  - {name: " + strings.Repeat("a", 59) + ", postgres: {url: \"postgres://u@h/d\"}, sink: {type: stdout}}\n", "databases[0].postgres.slot"},
		{"explicit slot with invalid characters", mk("      slot: Bad-Slot\n", ""), "databases[0].postgres.slot"},
		{"explicit slot starting with a digit", mk("      slot: 1abc\n", ""), "databases[0].postgres.slot"},
		{"explicit slot too long", mk("      slot: "+strings.Repeat("s", 64)+"\n", ""), "databases[0].postgres.slot"},
		{"publication too long", mk("      publication: "+strings.Repeat("p", 64)+"\n", ""), "databases[0].postgres.publication"},
		{"negative max_tx_events", mk("      max_tx_events: -1\n", ""), "databases[0].postgres.max_tx_events"},
		{"zero max_tx_bytes", mk("      max_tx_bytes: 0\n", ""), "databases[0].postgres.max_tx_bytes"},
		{"table without schema", mk("", "    tables:\n      - name: orders\n"), "databases[0].tables[0].name"},
		{"table too many parts", mk("", "    tables:\n      - name: a.b.c\n"), "databases[0].tables[0].name"},
		{"table empty part", mk("", "    tables:\n      - name: .t\n"), "databases[0].tables[0].name"},
		{"table quoted", mk("", "    tables:\n      - name: '\"my.schema\".t'\n"), "databases[0].tables[0].name"},
		{"table missing name", mk("", "    tables:\n      - ops: [insert]\n"), "databases[0].tables[0].name"},
		{"unknown op", mk("", "    tables:\n      - name: public.t\n        ops: [insert, upsert]\n"), "databases[0].tables[0].ops[1]"},
		{"op in upper case", mk("", "    tables:\n      - name: public.t\n        ops: [Insert]\n"), "databases[0].tables[0].ops[0]"},
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

func TestValidationReporting(t *testing.T) {
	tests := []struct {
		name        string
		doc         string
		contains    []string
		notContains []string
	}{
		{
			name:     "every problem is reported",
			doc:      mk("", "    on_permanent_error: x\n    retry: {policy: y}\n"),
			contains: []string{"on_permanent_error", "retry.policy"},
		},
		{
			name: "unnamed entries do not report slot duplicates",
			doc: "databases:\n" +
				"  - {postgres: {url: \"postgres://u@h/d\"}, sink: {type: stdout}}\n" +
				"  - {postgres: {url: \"postgres://u@h/d\"}, sink: {type: stdout}}\n",
			contains:    []string{"databases[1].name"},
			notContains: []string{"slot"},
		},
		{
			name:        "invalid escape in url does not echo the password",
			doc:         "databases:\n  - {name: a, postgres: {url: \"postgres://u:s3cret%zz@h/d\"}, sink: {type: stdout}}\n",
			contains:    []string{"databases[0].postgres.url", "invalid URL escape"},
			notContains: []string{"s3cret"},
		},
		{
			name:        "space in url does not echo the password",
			doc:         "databases:\n  - {name: a, postgres: {url: \"postgres://u:s3 cret@h:bad/d\"}, sink: {type: stdout}}\n",
			contains:    []string{"databases[0].postgres.url"},
			notContains: []string{"s3 cret", "s3cret"},
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

func TestValidationAccepts(t *testing.T) {
	tests := []struct{ name, doc string }{
		{"postgresql scheme", "databases:\n  - {name: a, postgres: {url: \"postgresql://u@h/d\"}, sink: {type: stdout}}\n"},
		{"same url, different slot", "databases:\n" +
			"  - {name: a, postgres: {url: \"postgres://u@h/d\"}, sink: {type: stdout}}\n" +
			"  - {name: b, postgres: {url: \"postgres://u@h/d\"}, sink: {type: stdout}}\n"},
		{"same slot on different servers", "databases:\n" +
			"  - {name: a, postgres: {url: \"postgres://u@h1/d\", slot: s}, sink: {type: stdout}}\n" +
			"  - {name: b, postgres: {url: \"postgres://u@h2/d\", slot: s}, sink: {type: stdout}}\n"},
		{"same slot on different ports", "databases:\n" +
			"  - {name: a, postgres: {url: \"postgres://u@h:5432/d\", slot: s}, sink: {type: stdout}}\n" +
			"  - {name: b, postgres: {url: \"postgres://u@h:5433/d\", slot: s}, sink: {type: stdout}}\n"},
		{"same publication name in different databases", "databases:\n" +
			"  - {name: a, postgres: {url: \"postgres://u@h/d1\", slot: s1, publication: p}, sink: {type: stdout}}\n" +
			"  - {name: b, postgres: {url: \"postgres://u@h/d2\", slot: s2, publication: p}, sink: {type: stdout}}\n"},
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
