package config_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/config"
)

func TestEnvExpansion(t *testing.T) {
	const plain = `
databases:
  - name: ${WALX_TEST_NAME}
    postgres: {url: "postgres://u@h/db"}
    sink: {type: stdout}
`
	tests := []struct {
		name string
		env  map[string]string
		doc  string
		got  func(cfg *config.Config) any
		want any
	}{
		{
			name: "inside a url",
			env:  map[string]string{"PGHOST": "db.internal", "PGDB": "orders"},
			doc: `
databases:
  - name: a
    postgres:
      url: postgres://u@${PGHOST}/${PGDB}
    sink: {type: stdout}
`,
			got:  func(c *config.Config) any { return c.Databases[0].Postgres.URL },
			want: "postgres://u@db.internal/orders",
		},
		{
			name: "set but empty is allowed",
			env:  map[string]string{"WALX_TEST_EMPTY": ""},
			doc:  mk("      slot: x${WALX_TEST_EMPTY}\n", ""),
			got:  func(c *config.Config) any { return c.Databases[0].Postgres.Slot },
			want: "x",
		},
		{
			name: "other dollars are left alone",
			doc:  mk("      slot: a$b$\n", ""),
			got:  func(c *config.Config) any { return c.Databases[0].Postgres.Slot },
			want: "a$b$",
		},
		{
			name: "comments are ignored",
			doc:  mk("", "    # uses ${WALX_TEST_UNSET}\n"),
			got:  func(c *config.Config) any { return c.Databases[0].Name },
			want: "a",
		},
		{
			name: "expanded value is not reinterpreted in a string field",
			env:  map[string]string{"WALX_TEST_NAME": "true"},
			doc:  plain,
			got:  func(c *config.Config) any { return c.Databases[0].Name },
			want: "true",
		},
		{
			name: "inside a list",
			env:  map[string]string{"WALX_TEST_COL": "raw"},
			doc:  mk("", "    tables:\n      - name: public.t\n        drop_columns: [\"${WALX_TEST_COL}\"]\n"),
			got:  func(c *config.Config) any { return c.Databases[0].Tables[0].DropColumns },
			want: []string{"raw"},
		},
		{
			name: "bool field",
			env:  map[string]string{"WALX_TEST_BOOL": "false"},
			doc:  mk("      auto_create: ${WALX_TEST_BOOL}\n", ""),
			got:  func(c *config.Config) any { return c.Databases[0].Postgres.AutoCreate },
			want: false,
		},
		{
			name: "int field",
			env:  map[string]string{"WALX_TEST_INT": "5"},
			doc:  mk("", "    retry:\n      policy: max_attempts\n      max_attempts: ${WALX_TEST_INT}\n"),
			got:  func(c *config.Config) any { return c.Databases[0].Retry.MaxAttempts },
			want: 5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for k, v := range tt.env {
				t.Setenv(k, v)
			}
			require.Equal(t, tt.want, tt.got(mustParse(t, tt.doc)))
		})
	}
}

func TestEnvErrors(t *testing.T) {
	tests := []struct {
		name  string
		env   map[string]string
		doc   string
		wants []string
	}{
		{
			name: "unset variable",
			doc: `
databases:
  - name: a
    postgres:
      url: ${WALX_TEST_UNSET}
    sink: {type: stdout}
`,
			wants: []string{"WALX_TEST_UNSET", "databases[0].postgres.url"},
		},
		{
			name:  "unset variable in a list",
			doc:   mk("", "    tables:\n      - name: public.t\n        drop_columns: [\"${WALX_TEST_UNSET}\"]\n"),
			wants: []string{"databases[0].tables[0].drop_columns[0]"},
		},
		{
			name:  "quoted value stays a string",
			env:   map[string]string{"WALX_TEST_BOOL": "false"},
			doc:   mk("      auto_create: \"${WALX_TEST_BOOL}\"\n", ""),
			wants: []string{"databases[0].postgres.auto_create"},
		},
		{
			name: "aliases are rejected",
			doc: `
databases:
  - name: a
    postgres: {url: &u "postgres://u@h/d"}
    sink: {type: stdout}
  - name: b
    postgres: {url: *u}
    sink: {type: stdout}
`,
			wants: []string{"databases[1].postgres.url", "aliases are not supported"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for k, v := range tt.env {
				t.Setenv(k, v)
			}
			_, err := config.Parse([]byte(tt.doc))
			require.Error(t, err)
			for _, want := range tt.wants {
				require.ErrorContains(t, err, want)
			}
		})
	}
}
