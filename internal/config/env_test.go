package config_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/config"
)

func TestEnvExpansion(t *testing.T) {
	t.Setenv("PGHOST", "db.internal")
	t.Setenv("PGDB", "orders")
	cfg := mustParse(t, `
databases:
  - name: a
    postgres:
      url: postgres://u@${PGHOST}/${PGDB}
    sink: {type: stdout}
`)
	require.Equal(t, "postgres://u@db.internal/orders", cfg.Databases[0].Postgres.URL)
}

func TestEnvUnsetIsAnError(t *testing.T) {
	_, err := config.Parse([]byte(`
databases:
  - name: a
    postgres:
      url: ${WALX_TEST_UNSET}
    sink: {type: stdout}
`))
	require.ErrorContains(t, err, "WALX_TEST_UNSET")
	require.ErrorContains(t, err, "databases[0].postgres.url")
}

func TestEnvSetButEmptyIsAllowed(t *testing.T) {
	t.Setenv("WALX_TEST_EMPTY", "")
	cfg := mustParse(t, mk("      slot: x${WALX_TEST_EMPTY}\n", ""))
	require.Equal(t, "x", cfg.Databases[0].Postgres.Slot)
}

func TestEnvLeavesOtherDollarsAlone(t *testing.T) {
	cfg := mustParse(t, mk("      slot: a$b$\n", ""))
	require.Equal(t, "a$b$", cfg.Databases[0].Postgres.Slot)
}

func TestEnvIgnoresCommentsAndKeys(t *testing.T) {
	cfg := mustParse(t, mk("", "    # uses ${WALX_TEST_UNSET}\n"))
	require.Equal(t, "a", cfg.Databases[0].Name)
}

func TestEnvExpandedValueIsNotReinterpreted(t *testing.T) {
	t.Setenv("WALX_TEST_NAME", "true")
	cfg := mustParse(t, `
databases:
  - name: ${WALX_TEST_NAME}
    postgres: {url: "postgres://u@h/db"}
    sink: {type: stdout}
`)
	require.Equal(t, "true", cfg.Databases[0].Name)
}

func TestEnvExpansionInLists(t *testing.T) {
	t.Setenv("WALX_TEST_COL", "raw")
	cfg := mustParse(t, mk("", "    tables:\n      - name: public.t\n        drop_columns: [\"${WALX_TEST_COL}\"]\n"))
	require.Equal(t, []string{"raw"}, cfg.Databases[0].Tables[0].DropColumns)

	_, err := config.Parse([]byte(mk("", "    tables:\n      - name: public.t\n        drop_columns: [\"${WALX_TEST_UNSET}\"]\n")))
	require.ErrorContains(t, err, "databases[0].tables[0].drop_columns[0]")
}
