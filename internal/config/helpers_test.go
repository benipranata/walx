package config_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/config"
)

// mk builds a one-database document. pg lines sit under postgres, db lines at
// the database level; both must carry their own indentation.
func mk(pg, db string) string {
	return "databases:\n" +
		"  - name: a\n" +
		"    postgres:\n" +
		"      url: postgres://u@h/db\n" + pg +
		"    sink:\n" +
		"      type: stdout\n" + db
}

func mustParse(t *testing.T, doc string) *config.Config {
	t.Helper()
	cfg, err := config.Parse([]byte(doc))
	require.NoError(t, err)
	return cfg
}
