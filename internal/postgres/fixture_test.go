package postgres

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// fixture is a recorded run of pgoutput messages, one hex string per message.
type fixture struct {
	Messages []string `json:"messages"`
}

// recordFixture runs setup, then dml, in a fresh logical slot of the
// database at url and returns the pgoutput messages dml produced, via
// pg_logical_slot_get_binary_changes. The session is pinned to UTC so
// timestamptz text does not depend on the server's setting.
func recordFixture(t *testing.T, url, setup, dml string) [][]byte {
	t.Helper()
	ctx := context.Background()
	c, err := pgx.Connect(ctx, url)
	require.NoError(t, err)
	defer c.Close(ctx)

	for _, q := range []string{
		`SET timezone = 'UTC'`,
		setup,
		`CREATE PUBLICATION rec FOR ALL TABLES`,
		`SELECT pg_create_logical_replication_slot('rec', 'pgoutput')`,
		dml,
	} {
		_, err = c.Exec(ctx, q)
		require.NoError(t, err, q)
	}
	rows, err := c.Query(ctx, `SELECT data FROM pg_logical_slot_get_binary_changes(
		'rec', NULL, NULL, 'proto_version', '1', 'publication_names', 'rec')`)
	require.NoError(t, err)
	defer rows.Close()
	var msgs [][]byte
	for rows.Next() {
		var data []byte
		require.NoError(t, rows.Scan(&data))
		msgs = append(msgs, data)
	}
	require.NoError(t, rows.Err())
	return msgs
}

// writeFixture stores msgs as testdata/<name>.json.
func writeFixture(t *testing.T, name string, msgs [][]byte) {
	t.Helper()
	f := fixture{}
	for _, m := range msgs {
		f.Messages = append(f.Messages, hex.EncodeToString(m))
	}
	b, err := json.MarshalIndent(f, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll("testdata", 0o755))
	require.NoError(t, os.WriteFile(filepath.Join("testdata", name+".json"), append(b, '\n'), 0o644))
}

// loadFixture parses testdata/<name>.json into pgoutput messages.
func loadFixture(t *testing.T, name string) []pglogrepl.Message {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name+".json"))
	require.NoError(t, err)
	var f fixture
	require.NoError(t, json.Unmarshal(b, &f))
	var out []pglogrepl.Message
	for _, h := range f.Messages {
		raw, err := hex.DecodeString(h)
		require.NoError(t, err)
		m, err := pglogrepl.Parse(raw)
		require.NoError(t, err)
		out = append(out, m)
	}
	return out
}
