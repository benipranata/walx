package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/benipranata/walx/internal/config"
)

// ReadKeys returns the key columns of every listed table, keyed
// "schema.table" as the keys argument of DecodeDML and NewTxBuffer expects:
// the configured key if set, else the primary key columns in index order, and
// only for a table with REPLICA IDENTITY FULL (a primary key outside another
// identity index would fail on delete, and those Relations flag the right
// columns already). A table with neither has no entry, so its key stays the
// columns the Relation message flags. Call it after EnsureReplicaIdentity. Pass the result to NewTxBuffer: under REPLICA IDENTITY FULL
// the Relation flags every column, so without it Key would be the whole row.
func ReadKeys(ctx context.Context, pg *pgconn.PgConn, db config.DatabaseConfig) (map[string][]string, error) {
	keys := map[string][]string{}
	for _, t := range db.Tables {
		if len(t.Key) > 0 {
			keys[t.Name] = t.Key
			continue
		}
		cols, err := readPrimaryKey(ctx, pg, t.Name)
		if err != nil {
			return nil, err
		}
		if len(cols) > 0 {
			keys[t.Name] = cols
		}
	}
	return keys, nil
}

func readPrimaryKey(ctx context.Context, pg *pgconn.PgConn, name string) ([]string, error) {
	rows, err := query(ctx, pg, "SELECT string_agg(a.attname::text, chr(31) ORDER BY array_position(i.indkey::int2[], a.attnum)) "+
		"FROM pg_index i JOIN pg_class c ON c.oid = i.indrelid JOIN pg_namespace n ON n.oid = c.relnamespace "+
		"JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey::int2[]) "+
		"WHERE n.nspname || '.' || c.relname = "+quoteLiteral(name)+" AND i.indisprimary AND c.relreplident = 'f'")
	if err != nil {
		return nil, fmt.Errorf("read primary key of %s: %w", name, err)
	}
	if len(rows) == 0 || rows[0][0] == nil {
		return nil, nil
	}
	return strings.Split(str(rows[0][0]), "\x1f"), nil
}
