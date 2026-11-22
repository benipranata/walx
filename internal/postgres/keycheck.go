package postgres

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/benipranata/walx/internal/config"
)

// ErrKey means a configured key does not fit the table.
var ErrKey = errors.New("key")

// CheckKeys validates the columns a listed table's config names: every column
// of key, where, on_change, fsm_column and drop_columns must exist (error
// wrapping ErrKey, naming the table, field and column; all missing columns of
// a table are reported together). For a custom key it also returns a warning
// when no unique index covers it, since the row it names may then not be
// unique. An index covers a key when all its key columns (not INCLUDE columns)
// are key columns and NULLs cannot repeat in it (NOT NULL columns or NULLS NOT
// DISTINCT). A delete whose key lies outside the replica identity is checked by
// EnsureReplicaIdentity.
func CheckKeys(ctx context.Context, pg *pgconn.PgConn, db config.DatabaseConfig) (warnings []string, err error) {
	for _, t := range db.Tables {
		refs := columnRefs(t)
		if len(refs) == 0 {
			continue
		}
		cols, uniques, err := readKeyInfo(ctx, pg, t.Name)
		if err != nil {
			return warnings, err
		}
		var missing []string
		for _, r := range refs {
			if !slices.Contains(cols, r.Column) {
				missing = append(missing, fmt.Sprintf("%s column %q is not a column of %s", r.Field, r.Column, t.Name))
			}
		}
		if len(missing) > 0 {
			return warnings, fmt.Errorf("table %s: %w: %s", t.Name, ErrKey, strings.Join(missing, "; "))
		}
		if len(t.Key) == 0 {
			continue
		}
		covered := slices.ContainsFunc(uniques, func(u []string) bool {
			return len(u) > 0 && !slices.ContainsFunc(u, func(c string) bool { return !slices.Contains(t.Key, c) })
		})
		if !covered {
			warnings = append(warnings, fmt.Sprintf("table %s: no unique index on NOT NULL columns (or NULLS NOT DISTINCT) covers key (%s), so it may not identify one row", t.Name, strings.Join(t.Key, ", ")))
		}
	}
	return warnings, nil
}

// readKeyInfo returns the live columns of the table and the column lists of
// its unique indexes that enforce uniqueness: plain (no expression, no
// predicate), valid, listing only key columns (INCLUDE columns are ignored),
// and either primary, NULLS NOT DISTINCT or with every column NOT NULL.
func readKeyInfo(ctx context.Context, pg *pgconn.PgConn, name string) (cols []string, uniques [][]string, err error) {
	lit := quoteLiteral(name)
	rows, err := query(ctx, pg, "SELECT a.attname::text FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid "+
		"JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname || '.' || c.relname = "+lit+
		" AND c.relkind IN ('r', 'p') AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum")
	if err != nil {
		return nil, nil, fmt.Errorf("read columns of %s: %w", name, err)
	}
	if len(rows) == 0 {
		return nil, nil, fmt.Errorf("table %s: %w: does not exist", name, ErrKey)
	}
	for _, r := range rows {
		cols = append(cols, str(r[0]))
	}
	rows, err = query(ctx, pg, "SELECT string_agg(a.attname::text, chr(31) ORDER BY k.ord) FROM pg_index i "+
		"JOIN pg_class c ON c.oid = i.indrelid JOIN pg_namespace n ON n.oid = c.relnamespace "+
		"CROSS JOIN LATERAL unnest(i.indkey::int2[]) WITH ORDINALITY AS k(attnum, ord) "+
		"JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum "+
		"WHERE n.nspname || '.' || c.relname = "+lit+" AND i.indisunique AND i.indpred IS NULL AND i.indexprs IS NULL "+
		"AND i.indisvalid AND k.ord <= i.indnkeyatts "+
		"GROUP BY i.indexrelid, i.indisprimary, i.indnullsnotdistinct "+
		"HAVING i.indisprimary OR i.indnullsnotdistinct OR bool_and(a.attnotnull)")
	if err != nil {
		return nil, nil, fmt.Errorf("read unique indexes of %s: %w", name, err)
	}
	for _, r := range rows {
		uniques = append(uniques, strings.Split(str(r[0]), "\x1f"))
	}
	return cols, uniques, nil
}
