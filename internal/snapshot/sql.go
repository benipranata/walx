package snapshot

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

const cursorName = "walx_snapshot"

// tablePlan is what is read from one table: the columns to select, the
// columns to order by, the key columns for Event.Key and an optional WHERE.
type tablePlan struct {
	Schema, Table string
	Columns       []string
	Order         []string // cursor ORDER BY: the key, empty when the table has none
	Key           []string // Event.Key columns
	Where         string
}

func createSlotSQL(slot string) string {
	return "CREATE_REPLICATION_SLOT " + slot + " TEMPORARY LOGICAL pgoutput (SNAPSHOT 'export')"
}

// beginSQL opens the reading transaction on the exported snapshot. name comes
// from the server and is checked by validSnapshotName.
func beginSQL(snapshot string) string {
	return "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET TRANSACTION SNAPSHOT '" + snapshot + "'"
}

// declareSQL declares the cursor that reads one table. It is sent with the
// extended protocol, which allows one statement only, so a where cannot end
// the read-only transaction or add statements.
func declareSQL(p tablePlan) string {
	var b strings.Builder
	b.WriteString("DECLARE " + cursorName + " NO SCROLL CURSOR FOR SELECT ")
	b.WriteString(identList(p.Columns))
	b.WriteString(" FROM " + pgx.Identifier{p.Schema, p.Table}.Sanitize())
	if p.Where != "" {
		b.WriteString(" WHERE (" + p.Where + ")")
	}
	if len(p.Order) > 0 {
		b.WriteString(" ORDER BY " + identList(p.Order))
	}
	return b.String()
}

func fetchSQL(n int) string {
	return fmt.Sprintf("FETCH FORWARD %d FROM %s", n, cursorName)
}

func identList(cols []string) string {
	q := make([]string, len(cols))
	for i, c := range cols {
		q[i] = pgx.Identifier{c}.Sanitize()
	}
	return strings.Join(q, ", ")
}

// splitTable splits "schema.table".
func splitTable(name string) (schema, table string, err error) {
	schema, table, _ = strings.Cut(name, ".")
	if schema == "" || table == "" || strings.Count(name, ".") != 1 {
		return "", "", fmt.Errorf("table %q must be schema.table", name)
	}
	return schema, table, nil
}

// Catalog queries, run inside the snapshot so they see the schema the rows
// have. $1 is a qualified, quoted table name cast with ::regclass.
const (
	// columnsQuery lists the columns a publication would send: generated
	// columns are not replicated, so they are not read either.
	columnsQuery = `SELECT attname::text FROM pg_attribute
WHERE attrelid = $1::regclass AND attnum > 0 AND NOT attisdropped AND attgenerated = '' ORDER BY attnum`
	primaryKeyQuery = `SELECT a.attname::text FROM pg_index i
JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey::int2[])
WHERE i.indrelid = $1::regclass AND i.indisprimary ORDER BY array_position(i.indkey::int2[], a.attnum)`
	// tablesQuery lists user tables, skipping partitions (their root is read
	// and holds the rows) and system schemas.
	tablesQuery = `SELECT n.nspname::text, c.relname::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind IN ('r', 'p') AND NOT c.relispartition
AND n.nspname NOT IN ('pg_catalog', 'information_schema') AND n.nspname NOT LIKE 'pg\_toast%' AND n.nspname NOT LIKE 'pg\_temp\_%'
ORDER BY 1, 2`
)
