package postgres

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/benipranata/walx/internal/config"
	"github.com/benipranata/walx/internal/event"
)

var (
	// ErrPublicationMissing means the publication does not exist and may not be created.
	ErrPublicationMissing = errors.New("publication does not exist")
	// ErrPublicationDrift means the publication differs from the config.
	ErrPublicationDrift = errors.New("publication differs from config")
)

// publishable lists the publication ops in the order Postgres prints them.
var publishable = []event.Op{event.OpInsert, event.OpUpdate, event.OpDelete, event.OpTruncate}

type pubSpec struct {
	Name      string
	AllTables bool
	Tables    []string // sorted, schema.table
	Ops       []event.Op
}

type pubState struct {
	AllTables                        bool
	Insert, Update, Delete, Truncate bool
	ViaRoot                          bool
	Tables                           []string
	Filtered                         []string // tables with a row filter or column list
}

// PublishOps returns the union of ops across tables in Postgres order. It is
// insert and update when no table is listed or none opts in to more.
func PublishOps(tables []config.TableConfig) []event.Op {
	want := map[event.Op]bool{}
	for _, t := range tables {
		ops := t.Ops
		if ops == nil {
			ops = []event.Op{event.OpInsert, event.OpUpdate}
		}
		for _, op := range ops {
			want[op] = true
		}
	}
	if len(tables) == 0 {
		want[event.OpInsert], want[event.OpUpdate] = true, true
	}
	var out []event.Op
	for _, op := range publishable {
		if want[op] {
			out = append(out, op)
		}
	}
	return out
}

// EnsurePublication creates the publication of db when it is missing and
// postgres.auto_create is set, and checks it against the config otherwise.
// It is FOR ALL TABLES when db lists no tables, else FOR TABLE with the
// listed ones, always with publish_via_partition_root and the union of table
// ops. On drift it fails with ErrPublicationDrift, unless postgres.auto_alter
// is set, in which case it runs ALTER PUBLICATION. Switching between FOR ALL
// TABLES and a table list cannot be altered and always fails.
func EnsurePublication(ctx context.Context, pg *pgconn.PgConn, db config.DatabaseConfig) error {
	spec := newPubSpec(db)
	rows, err := query(ctx, pg, pubQuery(spec.Name))
	if err != nil {
		return fmt.Errorf("read publication %q: %w", spec.Name, err)
	}
	if len(rows) == 0 {
		if !db.Postgres.AutoCreate {
			return fmt.Errorf("publication %q: %w (postgres.auto_create is false; run: %s)",
				spec.Name, ErrPublicationMissing, spec.createSQL())
		}
		if err := exec(ctx, pg, spec.createSQL()); err != nil {
			return spec.createError(err)
		}
		return nil
	}
	got, err := parsePub(rows[0])
	if err != nil {
		return fmt.Errorf("read publication %q: %w", spec.Name, err)
	}
	if !got.AllTables {
		trows, err := query(ctx, pg, pubTablesQuery(spec.Name))
		if err != nil {
			return fmt.Errorf("read tables of publication %q: %w", spec.Name, err)
		}
		for _, r := range trows {
			got.Tables = append(got.Tables, str(r[0]))
		}
		frows, err := query(ctx, pg, pubFilteredQuery(spec.Name))
		if err != nil {
			return fmt.Errorf("read filters of publication %q: %w", spec.Name, err)
		}
		for _, r := range frows {
			got.Filtered = append(got.Filtered, str(r[0]))
		}
	}
	problems, fixes, fixable := spec.drift(got)
	if len(problems) == 0 {
		return nil
	}
	if !db.Postgres.AutoAlter || !fixable {
		hint := "set postgres.auto_alter: true or fix it by hand"
		if !fixable {
			hint = "FOR ALL TABLES cannot be altered; drop the publication or change the config"
		}
		return fmt.Errorf("publication %q: %w: %s (%s)", spec.Name, ErrPublicationDrift, strings.Join(problems, "; "), hint)
	}
	// One simple-protocol query runs as one implicit transaction, so a failing
	// statement leaves the publication unchanged.
	if err := exec(ctx, pg, strings.Join(fixes, "; ")); err != nil {
		return fmt.Errorf("alter publication %q: %w", spec.Name, err)
	}
	return nil
}

func newPubSpec(db config.DatabaseConfig) pubSpec {
	spec := pubSpec{Name: db.Postgres.Publication, AllTables: len(db.Tables) == 0, Ops: PublishOps(db.Tables)}
	for _, t := range db.Tables {
		spec.Tables = append(spec.Tables, t.Name)
	}
	slices.Sort(spec.Tables)
	spec.Tables = slices.Compact(spec.Tables)
	return spec
}

func (s pubSpec) createSQL() string {
	target := "ALL TABLES"
	if !s.AllTables {
		target = "TABLE " + quoteTables(s.Tables)
	}
	return fmt.Sprintf("CREATE PUBLICATION %s FOR %s WITH (%s)", pgx.Identifier{s.Name}.Sanitize(), target, s.options())
}

func (s pubSpec) options() string {
	return "publish = " + quoteLiteral(opList(s.Ops)) + ", publish_via_partition_root = true"
}

func (s pubSpec) createError(err error) error {
	if pgCode(err) == codeInsufficientPrivilege {
		if s.AllTables {
			return fmt.Errorf("create publication %q FOR ALL TABLES needs superuser: create it by hand or list tables explicitly: %w", s.Name, err)
		}
		return fmt.Errorf("create publication %q needs CREATE on the database and ownership of every listed table: %w", s.Name, err)
	}
	return fmt.Errorf("create publication %q: %w", s.Name, err)
}

// drift compares the publication with the spec. fixable is false when only a
// drop and create can fix it.
func (s pubSpec) drift(got pubState) (problems, fixes []string, fixable bool) {
	switch {
	case s.AllTables && !got.AllTables:
		return []string{"is not FOR ALL TABLES, want FOR ALL TABLES"}, nil, false
	case !s.AllTables && got.AllTables:
		return []string{"is FOR ALL TABLES, want a table list"}, nil, false
	}
	setOptions := false
	if !slices.Equal(got.ops(), s.Ops) {
		problems = append(problems, fmt.Sprintf("publishes %s, want %s", opList(got.ops()), opList(s.Ops)))
		setOptions = true
	}
	if !got.ViaRoot {
		problems = append(problems, "publish_via_partition_root is off")
		setOptions = true
	}
	if setOptions {
		fixes = append(fixes, fmt.Sprintf("ALTER PUBLICATION %s SET (%s)", pgx.Identifier{s.Name}.Sanitize(), s.options()))
	}
	setTable := false
	if !s.AllTables && !slices.Equal(got.Tables, s.Tables) {
		problems = append(problems, fmt.Sprintf("tables are %s, want %s",
			strings.Join(got.Tables, ", "), strings.Join(s.Tables, ", ")))
		setTable = true
	}
	// A hand-added filter silently drops events, and SET TABLE resets it.
	if len(got.Filtered) > 0 {
		problems = append(problems, "row filter or column list on "+strings.Join(got.Filtered, ", "))
		setTable = true
	}
	if setTable {
		fixes = append(fixes, fmt.Sprintf("ALTER PUBLICATION %s SET TABLE %s", pgx.Identifier{s.Name}.Sanitize(), quoteTables(s.Tables)))
	}
	return problems, fixes, true
}

func (p pubState) ops() []event.Op {
	var out []event.Op
	for i, on := range []bool{p.Insert, p.Update, p.Delete, p.Truncate} {
		if on {
			out = append(out, publishable[i])
		}
	}
	return out
}

func pubQuery(name string) string {
	return "SELECT puballtables, pubinsert, pubupdate, pubdelete, pubtruncate, pubviaroot " +
		"FROM pg_publication WHERE pubname = " + quoteLiteral(name)
}

func pubTablesQuery(name string) string {
	return "SELECT schemaname || '.' || tablename FROM pg_publication_tables WHERE pubname = " +
		quoteLiteral(name) + " ORDER BY 1"
}

func pubFilteredQuery(name string) string {
	return "SELECT n.nspname || '.' || c.relname FROM pg_publication_rel r " +
		"JOIN pg_publication p ON p.oid = r.prpubid JOIN pg_class c ON c.oid = r.prrelid " +
		"JOIN pg_namespace n ON n.oid = c.relnamespace " +
		"WHERE p.pubname = " + quoteLiteral(name) + " AND (r.prqual IS NOT NULL OR r.prattrs IS NOT NULL) ORDER BY 1"
}

func parsePub(row [][]byte) (pubState, error) {
	if len(row) != 6 {
		return pubState{}, fmt.Errorf("expected 6 columns, got %d", len(row))
	}
	return pubState{
		AllTables: isTrue(row[0]), Insert: isTrue(row[1]), Update: isTrue(row[2]),
		Delete: isTrue(row[3]), Truncate: isTrue(row[4]), ViaRoot: isTrue(row[5]),
	}, nil
}

func opList(ops []event.Op) string {
	s := make([]string, len(ops))
	for i, op := range ops {
		s[i] = string(op)
	}
	return strings.Join(s, ", ")
}

func quoteTables(names []string) string {
	q := make([]string, len(names))
	for i, n := range names {
		schema, table, ok := strings.Cut(n, ".")
		if ok {
			q[i] = pgx.Identifier{schema, table}.Sanitize()
		} else {
			q[i] = pgx.Identifier{n}.Sanitize()
		}
	}
	return strings.Join(q, ", ")
}
