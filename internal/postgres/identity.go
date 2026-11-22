package postgres

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/benipranata/walx/internal/config"
	"github.com/benipranata/walx/internal/event"
)

// ErrReplicaIdentity means a published table's REPLICA IDENTITY does not fit
// what the config needs from it.
var ErrReplicaIdentity = errors.New("replica identity")

// replicaIdentityLockTimeout bounds how long the automatic ALTER TABLE waits
// for its ACCESS EXCLUSIVE lock; a variable so tests can shorten it.
var replicaIdentityLockTimeout = "10s"

// IdentityReport is the outcome of EnsureReplicaIdentity.
type IdentityReport struct {
	Altered  []string // tables set to REPLICA IDENTITY FULL, schema.table
	Warnings []string // problems on tables the config does not list
}

// IdentityPolicy holds what the config needs from the replica identity of
// each table.
type IdentityPolicy struct {
	rules  map[string]identityRule
	other  identityRule // tables the config does not list (FOR ALL TABLES)
	listed bool
}

type identityRule struct {
	needFull  string   // why Before is needed, empty when it is not
	needKey   bool     // updates or deletes are published
	deleteKey []string // custom key of a delete-enabled table
}

// tableIdentity is the replica identity of one table: the flag and the
// columns it identifies a row by (empty for f, n and a keyless d).
type tableIdentity struct {
	flag byte
	cols []string
}

type identityVerdict struct {
	needFull string // fixable by REPLICA IDENTITY FULL
	broken   string // not fixed by altering: no key and no Before needed
}

// NewIdentityPolicy derives the policy from the tables of db. Without a
// tables list every published table only needs a key, and only warns.
func NewIdentityPolicy(db config.DatabaseConfig) *IdentityPolicy {
	p := &IdentityPolicy{rules: map[string]identityRule{}, listed: len(db.Tables) > 0}
	for _, t := range db.Tables {
		ops := t.Ops
		if ops == nil {
			ops = []event.Op{event.OpInsert, event.OpUpdate}
		}
		r := identityRule{needKey: slices.Contains(ops, event.OpUpdate) || slices.Contains(ops, event.OpDelete)}
		deletes := slices.Contains(ops, event.OpDelete)
		switch {
		case t.FSMColumn != "":
			r.needFull = fmt.Sprintf("fsm_column %q needs Before", t.FSMColumn)
		case len(t.OnChange) > 0:
			r.needFull = "on_change needs Before to compare columns on updates that keep the key"
		case deletes && len(t.Where) > 0:
			r.needFull = "where on deletes needs Before"
		}
		if deletes {
			r.deleteKey = t.Key
		}
		p.rules[t.Name] = r
	}
	if !p.listed {
		ops := PublishOps(nil)
		p.other = identityRule{needKey: slices.Contains(ops, event.OpUpdate) || slices.Contains(ops, event.OpDelete)}
	}
	return p
}

// CheckRelation checks the identity a Relation message announces. An error
// (wrapping ErrReplicaIdentity) means a table the config lists is unusable;
// warn describes a problem on a table it does not list.
func (p *IdentityPolicy) CheckRelation(rel *pglogrepl.RelationMessage) (warn string, err error) {
	name := rel.Namespace + "." + rel.RelationName
	rule, listed := p.rules[name]
	if !listed {
		rule = p.other
	}
	ti := tableIdentity{flag: rel.ReplicaIdentity}
	for _, c := range rel.Columns {
		if c.Flags&1 != 0 {
			ti.cols = append(ti.cols, c.Name)
		}
	}
	v := rule.assess(ti)
	switch {
	case v.needFull != "":
		return "", fmt.Errorf("table %s: %w: %s; run %s or set it by hand", name, ErrReplicaIdentity, v.needFull, alterStatements(name))
	case v.broken != "" && listed:
		return "", fmt.Errorf("table %s: %w: %s", name, ErrReplicaIdentity, v.broken)
	case v.broken != "":
		return fmt.Sprintf("table %s: %s", name, v.broken), nil
	}
	return "", nil
}

func (r identityRule) assess(ti tableIdentity) identityVerdict {
	if ti.flag == 'f' {
		return identityVerdict{}
	}
	reason := r.needFull
	if reason == "" && len(r.deleteKey) > 0 {
		var outside []string
		for _, k := range r.deleteKey {
			if !slices.Contains(ti.cols, k) {
				outside = append(outside, k)
			}
		}
		if len(outside) > 0 {
			reason = fmt.Sprintf("key columns %s are outside the replica identity (%s) and deletes only carry that", strings.Join(outside, ", "), identityName(ti.flag))
		}
	}
	if reason != "" {
		return identityVerdict{needFull: reason}
	}
	if r.needKey && (ti.flag != 'd' && ti.flag != 'i' || len(ti.cols) == 0) {
		return identityVerdict{broken: fmt.Sprintf("UPDATE and DELETE fail on the source: replica identity is %s without a primary key or identity index", identityName(ti.flag))}
	}
	return identityVerdict{}
}

// EnsureReplicaIdentity checks the replica identity of every published table
// of db. Tables that need Before (fsm_column, on_change, where on deletes), or whose custom key a delete would not
// carry, get REPLICA IDENTITY FULL when postgres.auto_replica_identity is
// set (only those not already f, in one transaction); otherwise the ALTER
// statements are reported in the error. A listed table with no key and no
// need for Before cannot be fixed by altering: it is reported and nothing is
// altered. Problems on tables the config does not list are returned as
// warnings. Errors wrap ErrReplicaIdentity, except query failures.
func EnsureReplicaIdentity(ctx context.Context, pg *pgconn.PgConn, db config.DatabaseConfig) (IdentityReport, error) {
	var rep IdentityReport
	p := NewIdentityPolicy(db)
	names, err := identityTables(ctx, pg, db, p)
	if err != nil {
		return rep, err
	}
	have, err := readIdentities(ctx, pg, names)
	if err != nil {
		return rep, err
	}
	var fixes, broken []string
	for _, name := range names {
		ti, ok := have[name]
		if !ok {
			return rep, fmt.Errorf("table %s: %w: does not exist", name, ErrReplicaIdentity)
		}
		rule, listed := p.rules[name]
		if !listed {
			rule = p.other
		}
		v := rule.assess(ti)
		switch {
		case v.needFull != "":
			fixes = append(fixes, name)
			if !db.Postgres.AutoReplicaIdentity {
				broken = append(broken, fmt.Sprintf("%s: %s", name, v.needFull))
			}
		case v.broken != "" && listed:
			broken = append(broken, fmt.Sprintf("%s: %s", name, v.broken))
		case v.broken != "":
			rep.Warnings = append(rep.Warnings, fmt.Sprintf("table %s: %s", name, v.broken))
		}
	}
	if len(broken) > 0 {
		msg := strings.Join(broken, "; ")
		if len(fixes) > 0 && !db.Postgres.AutoReplicaIdentity {
			msg += fmt.Sprintf(" (postgres.auto_replica_identity is false; run: %s)", alterStatements(fixes...))
		}
		return rep, fmt.Errorf("%w: %s", ErrReplicaIdentity, msg)
	}
	if len(fixes) == 0 {
		return rep, nil
	}
	if err := exec(ctx, pg, alterFullSQL(fixes...)); err != nil {
		switch pgCode(err) {
		case codeInsufficientPrivilege:
			return rep, fmt.Errorf("set REPLICA IDENTITY FULL on %s needs ownership: grant ownership of the tables to the walx role, "+
				"or set postgres.auto_replica_identity: false and run by hand: %s: %w: %w", strings.Join(fixes, ", "), alterStatements(fixes...), ErrReplicaIdentity, err)
		case codeLockNotAvailable:
			return rep, fmt.Errorf("set REPLICA IDENTITY FULL on %s: another transaction holds a lock on the table (waited %s): "+
				"retry later, or set postgres.auto_replica_identity: false and run by hand: %s: %w: %w",
				strings.Join(fixes, ", "), replicaIdentityLockTimeout, alterStatements(fixes...), ErrReplicaIdentity, err)
		}
		return rep, fmt.Errorf("set REPLICA IDENTITY FULL on %s: %w", strings.Join(fixes, ", "), err)
	}
	rep.Altered = fixes
	return rep, nil
}

// identityTables returns the tables to check, sorted: the listed ones, or
// every table of the publication when the config lists none.
func identityTables(ctx context.Context, pg *pgconn.PgConn, db config.DatabaseConfig, p *IdentityPolicy) ([]string, error) {
	if p.listed {
		return newPubSpec(db).Tables, nil
	}
	rows, err := query(ctx, pg, pubTablesQuery(db.Postgres.Publication))
	if err != nil {
		return nil, fmt.Errorf("read tables of publication %q: %w", db.Postgres.Publication, err)
	}
	names := make([]string, len(rows))
	for i, r := range rows {
		names[i] = str(r[0])
	}
	return names, nil
}

func readIdentities(ctx context.Context, pg *pgconn.PgConn, names []string) (map[string]tableIdentity, error) {
	out := map[string]tableIdentity{}
	if len(names) == 0 {
		return out, nil
	}
	lits := make([]string, len(names))
	for i, n := range names {
		lits[i] = quoteLiteral(n)
	}
	rows, err := query(ctx, pg, identityQuery(lits))
	if err != nil {
		return nil, fmt.Errorf("read replica identity: %w", err)
	}
	for _, r := range rows {
		if len(r) != 3 {
			return nil, fmt.Errorf("read replica identity: expected 3 columns, got %d", len(r))
		}
		ti := tableIdentity{flag: str(r[1])[0]}
		if cols := str(r[2]); cols != "" {
			ti.cols = strings.Split(cols, "\x1f")
		}
		out[str(r[0])] = ti
	}
	return out, nil
}

// identityQuery returns each table with its relreplident flag and the
// columns of the index that flag points at, joined by unit separators.
func identityQuery(literals []string) string {
	return "SELECT n.nspname || '.' || c.relname, c.relreplident::text, coalesce((" +
		"SELECT string_agg(a.attname::text, chr(31) ORDER BY a.attnum) FROM pg_index i " +
		"JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) " +
		"WHERE i.indrelid = c.oid AND ((c.relreplident = 'd' AND i.indisprimary) OR (c.relreplident = 'i' AND i.indisreplident))" +
		"), '') FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace " +
		"WHERE c.relkind IN ('r', 'p') AND n.nspname || '.' || c.relname IN (" + strings.Join(literals, ", ") + ")"
}

// alterFullSQL is one simple-protocol query, so the statements run in one
// implicit transaction and a failing one leaves every table unchanged. SET
// LOCAL ends with that transaction, so the timeout never reaches later
// queries on the connection.
func alterFullSQL(names ...string) string {
	return "SET LOCAL lock_timeout = " + quoteLiteral(replicaIdentityLockTimeout) + "; " + alterStatements(names...)
}

// alterStatements is the ALTER TABLE part of alterFullSQL, for messages a
// person runs by hand.
func alterStatements(names ...string) string {
	stmts := make([]string, len(names))
	for i, n := range names {
		stmts[i] = "ALTER TABLE " + quoteTables([]string{n}) + " REPLICA IDENTITY FULL"
	}
	return strings.Join(stmts, "; ")
}

func identityName(flag byte) string {
	switch flag {
	case 'd':
		return "default"
	case 'n':
		return "nothing"
	case 'i':
		return "index"
	case 'f':
		return "full"
	}
	return string(flag)
}
