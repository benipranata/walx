package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"unicode"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/benipranata/walx/internal/config"
)

// tableFilter is the row filter and column list of one published table.
type tableFilter struct {
	Where   string   // row filter expression, empty for none
	Columns []string // column list, sorted; empty for all columns
}

func (f tableFilter) isZero() bool { return f.Where == "" && len(f.Columns) == 0 }

func newTableFilter(t config.TableConfig) tableFilter {
	f := tableFilter{Where: strings.TrimSpace(t.PublicationWhere)}
	if len(t.PublicationColumns) > 0 {
		f.Columns = slices.Sorted(slices.Values(t.PublicationColumns))
	}
	return f
}

// sql returns the filter as it follows a table name in CREATE PUBLICATION and
// ALTER PUBLICATION ... SET TABLE.
func (f tableFilter) sql() string {
	var b strings.Builder
	if len(f.Columns) > 0 {
		q := make([]string, len(f.Columns))
		for i, c := range f.Columns {
			q[i] = pgx.Identifier{c}.Sanitize()
		}
		b.WriteString(" (" + strings.Join(q, ", ") + ")")
	}
	if f.Where != "" {
		b.WriteString(" WHERE (" + f.Where + ")")
	}
	return b.String()
}

// filterProblems compares the live filter of table with the wanted one.
func filterProblems(table string, want, got tableFilter) (problems []string) {
	if normExpr(want.Where) != normExpr(got.Where) {
		problems = append(problems, fmt.Sprintf("row filter on %s is %s, want %s", table, quoteOrNone(got.Where), quoteOrNone(want.Where)))
	}
	if !slices.Equal(want.Columns, got.Columns) {
		problems = append(problems, fmt.Sprintf("column list on %s is %s, want %s", table, columnsOrAll(got.Columns), columnsOrAll(want.Columns)))
	}
	return problems
}

func quoteOrNone(s string) string {
	if s == "" {
		return "none"
	}
	return `"` + s + `"`
}

func columnsOrAll(cols []string) string {
	if len(cols) == 0 {
		return "all columns"
	}
	return "(" + strings.Join(cols, ", ") + ")"
}

// tableList renders the tables with their filters, for CREATE and SET TABLE.
func (s pubSpec) tableList() string {
	items := make([]string, len(s.Tables))
	for i, n := range s.Tables {
		items[i] = quoteTables([]string{n}) + s.Filters[n].sql()
	}
	return strings.Join(items, ", ")
}

// parseFilters reads the rows of pubFilteredQuery.
func parseFilters(rows [][][]byte) (map[string]tableFilter, error) {
	var out map[string]tableFilter
	for _, r := range rows {
		if len(r) != 3 {
			return nil, fmt.Errorf("expected 3 columns, got %d", len(r))
		}
		f := tableFilter{Where: str(r[1])}
		if cols := str(r[2]); cols != "" {
			f.Columns = strings.Split(cols, "\x1f")
		}
		if out == nil {
			out = map[string]tableFilter{}
		}
		out[str(r[0])] = f
	}
	return out, nil
}

// normExpr reduces an expression to a form that survives Postgres deparsing
// of redundant parentheses, spacing and keyword case; text in single or
// double quotes is kept as is.
func normExpr(s string) string {
	var b strings.Builder
	var quote rune
	for _, r := range s {
		switch {
		case quote != 0:
			b.WriteRune(r)
			if r == quote {
				quote = 0
			}
		case r == '\'' || r == '"':
			quote = r
			b.WriteRune(r)
		case unicode.IsSpace(r):
		default:
			b.WriteRune(unicode.ToLower(r))
		}
	}
	out := b.String()
	for wrapped(out) {
		out = out[1 : len(out)-1]
	}
	return out
}

// wrapped reports whether the first parenthesis closes at the very end.
func wrapped(s string) bool {
	if len(s) < 2 || s[0] != '(' || s[len(s)-1] != ')' {
		return false
	}
	depth := 0
	var quote byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case quote != 0:
			if c == quote {
				quote = 0
			}
		case c == '\'' || c == '"':
			quote = c
		case c == '(':
			depth++
		case c == ')':
			depth--
			if depth == 0 {
				return i == len(s)-1
			}
		}
	}
	return false
}

// canonicalizeFilters replaces the wanted row filters that differ in text
// from the live ones by the form Postgres deparses them to, so that
// `status = 'x'` is not drift against `(status = 'x'::text)`. It asks
// Postgres through a throwaway publication in a rolled back transaction; when
// that is not allowed the text stays and the comparison may report drift.
func (s pubSpec) canonicalizeFilters(ctx context.Context, pg *pgconn.PgConn, got pubState) pubSpec {
	out := s
	out.Filters = map[string]tableFilter{}
	for n, f := range s.Filters {
		if g, ok := got.Filters[n]; ok && f.Where != "" && normExpr(f.Where) != normExpr(g.Where) {
			if canon, err := probeRowFilter(ctx, pg, n, f.Where); err == nil {
				f.Where = canon
			}
		}
		out.Filters[n] = f
	}
	return out
}

func probeRowFilter(ctx context.Context, pg *pgconn.PgConn, table, expr string) (string, error) {
	var raw [4]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	results, err := pg.Exec(ctx, probeSQL("walx_probe_"+hex.EncodeToString(raw[:]), table, expr)).ReadAll()
	if err != nil {
		_ = exec(ctx, pg, "ROLLBACK")
		return "", err
	}
	for _, r := range results {
		if len(r.Rows) == 1 && len(r.Rows[0]) == 1 {
			return str(r.Rows[0][0]), nil
		}
	}
	return "", fmt.Errorf("no row filter returned for %s", table)
}

func probeSQL(name, table, expr string) string {
	id := pgx.Identifier{name}.Sanitize()
	return "BEGIN; CREATE PUBLICATION " + id + " FOR TABLE " + quoteTables([]string{table}) +
		" WHERE (" + expr + ") WITH (publish = 'insert'); " +
		"SELECT rowfilter FROM pg_publication_tables WHERE pubname = " + quoteLiteral(name) + "; ROLLBACK"
}
