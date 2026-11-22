package snapshot

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/benipranata/walx/internal/config"
	"github.com/benipranata/walx/internal/postgres"
)

// reader reads tables on a plain connection inside one REPEATABLE READ
// transaction that imported the exported snapshot, so every table is read as
// of the slot's consistent point. It is not safe for concurrent use.
type reader struct {
	pg    *pgconn.PgConn
	chunk int
}

// openReader connects and imports snapshot. The transaction is read-only and
// stays open until close.
func openReader(ctx context.Context, url, snapshot string, chunk int) (*reader, error) {
	pg, err := postgres.ConnectPlain(ctx, url)
	if err != nil {
		return nil, err
	}
	// Set before BEGIN: SET TRANSACTION SNAPSHOT must be the first statement of the transaction.
	if _, err := pg.Exec(ctx, "SET idle_in_transaction_session_timeout = 0").ReadAll(); err != nil {
		_ = pg.Close(context.WithoutCancel(ctx))
		return nil, fmt.Errorf("pin session settings: %w", err)
	}
	if _, err := pg.Exec(ctx, beginSQL(snapshot)).ReadAll(); err != nil {
		_ = pg.Close(context.WithoutCancel(ctx))
		return nil, fmt.Errorf("import snapshot: %w", err)
	}
	return &reader{pg: pg, chunk: chunk}, nil
}

// close ends the transaction and the connection.
func (r *reader) close(ctx context.Context) {
	_ = r.pg.Close(context.WithoutCancel(ctx))
}

// tables lists the user tables, "schema.table" sorted.
func (r *reader) tables(ctx context.Context) ([]string, error) {
	res, err := r.pg.Exec(ctx, tablesQuery).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("list tables: %w", err)
	}
	var out []string
	for _, row := range res[0].Rows {
		out = append(out, string(row[0])+"."+string(row[1]))
	}
	return out, nil
}

// plan resolves the columns, order and key of one table. The key is the
// configured key, else the primary key; the rows are read in key order. A
// publication column list limits the columns, as it does on the stream.
func (r *reader) plan(ctx context.Context, name, where string, tc *config.TableConfig) (tablePlan, error) {
	schema, table, err := splitTable(name)
	if err != nil {
		return tablePlan{}, err
	}
	p := tablePlan{Schema: schema, Table: table, Where: where}
	if tc != nil && tc.PublicationWhere != "" {
		p.Where = "(" + tc.PublicationWhere + ")"
		if where != "" {
			p.Where += " AND (" + where + ")"
		}
	}
	qualified := pgx.Identifier{schema, table}.Sanitize()
	if p.Columns, err = r.column(ctx, columnsQuery, qualified); err != nil {
		return tablePlan{}, fmt.Errorf("table %s: %w", name, err)
	}
	if tc != nil && len(tc.PublicationColumns) > 0 {
		p.Columns = tc.PublicationColumns
	}
	if tc != nil && len(tc.Key) > 0 {
		p.Key = tc.Key
	} else if p.Key, err = r.column(ctx, primaryKeyQuery, qualified); err != nil {
		return tablePlan{}, fmt.Errorf("table %s: %w", name, err)
	}
	for _, c := range p.Key {
		if !slices.Contains(p.Columns, c) {
			return tablePlan{}, fmt.Errorf("table %s: key column %q is not a readable column", name, c)
		}
	}
	p.Order = p.Key
	return p, nil
}

func (r *reader) column(ctx context.Context, sql, table string) ([]string, error) {
	res := r.pg.ExecParams(ctx, sql, [][]byte{[]byte(table)}, nil, nil, nil).Read()
	if err := res.Err; err != nil {
		var pe *pgconn.PgError
		if errors.As(err, &pe) && pe.Code == "42P01" {
			return nil, errors.New("table does not exist")
		}
		return nil, err
	}
	var out []string
	for _, row := range res.Rows {
		out = append(out, string(row[0]))
	}
	return out, nil
}

// read reads the plan's table in chunks and calls fn for each, in order, until
// the table is exhausted or fn fails.
func (r *reader) read(ctx context.Context, p tablePlan, fn func(fields []pgconn.FieldDescription, rows [][][]byte) error) error {
	if err := r.pg.ExecParams(ctx, declareSQL(p), nil, nil, nil, nil).Read().Err; err != nil {
		return fmt.Errorf("read %s.%s: %w", p.Schema, p.Table, err)
	}
	for {
		res, err := r.pg.Exec(ctx, fetchSQL(r.chunk)).ReadAll()
		if err != nil {
			return fmt.Errorf("read %s.%s: %w", p.Schema, p.Table, err)
		}
		if len(res[0].Rows) == 0 {
			break
		}
		if err := fn(res[0].FieldDescriptions, res[0].Rows); err != nil {
			return err
		}
	}
	_, err := r.pg.Exec(ctx, "CLOSE "+cursorName).ReadAll()
	return err
}
