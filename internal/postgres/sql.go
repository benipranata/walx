package postgres

import (
	"context"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// Postgres error codes walx reacts to.
const (
	codeInsufficientPrivilege = "42501"
	codeLockNotAvailable      = "55P03"
	codeUndefinedObject       = "42704"
	codeDuplicateObject       = "42710"
	codeObjectInUse           = "55006"
	codeNotInPrerequisite     = "55000"
)

// query runs one statement over the simple protocol, the only one a
// walsender accepts, and returns its rows. A NULL column is a nil slice.
func query(ctx context.Context, pg *pgconn.PgConn, sql string) ([][][]byte, error) {
	results, err := pg.Exec(ctx, sql).ReadAll()
	if err != nil {
		return nil, err
	}
	if len(results) != 1 {
		return nil, errors.New("expected one result set")
	}
	return results[0].Rows, nil
}

func exec(ctx context.Context, pg *pgconn.PgConn, sql string) error {
	_, err := pg.Exec(ctx, sql).ReadAll()
	return err
}

// quoteLiteral returns s as a string literal that is safe whatever
// standard_conforming_strings is.
func quoteLiteral(s string) string {
	s = strings.ReplaceAll(s, "\x00", "")
	s = strings.ReplaceAll(s, "'", "''")
	if strings.Contains(s, `\`) {
		return "E'" + strings.ReplaceAll(s, `\`, `\\`) + "'"
	}
	return "'" + s + "'"
}

func pgCode(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

func str(b []byte) string { return string(b) }

func isTrue(b []byte) bool { return string(b) == "t" }
