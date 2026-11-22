package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
)

// ConnectPlain dials a regular (non-replication) connection to url. It
// refuses Postgres older than 18 and pins the same output settings as Conn
// (sessionSettings), so text read with plain SQL decodes like the stream.
// Speak the simple or extended protocol on it; the caller closes it.
func ConnectPlain(ctx context.Context, url string) (*pgconn.PgConn, error) {
	cfg, err := pgconn.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse postgres url: %w", err)
	}
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = defaultConnectTimeout
	}
	pg, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	if err := checkVersion(pg.ParameterStatus("server_version")); err != nil {
		_ = pg.Close(context.WithoutCancel(ctx))
		return nil, err
	}
	if err := exec(ctx, pg, sessionSettings); err != nil {
		_ = pg.Close(context.WithoutCancel(ctx))
		return nil, fmt.Errorf("pin session settings: %w", err)
	}
	return pg, nil
}
