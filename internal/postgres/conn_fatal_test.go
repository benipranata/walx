package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/config"
)

// fakeServe answers the startup of one connection like a Postgres server with
// the given version, then, if drain is set, reads and discards everything
// until the client closes; otherwise it never reads again.
func fakeServe(conn net.Conn, version string, drain bool, hold <-chan struct{}) {
	defer conn.Close()
	be := pgproto3.NewBackend(conn, conn)
	if _, err := be.ReceiveStartupMessage(); err != nil {
		return
	}
	be.Send(&pgproto3.AuthenticationOk{})
	be.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: version})
	be.Send(&pgproto3.BackendKeyData{ProcessID: 1, SecretKey: []byte{1, 2, 3, 4}})
	be.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if be.Flush() != nil {
		return
	}
	if drain {
		_, _ = io.Copy(io.Discard, conn)
		return
	}
	<-hold
}

func TestIsFatalConnect(t *testing.T) {
	_, parseErr := replicationConfig("postgres://[bad")
	require.Error(t, parseErr)
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"unsupported version", fmt.Errorf("x: %w", checkVersion("17.5")), true},
		{"invalid password", &pgconn.PgError{Code: "28P01"}, true},
		{"invalid authorization", &pgconn.PgError{Code: "28000"}, true},
		{"invalid catalog name", &pgconn.PgError{Code: "3D000"}, true},
		{"wrapped pg error", fmt.Errorf("connect: %w", &pgconn.PgError{Code: "28P01"}), true},
		{"url parse error", parseErr, true},
		{"cannot connect now", &pgconn.PgError{Code: "57P03"}, false},
		{"too many connections", &pgconn.PgError{Code: "53300"}, false},
		{"network error", &net.OpError{Op: "dial", Err: errors.New("refused")}, false},
		{"plain error", errors.New("boom"), false},
		{"nil", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, isFatalConnect(tt.err))
		})
	}
	require.ErrorIs(t, checkVersion("17.5"), ErrUnsupportedVersion)
}

func TestReconnectFatal(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		wantTry int
	}{
		{"auth error stops at once", fmt.Errorf("connect: %w", &pgconn.PgError{Code: "28P01"}), 1},
		{"unsupported version stops at once", ErrUnsupportedVersion, 1},
		{"retryable keeps going", &pgconn.PgError{Code: "57P03"}, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tries, resets := 0, 0
			c := New(config.PostgresConfig{URL: "x"},
				config.RetryConfig{Initial: time.Millisecond, Max: time.Millisecond}, func() { resets++ })
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c.connect = func(context.Context) (pglogrepl.IdentifySystemResult, error) {
				tries++
				if tries >= 2 {
					cancel()
				}
				return pglogrepl.IdentifySystemResult{}, tt.err
			}
			_, err := c.Reconnect(ctx)
			require.Error(t, err)
			require.ErrorIs(t, err, tt.err)
			require.Equal(t, tt.wantTry, tries)
			require.Equal(t, 1, resets)
		})
	}
}

func TestReplicationConfigConnectTimeout(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want time.Duration
	}{
		{"default", "postgres://u@localhost/db", 10 * time.Second},
		{"explicit", "postgres://u@localhost/db?connect_timeout=3", 3 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := replicationConfig(tt.url)
			require.NoError(t, err)
			require.Equal(t, tt.want, cfg.ConnectTimeout)
		})
	}
}

func TestConnectTimeouts(t *testing.T) {
	tests := []struct {
		name  string
		serve func(net.Conn)
	}{
		{"server never answers the startup", func(c net.Conn) { defer c.Close(); _, _ = io.Copy(io.Discard, c) }},
		{"server never answers identify system", func(c net.Conn) { fakeServe(c, "18.0", true, nil) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			var wg sync.WaitGroup
			defer wg.Wait()
			defer ln.Close()
			go func() {
				for {
					conn, err := ln.Accept()
					if err != nil {
						return
					}
					wg.Add(1)
					go func() { defer wg.Done(); tt.serve(conn) }()
				}
			}()
			c := New(config.PostgresConfig{URL: fmt.Sprintf("postgres://u@%s/db?sslmode=disable&connect_timeout=1", ln.Addr())}, config.RetryConfig{}, nil)
			c.identifyTimeout = 200 * time.Millisecond
			start := time.Now()
			_, err = c.Connect(context.Background())
			require.Error(t, err)
			require.True(t, isTimeout(err), "got %v", err)
			require.Less(t, time.Since(start), 2500*time.Millisecond)
			require.Nil(t, c.PgConn())
		})
	}
}

func TestPgConnSendStatusDeadline(t *testing.T) {
	tests := []struct {
		name    string
		timeout time.Duration
	}{
		{"context deadline", 150 * time.Millisecond},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, server := net.Pipe()
			hold := make(chan struct{})
			done := make(chan struct{})
			go func() { defer close(done); fakeServe(server, "18.0", false, hold) }()
			defer func() { close(hold); <-done }()
			cfg, err := pgconn.ParseConfig("postgres://u@h/db?sslmode=disable")
			require.NoError(t, err)
			cfg.DialFunc = func(context.Context, string, string) (net.Conn, error) { return client, nil }
			cfg.LookupFunc = func(_ context.Context, host string) ([]string, error) { return []string{"127.0.0.1"}, nil }
			pg, err := pgconn.ConnectConfig(context.Background(), cfg)
			require.NoError(t, err)
			defer pg.Close(context.Background())
			// Closing the pipe first keeps Close from blocking on the Terminate write.
			defer func() { _ = client.Close() }()

			// net.Pipe is unbuffered and fakeServe never reads after startup,
			// so the write blocks until the deadline set from ctx.
			ctx, cancel := context.WithTimeout(context.Background(), tt.timeout)
			defer cancel()
			start := time.Now()
			err = pgConn{pg}.SendStatus(ctx, pglogrepl.StandbyStatusUpdate{WALWritePosition: 1, WALFlushPosition: 1, WALApplyPosition: 1})
			require.Error(t, err)
			require.ErrorIs(t, err, os.ErrDeadlineExceeded)
			require.Less(t, time.Since(start), 2*time.Second)
		})
	}
}

func TestReconnectBadURL(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{"unparseable url", "postgres://[bad"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resets := 0
			c := New(config.PostgresConfig{URL: tt.url}, config.RetryConfig{}, func() { resets++ })
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := c.Reconnect(ctx)
			require.Error(t, err)
			require.NoError(t, ctx.Err())
			require.True(t, isFatalConnect(err))
			require.Equal(t, 1, resets)
		})
	}
}
