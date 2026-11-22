package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/benipranata/walx/internal/config"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestReplicationConfig(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		wantErr bool
	}{
		{name: "url form", url: "postgres://u:p@localhost:5432/db"},
		{name: "url with query", url: "postgres://u:p@localhost/db?sslmode=disable"},
		{name: "keyword form", url: "host=localhost user=u dbname=db"},
		{name: "invalid", url: "postgres://[bad", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := replicationConfig(tt.url)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, "database", cfg.RuntimeParams["replication"])
		})
	}
}

func TestCheckVersion(t *testing.T) {
	tests := []struct {
		name    string
		version string
		wantErr bool
	}{
		{name: "18.0", version: "18.0"},
		{name: "18.2 with distro suffix", version: "18.2 (Debian 18.2-1.pgdg120+1)"},
		{name: "18 beta", version: "18beta1"},
		{name: "19", version: "19.1"},
		{name: "17", version: "17.5", wantErr: true},
		{name: "9.6", version: "9.6.24", wantErr: true},
		{name: "empty", version: "", wantErr: true},
		{name: "garbage", version: "abc", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkVersion(tt.version)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestBackoff(t *testing.T) {
	tests := []struct {
		name         string
		initial, max time.Duration
		attempt      int
		want         time.Duration
	}{
		{name: "first", initial: 100 * time.Millisecond, max: time.Second, attempt: 0, want: 100 * time.Millisecond},
		{name: "doubles", initial: 100 * time.Millisecond, max: time.Second, attempt: 2, want: 400 * time.Millisecond},
		{name: "capped", initial: 100 * time.Millisecond, max: time.Second, attempt: 10, want: time.Second},
		{name: "huge attempt no overflow", initial: 100 * time.Millisecond, max: time.Second, attempt: 500, want: time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, backoff(tt.initial, tt.max, tt.attempt))
		})
	}
}

func TestNewDefaults(t *testing.T) {
	tests := []struct {
		name        string
		retry       config.RetryConfig
		wantInitial time.Duration
		wantMax     time.Duration
	}{
		{name: "from config", retry: config.RetryConfig{Initial: time.Second, Max: time.Minute}, wantInitial: time.Second, wantMax: time.Minute},
		{name: "zero falls back", retry: config.RetryConfig{}, wantInitial: 100 * time.Millisecond, wantMax: 30 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := New(config.PostgresConfig{URL: "x"}, tt.retry, nil)
			require.Equal(t, tt.wantInitial, c.initial)
			require.Equal(t, tt.wantMax, c.max)
		})
	}
}

func TestConnectErrors(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{name: "bad url", url: "postgres://[bad"},
		{name: "unreachable", url: "postgres://u:p@127.0.0.1:1/db?connect_timeout=1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := New(config.PostgresConfig{URL: tt.url}, config.RetryConfig{}, nil)
			_, err := c.Connect(context.Background())
			require.Error(t, err)
			require.Nil(t, c.PgConn())
			require.NoError(t, c.Close(context.Background()))
		})
	}
}

func TestReconnectCancel(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{name: "unreachable", url: "postgres://u:p@127.0.0.1:1/db?connect_timeout=1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resets := 0
			c := New(config.PostgresConfig{URL: tt.url},
				config.RetryConfig{Initial: 5 * time.Millisecond, Max: 10 * time.Millisecond},
				func() { resets++ })
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			_, err := c.Reconnect(ctx)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Equal(t, 1, resets)
		})
	}
}

func TestShouldLogAttempt(t *testing.T) {
	tests := []struct {
		attempt int
		want    bool
	}{
		{1, true}, {2, true}, {5, true},
		{6, false}, {9, false}, {10, true},
		{15, false}, {20, true}, {25, false}, {100, true}, {105, false},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprint(tt.attempt), func(t *testing.T) {
			require.Equal(t, tt.want, shouldLogAttempt(tt.attempt))
		})
	}
}

func TestReconnectLogsAttempts(t *testing.T) {
	tests := []struct {
		name     string
		failures int
		nilLog   bool
		want     []string
		notWant  []string
	}{
		{name: "first five logged, sixth silent", failures: 6,
			want:    []string{"attempt=1", "attempt=5", "boom"},
			notWant: []string{"attempt=6"}},
		{name: "every tenth after five", failures: 11,
			want: []string{"attempt=10"}, notWant: []string{"attempt=9", "attempt=11"}},
		{name: "nil logger", failures: 2, nilLog: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			c := New(config.PostgresConfig{URL: "x"},
				config.RetryConfig{Initial: time.Microsecond, Max: time.Microsecond}, nil)
			if !tt.nilLog {
				c.SetLogger(slog.New(slog.NewTextHandler(&buf, nil)))
			}
			tries := 0
			c.connect = func(context.Context) (pglogrepl.IdentifySystemResult, error) {
				tries++
				if tries > tt.failures {
					return pglogrepl.IdentifySystemResult{}, nil
				}
				return pglogrepl.IdentifySystemResult{}, errors.New("boom")
			}
			_, err := c.Reconnect(context.Background())
			require.NoError(t, err)
			for _, w := range tt.want {
				require.Contains(t, buf.String(), w)
			}
			for _, w := range tt.notWant {
				require.NotContains(t, buf.String(), w)
			}
		})
	}
}
