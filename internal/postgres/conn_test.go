package postgres

import (
	"context"
	"testing"
	"time"

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
		num     string
		wantErr bool
	}{
		{name: "18.0", num: "180000"},
		{name: "18.2", num: "180002"},
		{name: "19", num: "190000"},
		{name: "17", num: "170005", wantErr: true},
		{name: "empty", num: "", wantErr: true},
		{name: "garbage", num: "abc", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkVersion(tt.num)
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
		{name: "bad url", url: "postgres://[bad"},
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
