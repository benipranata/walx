package config_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/config"
	"github.com/benipranata/walx/internal/event"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name  string
		env   map[string]string
		doc   string
		check func(t *testing.T, cfg *config.Config)
	}{
		{
			name: "defaults",
			doc:  mk("", ""),
			check: func(t *testing.T, cfg *config.Config) {
				require.Equal(t, 30*time.Second, cfg.ShutdownTimeout)
				require.Len(t, cfg.Databases, 1)
				d := cfg.Databases[0]
				require.Equal(t, "a", d.Name)
				require.Equal(t, "walx_a", d.Postgres.Slot)
				require.Equal(t, "walx_a", d.Postgres.Publication)
				require.True(t, d.Postgres.AutoCreate)
				require.True(t, d.Postgres.AutoReplicaIdentity)
				require.False(t, d.Postgres.AutoAlter)
				require.Equal(t, config.ByteSize(256<<20), d.Postgres.MaxTxBytes)
				require.Equal(t, config.RetryConfig{
					Policy:  "forever",
					Initial: 100 * time.Millisecond,
					Max:     30 * time.Second,
				}, d.Retry)
				require.Equal(t, "stop", d.OnPermanentError)
			},
		},
		{
			name: "defaults per entry",
			doc: `
databases:
  - name: a
    postgres: {url: "postgres://u@h/a"}
    sink: {type: stdout}
  - name: b
    postgres: {url: "postgres://u@h/b", auto_create: false}
    sink: {type: stdout}
`,
			check: func(t *testing.T, cfg *config.Config) {
				require.True(t, cfg.Databases[0].Postgres.AutoCreate)
				require.False(t, cfg.Databases[1].Postgres.AutoCreate)
				require.Equal(t, "walx_b", cfg.Databases[1].Postgres.Slot)
			},
		},
		{
			name: "explicit values",
			doc: `
shutdown_timeout: 5s
databases:
  - name: a
    postgres:
      url: postgres://u@h/db
      slot: s
      publication: p
      auto_create: false
      auto_replica_identity: false
      auto_alter: true
      max_tx_bytes: 1GiB
    tables:
      - name: public.orders
        ops: [insert, delete]
        on_change: [status]
        fsm_column: status
        drop_columns: [raw]
        key: [id]
        reference_only: true
        where: {tenant: 1}
    sink:
      type: file
      path: ./a.jsonl
    retry:
      policy: max_attempts
      initial: 1s
      max: 1m
      max_attempts: 5
    on_permanent_error: skip
`,
			check: func(t *testing.T, cfg *config.Config) {
				require.Equal(t, 5*time.Second, cfg.ShutdownTimeout)
				d := cfg.Databases[0]
				require.Equal(t, "s", d.Postgres.Slot)
				require.Equal(t, "p", d.Postgres.Publication)
				require.False(t, d.Postgres.AutoCreate)
				require.False(t, d.Postgres.AutoReplicaIdentity)
				require.True(t, d.Postgres.AutoAlter)
				require.Equal(t, config.ByteSize(1<<30), d.Postgres.MaxTxBytes)
				require.Equal(t, []config.TableConfig{{
					Name:          "public.orders",
					Ops:           []event.Op{event.OpInsert, event.OpDelete},
					FSMColumn:     "status",
					DropColumns:   []string{"raw"},
					Where:         map[string]any{"tenant": 1},
					OnChange:      []string{"status"},
					Key:           []string{"id"},
					ReferenceOnly: true,
				}}, d.Tables)
				require.Equal(t, config.SinkConfig{Type: "file", Path: "./a.jsonl"}, d.Sink)
				require.Equal(t, config.RetryConfig{
					Policy: "max_attempts", Initial: time.Second, Max: time.Minute, MaxAttempts: 5,
				}, d.Retry)
				require.Equal(t, "skip", d.OnPermanentError)
			},
		},
		{
			name: "table ops default",
			doc:  mk("", "    tables:\n      - name: public.t\n"),
			check: func(t *testing.T, cfg *config.Config) {
				require.Equal(t, []event.Op{event.OpInsert, event.OpUpdate}, cfg.Databases[0].Tables[0].Ops)
			},
		},
		{
			name: "plan example",
			env: map[string]string{
				"ORDERS_DATABASE_URL":  "postgres://u@h/orders",
				"BILLING_DATABASE_URL": "postgres://u@h/billing",
			},
			doc: `
shutdown_timeout: 30s               # shared by all databases

databases:
  - name: orders_db                 # required, unique, becomes Event.Source
    postgres:
      url: ${ORDERS_DATABASE_URL}   # walx adds replication=database itself
      slot: walx_orders_db
      publication: walx_orders_db
      auto_create: true
      auto_replica_identity: true
      auto_alter: false
      max_tx_bytes: 256MiB
    tables:
      - name: public.orders
        ops: [insert, update]
        on_change: [status]
        fsm_column: status
      - name: public.audit_log
        ops: [insert, update, delete]
        drop_columns: [raw_payload]
    sink:
      type: file                    # stdout | file (nats later)
      path: ./orders.jsonl
    retry:
      policy: forever               # forever | max_attempts | stop
      initial: 100ms
      max: 30s
      max_attempts: 0               # 0 = unlimited; used by policy max_attempts
    on_permanent_error: stop        # stop | skip

  - name: billing_db
    postgres:
      url: ${BILLING_DATABASE_URL}
    tables:
      - name: public.invoices
    sink:
      type: stdout
`,
			check: func(t *testing.T, cfg *config.Config) {
				require.Len(t, cfg.Databases, 2)
				require.Equal(t, "postgres://u@h/orders", cfg.Databases[0].Postgres.URL)
				require.Equal(t, "walx_billing_db", cfg.Databases[1].Postgres.Slot)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for k, v := range tt.env {
				t.Setenv(k, v)
			}
			tt.check(t, mustParse(t, tt.doc))
		})
	}
}

func TestParseErrors(t *testing.T) {
	tests := []struct {
		name, doc, want string
	}{
		{"empty document", "", ""},
		{"only a comment", "# only a comment\n", ""},
		{"top level is a list", "- a\n- b\n", ""},
		{"syntax error", "databases: [\n", ""},
		{"type error names the line", mk("", "    retry:\n      initial: soon\n"), "line "},
		{"type error on a line without a key", mk("", "    tables:\n      - name: public.t\n        drop_columns:\n          - {a: 1}\n"), "line "},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := config.Parse([]byte(tt.doc))
			require.ErrorContains(t, err, tt.want)
		})
	}
}

func TestLoad(t *testing.T) {
	tests := []struct {
		name  string
		setup func(t *testing.T) string
		// wantErr is nil when Load must succeed, else it returns the text the
		// error must contain for the given path.
		wantErr func(path string) string
	}{
		{
			name: "valid file",
			setup: func(t *testing.T) string {
				path := filepath.Join(t.TempDir(), "config.yaml")
				require.NoError(t, os.WriteFile(path, []byte(mk("", "")), 0o600))
				return path
			},
		},
		{
			name:    "missing file names the path",
			setup:   func(*testing.T) string { return "/nonexistent/config.yaml" },
			wantErr: func(path string) string { return path },
		},
		{
			name: "invalid config names the path",
			setup: func(t *testing.T) string {
				path := filepath.Join(t.TempDir(), "bad.yaml")
				require.NoError(t, os.WriteFile(path, []byte("databases: []\n"), 0o600))
				return path
			},
			wantErr: func(path string) string { return path },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := tt.setup(t)
			cfg, err := config.Load(path)
			if tt.wantErr != nil {
				require.ErrorContains(t, err, tt.wantErr(path))
				return
			}
			require.NoError(t, err)
			require.Equal(t, "a", cfg.Databases[0].Name)
		})
	}
}

func TestDefaultPath(t *testing.T) {
	tests := []struct {
		name, want string
	}{
		{"current directory", "./config.yaml"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, config.DefaultPath)
		})
	}
}
