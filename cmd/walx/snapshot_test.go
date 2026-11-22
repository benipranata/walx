package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/config"
	"github.com/benipranata/walx/internal/snapshot"
)

const snapshotConfig = `databases:
  - name: orders
    postgres:
      url: postgres://u:secret@127.0.0.1:1/orders?connect_timeout=1
    tables:
      - name: public.orders
      - name: public.items
    sink:
      type: stdout
  - name: billing
    postgres:
      url: postgres://u:secret@127.0.0.1:1/billing?connect_timeout=1
    sink:
      type: stdout
`

func loadSnapshotConfig(t *testing.T, doc string) *config.Config {
	t.Helper()
	cfg, err := config.Parse([]byte(doc))
	require.NoError(t, err)
	return cfg
}

func TestSelectSnapshot(t *testing.T) {
	single := `databases:
  - name: only
    postgres:
      url: postgres://u:p@127.0.0.1:1/only
    sink:
      type: stdout
`
	tests := []struct {
		name        string
		doc         string
		opts        snapshotOptions
		wantDB      string
		wantTargets []snapshot.Target
		wantErr     string
	}{
		{"configured tables by default", snapshotConfig, snapshotOptions{database: "orders"}, "orders",
			[]snapshot.Target{{Table: "public.orders"}, {Table: "public.items"}}, ""},
		{"no tables listed reads everything", snapshotConfig, snapshotOptions{database: "billing"}, "billing", []snapshot.Target{}, ""},
		{"table restricts", snapshotConfig, snapshotOptions{database: "orders", tables: []string{"public.items"}}, "orders",
			[]snapshot.Target{{Table: "public.items"}}, ""},
		{"table repeated and duplicated", snapshotConfig, snapshotOptions{database: "orders", tables: []string{"public.items", "public.orders", "public.items"}}, "orders",
			[]snapshot.Target{{Table: "public.items"}, {Table: "public.orders"}}, ""},
		{"table outside the config", snapshotConfig, snapshotOptions{database: "orders", tables: []string{"public.other"}}, "", nil, "not in the tables"},
		{"table on an entry without a list", snapshotConfig, snapshotOptions{database: "billing", tables: []string{"x.y"}}, "billing",
			[]snapshot.Target{{Table: "x.y"}}, ""},
		{"where with one table", snapshotConfig, snapshotOptions{database: "orders", tables: []string{"public.orders"}, where: "id > 5"}, "orders",
			[]snapshot.Target{{Table: "public.orders", Where: "id > 5"}}, ""},
		{"where with several tables", snapshotConfig, snapshotOptions{database: "orders", where: "id > 5"}, "", nil, "-where needs exactly one table"},
		{"where with no table", snapshotConfig, snapshotOptions{database: "billing", where: "id > 5"}, "", nil, "-where needs exactly one table"},
		{"unknown database", snapshotConfig, snapshotOptions{database: "nope"}, "", nil, `unknown database "nope", one of: orders, billing`},
		{"database required with several", snapshotConfig, snapshotOptions{}, "", nil, "-database is required, one of: orders, billing"},
		{"database optional with one", single, snapshotOptions{}, "only", []snapshot.Target{}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, targets, err := selectSnapshot(loadSnapshotConfig(t, tt.doc), tt.opts)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantDB, db.Name)
			require.Equal(t, len(tt.wantTargets), len(targets))
			for i := range tt.wantTargets {
				require.Equal(t, tt.wantTargets[i], targets[i])
			}
		})
	}
}

func TestParseSnapshotFlags(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		want    snapshotOptions
		wantErr string
	}{
		{"defaults", nil, snapshotOptions{config: config.DefaultPath, chunk: snapshot.DefaultChunk}, ""},
		{"all flags", []string{"-config", "c.yaml", "-database", "d", "-table", "a.b", "-table", "c.d", "-where", "x = 1", "-chunk", "50"},
			snapshotOptions{config: "c.yaml", database: "d", tables: []string{"a.b", "c.d"}, where: "x = 1", chunk: 50}, ""},
		{"zero chunk", []string{"-chunk", "0"}, snapshotOptions{}, "-chunk must be positive"},
		{"stray argument", []string{"orders"}, snapshotOptions{}, "unexpected argument"},
		{"unknown flag", []string{"-nope"}, snapshotOptions{}, "flag provided but not defined"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stderr bytes.Buffer
			got, err := parseSnapshotFlags(tt.args, &stderr)
			if tt.wantErr != "" {
				require.Error(t, err)
				require.Contains(t, stderr.String(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestRunSnapshotExitCodes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(snapshotConfig), 0o600))
	bad := filepath.Join(dir, "bad.yaml")
	require.NoError(t, os.WriteFile(bad, []byte("databases: nope"), 0o600))

	tests := []struct {
		name       string
		args       []string
		wantCode   int
		wantStderr string
	}{
		{"unknown database is a usage error", []string{"-config", path, "-database", "nope"}, 2, `unknown database "nope"`},
		{"database required", []string{"-config", path}, 2, "-database is required"},
		{"where without a table", []string{"-config", path, "-database", "orders", "-where", "x"}, 2, "-where needs exactly one table"},
		{"invalid config", []string{"-config", bad}, 1, "walx snapshot:"},
		{"unreachable database", []string{"-config", path, "-database", "billing"}, 1, "walx snapshot:"},
		{"help", []string{"-h"}, 0, "-database"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := runSnapshot(tt.args, &stdout, &stderr)
			require.Equal(t, tt.wantCode, code, stderr.String())
			require.Contains(t, stderr.String(), tt.wantStderr)
			require.NotContains(t, stderr.String(), "secret", fmt.Sprint("credentials leaked"))
		})
	}
}
