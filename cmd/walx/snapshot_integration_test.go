package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestSnapshotCommandIntegration(t *testing.T) {
	tests := []struct {
		name      string
		extraArgs []string
		wantRows  int
		wantCode  int
	}{
		{"configured tables", nil, 5, 0},
		{"one table with where", []string{"-table", "public.items", "-where", "id > 3"}, 2, 0},
		{"small chunks", []string{"-chunk", "2"}, 5, 0},
		{"table outside config", []string{"-table", "public.other"}, 0, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url := startPostgres(t)
			ctx := context.Background()
			conn, err := pgx.Connect(ctx, url)
			require.NoError(t, err)
			defer conn.Close(ctx)
			_, err = conn.Exec(ctx, `CREATE TABLE items (id int PRIMARY KEY, name text);
INSERT INTO items SELECT g, 'n' || g FROM generate_series(1, 5) g`)
			require.NoError(t, err)

			dir := t.TempDir()
			out := filepath.Join(dir, "out.jsonl")
			cfg := filepath.Join(dir, "config.yaml")
			doc := fmt.Sprintf("databases:\n  - name: snap\n    postgres:\n      url: %q\n    tables:\n      - name: public.items\n    sink:\n      type: file\n      path: %q\n", url, out)
			require.NoError(t, os.WriteFile(cfg, []byte(doc), 0o600))

			var stdout, stderr bytes.Buffer
			args := append([]string{"snapshot", "-config", cfg}, tt.extraArgs...)
			require.Equal(t, tt.wantCode, run(args, &stdout, &stderr), stderr.String())
			lines := readLines(t, out)
			require.Len(t, lines, tt.wantRows)
			for i, l := range lines {
				require.Equal(t, "snapshot", l["op"])
				require.Equal(t, "snap", l["source"])
				require.Regexp(t, fmt.Sprintf(`^snap-[0-9A-F]+/[0-9A-F]+-%d$`, i+1), l["id"])
			}
			require.NotContains(t, stderr.String(), "password")
			var slots int
			require.NoError(t, conn.QueryRow(ctx, `SELECT count(*) FROM pg_replication_slots`).Scan(&slots))
			require.Zero(t, slots)
		})
	}
}
