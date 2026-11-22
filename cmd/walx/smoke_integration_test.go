package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// syncBuffer is a bytes.Buffer safe for the exec copier and the test.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// startPostgres skips unless WALX_TEST_POSTGRES_URL is set, and returns the
// URL of a fresh throwaway database. It mirrors the helper in internal/engine.
func startPostgres(t *testing.T) string {
	t.Helper()
	base := os.Getenv("WALX_TEST_POSTGRES_URL")
	if base == "" {
		t.Skip("WALX_TEST_POSTGRES_URL not set")
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, base)
	require.NoError(t, err)
	name := fmt.Sprintf("walx_test_%d", time.Now().UnixNano())
	_, err = admin.Exec(ctx, "CREATE DATABASE "+name)
	require.NoError(t, err)
	t.Cleanup(func() {
		defer admin.Close(ctx)
		_, _ = admin.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1`, name)
		_, _ = admin.Exec(ctx, `SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE database = $1`, name)
		_, _ = admin.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})
	u, err := neturl.Parse(base)
	require.NoError(t, err)
	u.Path = "/" + name
	return u.String()
}

func buildBinary(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "walx")
	out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput()
	require.NoError(t, err, string(out))
	return bin
}

func readLines(t *testing.T, path string) []map[string]any {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	var out []map[string]any
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var m map[string]any
		require.NoError(t, json.Unmarshal(sc.Bytes(), &m), sc.Text())
		out = append(out, m)
	}
	return out
}

func TestSmokeIntegration(t *testing.T) {
	tests := []struct {
		name         string
		first, again syscall.Signal
	}{
		{"SIGTERM then SIGINT", syscall.SIGTERM, syscall.SIGINT},
		{"SIGINT then SIGTERM", syscall.SIGINT, syscall.SIGTERM},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) { smoke(t, tt.first, tt.again) })
	}
}

func smoke(t *testing.T, first, again syscall.Signal) {
	url := startPostgres(t)
	bin := buildBinary(t)
	ctx := context.Background()

	conn, err := pgx.Connect(ctx, url)
	require.NoError(t, err)
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, "CREATE TABLE items (id int PRIMARY KEY, name text)")
	require.NoError(t, err)

	dir := t.TempDir()
	out := filepath.Join(dir, "out.jsonl")
	cfg := filepath.Join(dir, "config.yaml")
	doc := fmt.Sprintf(`shutdown_timeout: 10s
metrics:
  listen: 127.0.0.1:0
databases:
  - name: smoke
    postgres:
      url: %q
    tables:
      - name: public.items
    sink:
      type: file
      path: %q
`, url, out)
	require.NoError(t, os.WriteFile(cfg, []byte(doc), 0o600))

	var stderr syncBuffer
	cmd := exec.Command(bin, "-config", cfg)
	cmd.Stderr = &stderr
	require.NoError(t, cmd.Start())
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	t.Cleanup(func() { _ = cmd.Process.Kill() })

	require.Eventually(t, func() bool { return strings.Contains(stderr.String(), "walx started") },
		30*time.Second, 50*time.Millisecond, stderr.String())

	_, err = conn.Exec(ctx, "INSERT INTO items VALUES (1, 'a'), (2, 'b')")
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "UPDATE items SET name = 'c' WHERE id = 2")
	require.NoError(t, err)

	require.Eventually(t, func() bool { return len(readLines(t, out)) >= 3 },
		30*time.Second, 50*time.Millisecond, stderr.String())
	lines := readLines(t, out)
	require.Len(t, lines, 3)
	seen := map[string]bool{}
	for _, l := range lines {
		id, _ := l["id"].(string)
		require.Regexp(t, `^smoke-[0-9A-F]+/[0-9A-F]+-[0-9]+$`, id)
		require.False(t, seen[id], "duplicate id %s", id)
		seen[id] = true
		require.Equal(t, "smoke", l["source"])
		require.Equal(t, "items", l["table"])
	}
	require.Equal(t, "insert", lines[0]["op"])
	require.Equal(t, "update", lines[2]["op"])

	addr := regexp.MustCompile(`metrics listening" addr=(\S+)`).FindStringSubmatch(stderr.String())
	require.NotNil(t, addr, stderr.String())
	require.Eventually(t, func() bool {
		resp, err := http.Get("http://" + addr[1] + "/metrics")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		scraped, _ := io.ReadAll(resp.Body)
		return strings.Contains(string(scraped), `walx_events_total{database="smoke"} 3`)
	}, 10*time.Second, 50*time.Millisecond, "events_total never reached 3")

	require.NoError(t, cmd.Process.Signal(first))
	select {
	case err := <-exited:
		require.NoError(t, err, stderr.String())
	case <-time.After(30 * time.Second):
		t.Fatalf("walx did not exit after SIGTERM:\n%s", stderr.String())
	}
	require.Contains(t, stderr.String(), "walx stopped")
	require.NotContains(t, stderr.String(), "level=ERROR")
	require.NotContains(t, stderr.String(), "password")

	// A restart resumes after the confirmed LSN: no event is redelivered.
	var stderr2 syncBuffer
	cmd2 := exec.Command(bin, "-config", cfg)
	cmd2.Stderr = &stderr2
	require.NoError(t, cmd2.Start())
	exited2 := make(chan error, 1)
	go func() { exited2 <- cmd2.Wait() }()
	t.Cleanup(func() { _ = cmd2.Process.Kill() })
	require.Eventually(t, func() bool { return strings.Contains(stderr2.String(), "walx started") },
		30*time.Second, 50*time.Millisecond, stderr2.String())
	_, err = conn.Exec(ctx, "INSERT INTO items VALUES (3, 'z')")
	require.NoError(t, err)
	require.Eventually(t, func() bool { return len(readLines(t, out)) >= 4 },
		30*time.Second, 50*time.Millisecond, stderr2.String())
	time.Sleep(300 * time.Millisecond)
	require.Len(t, readLines(t, out), 4)
	require.NoError(t, cmd2.Process.Signal(again))
	select {
	case err := <-exited2:
		require.NoError(t, err, stderr2.String())
	case <-time.After(30 * time.Second):
		t.Fatalf("walx did not exit after SIGINT:\n%s", stderr2.String())
	}
}
