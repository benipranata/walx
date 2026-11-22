package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func writeConfig(t *testing.T, doc string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(p, []byte(doc), 0o600))
	return p
}

func TestServeErrors(t *testing.T) {
	dir := t.TempDir()
	tests := []struct {
		name       string
		args       func(t *testing.T) []string
		wantCode   int
		wantStderr []string
		notStderr  []string
	}{
		{"unknown flag", func(*testing.T) []string { return []string{"-nope"} }, 2, []string{"flag provided but not defined: -nope"}, nil},
		{"config flag without value", func(*testing.T) []string { return []string{"-config"} }, 2, []string{"-config"}, nil},
		{"stray argument", func(*testing.T) []string { return []string{"-config", "x.yaml", "extra"} }, 2, []string{"unexpected argument"}, nil},
		{"missing config file", func(*testing.T) []string { return []string{"-config", filepath.Join(dir, "nope.yaml")} }, 1, []string{"walx:", "nope.yaml"}, nil},
		{"invalid config names the key", func(t *testing.T) []string {
			return []string{"-config", writeConfig(t, "databases:\n  - name: Bad-Name\n    postgres: {url: \"postgres://u:secretpw@h/d\"}\n    sink: {type: stdout}\n")}
		}, 1, []string{"databases[0].name"}, []string{"secretpw"}},
		{"unknown config key", func(t *testing.T) []string {
			return []string{"-config", writeConfig(t, "bogus: 1\ndatabases: []\n")}
		}, 1, []string{"bogus"}, nil},
		{"unwritable file sink fails before connecting", func(t *testing.T) []string {
			return []string{"-config", writeConfig(t, fmt.Sprintf("databases:\n  - name: a\n    postgres: {url: \"postgres://u:secretpw@127.0.0.1:1/d\"}\n    sink: {type: file, path: %q}\n", filepath.Join(dir, "missing", "out.jsonl")))}
		}, 1, []string{"not writable"}, []string{"secretpw"}},
		{"unreachable postgres", func(t *testing.T) []string {
			return []string{"-config", writeConfig(t, "databases:\n  - name: a\n    postgres: {url: \"postgres://u:secretpw@127.0.0.1:1/d\"}\n    sink: {type: stdout}\n")}
		}, 1, []string{`database "a"`}, []string{"secretpw"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run(tt.args(t), &stdout, &stderr)
			require.Equal(t, tt.wantCode, code, stderr.String())
			for _, w := range tt.wantStderr {
				require.Contains(t, stderr.String(), w)
			}
			for _, w := range tt.notStderr {
				require.NotContains(t, stderr.String(), w)
			}
		})
	}
}

func TestExitCode(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{"clean stop", nil, 0},
		{"canceled", context.Canceled, 0},
		{"wrapped canceled", fmt.Errorf("x: %w", context.Canceled), 0},
		{"other error", errors.New("boom"), 1},
		{"deadline", context.DeadlineExceeded, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, exitCode(tt.err))
		})
	}
}

func TestSignalContext(t *testing.T) {
	tests := []struct {
		name string
		sig  syscall.Signal
	}{
		{"SIGTERM", syscall.SIGTERM},
		{"SIGINT", syscall.SIGINT},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, stop := signalContext(context.Background())
			defer stop()
			require.NoError(t, ctx.Err())
			require.NoError(t, syscall.Kill(os.Getpid(), tt.sig))
			select {
			case <-ctx.Done():
			case <-time.After(5 * time.Second):
				t.Fatal("context not cancelled by signal")
			}
		})
	}
}
