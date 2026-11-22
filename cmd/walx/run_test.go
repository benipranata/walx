package main

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRun(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		wantCode   int
		wantStderr string
		wantStdout string
	}{
		{"snapshot is reserved", []string{"snapshot"}, 1, "walx snapshot: not implemented", ""},
		{"snapshot with database", []string{"snapshot", "-database", "orders"}, 1, "walx snapshot: not implemented", ""},
		{"snapshot with unknown flag", []string{"snapshot", "-nope"}, 2, "flag provided but not defined: -nope", ""},
		{"snapshot with stray argument", []string{"snapshot", "orders"}, 2, "unexpected argument", ""},
		{"unknown command", []string{"bogus"}, 2, `unknown command "bogus"`, ""},
		{"no command serves with the default config", nil, 1, "config.yaml", ""},
		{"help", []string{"help"}, 0, "", "usage: walx"},
		{"-h", []string{"-h"}, 0, "", "usage: walx"},
		{"--help", []string{"--help"}, 0, "", "usage: walx"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run(tt.args, &stdout, &stderr)
			require.Equal(t, tt.wantCode, code)
			require.Contains(t, stderr.String(), tt.wantStderr)
			require.Contains(t, stdout.String(), tt.wantStdout)
			if tt.wantStdout == "" {
				require.Empty(t, stdout.String())
			} else {
				require.Empty(t, stderr.String())
			}
		})
	}
}
