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
	}{
		{"snapshot is reserved", []string{"snapshot"}, 1, "walx snapshot: not implemented"},
		{"snapshot with database", []string{"snapshot", "-database", "orders"}, 1, "walx snapshot: not implemented"},
		{"snapshot with unknown flag", []string{"snapshot", "-nope"}, 2, "flag provided but not defined: -nope"},
		{"snapshot with stray argument", []string{"snapshot", "orders"}, 2, "unexpected argument"},
		{"unknown command", []string{"bogus"}, 2, `unknown command "bogus"`},
		{"no command", nil, 2, "usage: walx"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run(tt.args, &stdout, &stderr)
			require.Equal(t, tt.wantCode, code)
			require.Contains(t, stderr.String(), tt.wantStderr)
			require.Empty(t, stdout.String())
		})
	}
}
