package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/config"
)

func TestOpenDeadLetter(t *testing.T) {
	dir := t.TempDir()
	tests := []struct {
		name    string
		path    string
		wantNil bool
		wantErr string
	}{
		{name: "no path means no dead letter", wantNil: true},
		{name: "creates the file", path: filepath.Join(dir, "d.jsonl")},
		{name: "unwritable path", path: filepath.Join(dir, "nodir", "d.jsonl"), wantErr: "nodir"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dl, err := openDeadLetter(config.DatabaseConfig{Name: "a", Sink: config.SinkConfig{Type: "file"}, DLQ: config.DLQConfig{Path: tt.path}})
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				require.Nil(t, dl)
				return
			}
			require.NoError(t, err)
			if tt.wantNil {
				require.Nil(t, dl)
				return
			}
			require.NoError(t, dl.Close())
			_, err = os.Stat(tt.path)
			require.NoError(t, err)
		})
	}
}

func TestAppRunUnwritableDeadLetter(t *testing.T) {
	tests := []struct{ name string }{{"startup fails before connecting"}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bad := filepath.Join(t.TempDir(), "nodir", "d.jsonl")
			cfg := config.Config{Databases: []config.DatabaseConfig{{
				Name: "a", Sink: config.SinkConfig{Type: "stdout"}, OnPermanentError: "skip", DLQ: config.DLQConfig{Path: bad},
			}}}
			err := newApp(&cfg, newRegistry(), newLogger(io.Discard)).run(context.Background())
			require.ErrorContains(t, err, bad)
			require.ErrorContains(t, err, `database "a"`)
		})
	}
}
