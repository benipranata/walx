package appendlog_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/sink/appendlog"
)

type failFile struct {
	*os.File
	failWrite bool
	partial   bool
	failSync  bool
}

func (f *failFile) Write(p []byte) (int, error) {
	switch {
	case f.partial:
		n, _ := f.File.Write(p[:len(p)/2])
		return n, errors.New("disk full")
	case f.failWrite:
		return 0, errors.New("disk full")
	}
	return f.File.Write(p)
}

func (f *failFile) Sync() error {
	if f.failSync {
		return errors.New("sync failed")
	}
	return f.File.Sync()
}

func TestAppend(t *testing.T) {
	tests := []struct {
		name    string
		set     func(f *failFile)
		wantErr string
		want    string
	}{
		{name: "writes and syncs", set: func(*failFile) {}, want: "ab\n"},
		{name: "write error", set: func(f *failFile) { f.failWrite = true }, wantErr: "write: disk full"},
		{name: "fsync error", set: func(f *failFile) { f.failSync = true }, wantErr: "fsync: sync failed", want: "ab\n"},
		{name: "retry after a torn write starts a fresh line", set: func(f *failFile) { f.partial = true }, wantErr: "write: disk full", want: "a"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "x")
			f, err := os.Create(path)
			require.NoError(t, err)
			ff := &failFile{File: f}
			l := appendlog.New(ff)
			tt.set(ff)
			err = l.Append([]byte("ab\n"))
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
			got, _ := os.ReadFile(path)
			require.Equal(t, tt.want, string(got))
			if ff.partial {
				ff.partial = false
				require.NoError(t, l.Append([]byte("cd\n")))
				got, _ = os.ReadFile(path)
				require.Equal(t, "a\ncd\n", string(got))
			}
			require.NoError(t, l.Close())
			require.NoError(t, l.Close())
			require.ErrorIs(t, l.Append([]byte("x")), appendlog.ErrClosed)
		})
	}
}

func TestOpen(t *testing.T) {
	dir := t.TempDir()
	tests := []struct {
		name    string
		path    string
		wantErr string
	}{
		{name: "creates the file", path: filepath.Join(dir, "new")},
		{name: "missing directory", path: filepath.Join(dir, "nodir", "x"), wantErr: "nodir"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l, err := appendlog.Open(tt.path)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.NoError(t, l.Close())
			require.FileExists(t, tt.path)
		})
	}
}

func TestOpenEndingLine(t *testing.T) {
	tests := []struct {
		name     string
		existing string
		want     string
	}{
		{name: "empty file", existing: "", want: "new\n"},
		{name: "complete last line", existing: "old\n", want: "old\nnew\n"},
		{name: "torn last line", existing: "ol", want: "ol\nnew\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "x")
			require.NoError(t, os.WriteFile(path, []byte(tt.existing), 0o644))
			l, err := appendlog.OpenEndingLine(path)
			require.NoError(t, err)
			require.NoError(t, l.Append([]byte("new\n")))
			require.NoError(t, l.Close())
			got, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, tt.want, string(got))
		})
	}
}
