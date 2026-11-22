package file_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/benipranata/walx/internal/config"
	"github.com/benipranata/walx/internal/event"
	"github.com/benipranata/walx/internal/sink/file"
	"github.com/benipranata/walx/internal/sink/sinktest"
	"github.com/stretchr/testify/require"
)

func idsOf(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var out []string
	sc := bufio.NewScanner(bytes.NewReader(data))
	for sc.Scan() {
		var e struct {
			ID string `json:"id"`
		}
		require.NoError(t, json.Unmarshal(sc.Bytes(), &e))
		out = append(out, e.ID)
	}
	return out
}

// fakeFile wraps a real file with switchable write and sync failures.
type fakeFile struct {
	*os.File
	mu        sync.Mutex
	failWrite bool
	partial   int
	short     bool
	failSync  bool
	syncs     int
	closes    int
}

func (f *fakeFile) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failWrite {
		return 0, errors.New("disk full")
	}
	if f.short {
		n, _ := f.File.Write(p[:len(p)/2])
		return n, nil
	}
	if f.partial > 0 {
		n, _ := f.File.Write(p[:f.partial])
		return n, errors.New("disk full")
	}
	return f.File.Write(p)
}

func (f *fakeFile) Sync() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.syncs++
	if f.failSync {
		return errors.New("fsync failed")
	}
	return f.File.Sync()
}

func (f *fakeFile) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closes++
	return f.File.Close()
}

func newFake(t *testing.T) (*fakeFile, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "out.jsonl")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	require.NoError(t, err)
	return &fakeFile{File: f}, path
}

func TestContract(t *testing.T) {
	tests := []struct {
		name string
	}{
		{"file sink"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sinktest.Run(t, func(t *testing.T) sinktest.Target {
				f, path := newFake(t)
				return sinktest.Target{
					Sink:    file.New(f),
					Written: func() []string { return idsOf(t, path) },
					Break: func() func() {
						f.mu.Lock()
						f.failWrite = true
						f.mu.Unlock()
						return func() {
							f.mu.Lock()
							f.failWrite = false
							f.mu.Unlock()
						}
					},
					Unwritable: &event.Batch{Events: []*event.Event{
						{ID: "a-1", After: map[string]any{"v": 1}},
						{ID: "bad", After: map[string]any{"v": math.NaN()}},
					}},
				}
			})
		})
	}
}

func TestSyncFailure(t *testing.T) {
	tests := []struct {
		name string
	}{
		{"fsync error is retryable and the retry succeeds"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, path := newFake(t)
			s := file.New(f)
			t.Cleanup(func() { _ = s.Close() })
			b := sinktest.Batch("a", 2)

			f.failSync = true
			err := s.Send(context.Background(), b)
			require.ErrorContains(t, err, "fsync failed")
			require.False(t, event.IsPermanent(err))

			f.failSync = false
			require.NoError(t, s.Send(context.Background(), b))
			// The first attempt's lines stay: a retry duplicates them.
			require.Equal(t, []string{"a-1", "a-2", "a-1", "a-2"}, idsOf(t, path))
		})
	}
}

func TestTornLineIsTerminated(t *testing.T) {
	tests := []struct {
		name    string
		partial int
		want    []string
	}{
		{"partial write, retry starts a new line", 10, []string{"a-1", "a-2"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, path := newFake(t)
			s := file.New(f)
			t.Cleanup(func() { _ = s.Close() })
			b := sinktest.Batch("a", 2)

			f.partial = tt.partial
			require.Error(t, s.Send(context.Background(), b))
			f.partial = 0
			require.NoError(t, s.Send(context.Background(), b))

			data, err := os.ReadFile(path)
			require.NoError(t, err)
			var ids []string
			for _, line := range bytes.Split(data, []byte("\n")) {
				var e struct {
					ID string `json:"id"`
				}
				if json.Unmarshal(line, &e) == nil && e.ID != "" {
					ids = append(ids, e.ID)
				}
			}
			require.Equal(t, tt.want, ids)
		})
	}
}

func TestShortWrite(t *testing.T) {
	tests := []struct {
		name string
	}{
		{"short write without error is retryable"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, _ := newFake(t)
			f.short = true
			s := file.New(f)
			t.Cleanup(func() { _ = s.Close() })
			err := s.Send(context.Background(), sinktest.Batch("a", 2))
			require.ErrorIs(t, err, io.ErrShortWrite)
			require.False(t, event.IsPermanent(err))
			require.Zero(t, f.syncs)
		})
	}
}

func TestSyncsOncePerBatch(t *testing.T) {
	tests := []struct {
		name string
		n    int
	}{
		{"three events", 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, _ := newFake(t)
			s := file.New(f)
			require.NoError(t, s.Send(context.Background(), sinktest.Batch("a", tt.n)))
			require.Equal(t, 1, f.syncs)
			require.NoError(t, s.Close())
		})
	}
}

func TestSendErrors(t *testing.T) {
	tests := []struct {
		name string
		do   func(s event.Sink) error
		want string
	}{
		{
			name: "send after close",
			do: func(s event.Sink) error {
				_ = s.Close()
				return s.Send(context.Background(), sinktest.Batch("a", 1))
			},
			want: "closed",
		},
		{
			name: "cancelled context",
			do: func(s event.Sink) error {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return s.Send(ctx, sinktest.Batch("a", 1))
			},
			want: "canceled",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, path := newFake(t)
			err := tt.do(file.New(f))
			require.ErrorContains(t, err, tt.want)
			require.False(t, event.IsPermanent(err))
			require.Empty(t, idsOf(t, path))
		})
	}
}

func TestCloseClosesFileOnce(t *testing.T) {
	tests := []struct {
		name string
	}{
		{"close twice"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, _ := newFake(t)
			s := file.New(f)
			require.NoError(t, s.Close())
			require.NoError(t, s.Close())
			require.Equal(t, 1, f.closes)
		})
	}
}

func TestConcurrentSend(t *testing.T) {
	tests := []struct {
		name    string
		senders int
	}{
		{"batches never interleave", 8},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, path := newFake(t)
			s := file.New(f)
			var wg sync.WaitGroup
			for i := 0; i < tt.senders; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					require.NoError(t, s.Send(context.Background(), sinktest.Batch("x", 3)))
				}()
			}
			wg.Wait()
			require.NoError(t, s.Close())
			var want []string
			for i := 0; i < tt.senders; i++ {
				want = append(want, "x-1", "x-2", "x-3")
			}
			require.Equal(t, want, idsOf(t, path))
		})
	}
}

func TestFactory(t *testing.T) {
	dir := t.TempDir()
	existing := filepath.Join(dir, "existing.jsonl")
	require.NoError(t, os.WriteFile(existing, []byte("{\"id\":\"old\"}\n"), 0o644))
	tests := []struct {
		name    string
		path    string
		wantErr string
		wantIDs []string
		check   func(t *testing.T, path string)
	}{
		{name: "creates a missing file", path: filepath.Join(dir, "new.jsonl"), wantIDs: []string{"a-1"}, check: func(t *testing.T, path string) {
			st, err := os.Stat(path)
			require.NoError(t, err)
			require.Equal(t, os.FileMode(0o600), st.Mode().Perm()&0o600)
		}},
		{name: "appends to an existing file", path: existing, wantIDs: []string{"old", "a-1"}},
		{name: "missing directory", path: filepath.Join(dir, "nodir", "x.jsonl"), wantErr: "nodir"},
		{name: "path is a directory", path: dir, wantErr: dir},
		{name: "empty path", path: "", wantErr: "path"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := file.Factory("db", config.SinkConfig{Type: "file", Path: tt.path})
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				require.Nil(t, s)
				return
			}
			require.NoError(t, err)
			require.NoError(t, s.Send(context.Background(), sinktest.Batch("a", 1)))
			require.NoError(t, s.Close())
			require.Equal(t, tt.wantIDs, idsOf(t, tt.path))
			if tt.check != nil {
				tt.check(t, tt.path)
			}
		})
	}
}
