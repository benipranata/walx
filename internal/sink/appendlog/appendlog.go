// Package appendlog appends byte chunks to a file and fsyncs each one before
// reporting success. The file sink and the dead-letter sink share it.
package appendlog

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

// fileMode is the mode of a created file: owner read/write, others read, so
// log shippers running as another user can tail it (the umask still applies).
const fileMode = 0o644

// ErrClosed is returned by Append after Close.
var ErrClosed = errors.New("closed")

// File is the destination of a Log; *os.File implements it.
type File interface {
	io.WriteCloser
	Sync() error
}

// Log appends chunks to a File it owns and closes.
type Log struct {
	mu     sync.Mutex
	f      File
	closed bool
	torn   bool
}

// New returns a Log writing to f. Close closes f.
func New(f File) *Log { return &Log{f: f} }

// Open opens (and creates if missing) path in append mode, so an unusable
// path is an error naming the path.
func Open(path string) (*Log, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, fileMode)
	if err != nil {
		return nil, fmt.Errorf("path %q is not writable: %w", path, err)
	}
	return New(f), nil
}

// OpenEndingLine is Open for a file whose readers must not lose a line to a
// crash: a last line left torn by one is ended, so the next Append starts on
// a fresh line instead of gluing two entries together.
func OpenEndingLine(path string) (*Log, error) {
	l, err := Open(path)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		_ = l.Close()
		return nil, fmt.Errorf("path %q is not readable: %w", path, err)
	}
	defer f.Close()
	if st, err := f.Stat(); err == nil && st.Size() > 0 {
		var last [1]byte
		if _, err := f.ReadAt(last[:], st.Size()-1); err == nil {
			l.torn = last[0] != '\n'
		}
	}
	return l, nil
}

// Append writes buf in a single Write, then fsyncs; it returns nil only after
// the fsync. After a failed partial write the next call starts on a fresh
// line, so a retry never corrupts the first line it writes. A failed attempt
// may have left bytes in the file, so a retry can repeat them.
func (l *Log) Append(buf []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if l.torn {
		buf = append([]byte{'\n'}, buf...)
	}
	n, err := l.f.Write(buf)
	if err == nil && n < len(buf) {
		err = io.ErrShortWrite
	}
	if err != nil {
		l.torn = n > 0
		return fmt.Errorf("write: %w", err)
	}
	l.torn = false
	if err := l.f.Sync(); err != nil {
		return fmt.Errorf("fsync: %w", err)
	}
	return nil
}

// Close closes the file. Calling it again is a no-op.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	return l.f.Close()
}
