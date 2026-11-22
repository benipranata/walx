package sinktest

import (
	"context"
	"errors"
	"sync"

	"github.com/benipranata/walx/internal/event"
)

// PoisonID is the Event.ID a Fake refuses with a Permanent error.
const PoisonID = "poison"

// Fake is an in-memory sink that passes the contract suite. It records the
// IDs of the events it accepted.
type Fake struct {
	mu      sync.Mutex
	written []string
	broken  bool
	closed  bool
}

// NewFake returns an empty Fake.
func NewFake() *Fake { return &Fake{} }

// Send records the batch, or fails when the Fake is broken, closed or the
// batch holds a PoisonID event. A failed Send records nothing.
func (f *Fake) Send(ctx context.Context, b event.Batch) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return errors.New("fake sink closed")
	}
	for _, e := range b.Events {
		if e.ID == PoisonID {
			return event.Permanent(errors.New("fake sink: event can never be written"))
		}
	}
	if f.broken {
		return errors.New("fake sink: destination unavailable")
	}
	for _, e := range b.Events {
		f.written = append(f.written, e.ID)
	}
	return nil
}

// Close marks the Fake closed; calling it again is a no-op.
func (f *Fake) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
	return nil
}

// Written returns the IDs recorded so far, in order.
func (f *Fake) Written() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.written...)
}

// Break makes Send fail until the returned function is called.
func (f *Fake) Break() (restore func()) {
	f.mu.Lock()
	f.broken = true
	f.mu.Unlock()
	return func() {
		f.mu.Lock()
		f.broken = false
		f.mu.Unlock()
	}
}
