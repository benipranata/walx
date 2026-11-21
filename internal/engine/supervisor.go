package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
)

// Named is one database pipeline: its name for logging and the function that
// runs it, normally (*Engine).Run.
type Named struct {
	Name string
	Run  func(ctx context.Context) error
}

// Supervisor runs one pipeline per databases entry, concurrently. A failure
// in one stops all (fail fast).
type Supervisor struct {
	Engines []Named
	Logger  *slog.Logger // discards when nil
}

// Run starts every pipeline and waits for all of them. When one returns, the
// others are cancelled and awaited, so each finishes its in-flight sink call
// and confirms through its own shutdown path.
//
// It returns the first error that is not a supervisor-induced
// context.Canceled. A pipeline that returns nil or Canceled while nothing was
// cancelled counts as a failure ("stopped unexpectedly"). Otherwise, when ctx
// was cancelled, it returns ctx.Err(), as Engine.Run does, so the caller
// treats it as a clean stop. A genuine error wrapping Canceled (for example
// from a sink) is reported while the supervisor is live; once it is
// cancelling, such an error is taken as induced.
func (s *Supervisor) Run(ctx context.Context) error {
	if len(s.Engines) == 0 {
		return errors.New("supervisor: no databases to run")
	}
	log := s.Logger
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	parent := ctx
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	var (
		wg    sync.WaitGroup
		once  sync.Once
		first error
	)
	for _, n := range s.Engines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := n.Run(ctx)
			// Before cancel(): a Canceled return while ctx is still live
			// was not induced by this supervisor, so it is a failure.
			live := ctx.Err() == nil
			switch {
			case err == nil && live:
				err = fmt.Errorf("supervisor: pipeline %s stopped unexpectedly", n.Name)
			case errors.Is(err, context.Canceled) && live:
				err = fmt.Errorf("supervisor: pipeline %s stopped unexpectedly: %w", n.Name, err)
			}
			if err != nil && (live || !errors.Is(err, context.Canceled)) {
				log.Error("database pipeline failed, stopping the others", "db", n.Name, "err", err)
				once.Do(func() { first = err })
			}
			cancel()
		}()
	}
	wg.Wait()
	if first != nil {
		return first
	}
	return parent.Err()
}
