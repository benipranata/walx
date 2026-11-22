// Package sink builds the sink of a database from its configuration.
// Sink packages (internal/sink/<name>) do not import this package; the
// binary's wiring registers their factories.
package sink

import (
	"errors"
	"fmt"

	"github.com/benipranata/walx/internal/config"
	"github.com/benipranata/walx/internal/event"
)

// ErrUnknownType is returned by Build for a sink.type without a factory. It
// is a configuration error.
var ErrUnknownType = errors.New("unknown sink type")

// Factory builds the sink of one database. A cfg it cannot use (for example
// an unwritable path) is a configuration error and is returned as is.
type Factory func(database string, cfg config.SinkConfig) (event.Sink, error)

// Registry maps a sink.type to its Factory.
type Registry struct {
	factories map[string]Factory
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry {
	return &Registry{factories: map[string]Factory{}}
}

// Register adds the factory for typ. It panics on an empty type, a nil
// factory or a type registered twice, since all are programming errors.
func (r *Registry) Register(typ string, f Factory) {
	if typ == "" || f == nil {
		panic("sink: Register needs a type and a factory")
	}
	if _, ok := r.factories[typ]; ok {
		panic(fmt.Sprintf("sink: type %q registered twice", typ))
	}
	r.factories[typ] = f
}

// Build creates the sink for database from cfg.Type.
func (r *Registry) Build(database string, cfg config.SinkConfig) (event.Sink, error) {
	f, ok := r.factories[cfg.Type]
	if !ok {
		return nil, fmt.Errorf("database %q: %w %q", database, ErrUnknownType, cfg.Type)
	}
	s, err := f(database, cfg)
	if err != nil {
		return nil, fmt.Errorf("database %q: sink %s: %w", database, cfg.Type, err)
	}
	return s, nil
}
