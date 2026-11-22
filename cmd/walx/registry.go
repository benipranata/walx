package main

import (
	"github.com/benipranata/walx/internal/sink"
	"github.com/benipranata/walx/internal/sink/file"
	"github.com/benipranata/walx/internal/sink/nats"
	"github.com/benipranata/walx/internal/sink/stdout"
)

// newRegistry returns the sink registry with every built-in sink type.
func newRegistry() *sink.Registry {
	r := sink.NewRegistry()
	r.Register("stdout", stdout.Factory)
	r.Register("file", file.Factory)
	r.Register("nats", nats.Factory)
	return r
}
