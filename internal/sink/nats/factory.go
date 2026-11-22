package nats

import (
	"cmp"
	"errors"
	"fmt"

	"github.com/benipranata/walx/internal/config"
	"github.com/benipranata/walx/internal/event"
	"github.com/benipranata/walx/internal/sink/subject"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// maxPending is the client's limit of unacknowledged async publishes; it must
// exceed chunkSize.
const maxPending = 4 * chunkSize

// Factory builds the NATS sink of a database. It connects to cfg.URL (an
// unreachable server is a startup error; later outages are reconnected
// forever) and expects a stream capturing the subjects to exist: the sink
// never creates or alters streams.
func Factory(database string, cfg config.SinkConfig) (event.Sink, error) {
	if cfg.URL == "" {
		return nil, errors.New("nats sink needs sink.url")
	}
	tmpl, err := subject.Parse(cmp.Or(cfg.Subject, subject.DefaultTemplate))
	if err != nil {
		return nil, fmt.Errorf("nats sink: %w", err)
	}
	nc, err := nats.Connect(cfg.URL, nats.Name("walx-"+database), nats.MaxReconnects(-1))
	if err != nil {
		return nil, fmt.Errorf("nats sink: connect: %w", err)
	}
	js, err := jetstream.New(nc,
		jetstream.WithPublishAsyncMaxPending(maxPending),
		jetstream.WithPublishAsyncTimeout(DefaultAckWait),
	)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("nats sink: jetstream: %w", err)
	}
	return New(js, Options{
		Database:   database,
		Template:   tmpl,
		FSMColumns: cfg.FSMColumns,
		OnClose:    func() error { nc.Close(); return nil },
	}), nil
}
