package nats

import (
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Publisher publishes to JetStream without waiting for the ack;
// jetstream.JetStream implements it, and tests fake it.
type Publisher interface {
	PublishMsgAsync(msg *nats.Msg, opts ...jetstream.PublishOpt) (jetstream.PubAckFuture, error)
}
