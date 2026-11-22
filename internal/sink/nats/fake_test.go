package nats_test

import (
	"errors"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// fakePub is a Publisher whose destination can fail publishes or acks.
type fakePub struct {
	mu          sync.Mutex
	msgs        []*nats.Msg // accepted by the stream, in publish order
	publishErr  error       // returned by PublishMsgAsync
	ackErr      error       // delivered on the future instead of an ack
	hang        bool        // never acks
	ackDelay    time.Duration
	acked       int
	publishedN  int
	failPublish map[string]error // by Nats-Msg-Id
}

func (f *fakePub) PublishMsgAsync(m *nats.Msg, _ ...jetstream.PublishOpt) (jetstream.PubAckFuture, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.failPublish[m.Header.Get(nats.MsgIdHdr)]; err != nil {
		return nil, err
	}
	if f.publishErr != nil {
		return nil, f.publishErr
	}
	f.publishedN++
	fut := &fakeFuture{msg: m, ok: make(chan *jetstream.PubAck, 1), err: make(chan error, 1)}
	switch {
	case f.hang:
	case f.ackErr != nil:
		fut.err <- f.ackErr
	default:
		f.msgs = append(f.msgs, m)
		delay := f.ackDelay
		go func() {
			time.Sleep(delay)
			f.mu.Lock()
			f.acked++
			f.mu.Unlock()
			fut.ok <- &jetstream.PubAck{}
		}()
	}
	return fut, nil
}

func (f *fakePub) ids() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, m := range f.msgs {
		out = append(out, m.Header.Get(nats.MsgIdHdr))
	}
	return out
}

func (f *fakePub) set(fn func(f *fakePub)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(f)
}

type fakeFuture struct {
	msg *nats.Msg
	ok  chan *jetstream.PubAck
	err chan error
}

func (f *fakeFuture) Ok() <-chan *jetstream.PubAck { return f.ok }
func (f *fakeFuture) Err() <-chan error            { return f.err }
func (f *fakeFuture) Msg() *nats.Msg               { return f.msg }

var errBroken = errors.New("stream unavailable")

func (f *fakePub) ackedN() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.acked
}

func (f *fakePub) accepted() []*nats.Msg {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*nats.Msg(nil), f.msgs...)
}
