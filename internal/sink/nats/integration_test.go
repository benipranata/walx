package nats_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/benipranata/walx/internal/config"
	"github.com/benipranata/walx/internal/event"
	natssink "github.com/benipranata/walx/internal/sink/nats"
	"github.com/benipranata/walx/internal/sink/sinktest"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/require"
)

// natsEnv returns a JetStream handle and a unique stream capturing
// <prefix>.>, removed at cleanup. It skips without WALX_TEST_NATS_URL.
func natsEnv(t *testing.T, duplicates time.Duration) (url, prefix string, stream jetstream.Stream) {
	t.Helper()
	url = os.Getenv("WALX_TEST_NATS_URL")
	if url == "" {
		t.Skip("WALX_TEST_NATS_URL not set")
	}
	nc, err := nats.Connect(url)
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	require.NoError(t, err)
	prefix = fmt.Sprintf("walxtest%d", time.Now().UnixNano())
	cfg := jetstream.StreamConfig{Name: prefix, Subjects: []string{prefix + ".>"}, Storage: jetstream.MemoryStorage}
	if duplicates > 0 {
		cfg.Duplicates = duplicates
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err = js.CreateStream(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = js.DeleteStream(context.Background(), prefix) })
	return url, prefix, stream
}

type stored struct {
	subject string
	id      string
	hdr     nats.Header
	bodyID  string
}

func readAll(t *testing.T, stream jetstream.Stream) []stored {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	info, err := stream.Info(ctx)
	require.NoError(t, err)
	var out []stored
	for seq := info.State.FirstSeq; info.State.Msgs > 0 && seq <= info.State.LastSeq; seq++ {
		m, err := stream.GetMsg(ctx, seq)
		require.NoError(t, err)
		var body struct {
			ID string `json:"id"`
		}
		require.NoError(t, json.Unmarshal(m.Data, &body))
		out = append(out, stored{subject: m.Subject, id: m.Header.Get(nats.MsgIdHdr), hdr: m.Header, bodyID: body.ID})
	}
	return out
}

func TestJetStreamIntegration(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, url, prefix string, stream jetstream.Stream)
	}{
		{"published events are in the stream in order with subjects and headers", func(t *testing.T, url, prefix string, stream jetstream.Stream) {
			s := newSink(t, url, prefix)
			b := event.Batch{CommitLSN: 0x16B3748, Events: []*event.Event{
				{ID: "db-1-1", Source: "db", Op: event.OpInsert, Schema: "public", Table: "orders", After: map[string]any{"id": 1, "status": "new"}},
				{ID: "db-1-2", Source: "db", Op: event.OpUpdate, Schema: "public", Table: "orders",
					Before: map[string]any{"id": 1, "status": "new"}, After: map[string]any{"id": 1, "status": "delete"}},
				{ID: "db-1-3", Source: "db", Op: event.OpDelete, Schema: "public", Table: "users", Before: map[string]any{"id": 7}},
			}}
			require.NoError(t, s.Send(context.Background(), b))

			got := readAll(t, stream)
			require.Len(t, got, 3, "Send returned before the stream held every message")
			require.Equal(t, prefix+".public.orders.new", got[0].subject)
			require.Equal(t, prefix+".public.orders.state_delete", got[1].subject)
			require.Equal(t, prefix+".public.users.delete", got[2].subject)
			for i, e := range b.Events {
				require.Equal(t, e.ID, got[i].id)
				require.Equal(t, e.ID, got[i].bodyID)
			}
			require.Equal(t, "update", got[1].hdr.Get("Walx-Op"))
			require.Equal(t, "new", got[1].hdr.Get("Walx-State-From"))
			require.Equal(t, "delete", got[1].hdr.Get("Walx-State-To"))
			require.Equal(t, "0/16B3748", got[1].hdr.Get("Walx-Commit-Lsn"))
			require.Empty(t, got[2].hdr.Values("Walx-State-To"))
		}},
		{"redelivery with the same ids is deduplicated by the stream", func(t *testing.T, url, prefix string, stream jetstream.Stream) {
			s := newSink(t, url, prefix)
			b := sinktest.Batch("db-9", 5)
			require.NoError(t, s.Send(context.Background(), b))
			require.NoError(t, s.Send(context.Background(), b))
			// A retry that is a superset (partial first attempt) dedups too.
			require.NoError(t, s.Send(context.Background(), sinktest.Batch("db-9", 7)))

			var ids []string
			for _, m := range readAll(t, stream) {
				ids = append(ids, m.id)
			}
			require.Equal(t, []string{"db-9-1", "db-9-2", "db-9-3", "db-9-4", "db-9-5", "db-9-6", "db-9-7"}, ids)
		}},
		{"a batch larger than one chunk is fully acked", func(t *testing.T, url, prefix string, stream jetstream.Stream) {
			s := newSink(t, url, prefix)
			require.NoError(t, s.Send(context.Background(), sinktest.Batch("db-big", 2500)))
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			info, err := stream.Info(ctx)
			require.NoError(t, err)
			require.EqualValues(t, 2500, info.State.Msgs)
		}},
		{"no stream for the subject is a retryable error", func(t *testing.T, url, prefix string, stream jetstream.Stream) {
			s := newSink(t, url, "nostream"+prefix)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			err := s.Send(ctx, sinktest.Batch("db-x", 1))
			require.Error(t, err)
			require.False(t, event.IsPermanent(err))
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url, prefix, stream := natsEnv(t, 0)
			tt.run(t, url, prefix, stream)
		})
	}
}

func TestJetStreamContract(t *testing.T) {
	tests := []struct{ name string }{{"jetstream"}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sinktest.Run(t, func(t *testing.T) sinktest.Target {
				// A fresh stream per case: a purge keeps the dedup window, so
				// reused IDs would be dropped.
				url, prefix, stream := natsEnv(t, 0)
				return sinktest.Target{
					Sink: newSink(t, url, prefix),
					Written: func() []string {
						var ids []string
						for _, m := range readAll(t, stream) {
							ids = append(ids, m.id)
						}
						return ids
					},
					Unwritable: &event.Batch{Events: []*event.Event{
						{ID: "bad", Op: event.OpInsert, Schema: "public", Table: ""},
					}},
				}
			})
		})
	}
}

func newSink(t *testing.T, url, prefix string) event.Sink {
	t.Helper()
	s, err := natssink.Factory("db", config.SinkConfig{
		Type:       "nats",
		URL:        url,
		Subject:    strings.Join([]string{prefix, "{schema}", "{table}", "{token}"}, "."),
		FSMColumns: map[string]string{"public.orders": "status"},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}
