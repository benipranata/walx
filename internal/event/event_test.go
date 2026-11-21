package event_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/event"
)

func TestUnchangedMarshalJSON(t *testing.T) {
	b, err := json.Marshal(event.Unchanged)
	require.NoError(t, err)
	require.JSONEq(t, `{"$unchanged":true}`, string(b))
}

func TestUnchangedInsideRow(t *testing.T) {
	row := map[string]any{"id": 1, "body": event.Unchanged}
	b, err := json.Marshal(row)
	require.NoError(t, err)
	require.JSONEq(t, `{"id":1,"body":{"$unchanged":true}}`, string(b))
}

func TestOpJSON(t *testing.T) {
	for _, op := range []event.Op{event.OpInsert, event.OpUpdate, event.OpDelete, event.OpTruncate} {
		b, err := json.Marshal(op)
		require.NoError(t, err)
		require.Equal(t, `"`+string(op)+`"`, string(b))
	}
}

func TestLSNString(t *testing.T) {
	require.Equal(t, "0/0", event.LSN(0).String())
	require.Equal(t, "16/B374D848", event.LSN(0x16B374D848).String())
}

func TestEventJSON(t *testing.T) {
	e := event.Event{
		ID:     "main-0/10-1",
		Op:     event.OpUpdate,
		Schema: "public",
		Table:  "users",
		Before: map[string]any{"id": 1},
		After:  map[string]any{"id": 1, "bio": event.Unchanged},
	}
	b, err := json.Marshal(e)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"id":"main-0/10-1","op":"update","schema":"public","table":"users",
		"before":{"id":1},"after":{"id":1,"bio":{"$unchanged":true}}
	}`, string(b))
}
