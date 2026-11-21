package event_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/event"
)

func TestOpJSON(t *testing.T) {
	for _, op := range []event.Op{event.OpInsert, event.OpUpdate, event.OpDelete, event.OpTruncate, event.OpSnapshot} {
		b, err := json.Marshal(op)
		require.NoError(t, err)
		require.Equal(t, `"`+string(op)+`"`, string(b))
	}
}
