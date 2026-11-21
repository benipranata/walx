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
