package event_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/event"
)

func TestUnchangedMarshalJSON(t *testing.T) {
	tests := []struct {
		name string
		in   any
		want string
	}{
		{"standalone", event.Unchanged, `{"$unchanged":true}`},
		{"inside a row", map[string]any{"id": 1, "body": event.Unchanged}, `{"id":1,"body":{"$unchanged":true}}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := json.Marshal(tt.in)
			require.NoError(t, err)
			require.JSONEq(t, tt.want, string(b))
		})
	}
}
