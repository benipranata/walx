package event_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/event"
)

func TestOpJSON(t *testing.T) {
	tests := []struct {
		op   event.Op
		want string
	}{
		{event.OpInsert, `"insert"`},
		{event.OpUpdate, `"update"`},
		{event.OpDelete, `"delete"`},
		{event.OpTruncate, `"truncate"`},
		{event.OpSnapshot, `"snapshot"`},
	}
	for _, tt := range tests {
		t.Run(string(tt.op), func(t *testing.T) {
			b, err := json.Marshal(tt.op)
			require.NoError(t, err)
			require.Equal(t, tt.want, string(b))
		})
	}
}
