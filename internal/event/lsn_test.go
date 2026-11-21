package event_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/event"
)

func TestLSNString(t *testing.T) {
	tests := []struct {
		name string
		lsn  event.LSN
		want string
	}{
		{"zero", 0, "0/0"},
		{"both halves", 0x16B374D848, "16/B374D848"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.lsn.String())
		})
	}
}

func TestLSNMarshalJSON(t *testing.T) {
	tests := []struct {
		name string
		lsn  event.LSN
		want string
	}{
		{"zero", 0, `"0/0"`},
		{"above 2^53 keeps every bit", 0xFFFFFFFFFFFFFFFF, `"FFFFFFFF/FFFFFFFF"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := json.Marshal(tt.lsn)
			require.NoError(t, err)
			require.Equal(t, tt.want, string(b))
		})
	}
}
