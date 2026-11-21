package event_test

import (
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
