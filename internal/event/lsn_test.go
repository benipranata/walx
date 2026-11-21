package event_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/event"
)

func TestLSNString(t *testing.T) {
	require.Equal(t, "0/0", event.LSN(0).String())
	require.Equal(t, "16/B374D848", event.LSN(0x16B374D848).String())
}
