package config

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLineKeysNameKeepsOtherErrors(t *testing.T) {
	err := errors.New("boom")
	require.Same(t, err, lineKeys{1: "a"}.name(err))
}
