package config

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

func TestLineKeysNameKeepsOtherErrors(t *testing.T) {
	err := errors.New("boom")
	require.Same(t, err, lineKeys{1: "a"}.name(err))
}

func TestLineKeysNameKeepsMessagesWithoutLine(t *testing.T) {
	err := lineKeys{1: "a"}.name(&yaml.TypeError{Errors: []string{"no line here", "line 1: bad"}})
	require.EqualError(t, err, "no line here\na (line 1): bad")
}
