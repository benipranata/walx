package config

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

func TestLineKeysName(t *testing.T) {
	boom := errors.New("boom")
	tests := []struct {
		name     string
		in       error
		wantSame bool
		want     string
	}{
		{"a non-type error is returned as is", boom, true, "boom"},
		{
			"messages without a line pass through",
			&yaml.TypeError{Errors: []string{"no line here", "line 1: bad"}},
			false,
			"no line here\na (line 1): bad",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := lineKeys{1: "a"}.name(tt.in)
			require.EqualError(t, err, tt.want)
			if tt.wantSame {
				require.Same(t, tt.in, err)
			}
		})
	}
}
