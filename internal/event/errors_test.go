package event_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/event"
)

type permErr struct{ code int }

func (e *permErr) Error() string { return fmt.Sprint("code ", e.code) }

func TestPermanent(t *testing.T) {
	base := errors.New("boom")
	tests := []struct {
		name    string
		in      error
		wantNil bool
		wantMsg string
	}{
		{"wraps an error", base, false, "boom"},
		{"nil stays nil", nil, true, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := event.Permanent(tt.in)
			if tt.wantNil {
				require.NoError(t, err)
				return
			}
			require.True(t, event.IsPermanent(err))
			require.ErrorIs(t, err, tt.in)
			require.Equal(t, tt.wantMsg, err.Error())
		})
	}
}

func TestPermanentAs(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		wantCode int
	}{
		{"typed cause", event.Permanent(&permErr{code: 7}), 7},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var pe *permErr
			require.ErrorAs(t, tt.err, &pe)
			require.Equal(t, tt.wantCode, pe.code)
		})
	}
}

func TestIsPermanent(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"permanent", event.Permanent(errors.New("bad")), true},
		{"wrapped permanent", fmt.Errorf("send: %w", event.Permanent(errors.New("bad"))), true},
		{"transient", errors.New("transient"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, event.IsPermanent(tt.err))
		})
	}
}
