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
	err := event.Permanent(base)

	require.True(t, event.IsPermanent(err))
	require.ErrorIs(t, err, base)
	require.Equal(t, "boom", err.Error())
}

func TestPermanentAs(t *testing.T) {
	err := event.Permanent(&permErr{code: 7})

	var pe *permErr
	require.ErrorAs(t, err, &pe)
	require.Equal(t, 7, pe.code)
}

func TestPermanentNil(t *testing.T) {
	require.NoError(t, event.Permanent(nil))
	require.False(t, event.IsPermanent(nil))
}

func TestIsPermanentWrapped(t *testing.T) {
	err := fmt.Errorf("send: %w", event.Permanent(errors.New("bad")))
	require.True(t, event.IsPermanent(err))
	require.False(t, event.IsPermanent(errors.New("transient")))
}
