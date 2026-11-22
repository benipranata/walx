package postgres

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

func TestIsFatal(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"network error", errors.New("connection reset"), false},
		{"slot active retries", fmt.Errorf("slot: %w", ErrSlotActive), false},
		{"stream ended retries", ErrStreamEnded, false},
		{"unsupported version", fmt.Errorf("reconnect: %w", ErrUnsupportedVersion), true},
		{"auth failure", fmt.Errorf("connect: %w", &pgconn.PgError{Code: "28P01"}), true},
		{"slot missing", fmt.Errorf("x: %w", ErrSlotMissing), true},
		{"slot lost", fmt.Errorf("x: %w", ErrSlotLost), true},
		{"publication missing", fmt.Errorf("x: %w", ErrPublicationMissing), true},
		{"replica identity", fmt.Errorf("x: %w", ErrReplicaIdentity), true},
		{"tx too large", fmt.Errorf("x: %w", &TxTooLargeError{}), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, IsFatal(tt.err))
		})
	}
}
