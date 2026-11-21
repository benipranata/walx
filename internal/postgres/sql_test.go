package postgres

import (
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

func TestQuoteLiteral(t *testing.T) {
	tests := []struct{ name, in, want string }{
		{name: "plain", in: "walx_a", want: `'walx_a'`},
		{name: "quote doubled", in: "o'brien", want: `'o''brien'`},
		{name: "backslash uses E", in: `a\b`, want: `E'a\\b'`},
		{name: "backslash and quote", in: `a\'b`, want: `E'a\\''b'`},
		{name: "nul dropped", in: "a\x00b", want: `'ab'`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, quoteLiteral(tt.in))
		})
	}
}

func TestPgCode(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "pg error", err: &pgconn.PgError{Code: "42501"}, want: "42501"},
		{name: "wrapped", err: fmt.Errorf("x: %w", &pgconn.PgError{Code: "55006"}), want: "55006"},
		{name: "other", err: fmt.Errorf("boom"), want: ""},
		{name: "nil", err: nil, want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, pgCode(tt.err))
		})
	}
}
