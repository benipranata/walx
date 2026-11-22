package postgres

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConnectPlainErrors(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		wantErr string
	}{
		{"unparsable url", "postgres://%zz", "parse postgres url"},
		{"unreachable", "postgres://u:p@127.0.0.1:1/db?connect_timeout=1", "connect"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ConnectPlain(context.Background(), tt.url)
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}
