package config_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benipranata/walx/internal/config"
)

func TestByteSize(t *testing.T) {
	tests := []struct {
		in   string
		want config.ByteSize
	}{
		{"1024", 1024},
		{"512B", 512},
		{"512KiB", 512 << 10},
		{"256MiB", 256 << 20},
		{"2GiB", 2 << 30},
		{"1TiB", 1 << 40},
		{"1KB", 1000},
		{"3MB", 3_000_000},
		{"1GB", 1_000_000_000},
		{"1TB", 1_000_000_000_000},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			cfg := mustParse(t, mk("      max_tx_bytes: "+tt.in+"\n", ""))
			require.Equal(t, tt.want, cfg.Databases[0].Postgres.MaxTxBytes)
		})
	}
}

func TestByteSizeInvalid(t *testing.T) {
	tests := []struct{ name, in string }{
		{"not a number", "abc"},
		{"unit only", "MiB"},
		{"unknown unit", "1XiB"},
		{"fraction", "1.5MiB"},
		{"negative", "-1MiB"},
		{"sequence", "[1]"},
		{"overflow", "99999999999TiB"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := config.Parse([]byte(mk("      max_tx_bytes: "+tt.in+"\n", "")))
			require.ErrorContains(t, err, "max_tx_bytes")
		})
	}
}

func TestByteSizeString(t *testing.T) {
	tests := []struct {
		name string
		in   config.ByteSize
		want string
	}{
		{"zero", 0, "0B"},
		{"one binary unit", 1 << 10, "1KiB"},
		{"binary unit", 256 << 20, "256MiB"},
		{"plain bytes", 1500, "1500B"},
		{"decimal sizes are not used", 1000, "1000B"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.in.String())
		})
	}
}
