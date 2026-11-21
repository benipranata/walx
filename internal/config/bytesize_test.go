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
	for _, in := range []string{"abc", "MiB", "1XiB", "1.5MiB", "-1MiB", "[1]", "99999999999TiB"} {
		t.Run(in, func(t *testing.T) {
			_, err := config.Parse([]byte(mk("      max_tx_bytes: "+in+"\n", "")))
			require.ErrorContains(t, err, "max_tx_bytes")
		})
	}
}

func TestByteSizeString(t *testing.T) {
	require.Equal(t, "256MiB", config.ByteSize(256<<20).String())
	require.Equal(t, "1500B", config.ByteSize(1500).String())
}
