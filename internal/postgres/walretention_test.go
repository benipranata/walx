package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRetentionWarnings(t *testing.T) {
	i64 := func(v int64) *int64 { return &v }
	tests := []struct {
		name     string
		maxKeep  *int64
		idle     *int64
		wantWarn []string
	}{
		{"both unset", i64(-1), i64(0), []string{"max_slot_wal_keep_size", "idle_replication_slot_timeout"}},
		{"both set", i64(102400), i64(2592000), nil},
		{"only keep size unset", i64(-1), i64(2592000), []string{"max_slot_wal_keep_size"}},
		{"only idle timeout unset", i64(102400), i64(0), []string{"idle_replication_slot_timeout"}},
		{"idle timeout missing on an older server", i64(102400), nil, nil},
		{"keep size unset and idle timeout missing", i64(-1), nil, []string{"max_slot_wal_keep_size"}},
		{"both missing", nil, nil, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			warns := retentionWarnings(tt.maxKeep, tt.idle)
			require.Len(t, warns, len(tt.wantWarn))
			for i, w := range tt.wantWarn {
				require.Contains(t, warns[i], w)
			}
		})
	}
}

func TestCheckWALRetentionIntegration(t *testing.T) {
	url := startPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	check := func(t *testing.T) []string {
		t.Helper()
		c := connect(t, ctx, url)
		warns, err := CheckWALRetention(ctx, c.PgConn())
		require.NoError(t, err)
		return warns
	}
	// Defaults are -1 and 0, both of which warn; the settings are then set in
	// turn so each warning disappears on its own.
	require.Len(t, check(t), 2)
	setServerSetting(t, url, "max_slot_wal_keep_size", "100GB")
	warns := check(t)
	require.Len(t, warns, 1)
	require.Contains(t, warns[0], "idle_replication_slot_timeout")
	setServerSetting(t, url, "idle_replication_slot_timeout", "30d")
	require.Empty(t, check(t))
}
