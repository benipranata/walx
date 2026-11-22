package postgres

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	evmock "github.com/benipranata/walx/internal/event/mock"
)

// gaugeStore keeps the last value and labels of every Gauge call on its mock.
type gaugeStore struct {
	*evmock.MockMetrics
	mu     sync.Mutex
	gauges map[string]float64
	labels map[string][]string
}

func newGaugeMetrics(t *testing.T) *gaugeStore {
	m := &gaugeStore{MockMetrics: evmock.NewMockMetrics(t), gauges: map[string]float64{}, labels: map[string][]string{}}
	m.EXPECT().Gauge(mock.Anything, mock.Anything).Run(m.record).Maybe()
	m.EXPECT().Gauge(mock.Anything, mock.Anything, mock.Anything).Run(m.record).Maybe()
	return m
}

func (m *gaugeStore) record(name string, v float64, labels ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gauges[name], m.labels[name] = v, labels
}

func (m *gaugeStore) get(name string) (float64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.gauges[name]
	return v, ok
}

func TestWALStatusValue(t *testing.T) {
	tests := []struct {
		status string
		want   float64
	}{
		{"reserved", 0}, {"extended", 1}, {"unreserved", 2}, {"lost", 3}, {"", -1}, {"other", -1},
	}
	for _, tt := range tests {
		t.Run(tt.status, func(t *testing.T) {
			require.Equal(t, tt.want, WALStatusValue(tt.status))
		})
	}
}

func TestLagPollerPoll(t *testing.T) {
	boom := errors.New("boom")
	tests := []struct {
		name     string
		lag      SlotLag
		err      error
		wantLag  float64
		wantStat float64
		wantSet  bool
	}{
		{name: "reports lag and status", lag: SlotLag{Bytes: 1234, WALStatus: "extended"}, wantLag: 1234, wantStat: 1, wantSet: true},
		{name: "a read error reports nothing", err: boom},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newGaugeMetrics(t)
			p := &LagPoller{name: "main", metrics: m, read: func(context.Context) (SlotLag, error) { return tt.lag, tt.err }}
			require.ErrorIs(t, p.poll(context.Background()), tt.err)
			lag, ok := m.get("slot_lag_bytes")
			require.Equal(t, tt.wantSet, ok)
			require.Equal(t, tt.wantLag, lag)
			stat, _ := m.get("wal_status")
			require.Equal(t, tt.wantStat, stat)
			if tt.wantSet {
				require.Equal(t, []string{"database", "main"}, m.labels["slot_lag_bytes"])
				require.Equal(t, []string{"database", "main"}, m.labels["wal_status"])
			}
		})
	}
}

func TestLagPollerRun(t *testing.T) {
	tests := []struct {
		name string
		errs int
	}{
		{name: "polls until cancelled"},
		{name: "keeps polling after errors", errs: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newGaugeMetrics(t)
			var mu sync.Mutex
			calls := 0
			p := &LagPoller{name: "main", metrics: m, interval: time.Millisecond, read: func(context.Context) (SlotLag, error) {
				mu.Lock()
				defer mu.Unlock()
				calls++
				if calls <= tt.errs {
					return SlotLag{}, errors.New("down")
				}
				return SlotLag{Bytes: 7, WALStatus: "reserved"}, nil
			}}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- p.Run(ctx) }()
			require.Eventually(t, func() bool { v, ok := m.get("slot_lag_bytes"); return ok && v == 7 }, 5*time.Second, time.Millisecond)
			cancel()
			require.ErrorIs(t, <-done, context.Canceled)
		})
	}
}

func TestLagIntegration(t *testing.T) {
	url := startPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	admin, err := pgx.Connect(ctx, url)
	require.NoError(t, err)
	defer admin.Close(ctx)
	_, err = admin.Exec(ctx, `CREATE TABLE t (id int primary key, v text); CREATE PUBLICATION p FOR TABLE t`)
	require.NoError(t, err)
	c := connect(t, ctx, url)
	_, err = EnsureSlot(ctx, c.PgConn(), "lag_slot", true)
	require.NoError(t, err)

	m := newGaugeMetrics(t)
	p := NewLagPoller(url, "main", "lag_slot", time.Second, m, nil)
	defer p.Close(ctx)
	require.NoError(t, p.poll(ctx))
	before, ok := m.get("slot_lag_bytes")
	require.True(t, ok)
	stat, _ := m.get("wal_status")
	require.Equal(t, WALStatusValue("reserved"), stat)

	// Nothing confirms the slot, so its lag grows with the WAL written.
	_, err = admin.Exec(ctx, `INSERT INTO t SELECT g, repeat('x', 1000) FROM generate_series(1, 200) g`)
	require.NoError(t, err)
	require.NoError(t, p.poll(ctx))
	after, _ := m.get("slot_lag_bytes")
	require.Greater(t, after, before+100000)

	missing := NewLagPoller(url, "main", "no_such_slot", time.Second, m, nil)
	defer missing.Close(ctx)
	require.ErrorIs(t, missing.poll(ctx), ErrSlotMissing)
}
