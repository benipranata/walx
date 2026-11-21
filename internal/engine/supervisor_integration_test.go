package engine

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/benipranata/walx/internal/config"
	"github.com/benipranata/walx/internal/postgres"
)

// newDBEngine prepares a table, publication, slot and a not yet running
// engine for one throwaway database.
func newDBEngine(t *testing.T, ctx context.Context, name, slot string, sink *memSink) (*Engine, *pgx.Conn) {
	t.Helper()
	url := startPostgres(t)
	admin, err := pgx.Connect(ctx, url)
	require.NoError(t, err)
	t.Cleanup(func() { admin.Close(ctx) })
	_, err = admin.Exec(ctx, `CREATE TABLE t (id int PRIMARY KEY); CREATE PUBLICATION p FOR TABLE t`)
	require.NoError(t, err)
	conn := postgres.New(config.PostgresConfig{URL: url}, config.RetryConfig{}, nil)
	t.Cleanup(func() { _ = conn.Close(ctx) })
	_, err = conn.Connect(ctx)
	require.NoError(t, err)
	info, err := postgres.EnsureSlot(ctx, conn.PgConn(), slot, true)
	require.NoError(t, err)
	require.NoError(t, postgres.StartStream(ctx, conn.PgConn(), slot, "p", 0))
	sb, err := postgres.NewStandby(conn.PgConn(), info.ConfirmedFlushLSN, time.Second)
	require.NoError(t, err)
	buf := postgres.NewTxBuffer(name, postgres.NewRelations(), nil)
	return New(Config{Name: name, Source: sb, Buffer: buf, Sink: sink}), admin
}

func TestSupervisorIntegration(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	sinkA, sinkB := &memSink{}, &memSink{}
	a, adminA := newDBEngine(t, ctx, "dba", "sup_a", sinkA)
	b, adminB := newDBEngine(t, ctx, "dbb", "sup_b", sinkB)

	s := Supervisor{Engines: []Named{{"dba", a.Run}, {"dbb", b.Run}}}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- s.Run(runCtx) }()

	for i := 1; i <= 5; i++ {
		_, err := adminA.Exec(ctx, `INSERT INTO t VALUES ($1)`, i)
		require.NoError(t, err)
		_, err = adminB.Exec(ctx, `INSERT INTO t VALUES ($1)`, i+100)
		require.NoError(t, err)
	}
	require.Eventually(t, func() bool { return len(sinkA.Batches()) == 5 && len(sinkB.Batches()) == 5 }, 10*time.Second, 20*time.Millisecond)
	for i := 0; i < 5; i++ {
		require.Equal(t, "dba", sinkA.Batches()[i].Events[0].Source)
		require.EqualValues(t, i+1, sinkA.Batches()[i].Events[0].After["id"])
		require.Equal(t, "dbb", sinkB.Batches()[i].Events[0].Source)
		require.EqualValues(t, i+101, sinkB.Batches()[i].Events[0].After["id"])
	}
	stop()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(10 * time.Second):
		t.Fatal("supervisor did not stop")
	}
}
