package engine

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/benipranata/walx/internal/config"
	"github.com/benipranata/walx/internal/event"
	"github.com/benipranata/walx/internal/filter"
)

func eventIDs(b event.Batch) []string {
	ids := make([]string, len(b.Events))
	for i, e := range b.Events {
		ids[i] = e.ID
	}
	return ids
}

func TestFaultInjectionIntegration(t *testing.T) {
	url := startPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	admin, err := pgx.Connect(ctx, url)
	require.NoError(t, err)
	defer admin.Close(ctx)
	_, err = admin.Exec(ctx, `CREATE TABLE r (id int PRIMARY KEY, status text); CREATE PUBLICATION p FOR TABLE r`)
	require.NoError(t, err)

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "a crash between sink success and confirm redelivers the batch with the same IDs", run: func(t *testing.T) {
			defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
			const slot = "s_crash"
			sink1 := &memSink{}
			crashed := make(chan event.LSN, 1)
			var p1 *pipeline
			crash := func(e *Engine) {
				// The sink has succeeded; the confirm never reaches the slot.
				orig := e.confirm
				e.confirm = func(lsn event.LSN) {
					if len(sink1.Batches()) == 0 {
						orig(lsn) // idle advance before the batch exists
						return
					}
					select {
					case crashed <- lsn:
					default:
					}
					p1.cancel()
				}
			}
			p1 = startPipeline(t, ctx, url, slot, sink1, 4, nil, crash)
			before := slotFlush(t, ctx, admin, slot)
			_, err := admin.Exec(ctx, `INSERT INTO r VALUES (1, 'a'), (2, 'b')`)
			require.NoError(t, err)

			var endLSN event.LSN
			select {
			case endLSN = <-crashed:
			case <-time.After(15 * time.Second):
				t.Fatal("sink never succeeded")
			}
			select {
			case err := <-p1.done:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(10 * time.Second):
				t.Fatal("engine did not stop")
			}
			first := sink1.Batches()
			require.Len(t, first, 1)
			require.Equal(t, endLSN, first[0].EndLSN)
			require.Len(t, first[0].Events, 2)
			require.NoError(t, p1.conn.Close(ctx))

			// The slot must not have moved past the unconfirmed batch.
			require.Eventually(t, func() bool {
				var active bool
				err := admin.QueryRow(ctx,
					`SELECT active FROM pg_replication_slots WHERE slot_name = $1`, slot).Scan(&active)
				return err == nil && !active
			}, 10*time.Second, 50*time.Millisecond)
			require.Less(t, slotFlush(t, ctx, admin, slot), pglogrepl.LSN(first[0].CommitLSN))
			require.Equal(t, before, slotFlush(t, ctx, admin, slot), "slot advanced without a confirm")

			sink2 := &memSink{}
			p2 := startPipeline(t, ctx, url, slot, sink2, 4, nil)
			require.Eventually(t, func() bool { return len(sink2.Batches()) >= 1 }, 15*time.Second, 20*time.Millisecond)
			second := sink2.Batches()
			require.Len(t, second, 1, "only the unconfirmed batch is redelivered")
			require.Equal(t, eventIDs(first[0]), eventIDs(second[0]))
			require.Equal(t, first[0].CommitLSN, second[0].CommitLSN)
			require.Equal(t, first[0].EndLSN, second[0].EndLSN)
			require.Equal(t, first[0].Events[0].After, second[0].Events[0].After)
			require.Equal(t, fmt.Sprintf("itest-%s-1", first[0].CommitLSN), second[0].Events[0].ID)

			require.Eventually(t, func() bool {
				return slotFlush(t, ctx, admin, slot) >= pglogrepl.LSN(second[0].EndLSN)
			}, 10*time.Second, 100*time.Millisecond)
			p2.stop(t)
		}},
		{name: "several transitions of one row in one transaction arrive in order", run: func(t *testing.T) {
			defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
			for _, identity := range []string{"DEFAULT", "FULL"} {
				t.Run(identity, func(t *testing.T) {
					_, err := admin.Exec(ctx, `ALTER TABLE r REPLICA IDENTITY `+identity)
					require.NoError(t, err)
					slot := "s_order_" + strings.ToLower(identity)
					base := 100
					if identity == "FULL" {
						base = 200
					}
					chain := filter.New("fault", []config.TableConfig{{
						Name: "public.r",
						Ops:  []event.Op{event.OpInsert, event.OpUpdate},
					}}, nil, nil)
					sink := &memSink{}
					p := startPipeline(t, ctx, url, slot, sink, 4, chain.Apply)
					tx, err := admin.Begin(ctx)
					require.NoError(t, err)
					for _, q := range []string{
						fmt.Sprintf(`INSERT INTO r VALUES (%d, 'new')`, base),
						fmt.Sprintf(`UPDATE r SET status = 'paid' WHERE id = %d`, base),
						fmt.Sprintf(`UPDATE r SET status = 'shipped' WHERE id = %d`, base),
						fmt.Sprintf(`UPDATE r SET status = 'done' WHERE id = %d`, base),
					} {
						_, err := tx.Exec(ctx, q)
						require.NoError(t, err)
					}
					require.NoError(t, tx.Commit(ctx))

					require.Eventually(t, func() bool { return len(sink.Batches()) == 1 }, 15*time.Second, 20*time.Millisecond)
					b := sink.Batches()[0]
					require.Len(t, b.Events, 4)
					wantOps := []event.Op{event.OpInsert, event.OpUpdate, event.OpUpdate, event.OpUpdate}
					wantStatus := []string{"new", "paid", "shipped", "done"}
					for i, e := range b.Events {
						require.Equal(t, wantOps[i], e.Op)
						require.Equal(t, wantStatus[i], e.After["status"])
						require.Equal(t, fmt.Sprintf("itest-%s-%d", b.CommitLSN, i+1), e.ID)
						if identity == "FULL" && i > 0 {
							require.Equal(t, wantStatus[i-1], e.Before["status"], "update %d must follow update %d", i, i-1)
						}
					}
					require.Eventually(t, func() bool {
						return slotFlush(t, ctx, admin, slot) >= pglogrepl.LSN(b.EndLSN)
					}, 10*time.Second, 100*time.Millisecond)
					p.stop(t)
				})
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}
