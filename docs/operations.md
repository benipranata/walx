# Operations

## Retry and errors

Each database delivers its batches serially, in WAL order. When the sink returns an error the batch is retried according to `retry.*`, and the WAL position does not advance meanwhile; the reader keeps answering keepalives, so a slow sink does not drop the connection. Up to 16 decoded transactions queue behind the sink, then reading pauses (backpressure).

- `retry.policy: forever` (default): retry with exponential backoff (`retry.initial`, doubling, capped at `retry.max`, with jitter) until it succeeds or walx is stopped. Failures are logged at warn level (`sink send failed, retrying`) with the attempt number.
- `max_attempts`: stop the pipeline with `giving up after N attempts` after `retry.max_attempts` total attempts (0 is unlimited).
- `stop`: stop the pipeline on the first error.

Fail fast: when any pipeline stops, walx cancels the others, lets each finish its in-flight sink call, and exits with code 1. The first error is the one reported.

Connection loss: walx reconnects with the same backoff, resumes from the last confirmed position and discards a partly read transaction, which is read again. These stop the process instead of retrying: authentication errors, a server older than 18, a missing or lost slot, a missing publication, and a transaction over `max_tx_bytes` or `max_tx_events`. A slot still active from the dead connection is retried.

### Permanent errors

A sink returns a permanent error for a batch that can never be written (an event that cannot be encoded, a NATS message over the payload limit, a subject that cannot be built). Retry is not applied.

- `on_permanent_error: stop` (default): the pipeline stops, exit code 1.
- `skip`: the whole batch is logged at **error level with the full event payloads** (`skipping batch after permanent sink error`, attribute `batch_events`), counted in `events_skipped_total`, confirmed, and the stream continues. Row data therefore ends up in your logs: restrict and retain them accordingly, and use `drop_columns` for sensitive columns. This is the only record of the skipped events.

### Large transactions

A transaction is buffered until commit and delivered as one batch. Over `postgres.max_tx_bytes` (raw tuple bytes) or `max_tx_events` the pipeline stops with a non-retryable error naming the transaction. It is never skipped, even with `on_permanent_error: skip`. A restart reads the same transaction again, so raise the limit (or split the writer's transaction) before restarting.

## Shutdown and exit codes

SIGINT or SIGTERM starts a graceful stop: the in-flight sink call finishes (bounded by `shutdown_timeout`, default 30s), queued batches are not sent, a final position is confirmed and the sink is closed. A second signal kills the process immediately. If the in-flight call does not finish in time, walx exits without confirming that batch; it is redelivered on the next start (`shutdown timeout: ...`).

| Code | Meaning |
| --- | --- |
| 0 | Clean stop after SIGINT or SIGTERM |
| 1 | Config error, startup check failed, a pipeline stopped (sink, Postgres or limit error) or shutdown timed out. A pipeline that returns while walx was not asked to stop is also an error |
| 2 | Usage error (unknown command or flag) |

`walx snapshot [-database name]` is reserved and exits 1 with "not implemented".

## Logging

Structured text logs (`log/slog`) go to stderr at info level; there is no log configuration. Pipeline messages carry `db=<name>`. Notable messages:

| Level | Message |
| --- | --- |
| info | `startup checks passed` (slot, publication, `slot_created`), `set replica identity full` (per table altered), `walx started`, `reconnected`, `shutting down, waiting for in-flight sink call`, `walx stopped` |
| warn | `sink send failed, retrying`, `reconnecting`, `dropping events by config filter` (first drop per table and op; the rest only counted in `events_dropped_total`), startup warnings (`max_slot_wal_keep_size is -1 ...`, replica identity and key warnings), `slot lag poll failed` |
| error | `skipping batch after permanent sink error` (full payloads), `sink send failed` and `shutdown timed out` on stop, `database pipeline failed, stopping the others` |

stdout is reserved for the stdout sink's events.

## Metrics

Set `metrics.listen` (for example `127.0.0.1:9187`) to serve Prometheus metrics at `/metrics`; any other path is 404. There is no authentication or TLS: bind to localhost or a private interface, or put a proxy in front. Without `metrics.listen` nothing is collected. Names carry the `walx_` prefix and every metric has a `database` label (the entry name).

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `walx_events_total` | counter | `database` | Events delivered to the sink (after filters). `rate()` gives events per second |
| `walx_events_dropped_total` | counter | `database`, `op`, `table` | Events removed by a filter (table, op, `where`, `on_change`) |
| `walx_events_skipped_total` | counter | `database` | Events skipped after a permanent sink error |
| `walx_sink_latency_seconds` | histogram | `database` | Duration of each sink `Send` attempt, failures included. Buckets 5ms to 60s |
| `walx_slot_lag_bytes` | gauge | `database` | `pg_current_wal_lsn()` minus the slot's `confirmed_flush_lsn` |
| `walx_wal_status` | gauge | `database` | The slot's `wal_status`: reserved 0, extended 1, unreserved 2, lost 3, unknown -1 |

Slot lag and `wal_status` come from `pg_replication_slots`, read every `metrics.lag_interval` (default 15s) over a separate plain connection per database (the role needs a normal `pg_hba.conf` entry, see [setup](setup.md#pg_hbaconf)); a failed read logs a warning and retries.

Suggested alerts: `walx_wal_status >= 1` (the slot is past `max_wal_size`, act before it is lost), `walx_slot_lag_bytes` growing for several minutes, `rate(walx_events_skipped_total[5m]) > 0`, and a rising `walx_sink_latency_seconds` p99.

## WAL retention (max_slot_wal_keep_size)

A slot makes Postgres keep all WAL the consumer has not confirmed. If walx is down or its sink is stuck the WAL grows without bound unless you cap it:

- `max_slot_wal_keep_size`: when the retained WAL exceeds it, the slot is invalidated (`wal_status = lost`). Recommended `100GB` or what your disk affords. `-1` (unlimited) makes walx warn at startup.
- `idle_replication_slot_timeout` (Postgres 18): invalidates a slot inactive for that long. Recommended `30d`; `0` makes walx warn.

walx only reads and warns; the settings are yours. The cap trades disk safety for the risk of a lost slot, so alert on `walx_wal_status` first.

## Runbook: abandoned slot

Symptom: `walx_slot_lag_bytes` keeps growing, disk fills on the source, or `pg_replication_slots` shows an inactive slot with a large backlog. walx never drops slots, so removing a database entry, renaming it (the default slot name changes) or retiring walx leaves the old slot behind.

```sql
SELECT slot_name, active, wal_status,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained
FROM pg_replication_slots
WHERE slot_name LIKE 'walx\_%';
```

1. Decide whether the slot is still wanted. If walx is only down, start it again: it resumes from the confirmed position.
2. If it is abandoned, drop it. Postgres refuses while it is active (`active = true`; stop the consumer or `pg_terminate_backend(active_pid)` first):

   ```sql
   SELECT pg_drop_replication_slot('walx_orders');
   DROP PUBLICATION IF EXISTS walx_orders;
   ```

   The WAL is released at the next checkpoint.
3. If the disk is about to fill and walx must stay, set `max_slot_wal_keep_size` (a reload is enough) so Postgres invalidates the slot instead of crashing; then follow the lost-slot steps.

## Lost-slot recovery

A slot is lost when its WAL was removed (`max_slot_wal_keep_size` or `idle_replication_slot_timeout` invalidated it). walx detects `wal_status = lost` or an invalidation reason at startup and stops with `replication slot is lost`; a slot that disappears while running stops it with `replication slot does not exist` on the next reconnect. walx never recreates a running pipeline's slot silently, because the changes in the gap are gone for good and the consumers would drift without noticing.

Beware of the one exception: at startup, a slot that does not exist is created when `postgres.auto_create` is true, so a slot someone dropped by hand silently restarts the stream from the current position. Set `auto_create: false` if you want that to be an error.

A resync is a stage 2 snapshot, which is not implemented yet. Until then:

1. Stop walx and fix the cause (a stuck sink, too small a limit).
2. Drop the lost slot: `SELECT pg_drop_replication_slot('walx_orders');`
3. Start walx with `postgres.auto_create: true` (or create the slot by hand). It creates a new slot at the current position and streams from there; events from the gap are not recoverable from walx.
4. Rebuild the consumers' state from the source tables yourself (a copy taken after step 3 started, applied as idempotent upserts, then the stream applied on top). Events delivered while the copy runs may be older or newer than the copied rows, which is why consumers must apply them idempotently, last write wins by commit order.

## Postgres versions and privileges

Postgres 18 and later only; see [setup](setup.md) for server settings, the role, required privileges and the opt-out SQL.
