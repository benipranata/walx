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
- `skip`: the whole batch is counted in `events_skipped_total`, confirmed, and the stream continues. Where the events go depends on `dlq.path`:
  - With `dlq.path` the events are appended to the [dead-letter file](#replaying-the-dead-letter-file) and fsynced first; only then is the batch confirmed. If that write fails it is retried per `retry` and never skipped: the pipeline stops (exit 1) when the retry policy gives up, and the batch is not confirmed. The error log then holds the LSNs, the event count and the error, but no row data.
  - Without `dlq.path` the whole batch is logged at **error level with the full event payloads** (`skipping batch after permanent sink error`, attribute `batch_events`). Row data therefore ends up in your logs: restrict and retain them accordingly, and use `drop_columns` for sensitive columns. This is the only record of the skipped events.

`walx snapshot` follows the same rules for its chunks (dead-letter write included).

### Replaying the dead-letter file

`dlq.path` (needs `on_permanent_error: skip`) is a JSON-lines file with one envelope per skipped event, in the order the sink rejected them:

```json
{"failed_at":"2026-03-04T04:06:07.000000089Z","database":"orders","sink":"nats","error":"message too big","event":{"id":"orders-0/16B3748-1","source":"orders", ...}}
```

`failed_at` is when the batch was skipped (UTC, one value per batch), `error` the sink's error text, `sink` its `sink.type`. `event` is byte for byte the line the sink would have received, with its original `id`, so a replay is an ordinary at-least-once redelivery: consumers that dedupe by `id` need nothing new, and there is no replay marker or CLI. A consumer that must tell a replay apart reads the envelope, which only the operator holds.

The file is opened in append mode, fsynced before a batch is confirmed and never rotated (rotate it like the [file sink](sinks.md#file): copy-and-truncate, or restart walx). Like the file sink, walx ends a torn last line when it opens the file, so the next envelope starts on a fresh line; the file can still contain that torn line after a crash, repeated lines after a retried write, and the same event again after a restart, so readers skip lines that are not valid JSON and dedupe by `event.id`. An event that cannot be JSON-encoded at all (for example a float `NaN` in a column, which also makes a file sink fail) cannot be dead-lettered: that error is permanent for the dead letter too and stops the pipeline.

To replay:

1. Find and fix the cause. Read `error`: `jq -rR 'fromjson? | .error' orders.dlq.jsonl | sort | uniq -c` counts them per error. A message over the NATS payload limit needs a bigger server limit or `drop_columns`; an unbuildable subject needs a config or schema fix.
2. Extract the events, skipping torn lines, and keep only what you want to resend: `jq -cR 'fromjson? | .event' orders.dlq.jsonl > replay.jsonl` (add a filter such as `select(.error | contains("too big")) | .event` to pick some).
3. Feed `replay.jsonl` to the destination the sink writes to, as it would have received them:
   - stdout or file sink: append the lines to the consumer's input, for example `cat replay.jsonl >> /var/lib/walx/orders.jsonl` (stop walx first or accept interleaving; the file sink never reopens the file, so append only to a file nobody else truncates).
   - nats sink: publish each line with the subject the sink would have used (`{database}`, `{schema}`, `{table}` and the token, see [sinks](sinks.md#subjects)) and the `Nats-Msg-Id` header set to the event `id`, so the stream drops duplicates inside its window, for example with `nats pub --header Nats-Msg-Id:<id> <subject> '<line>'` per line (`source`, `schema`, `table`, `op` and `id` are in each event). Replayed events arrive after newer live events, so consumers must apply them idempotently, as with any redelivery.
4. Move or truncate the dead-letter file once the replay is done, so the next replay holds only new failures.

Replayed events keep their original order within the file, not relative to the live stream.

### Large transactions

A transaction is buffered until commit and delivered as one batch. Over `postgres.max_tx_bytes` (raw tuple bytes) or `max_tx_events` the pipeline stops with a non-retryable error naming the transaction. It is never skipped, even with `on_permanent_error: skip`. A restart reads the same transaction again, so raise the limit (or split the writer's transaction) before restarting.

## Shutdown and exit codes

SIGINT or SIGTERM starts a graceful stop: the in-flight sink call finishes (bounded by `shutdown_timeout`, default 30s), queued batches are not sent, a final position is confirmed and the sink is closed. A second signal kills the process immediately. If the in-flight call does not finish in time, walx exits without confirming that batch; it is redelivered on the next start (`shutdown timeout: ...`).

| Code | Meaning |
| --- | --- |
| 0 | Clean stop after SIGINT or SIGTERM |
| 1 | Config error, startup check failed, a pipeline stopped (sink, Postgres or limit error) or shutdown timed out. A pipeline that returns while walx was not asked to stop is also an error |
| 2 | Usage error (unknown command or flag) |

`walx snapshot` uses the same codes: 0 when finished, 1 for a config, Postgres, sink or interrupted run, 2 for a usage error (bad flag, unknown `-database`, `-table` outside the config).

## Logging

Structured text logs (`log/slog`) go to stderr at info level; there is no log configuration. Pipeline messages carry `db=<name>`. Notable messages:

| Level | Message |
| --- | --- |
| info | `startup checks passed` (slot, publication, `slot_created`), `set replica identity full` (per table altered), `walx started`, `reconnected`, `shutting down, waiting for in-flight sink call`, `walx stopped` |
| warn | `sink send failed, retrying`, `reconnecting`, `dropping events by config filter` (first drop per table and op; the rest only counted in `events_dropped_total`), startup warnings (`max_slot_wal_keep_size is -1 ...`, replica identity and key warnings), `slot lag poll failed` |
| error | `skipping batch after permanent sink error` (full payloads without `dlq.path`; with it the `... written to the dead-letter file` variant without payloads), `sink send failed` and `shutdown timed out` on stop, `database pipeline failed, stopping the others` |

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

## Snapshot

`walx snapshot` delivers the rows that exist now as `op: snapshot` events, through the filters and the sink of one `databases` entry. Use it to backfill a new consumer or to resync after a lost slot.

```
walx snapshot [-config path] [-database name] [-table schema.table]... [-where expr] [-chunk n]
```

- `-database`: the entry to read. Optional when the config has one entry.
- `-table`: restrict to these tables (repeatable). Default: the entry's `tables`, or every user table (partitions are read through their root) when it lists none. With a `tables` list, `-table` must name listed tables.
- `-where`: a SQL boolean expression limiting the rows, only with exactly one selected table, e.g. `-table public.orders -where "created_at > now() - interval '30 days'"`. It runs as one statement in a read-only transaction. The config `where` applies on top, as for live events, and so does `publication_where` (it is ANDed into the read, so the snapshot holds the rows the stream would send).
- `-chunk`: rows per batch (default 1000). Each chunk is one `Send`, with the entry's `retry`, `on_permanent_error` and `sink.timeout` (default 60s); a failed chunk stops the snapshot after the retry policy gives up.

How it works: walx creates a temporary logical slot (`walx_snap_<random>`) and exports its snapshot, imports it in a read-only `REPEATABLE READ` transaction on a second connection, and drops the slot at once. Every table is read in that transaction, so the rows are exactly the database as of the slot's consistent point, in key order (the configured `key`, else the primary key; a table with neither is read unordered and its events carry no `key`). The persistent slot, its confirmed position and the stream are never touched. The role needs `REPLICATION` and `SELECT` on the tables (`CONNECT` and the `pg_hba.conf` entry of the stream, see [setup](setup.md)).

Events: `op` is `snapshot`, `lsn` is the consistent point and `id` is `<name>-<consistentLSN>-<seq>` with `seq` counting rows from 1 over the whole snapshot, tables in name order (or in the order of the `tables` list or `-table` flags), before filters. Values render exactly like the `after` of an insert event ([type mapping](events.md#type-mapping)); generated columns are left out as in the stream, and `publication_columns` limits the columns. `commit_ts` is the time the snapshot started and `tx_id` is 0. A rerun gets a new consistent point and therefore new IDs, so IDs only dedupe a retried chunk within one run; consumers must apply snapshot events as idempotent upserts keyed by `key`.

Order of operations, so that nothing between the snapshot and the stream is lost:

1. Start the live walx first (or at least let it create its slot). The slot pins the WAL from its creation on.
2. Run `walx snapshot`. Its consistent point lies after the live slot's start, so every change committed after it is also in the live stream.
3. Consumers apply both as idempotent upserts. Changes committed before the consistent point are in the snapshot and may also appear in the stream (overlap, harmless). A snapshot row can arrive after a newer live event for the same key, so for exact results load the snapshot into the target before applying the live stream, or compare the `lsn` (a snapshot event's `lsn` is the consistent point, a live event with a higher commit position is newer).

Caveats: the snapshot holds a transaction open for its whole run, which delays vacuum of the source; a table without an index on its key sorts the whole table first. Deletes are not represented: rows deleted before the snapshot are simply absent. A NULL in a configured `key` column stops the snapshot (a primary key cannot be NULL). Logs show the table name, never row data or credentials; metrics are not served by this command.

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

A resync is a [snapshot](#snapshot):

1. Stop walx and fix the cause (a stuck sink, too small a limit).
2. Drop the lost slot: `SELECT pg_drop_replication_slot('walx_orders');`
3. Start walx with `postgres.auto_create: true` (or create the slot by hand). It creates a new slot at the current position and streams from there; events from the gap are not recoverable from walx.
4. Once walx streams again (step 3 created the slot), run `walx snapshot -database orders` to rebuild the consumers' state. Snapshot events and live events may overlap, which is why consumers must apply them idempotently.

## Postgres versions and privileges

Postgres 18 and later only; see [setup](setup.md) for server settings, the role, required privileges and the opt-out SQL.
