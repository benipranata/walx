# Configuration reference

walx reads one YAML file (default `./config.yaml`, override with `-config path`). [config.example.yaml](../config.example.yaml) is an annotated example that a test loads, so it stays valid.

Loading rules:

- Unknown keys are an error, reported with their path and line.
- `${NAME}` inside a value is replaced by the environment variable; an unset variable is an error, an empty one is not. Keys and comments are not expanded. A quoted scalar stays a string; an unquoted one (such as `${PORT}`) takes the type of the substituted value, so bool and int fields work. In flow style write the variable in block style, since `{` breaks flow mappings.
- YAML aliases and merge keys are rejected. The file holds one YAML document and no null list items.
- Durations are Go durations (`100ms`, `30s`, `15m`, `1h`). Sizes are an integer or a number with a unit (`B`, `KiB`, `MiB`, `GiB`, `TiB`, `KB`, `MB`, `GB`, `TB`).
- Every problem is reported at once, each naming its key (`databases[0].sink.path: required for a file sink`), and walx exits with code 1. Defaults apply per `databases` entry.

Top level and each `databases` entry below; the headings are the full key paths (`[]` is a list element).

## Top level

### `shutdown_timeout`
Duration, default `30s`, must be positive. How long a stopping pipeline waits for its in-flight sink call before exiting without confirming it. Shared by all databases.

### `metrics`
Section for the Prometheus endpoint and the slot lag poller, shared by all databases. See [operations](operations.md#metrics).

### `metrics.listen`
`host:port` or `:port` of the HTTP listener serving `/metrics`. Default empty: no listener, no metrics, no lag polling. A value that is not `host:port` is an error.

### `metrics.lag_interval`
Duration, default `15s`, must be positive. How often slot lag and `wal_status` are read (only when `metrics.listen` is set).

### `databases`
List with at least one entry. Each entry runs its own pipeline.

## databases[] entry

### `databases[].name`
Required and unique. Must match `^[a-z][a-z0-9_]{0,57}$` (58 characters, so `walx_<name>` fits Postgres's 63). It is `Event.Source`, the metrics and log label `database`/`db`, the prefix of `Event.ID`, and the default slot and publication name. Do not rename an entry that has a slot: its event IDs and default slot change.

### `databases[].postgres`
Connection, slot and publication of the entry.

### `databases[].postgres.url`
Required. A `postgres://` or `postgresql://` URL (a unix-socket URL names the server in the `host` query parameter). walx adds `replication=database` itself. Put the password in an environment variable: `postgres://walx:${PGPASSWORD}@host/db`.

### `databases[].postgres.slot`
Default `walx_<name>`. Matches `^[a-z_][a-z0-9_]*$`, at most 63 characters. Unique per server (host and port; scheme and credentials ignored), because slots are cluster-wide.

### `databases[].postgres.publication`
Default `walx_<name>`, at most 63 characters. Unique per server and database.

### `databases[].postgres.auto_create`
Bool, default `true`. Create the slot and publication when missing. `false` is validate-only: a missing object is an error that prints the SQL. See [setup](setup.md#what-walx-creates-auto_create-auto_replica_identity).

### `databases[].postgres.auto_replica_identity`
Bool, default `true`. Run `ALTER TABLE ... REPLICA IDENTITY FULL` for listed tables whose config needs the old row. `false` is validate-only and the error prints the statements.

### `databases[].postgres.auto_alter`
Bool, default `false`. Run `ALTER PUBLICATION` when the publication differs from the config (tables, ops, `publish_via_partition_root`, a row filter or column list that differs from `publication_where` and `publication_columns`). Without it drift is a startup error. Never switches between `FOR ALL TABLES` and a table list.

### `databases[].postgres.max_tx_bytes`
Size, default `256MiB`, must be positive. Cap on the raw tuple bytes of one buffered transaction. It is not a memory limit (decoded values use more). Exceeding it stops the pipeline with a non-retryable error; a restart reads the same transaction again, so raise the limit.

### `databases[].postgres.max_tx_events`
Integer, default `0` (unlimited), must not be negative. Cap on the events of one transaction; the same stop behaviour.

### `databases[].tables`
List of tables to stream. Omit it to publish `FOR ALL TABLES` (superuser) and stream every table with the default ops (insert, update); with a list only those tables are published, and events for others are dropped. Table names are unique within the entry.

### `databases[].tables[].name`
Required, `schema.table` (exactly one dot, no quotes). For partitioned tables use the root table.

### `databases[].tables[].ops`
List of `insert`, `update`, `delete`, `truncate`. Default `[insert, update]`; an explicit list must not be empty or hold duplicates. The publication's `publish` option is the union of ops across tables; the filter enforces each table's own list. A `delete` or `truncate` event is never emitted unless listed here.

### `databases[].tables[].fsm_column`
Optional column holding a state. The event keeps it (it cannot be in `drop_columns`), and the NATS sink uses the new value as the subject token and in the `Walx-State-From/To` headers. Needs `REPLICA IDENTITY FULL` to see the old state (set automatically). Several transitions of one row in one transaction give several events, never collapsed. Startup fails when the column does not exist.

### `databases[].tables[].where`
Map of column to scalar value. An event passes only when `After` equals every pair (`Before` for deletes; a truncate always passes). Plain equality, no expressions. Startup fails when a column does not exist. Numbers compare by value; a text value is compared with a configured number by exact decimal value. An `Unchanged` TOAST value in `After` is read from `Before`, so under the default identity an update whose where column is an unchanged TOAST value is dropped; set `REPLICA IDENTITY FULL` yourself for that case (auto-set covers `where` for deletes only).

### `databases[].tables[].on_change`
List of columns. An update passes only when one of them changed between `Before` and `After`. Applies to updates only; an unchanged TOAST value never counts as changed. Needs `REPLICA IDENTITY FULL` (set automatically); without an old row every update would be dropped. Startup fails when a column does not exist (a typo would otherwise drop every update).

### `databases[].tables[].drop_columns`
List of columns removed from `Before` and `After` (never from the key). Must not contain the `fsm_column`. Events are copied, never mutated. Startup fails when a column does not exist.

### `databases[].tables[].key`
List of columns forming `Event.Key`. Default: the primary key. Blank or duplicate names are errors, and startup fails when a column does not exist. Ideally covered by a unique index (startup warns when none is). On a delete the key must lie inside the replica identity: `FULL` covers any column and auto-set applies it for a delete-enabled table.

### `databases[].tables[].reference_only`
Bool, default `false`. Emit events without `Before` and `After`: only the key (and the `fsm_column` value in `After`) stays, so the consumer looks up the current row. Runs last, after `where`, `on_change` and `drop_columns`. A reference event has no `Before`, so no from-state; keep full events on tables that need transitions.

### `databases[].tables[].publication_where`
Optional SQL boolean expression, passed as the table's publication row filter (`FOR TABLE t WHERE (expr)`, Postgres 15+). Postgres then drops non-matching rows before walx sees them, which saves bandwidth; it is an optimization on top of `where`, not a replacement. Must not be blank or contain `;`; walx does not parse it, Postgres rejects an invalid one at startup. The expression may only use what Postgres allows in a row filter (no user functions, no system columns). **The filter rewrites ops:** an update that moves a row out of the filter arrives as a `delete`, one that moves it in arrives as an `insert` (see [events](events.md#publication-row-filters-rewrite-ops)). A table with `publication_where` that publishes `update` or `delete` needs `REPLICA IDENTITY FULL` (set automatically, since Postgres fails those statements on the source otherwise), so `delete` events carry the full old row. A hand-added filter that differs from this value is drift; walx compares the expression as Postgres prints it. See also `databases[].postgres.auto_alter`.

### `databases[].tables[].publication_columns`
Optional list of columns for the publication column list (`FOR TABLE t (a, b)`, Postgres 15+); other columns never reach walx. Must not be empty, hold duplicates or invalid identifiers, and must include the `key` columns, the `fsm_column`, the `where` columns and the `on_change` columns (the default primary key is not checked here: Postgres rejects updates and deletes when the column list misses the replica identity). Postgres also requires a column list to cover the replica identity of a table that publishes `update` or `delete`, so `REPLICA IDENTITY FULL` (needed by `fsm_column`, `on_change`, `publication_where` and `where` on deletes) (also when the table already has it) cannot be combined with a column list there; walx reports this at startup and does not alter the table. A column list that differs from the live publication is drift.

### `databases[].sink`
The one sink of the entry. See [sinks](sinks.md).

### `databases[].sink.type`
Required: `stdout`, `file` or `nats`.

### `databases[].sink.path`
File sink only, required there: the file to append to (created if missing). Two file sinks (or a file sink and a `dlq.path`) must not write to the same path.

### `databases[].sink.url`
NATS sink only, required there: a server URL or a comma-separated list. Schemes `nats`, `tls`, `ws`, `wss`.

### `databases[].sink.subject`
NATS sink only. Subject template; default `cdc.{schema}.{table}.{token}`. Placeholders `{database}`, `{schema}`, `{table}`, `{token}`; dot-separated tokens; literals must not hold whitespace, `*` or `>`.

### `databases[].sink.timeout`
Duration, default `0` (no deadline), must not be negative. Bounds each `Send` attempt. A timed-out call is retried, so a late success becomes a duplicate (dedupe by `Event.ID`). It still applies during shutdown. A sink that ignores its context is not bounded by it.

### `databases[].retry`
How a failed batch is retried. See [operations](operations.md#retry-and-errors).

### `databases[].retry.policy`
`forever` (default), `max_attempts` or `stop`. `stop` ends the pipeline on the first sink error.

### `databases[].retry.initial`
Duration, default `100ms`, must be positive. First backoff; it doubles after each failure and has jitter (both fixed).

### `databases[].retry.max`
Duration, default `30s`, must be positive and not below `initial`. Backoff ceiling.

### `databases[].retry.max_attempts`
Integer, default `0` (unlimited), must not be negative. Used by policy `max_attempts`: the total number of attempts (the first try included) before the pipeline stops with `giving up after N attempts`.

### `databases[].on_permanent_error`
`stop` (default) or `skip`. What happens when a sink reports a batch that can never succeed. `skip` counts the batch in `events_skipped_total`, confirms it and continues. Without a `dlq.path` the error log is the only record and it holds the full event payloads (your row data ends up in the logs); with one the events go to the dead-letter file and the log carries no payloads. See [operations](operations.md#permanent-errors).

### `databases[].dlq`
Optional dead-letter file for the entry. See [operations](operations.md#replaying-the-dead-letter-file).

### `databases[].dlq.path`
Default empty: no dead-letter file. The file receiving the batches skipped by `on_permanent_error: skip` (created if missing, appended to, fsynced before the batch is confirmed). Only valid with `on_permanent_error: skip`. Must not equal any file sink path or any other `dlq.path` of the config.

## Cross-entry checks

At least one database; names unique; no two entries on the same server share a slot, none on the same database share a publication; no two file sinks or dead-letter files share a path.
