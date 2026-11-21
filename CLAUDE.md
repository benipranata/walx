# CLAUDE.md

## Project

walx is a standalone Go executable (`cmd/walx`) that turns the Postgres WAL (pgoutput, Postgres 18+ only) into an ordered, at-least-once event stream written to a sink. One `config.yaml` lists several databases; each gets an independent pipeline (connection, slot, sink, retry, LSN checkpoint). No public Go API, no user Go code.

Status: milestones M1 (config, connection, slot, publication, stream start, standby writer, `cmd/walx` snapshot stub) and M2 (`internal/postgres`: relation cache, pgoutput decoding, TOAST marker, transaction buffering, large-transaction cap, schema change; T2.1 to T2.6) are implemented; `internal/event` and `internal/config` too. The postgres integration tests run against a real Postgres 18 (see Commands). The engine run loop (`internal/engine`, T3.1, T3.2 and T3.7) and `internal/filter` op filtering (T4.1) exist; permanent errors (T3.3) and idle advance (T3.4) graceful shutdown (T3.5) and fault-injection tests (T3.6) are done; M3 remaining: supervisor (T3.8); sinks are not started.

## Architecture rules

- A sink returns `nil` only when the batch is durable at the destination; only then does the engine confirm the batch's `EndLSN`. Retry, idle advance and shutdown follow from this.
- Every package except `cmd/walx` lives under `internal/`; each sink in `internal/sink/<name>`.
- `internal/event` holds the shared types (`Event`, `Batch`, `Op`, `Sink`, `Metrics`, `Unchanged`, `Permanent`). Engine code depends on the `event.Metrics` interface, never a concrete metrics package.
- `Event.ID` is `<name>-<commitLSN>-<seq>`, with `seq` counted over the raw transaction before filters, so IDs stay stable across redelivery and config changes.
- Delivery is serial and in WAL order within one database; no ordering across databases. Fail fast: one pipeline stopping shuts the others down and exits non-zero.
- Filters must not mutate shared events; copy first.
- Config: yaml `Node.Decode` ignores `KnownFields`, so `internal/config/keys.go` rejects unknown keys itself, before defaults and decoding.
- Config: YAML aliases and merge keys are rejected (they would skip env expansion and key checks). `${VAR}` expansion clears the `!!str` tag of changed plain scalars so bool and int fields work; quoted scalars stay strings. In YAML tests write `${VAR}` in block style, since `{` breaks flow mappings.
- Config: database names match `^[a-z][a-z0-9_]{0,57}$` (58 chars, so `walx_<name>` fits 63) and slots `[a-z_][a-z0-9_]{0,62}` (a leading digit breaks CREATE_REPLICATION_SLOT), since the name feeds the default slot, publication and `Event.ID`. Duplicate ops in a table are rejected; `serverOf` falls back to the `host` query parameter for unix-socket URLs. A config holds one YAML document and no null list items (yaml would drop them and shift error indexes).
- CLI: `cmd/walx` logic lives in `run(args, stdout, stderr) int` so exit codes and messages are testable; exit 2 for usage errors, 1 for runtime errors including "not implemented".
- Postgres: a replication=database walsender accepts plain SQL only over the simple protocol, so use `pgconn.Exec`, never parameterised `pgx` queries.
- Postgres: after `StartReplication` fails, pglogrepl leaves `ReadyForQuery` unread; reconnect instead of reusing the connection. The slot can stay active briefly after a killed backend, so the engine treats `ErrSlotActive` as retryable on reconnect and fatal at first start.
- Postgres: `pglogrepl.SendStandbyStatusUpdate` replaces a flush or apply LSN of 0 with the write position and would confirm unhandled WAL, so `NewStandby` rejects a zero start; seed it with `SlotInfo.ConfirmedFlushLSN`. `StartStream` still takes 0 to resume from the slot.
- Postgres: pgconn is not safe for concurrent use; `Standby.Run` is the single goroutine that receives (with a deadline so the timer fires) and writes. `ReceiveMessage` after a timeout loses no data; copy `WALData`, since pgconn reuses its buffer.
- Postgres: `START_REPLICATION` does not fail for a missing publication; pgoutput warns (55000) and then streams nothing, so `Standby.Run` ends with `ErrPublicationMissing` on that notice. The replication scanner has no `E''` strings, so quote option values with `''` doubling only (`replLiteral`); SQL statements use `quoteLiteral`.
- Postgres: send multi-statement DDL as one simple-protocol query so it runs in one transaction. A hand-added row filter or column list on a published table counts as drift.
- Postgres: `FOR ALL TABLES` needs superuser, also on 18. With `publish_via_partition_root`, `pg_publication_tables` lists the root table. `auto_alter` never switches between `FOR ALL TABLES` and a table list.
- Postgres: with REPLICA IDENTITY FULL the old tuple of an update carries the full TOAST value (recorded by `TestReplicaIdentityFullToastIntegration`), while the new tuple has `'u'` (`event.Unchanged`); so Before holds the value and After does not.
- Postgres: `recordFixture` runs `dml` as one simple-protocol query, so its statements share one implicit transaction; end each with `COMMIT;` and start the next with `BEGIN;` to record separate commits.
- Postgres: `TxBuffer` numbers `seq` from 1 over raw DML events (a truncate counts once per relation). pglogrepl returns commit times in the local zone, so `TxBuffer` converts to UTC.
- Postgres: `Conn.Reconnect` retries until ctx ends, except fatal errors (`isFatalConnect`: `ErrUnsupportedVersion`, SQLSTATE class 28 or 3D, URL parse errors), which return at once; never `event.Permanent`. The `onReset` hook must call `Relations.Reset` and `TxBuffer.Reset`. Connect defaults to a 10s timeout, and `IdentifySystem` and `pgConn.SendStatus` are bounded by deadlines.
- Postgres: `TxBuffer` caps a transaction by raw tuple bytes (`postgres.max_tx_bytes`, not a memory limit) and event count (`postgres.max_tx_events`, 0 = unlimited). Overflow returns a `*TxTooLargeError`, deliberately not `event.Permanent`: `on_permanent_error: skip` would silently drop the whole transaction.
- Postgres: the engine must stop on `*TxTooLargeError` without retry or confirm, and a restart hits the same transaction again until the limit is raised.
- Config: slot names are unique per server (host:port, scheme and credentials ignored) because slots are cluster-wide; publication names per server and database; table names per database.
- Config: yaml decode errors carry only a line, so a line with several keys (flow style) is reported without a key name.
- Postgres: record decode fixtures with `WALX_RECORD_FIXTURES=1 go test -run TestRecordFixtureIntegration ./internal/postgres` (writes `testdata/*.json` via `pg_logical_slot_get_binary_changes`, session pinned to UTC); unit tests decode the stored files.
- Postgres: under REPLICA IDENTITY FULL the Relation message flags every column as key, so `DecodeDML` falls back to the whole row; the identity check (T4.5) must pass the primary key in its `keys` map.
- Engine: `pglogrepl.Parse` panics on an empty payload, so `decodeMessage` guards `len(WALData)==0`. The `send`, `confirm`, `empty` and `decode` func fields on `Engine` are the seams for T3.2 to T3.6; extend them instead of rewiring `deliver`. An empty retry policy behaves as `stop` in the engine; config defaults supply `forever`.
- Engine: `withPermanent` wraps `withRetry`; skip returns nil so `deliver` confirms `EndLSN`; only `event.Permanent` is skippable; `events_skipped_total` counts events with label `database`.
- Engine: idle advance runs on the reader goroutine: `keepalive` confirms the server WAL end only when `pending == 0` (batches queued or in flight, incremented by the reader before queueing, decremented by `deliver` after confirm) and `!TxBuffer.InTx()`. Empty batches are confirmed by `deliver` via `confirmEmpty` so they stay in order after earlier batches. `Standby.Keepalives()` is a one-slot latest-wins channel and `Source` requires it (nil is fine for fakes). pgoutput emits no Begin/Commit for transactions touching no published table, so only keepalives advance the LSN for them.
- Engine: `Run` owns closing the sink (exactly once); `main` must not. The sink's `Send` ctx is not the run ctx: a cancel lets the in-flight call finish, bounded by `shutdown_timeout` (default 30s), then `ErrShutdownTimeout` without confirm (Close then runs unawaited). Queued batches are not sent after cancel and retry backoff ends on cancel. The Source ctx outlives the run ctx until the deliverer returns, so Standby's final status update carries the last confirm. A test sink waiting on its own ctx needs the fake `e.timer` or a release channel, since that ctx never ends on cancel.
- Engine tests: a `confirm` crash hook must delegate to the original until the batch under test reached the sink, since `confirmEmpty` and idle keepalives also call `confirm`. To restart on the same slot, close the first `Conn` and wait for `pg_replication_slots.active` to turn false, else `ErrSlotActive`. Slot names must be lowercase (uppercase gives a misleading "slot does not exist"). Check a crash test by mutation: if the hook also calls the original, the redelivery test must fail.
- Tests: a fake must not close a channel a feeder goroutine may still send on (panic); close only from the sender or leave it open.
- Postgres tests: `pg_stat_replication.application_name` is empty for walx; join `pg_replication_slots.active_pid = pg_stat_replication.pid` to find its walsender.

## Commands

```
go build ./...
go vet ./...
go test -race -cover ./...
go test -race -run TestName ./internal/event/
```

Integration tests need a Postgres 18 with `wal_level=logical` and skip unless `WALX_TEST_POSTGRES_URL` is set, so a green `go test` does not mean they ran. They know nothing about how it is run; `compose.dev.yaml` is one way (`docker compose -f compose.dev.yaml up -d --wait`, port 5432, then `WALX_TEST_POSTGRES_URL=postgres://walx:walx@localhost:5432/walx?sslmode=disable`). Use goleak for goroutine-leak checks.
Run engine tests with `-timeout 120s`: a test sink returning a plain error under policy `forever` retries forever.
Each test gets a throwaway database; tests must drop cluster-wide objects (roles, physical slots) themselves. Change server-wide settings only through `setServerSetting`, which refuses a customised value and resets it afterwards. If compose runs on a remote Docker context, forward the port locally. On macOS use `python3` or `gofmt` for in-place edits (BSD `sed -i` needs a suffix).

## Conventions

- Tests first, run with `-race`.
- Every exported Go identifier gets a short doc comment starting with its name; comment unexported ones only when the why is non-obvious.
- Go files: one concern per file, named after it (`sink.go`, `lsn.go`); at most one interface per file.
- Within a file: type, constructor, methods, then unexported helpers. Keep tests in a matching `_test.go`.
- Install dev tools as Go tools (`go get -tool`), not globally.
- Testify: prefer `require` over `assert`.
- Write every test table-driven, even with one case.
- Preferred tools: testify, goldie, tack, mockery, task.

## Commit messages

[Conventional Commits 1.0.0](https://www.conventionalcommits.org/en/v1.0.0/#specification): `<type>[scope][!]: <description>`. Types: `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `build`, `ci`, `chore`, `revert`. Description short, lowercase, imperative, no trailing period. Mark breaking changes with `!` and/or a `BREAKING CHANGE:` footer.
