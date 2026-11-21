# CLAUDE.md

## Project

walx is a standalone Go executable (`cmd/walx`) that turns the Postgres WAL (pgoutput, Postgres 18+ only) into an ordered, at-least-once event stream written to a sink. One `config.yaml` lists several databases; each gets an independent pipeline (connection, slot, sink, retry, LSN checkpoint). No public Go API, no user Go code.

Status: `internal/event`, `internal/config` and `internal/postgres` (T1.4 connection, T1.5 slot, publication and stream start, T1.6 standby writer) and the `cmd/walx` snapshot stub (T1.7) are implemented so far; the postgres integration tests have not yet run against a real Postgres.

## Architecture rules

- A sink returns `nil` only when the batch is durable at the destination; only then does the engine confirm the batch's `EndLSN`. Retry, idle advance and shutdown follow from this.
- Every package except `cmd/walx` lives under `internal/`; each sink in `internal/sink/<name>`.
- `internal/event` holds the shared types (`Event`, `Batch`, `Op`, `Sink`, `Metrics`, `Unchanged`, `Permanent`). Engine code depends on the `event.Metrics` interface, never a concrete metrics package.
- `Event.ID` is `<name>-<commitLSN>-<seq>`, with `seq` counted over the raw transaction before filters, so IDs stay stable across redelivery and config changes.
- Delivery is serial and in WAL order within one database; no ordering across databases. Fail fast: one pipeline stopping shuts the others down and exits non-zero.
- Filters must not mutate shared events; copy first.
- Config: yaml `Node.Decode` ignores `KnownFields`, so `internal/config/keys.go` rejects unknown keys itself, before defaults and decoding.
- Config: YAML aliases and merge keys are rejected (they would skip env expansion and key checks). `${VAR}` expansion clears the `!!str` tag of changed plain scalars so bool and int fields work; quoted scalars stay strings. In YAML tests write `${VAR}` in block style, since `{` breaks flow mappings.
- Config: database names match `^[a-z][a-z0-9_]*$` and slots `[a-z_][a-z0-9_]{0,62}` (a leading digit breaks CREATE_REPLICATION_SLOT), since the name feeds the default slot, publication and `Event.ID`. A config holds one YAML document and no null list items (yaml would drop them and shift error indexes).
- CLI: `cmd/walx` logic lives in `run(args, stdout, stderr) int` so exit codes and messages are testable; exit 2 for usage errors, 1 for runtime errors including "not implemented".
- Postgres: a replication=database walsender accepts plain SQL only over the simple protocol, so use `pgconn.Exec`, never parameterised `pgx` queries.
- Postgres: after `StartReplication` fails, pglogrepl leaves `ReadyForQuery` unread; reconnect instead of reusing the connection. The slot can stay active briefly after a killed backend, so the engine treats `ErrSlotActive` as retryable on reconnect and fatal at first start.
- Postgres: `pglogrepl.SendStandbyStatusUpdate` replaces a flush or apply LSN of 0 with the write position and would confirm unhandled WAL, so `NewStandby` rejects a zero start; seed it with `SlotInfo.ConfirmedFlushLSN`. `StartStream` still takes 0 to resume from the slot.
- Postgres: pgconn is not safe for concurrent use; `Standby.Run` is the single goroutine that receives (with a deadline so the timer fires) and writes. `ReceiveMessage` after a timeout loses no data; copy `WALData`, since pgconn reuses its buffer.
- Postgres: `FOR ALL TABLES` needs superuser, also on 18. With `publish_via_partition_root`, `pg_publication_tables` lists the root table. `auto_alter` never switches between `FOR ALL TABLES` and a table list.
- Config: yaml decode errors carry only a line, so a line with several keys (flow style) is reported without a key name.

## Commands

```
go build ./...
go vet ./...
go test -race -cover ./...
go test -race -run TestName ./internal/event/
```

Integration tests need a Postgres 18 with `wal_level=logical` and skip unless `WALX_TEST_POSTGRES_URL` is set, so a green `go test` does not mean they ran. They know nothing about how it is run; `compose.dev.yaml` is one way (`docker compose -f compose.dev.yaml up -d --wait`, port 5432, then `WALX_TEST_POSTGRES_URL=postgres://walx:walx@localhost:5432/walx?sslmode=disable`). Use goleak for goroutine-leak checks.
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
