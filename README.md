# walx

walx is a standalone executable that turns the Postgres write-ahead log (logical replication, `pgoutput`) into an ordered, at-least-once stream of JSON events and writes them to a sink: stdout, a file, or NATS JetStream. One `config.yaml` lists several databases; each gets its own independent pipeline (connection, slot, sink, retry policy and LSN checkpoint). There is no Go API and no user code: you configure it in YAML.

The one rule: a sink returns `nil` only when the batch is durable at the destination, and only then does walx confirm the WAL position. A crash or a failing sink therefore repeats events rather than losing them, so consumers must dedupe by `Event.ID` (see [events](docs/events.md)).

Requires Postgres 18 or later. `walx snapshot` backfills the current rows of a database through the same filters and sink (see [operations](docs/operations.md#snapshot)).

## Quick start

1. Prepare Postgres (`wal_level=logical`, a role with `REPLICATION`); see [docs/setup.md](docs/setup.md). For a throwaway server, `docker compose -f compose.dev.yaml up -d --wait` starts Postgres 18 and NATS with the settings walx needs (user, password and database `walx`).
2. Copy [config.example.yaml](config.example.yaml) to `config.yaml` and edit it. A minimal file:

   ```yaml
   databases:
     - name: orders
       postgres:
         url: ${ORDERS_DATABASE_URL}
       tables:
         - name: public.orders
       sink:
         type: stdout
   ```

3. Build and run:

   ```
   go build -o walx ./cmd/walx
   ORDERS_DATABASE_URL='postgres://walx:walx@localhost:5432/walx?sslmode=disable' ./walx -config config.yaml
   ```

walx creates the slot and publication on first start, then prints one JSON line per change to stdout. Logs go to stderr. Stop it with SIGINT or SIGTERM.

## Documentation

| File | Content |
| --- | --- |
| [docs/setup.md](docs/setup.md) | Server settings, role, `pg_hba.conf`, privileges, auto-create and auto-set, manual SQL, Postgres versions |
| [docs/configuration.md](docs/configuration.md) | Every YAML key with default and validation rule |
| [config.example.yaml](config.example.yaml) | Annotated example, checked by a test |
| [docs/events.md](docs/events.md) | Event shape, `Event.ID` and idempotency, type mapping, TOAST, event-oriented streams |
| [docs/sinks.md](docs/sinks.md) | stdout, file and NATS sinks |
| [docs/operations.md](docs/operations.md) | Retry and errors, shutdown and exit codes, logging, metrics, WAL retention, runbooks |

## Build and test

```
go build ./...
go vet ./...
go test -race -cover -timeout 120s ./...
```

Unit tests need nothing else. Integration tests skip unless `WALX_TEST_POSTGRES_URL` (Postgres 18, `wal_level=logical`) or `WALX_TEST_NATS_URL` (NATS with JetStream) is set, so they need a running server, not testcontainers. `compose.dev.yaml` provides both:

```
docker compose -f compose.dev.yaml up -d --wait
export WALX_TEST_POSTGRES_URL='postgres://walx:walx@localhost:5432/walx?sslmode=disable'
export WALX_TEST_NATS_URL='nats://localhost:4222'
go test -race -cover -timeout 120s ./...
```

Each integration test uses a throwaway database or NATS stream and cleans up after itself.
