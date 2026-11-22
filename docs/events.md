# Events

## Event shape

Every sink writes the same JSON object per change (field order as below; `before`, `after` and `key` are omitted when empty):

```json
{
  "id": "orders-0/16B3748-1",
  "source": "orders",
  "op": "update",
  "schema": "public",
  "table": "orders",
  "before": {"id": 7, "status": "new"},
  "after": {"id": 7, "status": "paid"},
  "key": {"id": 7},
  "lsn": "0/16B3700",
  "tx_id": 742,
  "commit_ts": "2026-01-02T15:04:05.123456Z"
}
```

| Field | Meaning |
| --- | --- |
| `id` | `<name>-<commitLSN>-<seq>`: the database entry name, the transaction's commit LSN, and the position of the change in the transaction counted from 1 over the raw transaction (before filters; a truncate counts once per table). |
| `source` | The `databases[].name`. |
| `op` | `insert`, `update`, `delete`, `truncate`, or `snapshot` for a row read by `walx snapshot` (it has `after` and `key` only, and its `lsn` is the snapshot's consistent point). |
| `before`, `after` | Row images. `before` is only present when the replica identity gives it (see below); a delete has `before` only under `REPLICA IDENTITY FULL` and otherwise just `key`; a truncate has neither. |
| `key` | The configured `key` columns, default the primary key. For an update it is the new key. A table without a key has none. |
| `lsn` | WAL start position of the change. Informational only, never an ordering key. |
| `tx_id`, `commit_ts` | Transaction id and commit time (UTC). |

Events are delivered serially and in commit order within one database. There is no ordering across databases. Events of one transaction reach the sink in one call.

## Idempotency: dedupe by Event.ID

Delivery is at-least-once. After a crash, a killed process, a reconnect, a sink timeout or a retry, Postgres redelivers from the last confirmed position, so the same events arrive again with the **same `id`** (IDs do not depend on filters or config, so they survive config changes). Consumers must tolerate duplicates:

- Apply events as idempotent upserts/deletes, or remember processed IDs.
- NATS: the sink sets `Nats-Msg-Id` to the ID and the stream drops duplicates inside its dedup window (default 2 minutes); beyond that window dedupe in the consumer.
- file/stdout: a failed write can leave a partial line and a retry repeats lines.

After a reconnect only the unconfirmed transaction is redelivered.

## Event-oriented, not a table mirror

The stream describes changes, not table state. Do not treat it as a replica:

- By default only inserts and updates are emitted. Deletes and truncates are off until a table lists them in `ops`, so a consumer that only upserts drifts from the source.
- Filters (`where`, `on_change`, `drop_columns`, `reference_only`) deliberately remove events and columns.
- Rows that existed before the slot was created are never streamed. Run `walx snapshot` to deliver them as `snapshot` events ([operations](operations.md#snapshot)).
- Several updates to one row in one transaction produce several events; nothing is collapsed.
- DDL is not an event. After `ALTER TABLE ... ADD COLUMN` rows simply carry the new column.
- Soft deletes (`deleted_at`, `status = 'cancelled'`) are ordinary updates.

`reference_only` tables send just the key: the consumer reads the current row itself, which may already have changed again.

## Before, After and replica identity

Under the default replica identity `before` is absent for updates, and a delete has no `before` either, only `key`. `before` is filled only under `REPLICA IDENTITY FULL`, which walx sets for tables that need it ([setup](setup.md)). If `FULL` is on, `Key` is still the configured key (or the primary key), not the whole row, with two exceptions where `Key` is the whole row: a listed table with neither a primary key nor a configured `key`, and any table in a `FOR ALL TABLES` config (no `tables` list, so walx reads no keys). Configure `key` for a listed table, or list the table, to get a narrow key.

## Unchanged TOAST values

Large values (TOAST) that an update does not rewrite are not in the WAL. Such a column is `{"$unchanged": true}` in `after` and means "keep the old value", not NULL. Under `REPLICA IDENTITY FULL` the old tuple carries the full value, so `before` holds the value and `after` the marker. Under the default identity there is no `before`, so the value is unknown to the stream.

## Publication row filters rewrite ops

With `publication_where` on a table, Postgres evaluates the filter on the row and rewrites the op: an update that moves a row out of the filter arrives as a `delete` (its `before` is the old row), one that moves a row into the filter arrives as an `insert`, and an update inside it stays an `update`. Rows that never match produce nothing. The table must therefore list `delete` (and `insert`) in `ops` to see these events; otherwise the walx filter drops them. A rewritten `delete` does not mean the row was deleted at the source. `seq` in `Event.ID` counts what Postgres sends, so changing the filter changes the IDs of a transaction that is redelivered afterwards. Use the filter as an optimization and keep `where` for the semantics you rely on.

## Replay and the snapshot marker

`op: snapshot` is the only marker the event contract carries, and it says where a row came from, not that it is a repeat. Replayed events are not marked: they keep their original `id`, so consumers that dedupe by `id` treat a replay like any other redelivery.

Decision: the event has no `replay` field. With `dlq.path` set, every event skipped by `on_permanent_error: skip` is wrapped in an envelope holding the failure metadata (`failed_at`, `database`, `sink`, `error`) around the untouched event JSON in `event`, so the event contract stays the same. Replaying means feeding the stored `event` objects back to the destination; there is no replay CLI (see [operations](operations.md#replaying-the-dead-letter-file)). A replay is at-least-once like everything else, and a consumer that must tell a replay from a live event reads the envelope's metadata.

## Type mapping

The decoder reads the text output of the walsender, and the connection pins `datestyle=ISO`, `bytea_output=hex`, `extra_float_digits=1` and `intervalstyle=iso_8601` so the output does not depend on database defaults.

| Postgres type | JSON value |
| --- | --- |
| `bool` | `true` / `false` |
| `int2`, `int4`, `int8` | number (an `int64`). Values above 2^53 lose precision in parsers that use doubles (JavaScript): read them as 64-bit integers or cast the column to text upstream |
| `float4`, `float8` | number (shortest round-trip form). `NaN`, `Infinity`, `-Infinity` become strings |
| `numeric` | string, e.g. `"12.50"`, to keep exact precision |
| `text`, `varchar`, `char(n)`, `name`, `uuid` | string (`char(n)` keeps its padding) |
| `bytea` | base64 string (standard JSON encoding of bytes) |
| `json`, `jsonb` | the parsed JSON value; numbers keep their exact digits, but object keys are re-sorted and whitespace is not preserved |
| `date` | RFC 3339 string at midnight UTC, e.g. `"2024-05-01T00:00:00Z"` |
| `timestamp` (without zone) | RFC 3339 string read as UTC, e.g. `"2024-05-01T10:00:00Z"`. The source has no zone, so treat the `Z` as a label, not a fact |
| `timestamptz` | RFC 3339 string converted to UTC |
| `NULL` | `null` |
| unchanged TOAST (any type) | `{"$unchanged": true}` |
| everything else (arrays, enums, `interval`, `time`, `timetz`, `inet`, `money`, geometric, ranges, composite, user types) | string in Postgres text output, e.g. arrays as `"{1,2,3}"`, `interval` as ISO 8601 (`"P1DT2H"`) |

A column of a domain type is a string on the live stream, because pgoutput reports the domain's OID and the decoder does not know the base type (a domain over `int4` arrives as `"5"`). `walx snapshot` reports the base type, so the same column arrives as `5` there.

Values that Go time or float types cannot hold (`infinity`, BC dates) also stay strings. Events carry no column types, so a consumer cannot tell a `text` column holding `"5"` from a `numeric` 5 other than by schema knowledge; the same applies to `where`, which compares a string with a configured number by exact decimal value.
