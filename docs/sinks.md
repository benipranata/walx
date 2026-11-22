# Sinks

One sink per `databases` entry, chosen by `sink.type`. A sink's `Send` returns `nil` only after the destination has the whole batch (one transaction's events) durably, and only then does walx confirm the WAL position. A sink can mark a batch that can never succeed as permanent (see [operations](operations.md#permanent-errors)). The [dead-letter file](operations.md#replaying-the-dead-letter-file) (`dlq.path`) is not a sink type: it only receives batches skipped after a permanent error, and shares the file sink's append, fsync and torn-line behaviour. All sinks encode events as the JSON in [events](events.md) with HTML escaping off.

## stdout

One JSON line per event on the process's standard output, written in one `Write` per batch. Logs go to stderr, so the streams do not mix. Lines of different databases interleave between batches. Useful for development and for piping into another tool; stdout gives no durability, so a pipe that closes early only produces write errors that are retried. A partial write can repeat lines on retry.

```yaml
sink:
  type: stdout
```

## file

Appends one JSON line per event to `sink.path` (created with mode `0644`, before the umask, if missing, opened in append mode), then calls `fsync`, and returns `nil` only after the fsync succeeds. A path that is not writable is a startup error naming the path.

Caveats:

- A write or fsync error is retried, and the failed attempt may already have left lines or a torn last line, so the retry can repeat lines. After a failed partial write walx starts the retry on a fresh line.
- A crash can leave a torn last line that walx does not repair on restart: the next line is appended right after it. Readers must skip lines that are not valid JSON and dedupe by `id`.
- walx never rotates and never reopens the file. Rotate with copy-and-truncate, or restart walx after moving the file.
- Two file sinks (or a file sink and a `dlq.path`) must not share a path (config error).

```yaml
sink:
  type: file
  path: /var/lib/walx/orders.jsonl
```

## nats

Publishes one JetStream message per event and returns `nil` only after the stream acknowledged every message of the batch. Messages are published asynchronously in order, 1000 at a time, then their acks are awaited.

Requirements and behaviour:

- The stream must already exist and capture the subjects (for the default template `cdc.>`). walx never creates or alters streams; publishing to subjects with no stream fails and is retried by the retry policy, so create the stream first.
- Connection: `sink.url` may be a comma-separated list (schemes `nats`, `tls`, `ws`, `wss`); credentials go in the URL. An unreachable server at startup is an error; later outages reconnect forever.
- The ack wait is 30s when `sink.timeout` is unset, otherwise `sink.timeout`.
- Deduplication: `Nats-Msg-Id` is `Event.ID`, so the stream drops a retried batch within its duplicate window (`duplicate_window`, 2 minutes by default). Set it above your longest expected outage if you rely on it. If a message of a batch fails while later ones were accepted, the retry republishes the whole batch, so those later events can appear twice and out of order beyond the window. Consumers still dedupe by `id`.
- A message bigger than the server's maximum payload is a permanent error.

### Subjects

`sink.subject` is a template of dot-separated tokens with the placeholders `{database}` (the entry name), `{schema}`, `{table}` and `{token}`; the default is `cdc.{schema}.{table}.{token}`. `{token}` is the new state of the row's `fsm_column` for tables that have one, otherwise the op (`insert`, `update`, `delete`, `truncate`). Rules:

- Every substituted value has `.`, `*`, `>`, whitespace and control characters replaced by `_`. Different inputs can collide (`a.b` and `a_b`).
- A state that equals an op name, or already starts with `state_`, is prefixed with `state_`, so a state can never collide with an op token.
- A placeholder that expands to nothing would make an invalid subject; such an event is a permanent error. A NULL or empty `fsm_column` value counts as no state, so the op is the token.
- Literals in the template must not contain whitespace, `*` or `>`.

Example: `public.orders` with `fsm_column: status`, updated to `paid`, publishes to `cdc.public.orders.paid`; an insert into `public.audit_log` publishes to `cdc.public.audit_log.insert`.

### Headers

Metadata for routing without decoding the body (values are limited to 1024 bytes and one line):

| Header | Value |
| --- | --- |
| `Nats-Msg-Id` | `Event.ID` |
| `Walx-Op` | `insert`, `update`, `delete`, `truncate` |
| `Walx-Source` | the database entry name |
| `Walx-Schema`, `Walx-Table` | the table |
| `Walx-Commit-Lsn` | commit LSN of the transaction |
| `Walx-State-From` | old `fsm_column` value; only when the event has both row images, so never on `reference_only` tables |
| `Walx-State-To` | new `fsm_column` value (from `before` when `after` holds the TOAST marker); not on deletes |

The state headers are set only for tables with an `fsm_column`.

```yaml
sink:
  type: nats
  url: nats://nats-1:4222,nats://nats-2:4222
  subject: cdc.{database}.{schema}.{table}.{token}
  timeout: 10s
```
