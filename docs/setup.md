# Database setup

You do this once per Postgres server. walx never creates roles, never changes server settings and never edits `pg_hba.conf`.

## Supported versions

Postgres 18 and later only. `walx` refuses to start against an older server (`postgres 18 or later required`); there is no compatibility code for older versions.

## Server settings

In `postgresql.conf` (a restart is needed for `wal_level`, `max_wal_senders` and `max_replication_slots`):

```
wal_level = logical
max_wal_senders = 10            # at least one per walx database, plus your other consumers
max_replication_slots = 10      # at least one per walx database
max_slot_wal_keep_size = 100GB  # recommended, see below
idle_replication_slot_timeout = 30d   # recommended (Postgres 18)
```

`max_slot_wal_keep_size` and `idle_replication_slot_timeout` stop a stuck or abandoned consumer from filling the source disk. Past the first limit the slot becomes lost and the consumer needs a resync ([operations](operations.md#lost-slot-recovery)). At startup walx reads both and logs a warning when `max_slot_wal_keep_size` is `-1` or `idle_replication_slot_timeout` is `0`; it never sets them. Size `max_slot_wal_keep_size` to the WAL you can afford to keep while walx is down or its sink is stuck.

## Role

```sql
CREATE ROLE walx LOGIN REPLICATION PASSWORD '...';
GRANT CONNECT ON DATABASE orders TO walx;
```

With the defaults (`postgres.auto_create` and `postgres.auto_replica_identity` true) the role also needs:

| Privilege | Why |
| --- | --- |
| `REPLICATION` attribute | Create the slot, run `START_REPLICATION`. Always required |
| `CREATE` on the database | `CREATE PUBLICATION` |
| Ownership of every listed table | `CREATE PUBLICATION ... FOR TABLE` and `ALTER TABLE ... REPLICA IDENTITY FULL` |
| Superuser | Only when the entry lists no `tables`: `FOR ALL TABLES` needs superuser, also on 18. Otherwise list the tables |

`walx snapshot` needs `REPLICATION` (temporary slot) and `SELECT` on the tables it reads, nothing else.

With both auto options off, only `REPLICATION` and `CONNECT` are needed, plus the objects you create by hand (below).

## pg_hba.conf

walx opens a logical replication connection to the database (`replication=database`) and, when `metrics.listen` is set, a second ordinary connection per database to read slot lag. In `pg_hba.conf` a logical walsender connection matches the database name, not the `replication` keyword (which covers physical replication only), so allow the role on the database:

```
host  orders  walx  10.0.0.0/8  scram-sha-256
```

Reload with `SELECT pg_reload_conf();`. Use `sslmode=require` or stronger in `postgres.url` outside a trusted network.

## What walx creates (auto_create, auto_replica_identity)

Both default to `true`. Turn them off per database entry (`postgres.auto_create: false`, `postgres.auto_replica_identity: false`) for locked-down production: walx then only validates and fails with the SQL to run.

Auto-create, in this order at startup:

- A publication named `postgres.publication` (default `walx_<name>`): `FOR TABLE <listed tables>`, or `FOR ALL TABLES` when the entry lists none, `WITH (publish = '<union of the tables' ops>', publish_via_partition_root = true)`. Partitioned tables therefore arrive under the root table name. A missing publication with auto_create off is an error that prints the exact `CREATE PUBLICATION`.
- A logical slot named `postgres.slot` (default `walx_<name>`) with the `pgoutput` plugin. It is created last, after every check, so a failed check does not leave a slot pinning WAL. With auto_create off a missing slot is an error. A slot that exists is validated (logical, `pgoutput`, this database, not lost, not active elsewhere).
- If a publication exists but differs from the config (tables, ops, `publish_via_partition_root` off, or a row filter or column list added by hand), walx fails with `publication differs from config`. Set `postgres.auto_alter: true` to let walx run `ALTER PUBLICATION`. It never switches a publication between `FOR ALL TABLES` and a table list (for example after adding or removing the `tables` list): stop walx, run `DROP PUBLICATION walx_<name>;` (or your `postgres.publication`) and start it again. The slot can stay. Pick the mode once: a table list needs no superuser and is explicit.
- `ALTER PUBLICATION` (by walx with `auto_alter`, or by hand) also reaches a transaction that is already open: its changes made before the ALTER keep the old setting and those made after it follow the new one. Such a transaction can therefore arrive partial (a table added or an op removed applies from the ALTER on), so alter while no important transaction is running. Transactions that start after the ALTER are not affected.

Auto-set replica identity (`REPLICA IDENTITY FULL`): a table needs it when its config needs the old row (`fsm_column`, `on_change`, `where` on deletes, or a custom `key` outside the identity columns on a table that publishes updates or deletes; an update that leaves a TOASTed key column unchanged carries no value for it). walx alters only tables not already `FULL`, in one transaction that starts with `SET LOCAL lock_timeout = '10s'` so the `ACCESS EXCLUSIVE` lock cannot hang startup, logs each table (`set replica identity full`) and never touches other tables. This changes a production table: a brief lock, and larger WAL records for updates and deletes. Without ownership, or when another transaction holds a lock, startup fails with the `ALTER TABLE` to run by hand.

Every table that publishes updates or deletes needs a key (primary key, or `REPLICA IDENTITY USING INDEX`, or `FULL`); a listed keyless table fails startup. Postgres decides this per publication, not per table: if any listed table publishes updates or deletes, every listed table needs a key, even one that lists only `insert`. Tables not in your config (with `FOR ALL TABLES`) only produce a warning, since an insert-only keyless table is harmless.

## Manual setup (opt-out mode)

Create what walx would create. The publication's `publish` list must equal the union of ops across the configured tables (default `insert, update`).

```sql
CREATE PUBLICATION walx_orders
  FOR TABLE public.orders, public.audit_log
  WITH (publish = 'insert, update, delete', publish_via_partition_root = true);

SELECT pg_create_logical_replication_slot('walx_orders', 'pgoutput');

-- for each table that needs the old row (see above)
ALTER TABLE public.orders REPLICA IDENTITY FULL;
```

For all tables: `CREATE PUBLICATION walx_orders FOR ALL TABLES WITH (publish = 'insert, update', publish_via_partition_root = true);` (superuser).

Create the slot only after the replica identity changes, so no WAL it covers was written under the old identity.
