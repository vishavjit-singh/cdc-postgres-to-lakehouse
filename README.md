# CDC: PostgreSQL to Lakehouse

[![CI](https://github.com/vishavjit-singh/cdc-postgres-to-lakehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/vishavjit-singh/cdc-postgres-to-lakehouse/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/python-3.11%20%7C%203.12-3776AB?logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16%20logical%20WAL-4169E1?logo=postgresql&logoColor=white)
![Debezium](https://img.shields.io/badge/Debezium-2.7-91D443)
![Kafka](https://img.shields.io/badge/Kafka-KRaft-231F20?logo=apachekafka&logoColor=white)
![License](https://img.shields.io/badge/license-MIT-green)

Change data capture from an OLTP PostgreSQL database into a lake. **Debezium** reads the write-ahead log through a `pgoutput` slot and publishes every insert, update and delete to **Kafka**, keyed by primary key. A Python consumer then writes three things:

- a **bronze change log** with every record as it was delivered
- **silver current-state tables**, merged by primary key in **WAL order**, with deletes kept as soft deletes
- an **SCD2 history** of every version, for point-in-time queries

A checkpoint stores Kafka offsets and the merged state together in one atomic write, so a crash at any point leads to the same lake as an uninterrupted run. A reconciliation step then compares the lake with the source row by row and by SHA-256 checksum.

![Architecture](docs/images/architecture.png)

## Why this project

CDC looks like "copy the rows". In practice the stream is at-least-once, partitioned and full of edge cases:

| Problem in a real CDC stream | How the pipeline handles it |
|---|---|
| Kafka Connect restarts and re-sends records it already delivered (at-least-once) | Each key remembers every applied **position `(source.lsn, op)`**. A redelivered record matches an applied position and is ignored |
| A record arrives after a newer change to the same key (producer retries, rebalancing) | Positions only move forward. An older record never overwrites the current row, but it is **slotted into the SCD2 history** so point-in-time queries stay correct |
| The initial snapshot and the live stream overlap | Snapshot reads (`op = r`) sort before streamed changes at the same LSN |
| Deletes arrive as a delete event plus a tombstone | **Soft delete** (`_deleted = true`, last values kept) so downstream merges can propagate it. Tombstones are counted and skipped; they exist for log compaction |
| A primary-key `UPDATE` (renumbering an order line) | Debezium emits delete-old-key + create-new-key. Both keys end up correct, with no special case needed |
| `ALTER TABLE customers ADD COLUMN loyalty_points` (Debezium emits no DDL event) | **Schema evolution:** the first event carrying a new column adds it to silver. Older rows read null, as PostgreSQL does. The change is logged in `_schema_changes.json` |
| The consumer crashes after writing bronze but before recording progress | **Commit protocol:** bronze is written under batch-specific names, then one atomic `_checkpoint.json` (offsets + state). On restart, bronze files of the uncommitted batch are deleted and the batch is replayed |
| "Did we lose anything?" | **Reconciliation:** row counts, missing, extra and mismatched keys, and an order-independent SHA-256 per table against the source (the simulator's final state, or live PostgreSQL in the stack) |
| Malformed records, unknown topics, heartbeats | A dead-letter folder for unparseable records, counters for the rest. Nothing stops the stream |

## Results from the demo run

`make demo` runs without Docker. A seeded simulator executes a **synthetic** OLTP workload and writes the exact Kafka records Debezium would produce for it, into one log per topic-partition (3 partitions per table). It then makes the stream misbehave on purpose: a connector restart re-sends a range of records, and a few updates are delivered after a newer update for the same key. The consumer processes 6 micro-batches, **crashes in batch 9** after writing bronze but before the checkpoint, restarts, finishes and reconciles. These charts are rendered from the lake it wrote.

**What was captured.** 900 transactions after the snapshot; the dashed line marks the first event carrying the column added by `ALTER TABLE`:

![Events over time](docs/images/events_timeline.png)

**Delivery order versus WAL position.** Partitions are consumed interleaved, so LSNs jump around globally while staying ordered per key. Orange points are redeliveries from the simulated connector restart. Red points arrived after a newer version of their key:

![Ordering](docs/images/ordering.png)

**Reconciliation** against the simulator's final database state:

![Reconciliation](docs/images/reconciliation.png)

**SCD2 history** for one customer: the email changes, `loyalty_points` appears after the schema change, and each version has its validity window:

![SCD2 history](docs/images/scd2_history.png)

| Metric (demo run, synthetic data, seed 23) | Value |
|---|---|
| Source | 1,261 rows in the snapshot, then 900 transactions → 1,810 change events (c/u/d) |
| Kafka records replayed | 3,451, including 253 tombstones, 26 heartbeats and 120 redelivered records |
| Consumer | 14 micro-batches of 250 records; a crash in batch 9, recovered on restart (3 uncommitted bronze files removed) |
| Applied | 3,060 events |
| Ignored as duplicates | 101 (the redelivered records that were change events; the rest were tombstones) |
| Late: older than the key's current version (history only) | 11 |
| Silver live rows | customers 232 · orders 588 · order_items 1,307 |
| Soft-deleted keys | customers 35 · order_items 199 (cancellations and primary-key changes) |
| SCD2 versions | customers 469 · orders 897 · order_items 1,705 |
| Schema changes detected | `customers.loyalty_points` |
| Reconciliation | **3/3 tables match**: 0 missing, 0 extra, 0 mismatched, equal SHA-256 checksums |

> Every number comes from `python -m cdc_lake summary` after `make demo` and is reproducible: the workload, partitioning, redelivery and late delivery are all seeded. The data is synthetic (names from short word lists, `example.com` emails).

## Two ways to run it

| | Local replay (`make demo`, CI) | Compose stack (`make stack`, [`stack.yml`](.github/workflows/stack.yml)) |
|---|---|---|
| Source database | in-memory simulator, same schema as [`init.sql`](docker/postgres/init.sql) | PostgreSQL 16, `wal_level=logical`, `REPLICA IDENTITY FULL` |
| Change capture | simulator emits Debezium-format envelopes | Debezium 2.7 PostgreSQL connector, `pgoutput`, initial snapshot, heartbeats ([config](docker/connect/register-postgres.json)) |
| Transport | JSONL log per topic-partition, interleaved reads | Kafka 3.8 (KRaft), 3 partitions per topic |
| Consumer | `ReplaySource` | `KafkaSource` (confluent-kafka, auto-commit off, seeks to the lake checkpoint) |
| Reconcile against | the simulator's final state | live PostgreSQL (`--source-dsn`) |

The consumer, merge rules, lake layout and reconciliation are the same code in both. `init.sql` and the workload's trickier statements (primary-key renumbering, status advance, `ALTER TABLE`) were run against a local PostgreSQL 16 with `wal_level=logical`, and its logical decoding output was checked. **The full compose stack has not been run yet**: its first run will be the `Debezium stack` GitHub Actions workflow.

## Quickstart

```bash
git clone https://github.com/vishavjit-singh/cdc-postgres-to-lakehouse.git
cd cdc-postgres-to-lakehouse
python -m venv .venv && source .venv/bin/activate
make install
make demo      # simulate → 6 batches → crash → restart → finish → reconcile → charts → summary
make test      # 35 tests
```

### With real PostgreSQL, Kafka and Debezium (Docker)

```bash
make stack        # up postgres/kafka/connect → register connector → workload → consume → reconcile
make stack-down
```

## CLI

```bash
python -m cdc_lake simulate                              # write the synthetic Debezium stream
python -m cdc_lake consume [--max-batches N]             # resume from the lake checkpoint
python -m cdc_lake consume --crash-after-bronze 9        # crash test: bronze written, no checkpoint
python -m cdc_lake consume --source kafka --bootstrap localhost:9094
python -m cdc_lake reconcile [--source-dsn postgresql://…]   # exit 1 on any difference
python -m cdc_lake report | summary | reset
python -m cdc_lake register-connector --connect-url http://localhost:8083
python -m cdc_lake workload --dsn postgresql://postgres:postgres@localhost:5432/shop
```

## Lake layout

```
data/lake/
├── bronze/changelog/table=<t>/date=<d>/part-<batch>.parquet   op, lsn, txId, ts, key, before, after, outcome
├── bronze/dead_letter/part-<batch>.parquet
├── silver/<table>/current.parquet                             one row per key: columns + _op _lsn _tx_id _deleted
├── silver/<table>_history/current.parquet                     SCD2: valid_from_ms, valid_to_ms, is_current, is_deleted
├── silver/_schema_changes.json
└── _checkpoint.json                                           next offset per topic-partition + merged state
```

Parquet is used when `pyarrow` is installed (it is in `requirements.txt`); otherwise CSV, so the demo runs in a bare environment.

## Project structure

```
├── config/pipeline.yaml        # tables + primary keys, paths, batch size, simulator settings
├── src/cdc_lake/
│   ├── envelope.py             # Debezium JSON envelopes (with or without schemas), tombstones, validation
│   ├── state.py                # per-key positions: applied / duplicate / late, soft deletes, SCD2, schema evolution
│   ├── consumer.py             # micro-batch loop and commit protocol, crash injection
│   ├── lake.py                 # bronze writer, atomic checkpoint, silver publish, uncommitted-file cleanup
│   ├── sources.py              # ReplaySource (demo) and KafkaSource (confluent-kafka)
│   ├── reconcile.py            # counts, key diffs, SHA-256; reading live PostgreSQL
│   ├── simulator.py            # synthetic workload → exact Debezium records, redelivery, late delivery
│   ├── stack.py                # connector registration, PostgreSQL workload
│   ├── report.py · cli.py
├── docker/postgres/init.sql    # source schema + seed rows
├── docker/connect/register-postgres.json
├── docker-compose.yml · Dockerfile · Makefile
├── tests/                      # 35 tests
└── .github/workflows/          # ci.yml (lint, tests, demo, image build) · stack.yml (full Debezium run)
```

## Testing

```bash
make lint && make test
```

- **Envelopes:** plain and schema-wrapped JSON, bytes, tombstones, key fallback to the row, every malformed case
- **Merge rules:** insert, update, duplicate, late event (history only), soft delete and re-insert, a stale event that must not resurrect a deleted row, truncate, schema evolution, point-in-time `as_of`, state round-trip
- **End to end:** the lake reconciles with the simulator's truth; a stop and restart gives the same result; a crash between bronze and checkpoint leaves no orphaned or doubled bronze rows; re-reading the whole stream from offset 0 changes nothing; dead-letter handling; silver files published
- **Sources:** per-partition order and resume offsets for the replay; the Kafka source seeks to the lake checkpoint and mirrors commits (fake consumer)
- **Reconciliation:** detects missing, extra and mismatched keys. The checksum ignores row order and representation (`5` vs `5.0`, `…Z` vs `…+05:30` timestamps)

## Limitations and roadmap

- The local demo uses a simulator, not a real database. The compose stack runs real PostgreSQL + Debezium + Kafka, but it has only been written, not run, in this repo so far; see the `Debezium stack` workflow.
- Silver tables are rewritten in full each batch. That is fine at demo scale, but a production version would write Delta Lake or Iceberg tables with `MERGE` (as in my [databricks-medallion-lakehouse](https://github.com/vishavjit-singh/databricks-medallion-lakehouse)) and keep consumer state in a key-value store.
- Late records are classified per key. Cross-table consistency (an order item arriving before its order) is not enforced; Debezium's transaction metadata topic would allow transaction-consistent batches.
- Next: Avro with Schema Registry, `truncate` events from PostgreSQL, a lag metric (`now - source.ts_ms`) exported to Prometheus, and an Iceberg sink.

## Author

**Vishavjit Singh Saini**, Data Engineer
[Portfolio](https://vishavjit-singh.pages.dev/) · [LinkedIn](https://www.linkedin.com/in/vishavjit-singh-7011211ab/) · [GitHub](https://github.com/vishavjit-singh)

## License

[MIT](LICENSE)
