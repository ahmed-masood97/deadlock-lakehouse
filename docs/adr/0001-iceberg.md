# ADR 0001: Apache Iceberg as the table format

**Status:** accepted

## Context
The platform needs row-level changes (MERGE for SCD2, delete and insert for late data), cheap incremental reads, a way to stage writes and audit them before readers see them, safe backfills, and tables readable by more than one engine.

## Decision
Use Apache Iceberg (format v2) for every table, through Spark's `SparkCatalog`.

## Why
- **Branches and fast-forward** give write-audit-publish and isolated backfills with no copy of the data (ADR 0004).
- **Snapshot-based incremental reads** turn an append-only table's commit log into a change feed (ADR 0005).
- **Hidden partitioning** (`days(start_time)`): writers and readers filter on the real column, and partition pruning still applies. Partition evolution needs no rewrite.
- **Engine neutrality**: the same tables serve Spark, Trino, Flink and warehouse engines.
- **Time travel and rollback** for incident response (`VERSION AS OF`, `rollback_to_snapshot`).

## Consequences
- Maintenance is our job: compaction, snapshot expiry and orphan cleanup run weekly.
- Commits are per table. Multi-table atomicity needs a catalog that supports it (noted in the README).
- The Hadoop catalog is used locally only. Production would use REST, Glue or Hive, configured in `pipeline.yaml`.
