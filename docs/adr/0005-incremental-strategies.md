# ADR 0005: Snapshot diffs for append-only data, high-water marks for merged data

**Status:** accepted

## Context
Full refreshes cost more every day and hide late-data problems. One incremental technique does not fit every hop.

## Decision
- **bronze → silver**: Iceberg incremental read between the last consumed snapshot and the current one. Bronze is append-only, so the snapshot log is an exact change log. If the stored snapshot has been expired, fall back to its commit time.
- **silver → gold**: high-water mark on `silver.*._updated_at`. Silver is changed with MERGE and DELETE, and Iceberg incremental reads only support appends.
- **dimensions**: the last applied snapshot date, because SCD2 must apply snapshots in order.
- **Backfills** never move incremental watermarks.

## Consequences
- Late-arriving matches (old `start_time`, new ingestion) flow through without special handling.
- Correctness relies on `_updated_at` being stamped by every silver write. That is centralised in the silver job.
- Gold watermarks are staged and committed after publish, so a failed audit replays the same window.
