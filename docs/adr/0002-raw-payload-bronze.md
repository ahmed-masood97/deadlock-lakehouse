# ADR 0002: Store raw payloads as text in bronze

**Status:** accepted

## Context
The producer (a community API over game-client data) changes shape with game patches: fields appear, and enums gain values. Parsing during ingestion couples ingestion to the schema.

## Decision
Bronze stores each record's raw JSON in a `payload STRING` column, plus a few cheaply extracted columns (`record_key`, `event_ts`, `snapshot_date`) and lineage columns (`ingest_run_id`, `source_file`, `payload_sha256`). All parsing happens in silver, driven by the source contract.

## Consequences
- Ingestion cannot fail because of a schema change. The worst case is quarantine in silver, with the record kept.
- Any historical record can be re-parsed with a newer contract. A silver backfill is a replay, not a re-download.
- One generic bronze schema and loader serve every source.
- The cost is storage (raw JSON compresses well with zstd) and parsing on every silver read. That is acceptable because silver reads are incremental.
