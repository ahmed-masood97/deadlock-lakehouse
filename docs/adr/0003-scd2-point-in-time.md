# ADR 0003: SCD2 dimensions with point-in-time joins and deterministic keys

**Status:** accepted

## Context
Item prices and hero kits change between patches, but the API only returns the current definitions. Valuing a 2026-10-09 purchase at today's price would silently corrupt every economics metric.

## Decision
- Snapshot reference data every run and build Type 2 history ourselves (`gold/Scd2.scala`).
- Facts look up the version valid at match start (`start_time >= valid_from AND start_time < valid_to`), with the dimensions broadcast.
- Surrogate key = `xxhash64(natural_key, valid_from)`.
- First version valid from `1900-01-01`, open end `9999-12-31`.
- Facts whose dimension row is missing get the unknown member `-1`, and a backfill repairs them.
- `dim_player` stays Type 1: its history has no analytical use.

## Consequences
- Rebuilding a dimension from bronze reproduces identical keys, so dimension backfills never orphan facts.
- Range joins against broadcast dimensions avoid shuffles. A dimension too large to broadcast would need a different approach (bucketed or interval join).
- Out-of-order snapshots are skipped, not merged into the middle of history. A full rebuild handles corrections.
