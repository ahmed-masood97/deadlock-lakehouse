# Design document: Deadlock Lakehouse

| | |
|---|---|
| Author | Ahmed Masood |
| Status | Implemented (v0.1) |
| Stack | Scala 2.12 · Spark 3.5 · Apache Iceberg 1.10 · Airflow 3 |

## 1. Problem and goals

Players, coaches and balance analysts want trustworthy answers to questions like these:

- Which heroes are strong in *this* patch, at *my* skill bracket, and how sure are we?
- Which items are worth their price, and did the economy patch change that?
- Where do souls (the in-game currency) come from, and where do they go?

The raw material is a public API that returns current reference data (no history), match payloads that change shape between game patches, and the occasional broken record. The platform has to turn that into a dimensional model that is **historically correct, reconcilable and safe to change**.

**Goals**
1. Correct history. A purchase on 2026-10-09 is valued at that day's price, even after the price changes.
2. Incremental by default, with any window rebuildable on demand (backfill) and no double counting.
3. Bad data never reaches consumers, and every rejection is explainable.
4. Adding a source or a mart takes configuration and SQL, not new pipeline code.
5. Operable: every run is auditable, and every table is traceable to the snapshots it was built from.

**Non-goals**: real-time serving (the API has a separate live-events product), and ML features.

## 2. Sources and contracts

| Source | Endpoint | Shape | Kind | History from producer? |
|---|---|---|---|---|
| Match metadata | `GET /v1/matches/metadata` | NDJSON, paged by `min_match_id` | `paged_ndjson` | Yes (immutable events) |
| Heroes | `GET /v1/assets/heroes` | JSON array, current state | `json_snapshot` | **No**: we build it (SCD2) |
| Items | `GET /v1/assets/items` | JSON array, current state | `json_snapshot` | **No**: we build it (SCD2) |
| Patches | `GET /v1/patches` | RSS-derived JSON | `json_snapshot` | Partial |

Each source has a **contract** (`contracts/*.yaml`). The contract states the grain, primary key, field types, required fields, accepted values, numeric ranges, array sizes, PII flags, freshness SLA and evolution policy. The contract is the single source for:

- the Spark schema used to parse payloads (`Contract.sparkSchema`), never hand-written;
- validation rules, compiled to column expressions. Rules inside arrays of structs use higher-order functions (`exists`), so a match with one bad player is caught without exploding 12 rows;
- drift detection. JSON keys observed at every object level are compared with the declared fields.

**Evolution policy.** `additive` (the default) logs new fields to `ops.schema_drift` and carries on. That is the right call for a live game API, where new stats appear with patches. Missing required fields, wrong types and out-of-range values quarantine the record. Nothing is dropped silently. `strict` would turn additive drift into a failure, which suits financial sources.

**Onboarding a new source** takes one block in `conf/pipeline.yaml` and one contract. The bronze loader, API client and validation code are generic.

## 3. Layers

### Landing
Immutable NDJSON files are written atomically (temp file, then rename), one directory per source and batch or snapshot date. The API extractor advances its watermark only after a page is safely on disk, so a crash resumes from the last good page.

### Bronze (append-only)
Every source shares one schema: `ingest_run_id, ingested_at, source, source_file, record_key, event_ts, snapshot_date, payload, payload_sha256`. The payload is stored as text. Loads are idempotent because files already present in bronze (by canonical path) are skipped. Tables are partitioned by `days(ingested_at)` (hidden partitioning).

### Silver (source grain, conformed)
`silver_matches` reads bronze **incrementally by Iceberg snapshot**. It stores the last consumed snapshot id and reads only the files appended since then. If maintenance has expired that snapshot, it falls back to the snapshot's commit time. The steps:

1. Detect drift and write `ops.schema_drift`.
2. Parse and validate against the contract. Invalid records go to `silver.quarantine` with reasons.
3. Deduplicate per `match_id`, keeping the latest ingestion (the API re-sends matches).
4. Change detection on `payload_sha256`. Unchanged re-sends are no-ops.
5. Idempotent multi-table write. **Children first** (delete and insert the affected matches in `match_players` and `item_purchases`, with literal partition bounds for pruning), **parent last** (`MERGE` into `matches`, which stamps the new hash). If the job dies in between, the parent still has the old hash, so the next run redoes the children.

### Gold (dimensional, published through write-audit-publish)
- `dim_hero`, `dim_item`: **SCD Type 2** (§4).
- `dim_patch`: patch validity windows, rebuilt each run (small, fully derivable).
- `dim_player`: **SCD Type 1** on purpose. Only the latest profile matters. Aggregates are recomputed from silver history for affected players, so re-runs never double count.
- `fct_player_match` (player × match), `fct_item_purchase` (purchase), `fct_player_economy` (player × match).
- Marts in SQL: hero × patch × bracket performance with Wilson intervals, item unit economics, and the daily soul economy.

## 4. Historical correctness: SCD2 and point-in-time joins

The API only ever returns *current* hero and item definitions. We snapshot them on every run, and `Scd2` turns the sequence of snapshots into history:

- **Change detection** uses a SHA-256 of the tracked attributes, which makes it NULL-safe with one comparison.
- **One atomic `MERGE` per snapshot.** Staged rows with `merge_key = natural key` expire the current version. Rows with `merge_key = NULL` insert the new version.
- **Deterministic surrogate keys**: `xxhash64(natural_key, valid_from)`. A full rebuild from bronze reproduces identical keys, so facts stay joinable. This is what makes dimension backfills safe.
- **First version valid from 1900-01-01.** We do not know what existed before our first snapshot, and this lets late-arriving dimension rows resolve on a re-run.
- **Open end `9999-12-31`, not NULL.** Point-in-time joins become `start_time >= valid_from AND start_time < valid_to`, with no NULL handling and no overlap ambiguity.
- **Out-of-order guard.** A snapshot older than the current version is skipped and logged.

Facts resolve dimensions **as of match start**. The dimensions are broadcast, so the range join is evaluated map-side. A fact whose dimension row does not exist yet gets the **unknown member (-1)**. A DQ check measures the unknown rate (a warning, so it does not block), and a backfill from the dimension repairs it. The demo plays this scenario out with the hero released in patch 3.

## 5. Incremental processing

Two strategies are used, each where it fits (ADR 0005):

| Hop | Strategy | Why |
|---|---|---|
| bronze → silver | **Iceberg snapshot diff** (`start-snapshot-id` → `end-snapshot-id`) | Bronze is append-only, so the snapshot log *is* the change log. Cost is proportional to new files. |
| silver → gold | **High-water mark** on `_updated_at` | Silver is maintained with MERGE and DELETE (not append-only), so snapshot diffs do not apply. Late matches get a fresh `_updated_at` whatever their `start_time`. |
| dimensions | **Snapshot-date watermark** | Snapshots must be applied in order. |

Gold watermarks are **staged** and only committed after the gold branch is published. If the audit fails, the next run starts from the same place.

## 6. Write-audit-publish

Every gold table has `write.wap.enabled=true`. A daily run:

1. Creates branch `wap_<run_id>` on all gold tables.
2. Sets `spark.wap.branch`. All gold writes go to the branch, and reads of those tables in the session resolve to the branch.
3. Runs the gold DQ suites against the branch.
4. If no blocking check failed: checks that main has not moved on any table (a concurrent writer), then fast-forwards main to the branch table by table (**dimensions, then facts, then marts**) and drops the branch.
5. Otherwise it keeps the branch, fails the run, and main still serves the last good data.

Iceberg commits are per table, so publish is atomic per table. Publishing in order means no reader sees a fact whose dimension row is unpublished. Cross-table atomicity would need catalog-level multi-table transactions. That is noted as a limitation.

## 7. Backfills and migrations

**Backfill** (`backfill --start-at X --from D --to D`):
1. *Plan.* Downstream closure of `X` in the job graph, in topological order (`--dry-run` prints it).
2. *Rebuild.* Silver jobs rewrite the window in place (idempotent). Gold jobs write to branch `backfill_<run_id>`. In date-range mode, dimension jobs rebuild fully and deterministically from bronze.
3. *Validate.* The usual DQ suites, plus `ops.backfill_diffs`: per table, per day, row counts and key measures, main vs branch (`VERSION AS OF`).
4. *Cutover.* Fast-forward, or `--no-publish` to leave the branch for review and then `publish --branch`.

In Airflow, long ranges are split into monthly chunks that run one after another, so each is small and independently retryable.

**Migrations** are versioned DDL files (`V00N__*.sql`). They are applied in order and recorded with a checksum in `ops.schema_migrations`. Editing an applied migration fails the run (Flyway semantics). V006 shows evolution after data exists: `ADD COLUMNS` (metadata-only in Iceberg) and `WRITE ORDERED BY` (picked up by the next compaction).

## 8. Data quality, reconciliation and observability

Checks are declared per table in `Expectations` and scoped to the rows a run touched, so cost tracks the batch, not the table. Severity `error` blocks publish and `warn` only alerts.

| Kind | Examples |
|---|---|
| Grain | `unique(match_id, player_slot)` on every fact |
| Completeness | `not_null` on keys, `exactly_one_current_version` per SCD2 key |
| Validity | accepted values, `valid_from < valid_to`, `win_rate` inside its interval |
| Integrity | `references(hero_sk → dim_hero)` (unknown member allowed), SCD2 versions contiguous with no gaps or overlaps |
| Reconciliation | fact rows = silver players, Σ kills fact = Σ kills silver, Σ net worth economy = silver, Σ spend = Σ item purchase net cost |
| Business sanity | half of the players in each match win; soul sources reconcile to net worth (warning at > 1% of rows) |

Everything is persisted: `ops.dq_results` (each check, each run, on which branch), `ops.job_runs` (status, duration, metrics map, error), and `ops.lineage` (each job's input and output tables **with snapshot ids**). Together they answer "which bronze snapshot produced this gold snapshot?" and "when did this check start failing?" with SQL.

## 9. Governance

- PII is declared in contracts (`pii: true`). `Contract.piiPaths` lists it, and a test pins the list.
- Steam account ids never leave silver unhashed. Gold uses `sha256(salt || ':' || account_id)`, with the salt from a secret (`DEADLOCK_PLAYER_SALT`). Id `0` (anonymous) maps to NULL.
- Table and column comments document meaning at the source of truth (the catalog).

## 10. Performance and cost

See [`performance.md`](performance.md). In short: broadcast dimensions, AQE (coalescing and skew splits), hidden day partitions with literal bounds on deletes, write ordering on hot facts, partition-scoped mart rebuilds, compaction and snapshot expiry, and job-scoped caching (a cache that outlives its job serves stale data, a bug this project hit and fixed).

## 11. Alternatives considered

- **Delta Lake** is equally capable for this design. Iceberg was chosen for engine neutrality (Spark, Trino, Flink, Snowflake), first-class branches and tags, and hidden partitioning (ADR 0001).
- **Parse in bronze.** Rejected, because the first producer schema change would break ingestion (ADR 0002).
- **dbt for all transformations.** The marts are SQL and could move to dbt unchanged. SCD2, contracts and the WAP mechanics stay in Scala, where they can be unit-tested and shared.
- **Full refresh every day.** It is simpler, but cost grows with history and it hides late-data problems instead of handling them.

## 12. Testing

- **Unit**: contract compilation and validation, drift detection, SCD2 planning (new, unchanged, changed, stale, deterministic rebuild), point-in-time lookups, graph ordering, impact analysis and cycle detection, DQ check semantics, PII hashing, migration parsing.
- **Integration**: a 21-day synthetic season through the real pipeline on a temporary Iceberg catalog. It covers idempotent re-ingest, no-op incremental runs, point-in-time prices across the repricing patch, a failed audit leaving main untouched, and a dimension backfill with diff and cutover.
