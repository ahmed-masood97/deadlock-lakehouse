# On-call runbook: Deadlock Lakehouse

Each section covers what you will see, how to diagnose it, and what to do. All queries run with `sbt "run sql \"...\""`, or from any Iceberg-aware engine (Trino, Spark SQL).

**First, always:**
```sql
SELECT run_id, job, status, duration_ms, metrics, error
FROM lake.ops.job_runs ORDER BY started_at DESC LIMIT 20;
```

---

## 1. `DataQualityException`: the gold audit failed (publish blocked)

**Impact:** none for consumers. Main still serves the previous publish. Data is stale, not wrong.

**Diagnose**
```sql
SELECT table_name, check_name, severity, observed, expected, details
FROM lake.ops.dq_results
WHERE run_id = '<run_id>' AND status = 'fail';
```
The data that failed is still on branch `wap_<run_id>`:
```sql
SELECT * FROM lake.gold.fct_player_match VERSION AS OF 'wap_<run_id>' WHERE _run_id = '<run_id>' LIMIT 50;
```

**Act**
- **Upstream data problem** (for example the API sent half-empty matches): leave main as is, tell consumers that freshness is delayed, and re-run once the source is fixed. The watermark did not advance, so the next run reprocesses the same window.
- **Pipeline bug:** fix it, ship it, re-run `daily`.
- **Check too strict** (a legitimate change, say a new game mode): change the expectation in a reviewed PR. Never skip the audit.
- Afterwards, drop the dead branch: `ALTER TABLE lake.gold.<t> DROP BRANCH \`wap_<run_id>\`` (maintenance leaves branches alone).

## 2. "main moved on … since branch … was created"

**Cause:** two writers on gold at once (for example a manual backfill during the daily run). Publish refuses rather than discard someone's commit. Nothing was published.

**Act:** let the other run finish, then re-run. The rebuild starts from the new main. The Airflow DAGs use `max_active_runs=1` to prevent this.

## 3. Unknown-member warning (`max_ratio(unknown_hero_member)` or `unknown_item_member`)

**Cause:** a new hero or item appears in matches before the assets endpoint lists it (a late-arriving dimension). This is normal on patch day.

**Act:** once the next snapshot contains it (`SELECT * FROM lake.gold.dim_hero WHERE hero_id = …`), repair the affected days:
```bash
sbt "run backfill --start-at gold.dim_hero --from <first day> --to <last day>"
```
In the diff output, `unknown_hero_rows` goes to 0 and row counts stay unchanged.

## 4. Quarantine spike (`zero_rows(quarantine_rate_below_5pct)`)

```sql
SELECT explode(reasons) AS reason, count(*) FROM lake.silver.quarantine
WHERE run_id = '<run_id>' GROUP BY 1 ORDER BY 2 DESC;
```
- A **new enum value** (for example a new `game_mode`) means the producer evolved. Add it to the contract in a new contract version, then replay: quarantined payloads are still in bronze, so `backfill --start-at silver_matches --from … --to …`.
- **Malformed JSON** usually means a truncated page from the API. Re-extract that batch.

## 5. Schema drift logged

```sql
SELECT * FROM lake.ops.schema_drift ORDER BY detected_at DESC;
```
This is informational under `evolution: additive`. If a new field is useful, add it to the contract, add the column (new migration, metadata-only), and backfill the window you want populated. `absent_in_batch` on a required field shows up together with quarantine. Treat it as a producer regression.

## 6. Reconciliation failure (`reconcile(...)`)

A layer lost or duplicated data. Compare the two sides for the run's matches:
```sql
SELECT match_id, count(*) FROM lake.gold.fct_player_match WHERE _run_id = '<run_id>' GROUP BY 1 HAVING count(*) <> 12;
```
Typical cause: a partial earlier run. The silver write protocol (children first, parent last) makes a plain re-run safe.

## 7. Incremental reader warns `watermark_snapshot_expired`

Maintenance expired the bronze snapshot that silver last consumed. The job automatically falls back to the snapshot's commit time, so no action is needed. If it happens often, increase `retainDays` for bronze in `Maintenance.Policy`.

## 8. Slow run

1. `ops.job_runs.duration_ms` by job: find the job that regressed.
2. Spark UI for that job: look for a stage whose max task time is far above the median (skew; see `docs/performance.md`), or a scan that reads many files (small files: run `maintenance`).
3. Check that facts still broadcast the dimensions (`BroadcastHashJoin` in the plan). A dimension that grew past `autoBroadcastJoinThreshold` silently turns into a sort-merge join.

## 9. Rolling back a bad publish

Iceberg keeps the history:
```sql
SELECT made_current_at, snapshot_id FROM lake.gold.fct_player_match.history ORDER BY made_current_at DESC;
CALL lake.system.rollback_to_snapshot('gold.fct_player_match', <previous_snapshot_id>);
```
Roll back the dependent tables (facts and marts) to the same point, then fix forward with a backfill.

## 10. Backfill checklist

1. `--dry-run` and paste the plan in the ticket.
2. Run with `--no-publish`, then review `ops.backfill_diffs` (row deltas should be explainable).
3. Announce the cutover window, pause the daily DAG, then `publish --branch backfill_<run_id>`.
4. Unpause and confirm the next daily run publishes cleanly.
