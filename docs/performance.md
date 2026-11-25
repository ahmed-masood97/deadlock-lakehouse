# Performance and cost

The choices below decide how this pipeline scales from a laptop to billions of rows. For each one: what was done, why, and what to watch.

## 1. Joins: broadcast the dimensions, never shuffle the facts

Every fact needs two or three point-in-time lookups (hero, item, patch). These are **range joins** (`start_time >= valid_from AND start_time < valid_to`). A sort-merge join on a range condition has to shuffle the fact table on the equality key and then evaluate the range per match. Broadcasting the dimension (hundreds of rows) turns the whole lookup into a map-side `BroadcastHashJoin` with a filter, with no shuffle of the facts at all.

`Lookups.heroAsOf / itemAsOf / patchAsOf` use explicit `broadcast()` hints instead of relying on size estimates.

*Watch:* a dimension that grows past `spark.sql.autoBroadcastJoinThreshold` (set to 32 MB) without a hint silently falls back to a sort-merge join. The runbook's "slow run" section checks for this.

## 2. Skew: popular items and heroes

Starter items appear in almost every build, so any join or aggregation keyed on `item_id` is skewed by nature. `SkewLab` reproduces this: 4M purchases with 60% on one item, joined to a per-item table that cannot be broadcast.

Results on a 2-core laptop (`sbt "run skew-lab"`):

| Strategy | Wall time | Max task | Median task | Max / median |
|---|---:|---:|---:|---:|
| Sort-merge join, AQE off | 5.5 s | 950 ms | 95 ms | 10.0 |
| **AQE skew join** (`spark.sql.adaptive.skewJoin.enabled`) | **2.9 s** | 718 ms | 85 ms | 8.4 |
| Manual salting (16 buckets) | 3.8 s | 625 ms | 134 ms | 4.7 |

Takeaways:
- **AQE first.** It finds and splits the hot partition at runtime, with no code change, and roughly halved the wall time here. It is enabled globally in `SparkSessionFactory`.
- **Salting** spreads the work most evenly (lowest max/median) but pays for it: the small side is replicated N times and the code gets harder to read. Use it when AQE cannot help, for example skewed aggregations with non-splittable keys, or Spark versions without skew-join support.
- **Best of all is not to shuffle**: the production jobs broadcast every dimension, so this skew never reaches a shuffle in the real pipeline.

Spark's built-in partial aggregation (map-side combine) already absorbs skew in `groupBy`, which is why the mart SQL needs no special handling.

## 3. Partitioning and pruning

- Event tables use **hidden partitioning** on `days(start_time)`. Users filter on the timestamp they already know, and Iceberg prunes partitions, with no separate `dt` column to keep in sync.
- Deletes in the silver and gold "replace these matches" pattern carry **literal time bounds** computed from the batch (`Writes.startTimeBounds`) as well as the `match_id IN (...)` subquery. Iceberg can prune on literals before it evaluates the subquery, so a late-arriving match from last week rewrites one partition instead of scanning the table.
- The bounds are formatted in SQL in the session time zone (UTC), never with `java.sql.Timestamp.toString`, which uses the JVM's zone. On a laptop in New York the JVM approach would shift the bounds by five hours and silently miss rows.
- Marts are partitioned by `patch_key` or `event_date` and rebuilt with **dynamic partition overwrite** for only the partitions a run touched.

## 4. File layout

- `WRITE ORDERED BY hero_id, match_id` on `fct_player_match` (and `item_id, match_id` on purchases) clusters rows inside each day. Parquet min/max statistics then let hero- or item-filtered queries skip most row groups.
- Small files from daily MERGEs are compacted weekly (`rewrite_data_files`). The ordered tables use the `sort` strategy, so compaction restores clustering, and the rest use `binpack`. Manifests are rewritten for faster planning.
- Parquet with **zstd** for bronze. Raw JSON compresses very well, which is what makes ADR 0002 (store raw payloads) affordable.

## 5. Incremental everything

- bronze → silver reads only the data files added since the last consumed snapshot. Cost is proportional to new data, not to the table.
- Change detection on `payload_sha256` means re-sent matches cost one hash comparison and no writes.
- Data quality checks are scoped to the rows a run touched, so the audit cost grows with the batch, not with history.

## 6. Caching discipline

Caches are **job-scoped**: `OpsStore.execute` clears the cache before and after every job. This came from a real bug found while building the project. A DataFrame cached by one job (a full read of `silver.matches`) was silently reused by the next job's query, because Spark's cache manager matches plans structurally. That query then missed every row committed in between. Caching is still used inside jobs where a DataFrame is reused (batch parsing, staged rows). SCD2 staging uses `localCheckpoint` so the MERGE reads a fixed result rather than re-deriving it from the table it is modifying.

## 7. Cost levers in production

| Lever | Where |
|---|---|
| Snapshot expiry (7 days, keep the last 10) and orphan cleanup | `Maintenance` |
| Bronze snapshots kept back to the oldest incremental watermark | `Maintenance` |
| Dynamic allocation, AQE partition coalescing (fewer, larger tasks) | `airflow/dags/deadlock_daily.py`, `SparkSessionFactory` |
| Partition-scoped mart rebuilds instead of full refreshes | `MartsJob` |
| One Spark application per stage, so a retry re-runs only what failed | Airflow DAG |
