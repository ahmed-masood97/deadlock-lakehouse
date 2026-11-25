-- Platform state and observability. Plain Iceberg tables (no WAP): they record what happened,
-- including failures, so they must always be writable.
CREATE NAMESPACE IF NOT EXISTS ${catalog}.ops;

CREATE TABLE IF NOT EXISTS ${catalog}.ops.job_runs (
  run_id      STRING,
  job         STRING,
  run_window  STRING,
  status      STRING,
  started_at  TIMESTAMP,
  finished_at TIMESTAMP,
  duration_ms BIGINT,
  metrics     MAP<STRING, BIGINT>,
  error       STRING
) USING iceberg
PARTITIONED BY (days(started_at));

CREATE TABLE IF NOT EXISTS ${catalog}.ops.dq_results (
  run_id     STRING,
  table_name STRING,
  check_name STRING,
  severity   STRING,
  status     STRING,
  observed   STRING,
  expected   STRING,
  details    STRING,
  ref        STRING,
  checked_at TIMESTAMP
) USING iceberg
PARTITIONED BY (days(checked_at));

CREATE TABLE IF NOT EXISTS ${catalog}.ops.watermarks (
  job          STRING,
  source_table STRING,
  snapshot_id  BIGINT,
  snapshot_ts  TIMESTAMP,
  value        STRING,
  updated_at   TIMESTAMP,
  run_id       STRING
) USING iceberg;

CREATE TABLE IF NOT EXISTS ${catalog}.ops.lineage (
  run_id      STRING,
  job         STRING,
  direction   STRING,
  table_name  STRING,
  ref         STRING,
  snapshot_id BIGINT,
  recorded_at TIMESTAMP
) USING iceberg
PARTITIONED BY (days(recorded_at));

CREATE TABLE IF NOT EXISTS ${catalog}.ops.schema_drift (
  run_id            STRING,
  source            STRING,
  field_path        STRING,
  change            STRING,
  records_affected  BIGINT,
  sample_record_key STRING,
  detected_at       TIMESTAMP
) USING iceberg;

CREATE TABLE IF NOT EXISTS ${catalog}.ops.backfill_diffs (
  backfill_id  STRING,
  table_name   STRING,
  event_date   DATE,
  main_rows    BIGINT,
  branch_rows  BIGINT,
  metric       STRING,
  main_value   DOUBLE,
  branch_value DOUBLE,
  checked_at   TIMESTAMP
) USING iceberg;
