-- Bronze: one generic, append-only layout for every source. The raw payload is stored as text,
-- so a producer schema change can never break ingestion; parsing happens in silver under a contract.
-- Partitioned by ingestion day (hidden partitioning: no extra column for users to filter on).
CREATE NAMESPACE IF NOT EXISTS ${catalog}.bronze;

CREATE TABLE IF NOT EXISTS ${catalog}.bronze.raw_matches (
  ingest_run_id  STRING    COMMENT 'Run that landed this record',
  ingested_at    TIMESTAMP COMMENT 'When the record entered the lake',
  source         STRING,
  source_file    STRING    COMMENT 'Landing file the record came from (idempotent re-ingest key)',
  record_key     STRING    COMMENT 'Business key extracted from the payload',
  event_ts       TIMESTAMP COMMENT 'Event time extracted from the payload, used for backfill windows',
  snapshot_date  DATE      COMMENT 'For snapshot sources: the day the snapshot was taken',
  payload        STRING    COMMENT 'Raw JSON exactly as received',
  payload_sha256 STRING
) USING iceberg
PARTITIONED BY (days(ingested_at))
TBLPROPERTIES ('write.parquet.compression-codec' = 'zstd', 'format-version' = '2');

CREATE TABLE IF NOT EXISTS ${catalog}.bronze.raw_heroes  USING iceberg PARTITIONED BY (days(ingested_at))
  TBLPROPERTIES ('write.parquet.compression-codec' = 'zstd', 'format-version' = '2')
  AS SELECT * FROM ${catalog}.bronze.raw_matches WHERE false;

CREATE TABLE IF NOT EXISTS ${catalog}.bronze.raw_items   USING iceberg PARTITIONED BY (days(ingested_at))
  TBLPROPERTIES ('write.parquet.compression-codec' = 'zstd', 'format-version' = '2')
  AS SELECT * FROM ${catalog}.bronze.raw_matches WHERE false;

CREATE TABLE IF NOT EXISTS ${catalog}.bronze.raw_patches USING iceberg PARTITIONED BY (days(ingested_at))
  TBLPROPERTIES ('write.parquet.compression-codec' = 'zstd', 'format-version' = '2')
  AS SELECT * FROM ${catalog}.bronze.raw_matches WHERE false;
