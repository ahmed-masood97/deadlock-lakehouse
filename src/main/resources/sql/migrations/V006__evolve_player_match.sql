-- Schema evolution example, applied after data already exists.
-- 1) Adding columns in Iceberg is a metadata-only change: no data files are rewritten and old
--    files simply read the new columns as NULL until the next backfill fills them.
-- 2) A write order clusters rows by hero inside each daily partition, so hero-filtered queries
--    skip most row groups using Parquet min/max stats. Existing files pick it up on compaction.
ALTER TABLE ${catalog}.gold.fct_player_match ADD COLUMNS (
  player_damage  BIGINT COMMENT 'Added in V006',
  player_healing BIGINT COMMENT 'Added in V006'
);

ALTER TABLE ${catalog}.gold.fct_player_match WRITE ORDERED BY hero_id, match_id;
ALTER TABLE ${catalog}.gold.fct_item_purchase WRITE ORDERED BY item_id, match_id;
