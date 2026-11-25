-- Gold dimensions. All gold tables have write.wap.enabled so a run writes to a branch,
-- is audited there, and only then becomes visible on main (write-audit-publish).
CREATE NAMESPACE IF NOT EXISTS ${catalog}.gold;

-- SCD Type 2: every version of a hero is kept. valid_to uses a far-future sentinel instead of NULL
-- so point-in-time joins are a simple range predicate.
CREATE TABLE IF NOT EXISTS ${catalog}.gold.dim_hero (
  hero_sk           BIGINT  COMMENT 'Surrogate key: deterministic hash(hero_id, valid_from), stable across reruns',
  hero_id           INT     COMMENT 'Natural key from the game',
  class_name        STRING,
  hero_name         STRING,
  hero_type         STRING,
  complexity        INT,
  player_selectable BOOLEAN,
  disabled          BOOLEAN,
  base_max_health   DOUBLE,
  row_hash          STRING  COMMENT 'Hash of tracked attributes; a change opens a new version',
  valid_from        TIMESTAMP,
  valid_to          TIMESTAMP,
  is_current        BOOLEAN,
  _run_id           STRING
) USING iceberg
TBLPROPERTIES ('write.wap.enabled' = 'true', 'format-version' = '2');

-- SCD Type 2: item prices change between patches. Purchases are valued at the price in force
-- when the match was played, which is what makes the economy facts historically correct.
CREATE TABLE IF NOT EXISTS ${catalog}.gold.dim_item (
  item_sk     BIGINT,
  item_id     BIGINT,
  class_name  STRING,
  item_name   STRING,
  item_type   STRING,
  slot_type   STRING,
  tier        INT,
  cost_souls  BIGINT,
  shopable    BOOLEAN,
  disabled    BOOLEAN,
  row_hash    STRING,
  valid_from  TIMESTAMP,
  valid_to    TIMESTAMP,
  is_current  BOOLEAN,
  _run_id     STRING
) USING iceberg
TBLPROPERTIES ('write.wap.enabled' = 'true', 'format-version' = '2');

-- Patch windows: each patch is valid until the next one is published.
CREATE TABLE IF NOT EXISTS ${catalog}.gold.dim_patch (
  patch_key   BIGINT,
  patch_guid  STRING,
  title       STRING,
  released_at TIMESTAMP,
  valid_from  TIMESTAMP,
  valid_to    TIMESTAMP,
  link        STRING,
  is_current  BOOLEAN,
  _run_id     STRING
) USING iceberg
TBLPROPERTIES ('write.wap.enabled' = 'true', 'format-version' = '2');

-- SCD Type 1 on purpose: we only need the latest profile; history of a player's "last seen"
-- has no analytical value. The natural key is a salted hash, never the raw Steam id.
CREATE TABLE IF NOT EXISTS ${catalog}.gold.dim_player (
  player_key          STRING COMMENT 'sha256(salt || account_id)',
  first_seen_at       TIMESTAMP,
  last_seen_at        TIMESTAMP,
  matches_played      BIGINT,
  wins                BIGINT,
  latest_badge        INT,
  most_played_hero_id INT,
  _updated_at         TIMESTAMP,
  _run_id             STRING
) USING iceberg
TBLPROPERTIES ('write.wap.enabled' = 'true', 'format-version' = '2');
