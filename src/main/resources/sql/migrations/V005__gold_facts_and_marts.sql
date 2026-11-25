-- Facts. Grain is stated on every table and enforced by a uniqueness check.

-- Grain: one row per player per match.
CREATE TABLE IF NOT EXISTS ${catalog}.gold.fct_player_match (
  match_id       BIGINT,
  player_slot    INT,
  start_time     TIMESTAMP,
  date_key       INT,
  patch_key      BIGINT,
  hero_sk        BIGINT,
  hero_id        INT,
  player_key     STRING,
  team           STRING,
  won            BOOLEAN,
  match_mode     STRING,
  average_badge  INT,
  badge_bucket   STRING,
  duration_s     INT,
  kills          INT,
  deaths         INT,
  assists        INT,
  kda            DOUBLE,
  net_worth      BIGINT,
  souls_per_min  DOUBLE,
  last_hits      INT,
  denies         INT,
  abandoned      BOOLEAN,
  _run_id        STRING,
  _loaded_at     TIMESTAMP
) USING iceberg
PARTITIONED BY (days(start_time))
TBLPROPERTIES ('write.wap.enabled' = 'true', 'write.distribution-mode' = 'hash', 'format-version' = '2');

-- Grain: one row per item purchase (shop upgrades only; ability unlocks are excluded).
CREATE TABLE IF NOT EXISTS ${catalog}.gold.fct_item_purchase (
  match_id        BIGINT,
  player_slot     INT,
  purchase_seq    INT,
  start_time      TIMESTAMP,
  date_key        INT,
  patch_key       BIGINT,
  hero_sk         BIGINT,
  hero_id         INT,
  player_key      STRING,
  item_sk         BIGINT,
  item_id         BIGINT,
  slot_type       STRING,
  tier            INT,
  cost_souls      BIGINT COMMENT 'Price in force at match start (point-in-time join to dim_item)',
  refund_souls    BIGINT COMMENT 'Half the cost back if the item was sold',
  net_cost_souls  BIGINT,
  game_time_s     INT,
  purchase_minute INT,
  sold            BOOLEAN,
  won             BOOLEAN,
  _run_id         STRING,
  _loaded_at      TIMESTAMP
) USING iceberg
PARTITIONED BY (days(start_time))
TBLPROPERTIES ('write.wap.enabled' = 'true', 'write.distribution-mode' = 'hash', 'format-version' = '2');

-- Grain: one row per player per match. "Soul economics": souls earned by source (revenue)
-- against souls spent by item category (cost drivers), with an explicit reconciliation gap.
CREATE TABLE IF NOT EXISTS ${catalog}.gold.fct_player_economy (
  match_id             BIGINT,
  player_slot          INT,
  start_time           TIMESTAMP,
  date_key             INT,
  patch_key            BIGINT,
  hero_sk              BIGINT,
  hero_id              INT,
  player_key           STRING,
  won                  BOOLEAN,
  badge_bucket         STRING,
  duration_s           INT,
  net_worth            BIGINT,
  souls_players        BIGINT,
  souls_lane           BIGINT,
  souls_neutral        BIGINT,
  souls_objectives     BIGINT,
  souls_treasure       BIGINT,
  souls_denied         BIGINT,
  souls_earned         BIGINT,
  souls_lost_on_death  BIGINT,
  spend_weapon         BIGINT,
  spend_vitality       BIGINT,
  spend_spirit         BIGINT,
  spend_total          BIGINT,
  items_bought         INT,
  souls_per_min        DOUBLE,
  spend_ratio          DOUBLE,
  reconciliation_gap   BIGINT COMMENT 'souls_earned - net_worth; should be ~0',
  _run_id              STRING,
  _loaded_at           TIMESTAMP
) USING iceberg
PARTITIONED BY (days(start_time))
TBLPROPERTIES ('write.wap.enabled' = 'true', 'format-version' = '2');

-- Marts: rebuilt per affected partition with dynamic partition overwrite.
CREATE TABLE IF NOT EXISTS ${catalog}.gold.mart_hero_patch_performance (
  patch_key         BIGINT,
  patch_title       STRING,
  hero_id           INT,
  hero_name         STRING,
  badge_bucket      STRING,
  matches           BIGINT,
  wins              BIGINT,
  win_rate          DOUBLE,
  win_rate_ci_low   DOUBLE,
  win_rate_ci_high  DOUBLE,
  pick_rate         DOUBLE,
  avg_kda           DOUBLE,
  avg_souls_per_min DOUBLE,
  _run_id           STRING,
  _built_at         TIMESTAMP
) USING iceberg
PARTITIONED BY (patch_key)
TBLPROPERTIES ('write.wap.enabled' = 'true', 'format-version' = '2');

CREATE TABLE IF NOT EXISTS ${catalog}.gold.mart_item_unit_economics (
  patch_key           BIGINT,
  patch_title         STRING,
  item_id             BIGINT,
  item_name           STRING,
  slot_type           STRING,
  tier                INT,
  cost_souls          BIGINT,
  purchases           BIGINT,
  buyer_win_rate      DOUBLE,
  non_buyer_win_rate  DOUBLE,
  win_rate_lift       DOUBLE,
  lift_per_1k_souls   DOUBLE,
  avg_purchase_minute DOUBLE,
  sell_rate           DOUBLE,
  souls_spent         BIGINT,
  _run_id             STRING,
  _built_at           TIMESTAMP
) USING iceberg
PARTITIONED BY (patch_key)
TBLPROPERTIES ('write.wap.enabled' = 'true', 'format-version' = '2');

CREATE TABLE IF NOT EXISTS ${catalog}.gold.mart_economy_daily (
  event_date             DATE,
  patch_key              BIGINT,
  badge_bucket           STRING,
  player_matches         BIGINT,
  avg_souls_per_min      DOUBLE,
  share_players          DOUBLE,
  share_lane             DOUBLE,
  share_neutral          DOUBLE,
  share_objectives       DOUBLE,
  share_treasure         DOUBLE,
  avg_spend_ratio        DOUBLE,
  avg_net_worth          DOUBLE,
  reconciliation_gap_pct DOUBLE,
  _run_id                STRING,
  _built_at              TIMESTAMP
) USING iceberg
PARTITIONED BY (event_date)
TBLPROPERTIES ('write.wap.enabled' = 'true', 'format-version' = '2');
