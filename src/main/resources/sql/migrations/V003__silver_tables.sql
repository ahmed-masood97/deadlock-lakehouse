-- Silver: parsed, contract-validated, deduplicated. Partitioned by event day so late-arriving
-- matches land in the right partition and date-range backfills prune to exactly what they touch.
CREATE NAMESPACE IF NOT EXISTS ${catalog}.silver;

CREATE TABLE IF NOT EXISTS ${catalog}.silver.matches (
  match_id            BIGINT,
  start_time          TIMESTAMP,
  duration_s          INT,
  winning_team        STRING,
  match_outcome       STRING,
  match_mode          STRING,
  game_mode           STRING,
  average_badge_team0 INT,
  average_badge_team1 INT,
  not_scored          BOOLEAN,
  banned_hero_ids     ARRAY<INT>,
  player_count        INT,
  payload_sha256      STRING    COMMENT 'Change detection: a re-ingested match only rewrites if this differs',
  ingest_run_id       STRING,
  ingested_at         TIMESTAMP,
  _updated_at         TIMESTAMP COMMENT 'High-water mark column for downstream incremental loads',
  _run_id             STRING
) USING iceberg
PARTITIONED BY (days(start_time))
TBLPROPERTIES ('format-version' = '2');

CREATE TABLE IF NOT EXISTS ${catalog}.silver.match_players (
  match_id             BIGINT,
  start_time           TIMESTAMP,
  player_slot          INT,
  account_id           BIGINT COMMENT 'PII: never leaves silver unhashed',
  hero_id              INT,
  team                 STRING,
  won                  BOOLEAN,
  kills                INT,
  deaths               INT,
  assists              INT,
  net_worth            INT,
  last_hits            INT,
  denies               INT,
  ability_points       INT,
  assigned_lane        INT,
  player_level         INT,
  abandon_match_time_s INT,
  mvp_rank             INT,
  hero_build_id        BIGINT,
  final_stats STRUCT<
    net_worth: BIGINT, gold_player: BIGINT, gold_player_orbs: BIGINT, gold_lane_creep: BIGINT,
    gold_lane_creep_orbs: BIGINT, gold_neutral_creep: BIGINT, gold_neutral_creep_orbs: BIGINT,
    gold_boss: BIGINT, gold_boss_orb: BIGINT, gold_treasure: BIGINT, gold_denied: BIGINT,
    gold_death_loss: BIGINT, player_damage: BIGINT, player_healing: BIGINT, boss_damage: BIGINT,
    creep_damage: BIGINT, shots_hit: BIGINT, shots_missed: BIGINT>,
  _updated_at          TIMESTAMP,
  _run_id              STRING
) USING iceberg
PARTITIONED BY (days(start_time))
TBLPROPERTIES ('format-version' = '2');

CREATE TABLE IF NOT EXISTS ${catalog}.silver.item_purchases (
  match_id          BIGINT,
  start_time        TIMESTAMP,
  player_slot       INT,
  purchase_seq      INT COMMENT 'Position in the player item timeline',
  item_id           BIGINT,
  upgrade_id        BIGINT,
  game_time_s       INT,
  sold_time_s       INT,
  flags             INT,
  imbued_ability_id BIGINT,
  _updated_at       TIMESTAMP,
  _run_id           STRING
) USING iceberg
PARTITIONED BY (days(start_time))
TBLPROPERTIES ('format-version' = '2');

CREATE TABLE IF NOT EXISTS ${catalog}.silver.quarantine (
  run_id         STRING,
  source         STRING,
  record_key     STRING,
  reasons        ARRAY<STRING>,
  payload        STRING,
  ingest_run_id  STRING,
  quarantined_at TIMESTAMP
) USING iceberg
PARTITIONED BY (days(quarantined_at));
