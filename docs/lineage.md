# Lineage

Generated from the code with `sbt "run lineage"`: each job declares the tables it reads and writes, so this diagram cannot drift from the implementation. Per-run, snapshot-level lineage is recorded in `ops.lineage`.

```mermaid
flowchart LR
  job_bronze_ingest_matches{{"bronze_ingest_matches"}}
  job_bronze_ingest_matches --> bronze_raw_matches[("bronze.raw_matches")]
  job_bronze_ingest_heroes{{"bronze_ingest_heroes"}}
  job_bronze_ingest_heroes --> bronze_raw_heroes[("bronze.raw_heroes")]
  job_bronze_ingest_items{{"bronze_ingest_items"}}
  job_bronze_ingest_items --> bronze_raw_items[("bronze.raw_items")]
  job_bronze_ingest_patches{{"bronze_ingest_patches"}}
  job_bronze_ingest_patches --> bronze_raw_patches[("bronze.raw_patches")]
  job_silver_matches{{"silver_matches"}}
  bronze_raw_matches[("bronze.raw_matches")] --> job_silver_matches
  job_silver_matches --> silver_matches[("silver.matches")]
  job_silver_matches --> silver_match_players[("silver.match_players")]
  job_silver_matches --> silver_item_purchases[("silver.item_purchases")]
  job_silver_matches --> silver_quarantine[("silver.quarantine")]
  job_dim_hero{{"dim_hero"}}
  bronze_raw_heroes[("bronze.raw_heroes")] --> job_dim_hero
  job_dim_hero --> gold_dim_hero[("gold.dim_hero")]
  job_dim_item{{"dim_item"}}
  bronze_raw_items[("bronze.raw_items")] --> job_dim_item
  job_dim_item --> gold_dim_item[("gold.dim_item")]
  job_dim_patch{{"dim_patch"}}
  bronze_raw_patches[("bronze.raw_patches")] --> job_dim_patch
  job_dim_patch --> gold_dim_patch[("gold.dim_patch")]
  job_dim_player{{"dim_player"}}
  silver_match_players[("silver.match_players")] --> job_dim_player
  silver_matches[("silver.matches")] --> job_dim_player
  job_dim_player --> gold_dim_player[("gold.dim_player")]
  job_fct_player_match{{"fct_player_match"}}
  silver_matches[("silver.matches")] --> job_fct_player_match
  silver_match_players[("silver.match_players")] --> job_fct_player_match
  gold_dim_hero[("gold.dim_hero")] --> job_fct_player_match
  gold_dim_patch[("gold.dim_patch")] --> job_fct_player_match
  job_fct_player_match --> gold_fct_player_match[("gold.fct_player_match")]
  job_fct_item_purchase{{"fct_item_purchase"}}
  silver_item_purchases[("silver.item_purchases")] --> job_fct_item_purchase
  silver_match_players[("silver.match_players")] --> job_fct_item_purchase
  gold_dim_item[("gold.dim_item")] --> job_fct_item_purchase
  gold_dim_hero[("gold.dim_hero")] --> job_fct_item_purchase
  gold_dim_patch[("gold.dim_patch")] --> job_fct_item_purchase
  job_fct_item_purchase --> gold_fct_item_purchase[("gold.fct_item_purchase")]
  job_fct_player_economy{{"fct_player_economy"}}
  silver_match_players[("silver.match_players")] --> job_fct_player_economy
  silver_matches[("silver.matches")] --> job_fct_player_economy
  gold_fct_item_purchase[("gold.fct_item_purchase")] --> job_fct_player_economy
  gold_dim_hero[("gold.dim_hero")] --> job_fct_player_economy
  gold_dim_patch[("gold.dim_patch")] --> job_fct_player_economy
  job_fct_player_economy --> gold_fct_player_economy[("gold.fct_player_economy")]
  job_marts{{"marts"}}
  gold_fct_player_match[("gold.fct_player_match")] --> job_marts
  gold_fct_item_purchase[("gold.fct_item_purchase")] --> job_marts
  gold_fct_player_economy[("gold.fct_player_economy")] --> job_marts
  gold_dim_hero[("gold.dim_hero")] --> job_marts
  gold_dim_item[("gold.dim_item")] --> job_marts
  gold_dim_patch[("gold.dim_patch")] --> job_marts
  job_marts --> gold_mart_hero_patch_performance[("gold.mart_hero_patch_performance")]
  job_marts --> gold_mart_item_unit_economics[("gold.mart_item_unit_economics")]
  job_marts --> gold_mart_economy_daily[("gold.mart_economy_daily")]
```
