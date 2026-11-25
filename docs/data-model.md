# Data model and metric definitions

All tables live in the Iceberg catalog `lake`. Timestamps are UTC.

## Silver (source grain)

| Table | Grain | Key | Partitioning | Notes |
|---|---|---|---|---|
| `silver.matches` | match | `match_id` | `days(start_time)` | `payload_sha256` drives change detection; `_updated_at` is the downstream high-water mark |
| `silver.match_players` | player × match | `match_id, player_slot` | `days(start_time)` | `account_id` is PII and never leaves silver unhashed |
| `silver.item_purchases` | item event | `match_id, player_slot, purchase_seq` | `days(start_time)` | Includes ability unlocks; gold filters them out |
| `silver.quarantine` | rejected record | `run_id, record_key` | `days(quarantined_at)` | `reasons` holds every violated rule; `payload` lets you replay it |

## Gold dimensions

| Table | Type | Natural key | Surrogate key | Tracked attributes |
|---|---|---|---|---|
| `gold.dim_hero` | SCD2 | `hero_id` | `hero_sk = xxhash64(hero_id, valid_from)` | name, type, complexity, selectable, disabled, base max health |
| `gold.dim_item` | SCD2 | `item_id` | `item_sk = xxhash64(item_id, valid_from)` | name, type, slot, tier, **cost_souls**, shopable, disabled |
| `gold.dim_patch` | rebuilt | `patch_guid` | `patch_key = xxhash64(guid)` | validity window `[valid_from, valid_to)` = release until next release |
| `gold.dim_player` | SCD1 | `player_key` | n/a | first and last seen, matches, wins, latest badge, most played hero |

The unknown member is `-1` in every surrogate-key column. It is used when a fact arrives before its dimension row.

## Gold facts

| Table | Grain | Keys |
|---|---|---|
| `gold.fct_player_match` | one row per player per match | `match_id, player_slot` |
| `gold.fct_item_purchase` | one row per shop purchase | `match_id, player_slot, purchase_seq` |
| `gold.fct_player_economy` | one row per player per match | `match_id, player_slot` |

All facts carry `date_key` (yyyyMMdd), `patch_key`, `hero_sk` (as of `start_time`), `player_key`, `_run_id` and `_loaded_at`.

## Metric definitions

Defined once in code (`core/Model.scala`, the fact jobs and the mart SQL), so every table uses the same formula.

| Metric | Definition |
|---|---|
| `kda` | `(kills + assists) / max(deaths, 1)` |
| `souls_per_min` | `net_worth / (duration_s / 60)` |
| `badge_bucket` | Badge = tier × 10 + subtier. Tiers 1–4 *low*, 5–7 *mid*, 8–9 *high*, 10–11 *top* |
| `cost_souls` | Item price **in force at match start** (point-in-time join to `dim_item`) |
| `refund_souls` | `floor(cost / 2)` if the item was sold, else 0 |
| `net_cost_souls` | `cost_souls − refund_souls` |
| `souls_players` | `gold_player + gold_player_orbs` (kills and assists) |
| `souls_lane` | `gold_lane_creep + gold_lane_creep_orbs` |
| `souls_neutral` | `gold_neutral_creep + gold_neutral_creep_orbs` |
| `souls_objectives` | `gold_boss + gold_boss_orb` |
| `souls_earned` | sum of all positive sources (players, lane, neutral, objectives, treasure, denies) |
| `reconciliation_gap` | `souls_earned − souls_lost_on_death − final_stats.net_worth` (should be ~0) |
| `spend_ratio` | `spend_total / net_worth` |
| `win_rate` | `wins / matches`, excluding abandoned games |
| `win_rate_ci_low/high` | 95% Wilson score interval: `(p + z²/2n ± z·√(p(1−p)/n + z²/4n²)) / (1 + z²/n)`, with z = 1.96 |
| `pick_rate` | matches with the hero / matches in the patch and bracket (a hero appears at most once per match) |
| `buyer_win_rate` | win rate of player-matches that bought the item |
| `non_buyer_win_rate` | **hero-mix controlled**: per hero, the win rate of non-buyers in the same patch, averaged with weights equal to that hero's buyer count |
| `win_rate_lift` | `buyer_win_rate − non_buyer_win_rate` (correlational) |
| `lift_per_1k_souls` | `win_rate_lift / (cost_souls / 1000)`, which makes cheap and expensive items comparable |
| `reconciliation_gap_pct` | `Σ|reconciliation_gap| / Σ net_worth` per day and bracket. A rise is an early warning that the producer changed how souls are reported |

## Marts

| Table | Grain | Rebuilt by |
|---|---|---|
| `gold.mart_hero_patch_performance` | patch × hero × badge bucket | patches touched by the run (dynamic partition overwrite) |
| `gold.mart_item_unit_economics` | patch × item | patches touched by the run |
| `gold.mart_economy_daily` | day × patch × badge bucket | days touched by the run |

## Ops

| Table | Purpose |
|---|---|
| `ops.job_runs` | One row per job execution: window, status, duration, metrics map, error |
| `ops.dq_results` | Every check result, with the branch (ref) it ran against |
| `ops.lineage` | Each job's input and output tables with the exact snapshot ids |
| `ops.watermarks` | Incremental state per job (snapshot id, high-water mark, or snapshot date) |
| `ops.schema_drift` | Undeclared or absent fields per run |
| `ops.backfill_diffs` | Main vs branch, per table, per day, per measure, for every backfill |
| `ops.schema_migrations` | Applied DDL versions with checksums |
