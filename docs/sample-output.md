# Sample output

Output of `sbt "run demo"` (250 matches per day, 30-day synthetic season, laptop run). The pipeline's own structured JSON event log is omitted here; it goes to stdout alongside this.

```
====================================================================================================
1. Days 1-20 land; first daily run (patches 1 and 2, item repricing on day 10)
====================================================================================================
====================================================================================================
2. Day 21 lands: patch 3 releases Holliday, but the hero feed has not caught up yet
====================================================================================================
=== Facts that reference a hero the dimension does not know yet (unknown member -1) ===
+----------+----+
|day       |rows|
+----------+----+
|2026-10-21|146 |
+----------+----+
====================================================================================================
3. Days 22-30 land, including the hero snapshot with Holliday
====================================================================================================
====================================================================================================
4. Backfill day 21 from gold.dim_hero downstream: rebuilt in a branch, diffed, then cut over
====================================================================================================
Backfill plan for gold.dim_hero over range[2026-10-21..2026-10-21]:
   1. dim_hero               writes gold.dim_hero
   2. fct_player_match       writes gold.fct_player_match
   3. fct_item_purchase      writes gold.fct_item_purchase
   4. fct_player_economy     writes gold.fct_player_economy
   5. marts                  writes gold.mart_hero_patch_performance, gold.mart_item_unit_economics, gold.mart_economy_daily
Backfill diff (main vs branch):
+-----------------------+----------+---------+-----------+-----------------+------------+------------+------+
|table_name             |event_date|main_rows|branch_rows|metric           |main_value  |branch_value|delta |
+-----------------------+----------+---------+-----------+-----------------+------------+------------+------+
|gold.fct_item_purchase |2026-10-21|45368    |45368      |net_cost_souls   |1.259496E8  |1.259496E8  |0.0   |
|gold.fct_item_purchase |2026-10-21|45368    |45368      |unknown_item_rows|0.0         |0.0         |0.0   |
|gold.fct_player_economy|2026-10-21|3000     |3000       |souls_earned     |1.85078659E8|1.85078659E8|0.0   |
|gold.fct_player_match  |2026-10-21|3000     |3000       |net_worth        |1.77997424E8|1.77997424E8|0.0   |
|gold.fct_player_match  |2026-10-21|3000     |3000       |unknown_hero_rows|146.0       |0.0         |-146.0|
+-----------------------+----------+---------+-----------+-----------------+------------+------------+------+
=== Unknown hero members after the backfill ===
+-----------------+
|unknown_hero_rows|
+-----------------+
|0                |
+-----------------+
====================================================================================================
5. What the platform recorded
====================================================================================================
=== SCD2: one item across the patch-2 repricing ===
+---------+------------+----------+-------------------+-------------------+----------+
|item_id  |item_name   |cost_souls|valid_from         |valid_to           |is_current|
+---------+------------+----------+-------------------+-------------------+----------+
|549087390|Extra Health|500       |1900-01-01 00:00:00|2026-10-10 00:00:00|false     |
|549087390|Extra Health|800       |2026-10-10 00:00:00|9999-12-31 00:00:00|true      |
+---------+------------+----------+-------------------+-------------------+----------+
=== SCD2: the Abrams rework in patch 3 ===
+-------+---------+---------+---------------+-------------------+-------------------+----------+
|hero_id|hero_name|hero_type|base_max_health|valid_from         |valid_to           |is_current|
+-------+---------+---------+---------------+-------------------+-------------------+----------+
|6      |Abrams   |brawler  |780.0          |1900-01-01 00:00:00|2026-10-21 00:00:00|false     |
|6      |Abrams   |assassin |900.0          |2026-10-21 00:00:00|9999-12-31 00:00:00|true      |
+-------+---------+---------+---------------+-------------------+-------------------+----------+
=== Quarantined records by reason ===
+----------------------------------+---+
|reason                            |n  |
+----------------------------------+---+
|duration_s: below min 60.0        |30 |
|winning_team: not in [Team0,Team1]|30 |
|payload: malformed JSON           |6  |
+----------------------------------+---+
=== Schema drift detected ===
+-------+-----------------+------+-------+
|source |field_path       |change|records|
+-------+-----------------+------+-------+
|matches|players[].hero_xp|added |2527   |
|matches|rewards_eligible |added |2527   |
+-------+-----------------+------+-------+
=== Data quality: failures and warnings across all runs ===
+--------------------------+------------------------------+--------+------------------+--------+----+
|table_name                |check_name                    |severity|observed          |expected|runs|
+--------------------------+------------------------------+--------+------------------+--------+----+
|lake.gold.fct_player_match|max_ratio(unknown_hero_member)|warn    |0.04867 (146/3000)|<= 0.001|1   |
+--------------------------+------------------------------+--------+------------------+--------+----+
=== Job runs ===
+--------------------------+------------------+-----------------------------+---------+-----------+------------------------------------------------------------------------------------------------------------+
|run_id                    |job               |run_window                   |status   |duration_ms|metrics                                                                                                     |
+--------------------------+------------------+-----------------------------+---------+-----------+------------------------------------------------------------------------------------------------------------+
|demo_20261123091847_0c40d6|marts             |range[2026-10-21..2026-10-21]|SUCCEEDED|5488       |{rows_mart_hero_patch_performance -> 84, rows_mart_item_unit_economics -> 41, rows_mart_economy_daily -> 4} |
|demo_20261123091847_0c40d6|fct_player_economy|range[2026-10-21..2026-10-21]|SUCCEEDED|1771       |{matches -> 250, rows_written -> 3000}                                                                      |
|demo_20261123091847_0c40d6|fct_item_purchase |range[2026-10-21..2026-10-21]|SUCCEEDED|2118       |{matches -> 250, rows_written -> 45368}                                                                     |
|demo_20261123091847_0c40d6|fct_player_match  |range[2026-10-21..2026-10-21]|SUCCEEDED|1712       |{matches -> 250, rows_written -> 3000}                                                                      |
|demo_20261123091847_0c40d6|dim_hero          |range[2026-10-21..2026-10-21]|SUCCEEDED|6574       |{invalid_records -> 0, unchanged -> 120, new_versions -> 1, new_keys -> 21, snapshots_applied -> 7}         |
|demo_20261123091708_f3fd2a|marts             |incremental                  |SUCCEEDED|5577       |{rows_mart_hero_patch_performance -> 84, rows_mart_item_unit_economics -> 41, rows_mart_economy_daily -> 36}|
|demo_20261123091708_f3fd2a|fct_player_economy|incremental                  |SUCCEEDED|3592       |{matches -> 2250, rows_written -> 27000}                                                                    |
|demo_20261123091708_f3fd2a|fct_item_purchase |incremental                  |SUCCEEDED|5487       |{matches -> 2250, rows_written -> 406790}                                                                   |
|demo_20261123091708_f3fd2a|fct_player_match  |incremental                  |SUCCEEDED|3573       |{matches -> 2250, rows_written -> 27000}                                                                    |
|demo_20261123091708_f3fd2a|dim_player        |incremental                  |SUCCEEDED|4381       |{players_upserted -> 4067}                                                                                  |
|demo_20261123091708_f3fd2a|dim_patch         |incremental                  |SUCCEEDED|489        |{patches -> 3}                                                                                              |
|demo_20261123091708_f3fd2a|dim_item          |incremental                  |SUCCEEDED|1487       |{invalid_records -> 0, unchanged -> 92, new_versions -> 0, new_keys -> 0, snapshots_applied -> 2}           |
+--------------------------+------------------+-----------------------------+---------+-----------+------------------------------------------------------------------------------------------------------------+
only showing top 12 rows
====================================================================================================
6. Marts
====================================================================================================
=== Hero win rates by patch, all brackets combined (per-bracket rows carry 95% Wilson intervals) ===
+------------------------+----------+-------+--------+
|patch_title             |hero_name |matches|win_rate|
+------------------------+----------+-------+--------+
|Economy Rebalance Update|Lash      |1650   |0.6788  |
|Economy Rebalance Update|McGinnis  |1658   |0.5887  |
|Economy Rebalance Update|Dynamo    |1602   |0.5855  |
|Economy Rebalance Update|Yamato    |1638   |0.58    |
|Economy Rebalance Update|Vindicta  |1694   |0.5762  |
|Economy Rebalance Update|Grey Talon|1656   |0.5519  |
|Economy Rebalance Update|Haze      |1650   |0.5364  |
|Economy Rebalance Update|Wraith    |1634   |0.533   |
|Economy Rebalance Update|Shiv      |1625   |0.5298  |
|Economy Rebalance Update|Bebop     |1653   |0.5021  |
|Economy Rebalance Update|Lady Geist|1645   |0.4973  |
|Economy Rebalance Update|Viscous   |1634   |0.4951  |
|Economy Rebalance Update|Infernus  |1692   |0.477   |
|Economy Rebalance Update|Ivy       |1629   |0.4684  |
|Economy Rebalance Update|Seven     |1644   |0.444   |
+------------------------+----------+-------+--------+
only showing top 15 rows
=== Abrams before and after the patch-3 rework ===
+------------------------+-------------------------+-------+--------+---------------+----------------+
|patch_title             |badge_bucket             |matches|win_rate|win_rate_ci_low|win_rate_ci_high|
+------------------------+-------------------------+-------+--------+---------------+----------------+
|Economy Rebalance Update|1_low (Initiate-Arcanist)|612    |0.3382  |0.3019         |0.3766          |
|Economy Rebalance Update|2_mid (Ritualist-Archon) |462    |0.3831  |0.3399         |0.4282          |
|Economy Rebalance Update|3_high (Oracle-Phantom)  |271    |0.3875  |0.3314         |0.4466          |
|Economy Rebalance Update|4_top (Ascendant-Eternus)|282    |0.4043  |0.3486         |0.4624          |
|New Hero: Holliday      |1_low (Initiate-Arcanist)|518    |0.5965  |0.5537         |0.6379          |
|New Hero: Holliday      |2_mid (Ritualist-Archon) |378    |0.582   |0.5317         |0.6307          |
|New Hero: Holliday      |3_high (Oracle-Phantom)  |266    |0.5075  |0.4478         |0.5671          |
|New Hero: Holliday      |4_top (Ascendant-Eternus)|259    |0.5328  |0.472          |0.5927          |
|Season Kickoff Update   |1_low (Initiate-Arcanist)|493    |0.4888  |0.445          |0.5329          |
|Season Kickoff Update   |2_mid (Ritualist-Archon) |347    |0.5303  |0.4777         |0.5822          |
|Season Kickoff Update   |3_high (Oracle-Phantom)  |215    |0.4605  |0.3951         |0.5272          |
|Season Kickoff Update   |4_top (Ascendant-Eternus)|267    |0.4382  |0.38           |0.4982          |
+------------------------+-------------------------+-------+--------+---------------+----------------+
=== Item unit economics: best win-rate lift per 1,000 souls (latest patch) ===
+------------------+---------+----+----------+---------+--------------+------------------+-------------+-----------------+
|item_name         |slot_type|tier|cost_souls|purchases|buyer_win_rate|non_buyer_win_rate|win_rate_lift|lift_per_1k_souls|
+------------------+---------+----+----------+---------+--------------+------------------+-------------+-----------------+
|Healing Rite      |vitality |1   |800       |9752     |0.5093        |0.4975            |0.0118       |0.0147           |
|Crippling Headshot|weapon   |4   |6400      |13859    |0.5496        |0.4564            |0.0932       |0.0146           |
|Glass Cannon      |weapon   |4   |6400      |13853    |0.5488        |0.4562            |0.0926       |0.0145           |
|Leech             |vitality |4   |6400      |10320    |0.552         |0.4731            |0.0789       |0.0123           |
|Boundless Spirit  |spirit   |4   |6400      |11081    |0.537         |0.4626            |0.0743       |0.0116           |
|Extra Health      |vitality |1   |800       |9739     |0.5082        |0.4993            |0.0088       |0.011            |
|Unstoppable       |vitality |4   |6400      |10406    |0.5469        |0.4772            |0.0697       |0.0109           |
|Colossus          |vitality |4   |6400      |10270    |0.5471        |0.4782            |0.0689       |0.0108           |
|Refresher         |spirit   |4   |6400      |11164    |0.536         |0.4676            |0.0685       |0.0107           |
|Echo Shard        |spirit   |4   |6400      |11215    |0.5346        |0.4663            |0.0682       |0.0107           |
+------------------+---------+----+----------+---------+--------------+------------------+-------------+-----------------+
only showing top 10 rows
=== Soul economy: revenue mix and reconciliation by day ===
+----------+--------------+------+-----+-------+-------+-----------+--------+
|event_date|player_matches|spm   |lane |neutral|players|spend_ratio|gap_pct |
+----------+--------------+------+-----+-------+-------+-----------+--------+
|2026-10-01|3000          |1569.1|0.437|0.218  |0.179  |0.707      |6.58E-4 |
|2026-10-02|3000          |1570.3|0.437|0.218  |0.179  |0.703      |6.21E-4 |
|2026-10-03|3000          |1569.1|0.437|0.218  |0.178  |0.709      |7.5E-4  |
|2026-10-04|3000          |1568.3|0.437|0.218  |0.179  |0.71       |8.76E-4 |
|2026-10-05|3000          |1572.6|0.437|0.218  |0.179  |0.715      |6.42E-4 |
|2026-10-06|3000          |1570.2|0.436|0.218  |0.179  |0.704      |0.001132|
|2026-10-07|3000          |1572.2|0.437|0.218  |0.179  |0.708      |9.91E-4 |
|2026-10-08|3000          |1564.4|0.437|0.218  |0.178  |0.712      |3.79E-4 |
|2026-10-09|3000          |1567.6|0.437|0.218  |0.178  |0.703      |8.12E-4 |
|2026-10-10|3000          |1568.6|0.437|0.219  |0.178  |0.732      |6.15E-4 |
|2026-10-11|3000          |1573.4|0.437|0.218  |0.178  |0.725      |5.08E-4 |
|2026-10-12|3000          |1567.4|0.437|0.218  |0.178  |0.742      |6.11E-4 |
|2026-10-13|3000          |1569.8|0.437|0.218  |0.178  |0.735      |7.38E-4 |
|2026-10-14|3000          |1567.2|0.437|0.218  |0.178  |0.736      |4.47E-4 |
|2026-10-15|3000          |1567.9|0.437|0.218  |0.178  |0.731      |0.001074|
|2026-10-16|3000          |1568.7|0.437|0.218  |0.178  |0.727      |6.98E-4 |
|2026-10-17|3000          |1566.1|0.437|0.218  |0.179  |0.734      |7.08E-4 |
|2026-10-18|3000          |1563.7|0.436|0.218  |0.179  |0.731      |9.12E-4 |
|2026-10-19|3000          |1565.0|0.437|0.218  |0.178  |0.731      |8.1E-4  |
|2026-10-20|3000          |1574.3|0.437|0.218  |0.179  |0.727      |2.78E-4 |
|2026-10-21|3000          |1568.3|0.437|0.218  |0.178  |0.727      |6.79E-4 |
|2026-10-22|3000          |1569.4|0.436|0.218  |0.179  |0.742      |9.42E-4 |
|2026-10-23|3000          |1566.8|0.436|0.218  |0.179  |0.736      |4.86E-4 |
|2026-10-24|3000          |1566.4|0.437|0.218  |0.179  |0.738      |8.93E-4 |
|2026-10-25|3000          |1569.5|0.437|0.218  |0.178  |0.731      |0.001087|
|2026-10-26|3000          |1567.0|0.437|0.218  |0.178  |0.729      |7.1E-4  |
|2026-10-27|3000          |1566.5|0.437|0.218  |0.179  |0.74       |9.02E-4 |
|2026-10-28|3000          |1571.2|0.437|0.218  |0.179  |0.735      |7.91E-4 |
|2026-10-29|3000          |1562.6|0.437|0.218  |0.179  |0.729      |5.35E-4 |
|2026-10-30|3000          |1571.2|0.437|0.218  |0.178  |0.739      |5.83E-4 |
+----------+--------------+------+-----+-------+-------+-----------+--------+
====================================================================================================
7. Time travel: the fact table as it was before the backfill
====================================================================================================
lake.gold.fct_player_match has 9 snapshots; the first one is 3748067955748062754
=== Row counts per snapshot of fct_player_match (last 5) ===
+-----------------------+-------------------+---------+-------------+
|made_current_at        |snapshot_id        |operation|total_records|
+-----------------------+-------------------+---------+-------------+
|2026-11-23 09:19:17.051|1783394189333524189|append   |90000        |
|2026-11-23 09:18:44.983|1630881428351012723|append   |90000        |
|2026-11-23 09:17:04.789|48821111421891789  |append   |63000        |
|2026-11-23 09:16:17.399|1507719617399714877|append   |60000        |
+-----------------------+-------------------+---------+-------------+
Demo complete. Explore with: sql "SELECT ... FROM lake.gold...."
```
