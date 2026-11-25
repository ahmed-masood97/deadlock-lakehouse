-- Daily soul economy by skill bracket: where souls come from (revenue mix), how much of it is
-- spent, and how well the producer's numbers reconcile. A rising reconciliation_gap_pct is an
-- early warning that the API changed how it reports souls.
SELECT
  to_date(start_time)                                             AS event_date,
  patch_key,
  badge_bucket,
  count(*)                                                        AS player_matches,
  round(avg(souls_per_min), 1)                                    AS avg_souls_per_min,
  round(sum(souls_players)    / sum(souls_earned), 4)             AS share_players,
  round(sum(souls_lane)       / sum(souls_earned), 4)             AS share_lane,
  round(sum(souls_neutral)    / sum(souls_earned), 4)             AS share_neutral,
  round(sum(souls_objectives) / sum(souls_earned), 4)             AS share_objectives,
  round(sum(souls_treasure)   / sum(souls_earned), 4)             AS share_treasure,
  round(avg(spend_ratio), 4)                                      AS avg_spend_ratio,
  round(avg(net_worth), 1)                                        AS avg_net_worth,
  round(sum(abs(reconciliation_gap)) / sum(net_worth), 6)         AS reconciliation_gap_pct,
  '${run_id}'                                                     AS _run_id,
  current_timestamp()                                             AS _built_at
FROM ${catalog}.gold.fct_player_economy
WHERE ${date_filter}
GROUP BY to_date(start_time), patch_key, badge_bucket
