-- Item unit economics per patch: what does an item cost, and what is it associated with?
--
-- Lift is hero-mix controlled: for every hero we compare players who bought the item with players
-- on the same hero, same patch, who did not; the per-hero differences are then averaged, weighted
-- by buyers. Without this, items popular on strong heroes would look good for the wrong reason.
-- This is still correlation, not causation: late, expensive items are bought by players who are
-- already ahead. lift_per_1k_souls normalises by price so cheap and expensive items compare.
WITH buyer_players AS (
  SELECT patch_key, hero_id, item_id, match_id, player_slot,
         max(CAST(won AS INT))   AS won,
         max(cost_souls)         AS cost_souls,
         sum(net_cost_souls)     AS spent,
         min(purchase_minute)    AS purchase_minute,
         max(CAST(sold AS INT))  AS sold
  FROM ${catalog}.gold.fct_item_purchase
  WHERE ${patch_filter} AND item_sk <> -1
  GROUP BY patch_key, hero_id, item_id, match_id, player_slot
),
hero_base AS (
  SELECT patch_key, hero_id, count(*) AS n, sum(CAST(won AS INT)) AS w
  FROM ${catalog}.gold.fct_player_match
  WHERE ${patch_filter}
  GROUP BY patch_key, hero_id
),
per_hero AS (
  SELECT b.patch_key, b.item_id, b.hero_id,
         count(*)                 AS bn,
         sum(b.won)               AS bw,
         max(h.n)                 AS n,
         max(h.w)                 AS w,
         sum(b.spent)             AS spent,
         sum(b.purchase_minute)   AS minute_sum,
         sum(b.sold)              AS sold_n,
         max(b.cost_souls)        AS cost_souls
  FROM buyer_players b
  JOIN hero_base h ON b.patch_key = h.patch_key AND b.hero_id = h.hero_id
  GROUP BY b.patch_key, b.item_id, b.hero_id
),
per_item AS (
  SELECT patch_key, item_id,
         sum(bn)                                                         AS purchases,
         sum(bw) / sum(bn)                                               AS buyer_win_rate,
         sum(CASE WHEN n > bn THEN bn * ((w - bw) / (n - bn)) END)
           / sum(CASE WHEN n > bn THEN bn END)                           AS non_buyer_win_rate,
         sum(minute_sum) / sum(bn)                                       AS avg_purchase_minute,
         sum(sold_n) / sum(bn)                                           AS sell_rate,
         sum(spent)                                                      AS souls_spent,
         max(cost_souls)                                                 AS cost_souls
  FROM per_hero
  GROUP BY patch_key, item_id
)
SELECT
  i.patch_key,
  p.title                                                      AS patch_title,
  i.item_id,
  d.item_name,
  d.slot_type,
  d.tier,
  i.cost_souls,
  i.purchases,
  round(i.buyer_win_rate, 4)                                   AS buyer_win_rate,
  round(i.non_buyer_win_rate, 4)                               AS non_buyer_win_rate,
  round(i.buyer_win_rate - i.non_buyer_win_rate, 4)            AS win_rate_lift,
  round((i.buyer_win_rate - i.non_buyer_win_rate) / (i.cost_souls / 1000.0), 4) AS lift_per_1k_souls,
  round(i.avg_purchase_minute, 2)                              AS avg_purchase_minute,
  round(i.sell_rate, 4)                                        AS sell_rate,
  i.souls_spent,
  '${run_id}'                                                  AS _run_id,
  current_timestamp()                                          AS _built_at
FROM per_item i
LEFT JOIN ${catalog}.gold.dim_patch p ON i.patch_key = p.patch_key
LEFT JOIN (SELECT item_id, item_name, slot_type, tier FROM ${catalog}.gold.dim_item WHERE is_current) d ON i.item_id = d.item_id
