-- Hero performance per patch and skill bracket.
-- Win rates come with a 95% Wilson score interval: with a few hundred games a hero at 54% can be
-- noise, and the interval says so. Abandoned games are excluded (they are not a fair contest).
-- ${patch_filter} limits the rebuild to the patches touched by this run.
WITH f AS (
  SELECT *
  FROM ${catalog}.gold.fct_player_match
  WHERE ${patch_filter} AND NOT abandoned
),
bracket_totals AS (
  SELECT patch_key, badge_bucket, count(DISTINCT match_id) AS total_matches
  FROM f
  GROUP BY patch_key, badge_bucket
),
agg AS (
  SELECT patch_key, hero_id, badge_bucket,
         count(*)                                AS matches,
         sum(CASE WHEN won THEN 1 ELSE 0 END)    AS wins,
         avg(kda)                                AS avg_kda,
         avg(souls_per_min)                      AS avg_souls_per_min
  FROM f
  GROUP BY patch_key, hero_id, badge_bucket
),
scored AS (
  SELECT a.*, CAST(a.wins AS DOUBLE) / a.matches AS p, CAST(a.matches AS DOUBLE) AS n, 1.96 AS z
  FROM agg a
)
SELECT
  s.patch_key,
  p.title                                                    AS patch_title,
  s.hero_id,
  h.hero_name,
  s.badge_bucket,
  s.matches,
  s.wins,
  round(s.p, 4)                                              AS win_rate,
  round((s.p + s.z * s.z / (2 * s.n) - s.z * sqrt(s.p * (1 - s.p) / s.n + s.z * s.z / (4 * s.n * s.n)))
        / (1 + s.z * s.z / s.n), 4)                          AS win_rate_ci_low,
  round((s.p + s.z * s.z / (2 * s.n) + s.z * sqrt(s.p * (1 - s.p) / s.n + s.z * s.z / (4 * s.n * s.n)))
        / (1 + s.z * s.z / s.n), 4)                          AS win_rate_ci_high,
  round(s.matches / t.total_matches, 4)                      AS pick_rate,
  round(s.avg_kda, 3)                                        AS avg_kda,
  round(s.avg_souls_per_min, 1)                              AS avg_souls_per_min,
  '${run_id}'                                                AS _run_id,
  current_timestamp()                                        AS _built_at
FROM scored s
JOIN bracket_totals t ON s.patch_key = t.patch_key AND s.badge_bucket = t.badge_bucket
LEFT JOIN ${catalog}.gold.dim_patch p ON s.patch_key = p.patch_key
-- Display name: the hero's current name (type-1 attribute for reporting).
LEFT JOIN (SELECT hero_id, hero_name FROM ${catalog}.gold.dim_hero WHERE is_current) h ON s.hero_id = h.hero_id
