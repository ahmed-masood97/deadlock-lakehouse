package com.ahmedmasood.deadlock.quality

import com.ahmedmasood.deadlock.core.{JobContext, Tables}
import com.ahmedmasood.deadlock.quality.Check._
import com.ahmedmasood.deadlock.quality.Severity._

/**
 * What "correct" means for each table, in one place. Checks are scoped to the rows a run
 * touched, so they stay cheap as tables grow, while still testing whole-table invariants where
 * it matters (dimension integrity).
 */
object Expectations {

  private def touched(ctx: JobContext, table: String): String =
    s"match_id IN (SELECT match_id FROM ${ctx.t(table)} WHERE _run_id = '${ctx.runId}')"

  def silver(ctx: JobContext): Seq[Suite] = {
    val m = ctx.t(Tables.Matches)
    val p = ctx.t(Tables.MatchPlayers)
    val q = ctx.t(Tables.Quarantine)
    val run = ctx.runId
    Seq(
      Suite(Tables.Matches, s"_run_id = '$run'", Seq(
        NotNull(Seq("match_id", "start_time", "winning_team", "duration_s")),
        Unique(Seq("match_id")),
        AcceptedValues("winning_team", Seq("Team0", "Team1")),
        RowCondition("plausible_duration", "duration_s BETWEEN 60 AND 7000"),
        ZeroRows("player_rows_match_header",
          s"""SELECT count(*) FROM $m mm LEFT JOIN (SELECT match_id, count(*) n FROM $p GROUP BY match_id) pp
             |ON mm.match_id = pp.match_id WHERE mm._run_id = '$run' AND coalesce(pp.n, 0) <> mm.player_count""".stripMargin),
        ZeroRows("quarantine_rate_below_5pct",
          s"""SELECT CASE WHEN q > 0.05 * (q + w) THEN 1 ELSE 0 END FROM
             |(SELECT (SELECT count(*) FROM $q WHERE run_id = '$run') q, (SELECT count(*) FROM $m WHERE _run_id = '$run') w)""".stripMargin,
          Warn)
      )),
      Suite(Tables.MatchPlayers, s"_run_id = '$run'", Seq(
        NotNull(Seq("match_id", "player_slot", "hero_id", "team")),
        Unique(Seq("match_id", "player_slot")),
        RowCondition("non_negative_kda", "kills >= 0 AND deaths >= 0 AND assists >= 0")
      ))
    )
  }

  private def scd2Checks(key: String, sk: String): Seq[Check] = Seq(
    NotNull(Seq(sk, key, "valid_from", "valid_to")),
    Unique(Seq(sk)),
    Unique(Seq(key, "valid_from")),
    ZeroRows("exactly_one_current_version",
      s"SELECT count(*) FROM (SELECT $key FROM $${table} GROUP BY $key HAVING sum(CASE WHEN is_current THEN 1 ELSE 0 END) <> 1)"),
    ZeroRows("versions_contiguous_no_gaps_or_overlaps",
      s"""SELECT count(*) FROM (SELECT valid_to, lead(valid_from) OVER (PARTITION BY $key ORDER BY valid_from) nxt FROM $${table})
         |WHERE nxt IS NOT NULL AND nxt <> valid_to""".stripMargin),
    RowCondition("valid_from_before_valid_to", "valid_from < valid_to")
  )

  def gold(ctx: JobContext): Seq[Suite] = {
    val fpm = ctx.t(Tables.FctPlayerMatch)
    val fip = ctx.t(Tables.FctItemPurchase)
    val fpe = ctx.t(Tables.FctPlayerEconomy)
    val sp = ctx.t(Tables.MatchPlayers)
    val sip = ctx.t(Tables.ItemPurchases)
    val pmScope = touched(ctx, Tables.FctPlayerMatch)
    val ipScope = touched(ctx, Tables.FctItemPurchase)
    val ecScope = touched(ctx, Tables.FctPlayerEconomy)
    val run = ctx.runId

    Seq(
      Suite(Tables.DimHero, "true", scd2Checks("hero_id", "hero_sk") :+ NotNull(Seq("hero_name"))),
      Suite(Tables.DimItem, "true", scd2Checks("item_id", "item_sk") :+
        RowCondition("shop_items_have_a_price", "item_type <> 'upgrade' OR cost_souls > 0")),
      Suite(Tables.DimPatch, "true", Seq(
        Unique(Seq("patch_key")),
        ZeroRows("exactly_one_current_patch", "SELECT CASE WHEN count(*) = 1 THEN 0 ELSE 1 END FROM ${table} WHERE is_current"))),
      Suite(Tables.DimPlayer, s"_run_id = '$run'", Seq(
        Unique(Seq("player_key")),
        RowCondition("wins_not_above_matches", "wins <= matches_played"))),

      Suite(Tables.FctPlayerMatch, pmScope, Seq(
        NotNull(Seq("match_id", "player_slot", "hero_sk", "patch_key", "date_key")),
        Unique(Seq("match_id", "player_slot")),
        References("hero_sk", ctx.t(Tables.DimHero), "hero_sk"),
        References("patch_key", ctx.t(Tables.DimPatch), "patch_key"),
        MaxRatio("unknown_hero_member", "hero_sk = -1", 0.001, Warn),
        ZeroRows("half_of_players_win",
          s"""SELECT count(*) FROM (SELECT match_id FROM $fpm WHERE $pmScope GROUP BY match_id
             |HAVING 2 * sum(CASE WHEN won THEN 1 ELSE 0 END) <> count(*))""".stripMargin),
        Reconcile("row_count_vs_silver_players",
          s"SELECT count(*) FROM $fpm WHERE $pmScope",
          s"SELECT count(*) FROM $sp WHERE match_id IN (SELECT match_id FROM $fpm WHERE _run_id = '$run')"),
        Reconcile("kills_vs_silver_players",
          s"SELECT sum(kills) FROM $fpm WHERE $pmScope",
          s"SELECT sum(kills) FROM $sp WHERE match_id IN (SELECT match_id FROM $fpm WHERE _run_id = '$run')")
      )),
      Suite(Tables.FctItemPurchase, ipScope, Seq(
        NotNull(Seq("match_id", "player_slot", "purchase_seq", "item_sk", "cost_souls")),
        Unique(Seq("match_id", "player_slot", "purchase_seq")),
        References("item_sk", ctx.t(Tables.DimItem), "item_sk"),
        MaxRatio("unknown_item_member", "item_sk = -1", 0.001, Warn),
        RowCondition("net_cost_within_cost", "net_cost_souls >= 0 AND net_cost_souls <= cost_souls"),
        ZeroRows("purchases_not_more_than_silver",
          s"SELECT CASE WHEN f > s THEN 1 ELSE 0 END FROM (SELECT (SELECT count(*) FROM $fip WHERE $ipScope) f, " +
            s"(SELECT count(*) FROM $sip WHERE match_id IN (SELECT match_id FROM $fip WHERE _run_id = '$run')) s)")
      )),
      Suite(Tables.FctPlayerEconomy, ecScope, Seq(
        Unique(Seq("match_id", "player_slot")),
        Reconcile("net_worth_vs_silver_players",
          s"SELECT sum(net_worth) FROM $fpe WHERE $ecScope",
          s"SELECT sum(net_worth) FROM $sp WHERE match_id IN (SELECT match_id FROM $fpe WHERE _run_id = '$run')"),
        Reconcile("spend_vs_item_purchases",
          s"SELECT sum(spend_total) FROM $fpe WHERE $ecScope",
          s"SELECT sum(net_cost_souls) FROM $fip WHERE match_id IN (SELECT match_id FROM $fpe WHERE _run_id = '$run')"),
        MaxRatio("souls_do_not_reconcile_to_net_worth", "abs(reconciliation_gap) > 0.01 * net_worth", 0.01, Warn),
        RowCondition("spend_ratio_plausible", "spend_ratio IS NULL OR spend_ratio BETWEEN 0 AND 1.2", Warn)
      )),
      Suite(Tables.MartHeroPatch, s"_run_id = '$run'", Seq(
        Unique(Seq("patch_key", "hero_id", "badge_bucket")),
        RowCondition("win_rate_inside_its_interval", "win_rate BETWEEN win_rate_ci_low AND win_rate_ci_high"),
        RowCondition("pick_rate_is_a_share", "pick_rate > 0 AND pick_rate <= 1"))),
      Suite(Tables.MartItemEconomy, s"_run_id = '$run'", Seq(
        Unique(Seq("patch_key", "item_id")),
        RowCondition("rates_are_shares", "buyer_win_rate BETWEEN 0 AND 1 AND sell_rate BETWEEN 0 AND 1"))),
      Suite(Tables.MartEconomyDaily, s"_run_id = '$run'", Seq(
        Unique(Seq("event_date", "patch_key", "badge_bucket")),
        RowCondition("revenue_shares_sum_below_one",
          "share_players + share_lane + share_neutral + share_objectives + share_treasure <= 1.0001")))
    )
  }
}
