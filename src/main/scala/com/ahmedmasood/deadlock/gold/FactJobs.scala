package com.ahmedmasood.deadlock.gold

import java.sql.Timestamp
import java.time.Instant

import com.ahmedmasood.deadlock.core._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/** Point-in-time dimension lookups shared by every fact. */
object Lookups {

  /** hero_sk valid at match start; unknown member (-1) when the hero is not in the dimension yet. */
  def heroAsOf(spark: SparkSession, ctx: JobContext, facts: DataFrame): DataFrame = {
    val d = spark.table(ctx.t(Tables.DimHero)).select(col("hero_sk"), col("hero_id").as("_d_hero_id"), col("valid_from").as("_h_from"), col("valid_to").as("_h_to"))
    // Dimensions are small: broadcast them, so the range condition is evaluated map-side with no shuffle.
    facts.join(broadcast(d), facts("hero_id") === d("_d_hero_id") && facts("start_time") >= d("_h_from") && facts("start_time") < d("_h_to"), "left")
      .withColumn("hero_sk", coalesce(col("hero_sk"), lit(Tables.UnknownSk)))
      .drop("_d_hero_id", "_h_from", "_h_to")
  }

  /** Patch in force at match start. */
  def patchAsOf(spark: SparkSession, ctx: JobContext, facts: DataFrame): DataFrame = {
    val p = spark.table(ctx.t(Tables.DimPatch)).select(col("patch_key"), col("valid_from").as("_p_from"), col("valid_to").as("_p_to"))
    facts.join(broadcast(p), facts("start_time") >= p("_p_from") && facts("start_time") < p("_p_to"), "left")
      .withColumn("patch_key", coalesce(col("patch_key"), lit(Tables.UnknownSk)))
      .drop("_p_from", "_p_to")
  }

  /** Item version (and therefore price) in force at match start. */
  def itemAsOf(spark: SparkSession, ctx: JobContext, facts: DataFrame): DataFrame = {
    val d = spark.table(ctx.t(Tables.DimItem)).select(col("item_sk"), col("item_id").as("_d_item_id"), col("item_type"),
      col("slot_type"), col("tier"), col("cost_souls"), col("valid_from").as("_i_from"), col("valid_to").as("_i_to"))
    facts.join(broadcast(d), facts("item_id") === d("_d_item_id") && facts("start_time") >= d("_i_from") && facts("start_time") < d("_i_to"), "left")
      .withColumn("item_sk", coalesce(col("item_sk"), lit(Tables.UnknownSk)))
      .drop("_d_item_id", "_i_from", "_i_to")
  }
}

/** Shared skeleton: compute rows for the affected matches, replace them, stage the watermark. */
abstract class MatchGrainFactJob extends Job {
  protected def target: String
  protected def build(ctx: JobContext, keysView: String, loadedAt: Timestamp): DataFrame

  def run(ctx: JobContext): JobResult = {
    val spark = ctx.spark
    val window = GoldWindow.affectedMatches(ctx, name)
    if (window.count == 0) { window.commit(); return JobResult(Map("matches" -> 0L, "rows_written" -> 0L)) }

    val keysView = s"${name}_keys"
    window.keys.createOrReplaceTempView(keysView)
    val rows = build(ctx, keysView, Timestamp.from(Instant.now())).cache()
    val written = rows.count()
    Writes.replaceMatches(spark, ctx.t(target), keysView, Writes.startTimeBounds(spark, keysView), rows)
    rows.unpersist()
    window.keys.unpersist()
    window.commit()
    JobResult(Map("matches" -> window.count, "rows_written" -> written))
  }
}

/** Grain: one row per player per match. */
object FactPlayerMatchJob extends MatchGrainFactJob {
  val name = "fct_player_match"
  val inputs = Seq(Tables.Matches, Tables.MatchPlayers, Tables.DimHero, Tables.DimPatch)
  val outputs = Seq(Tables.FctPlayerMatch)
  protected val target = Tables.FctPlayerMatch

  protected def build(ctx: JobContext, keysView: String, loadedAt: Timestamp): DataFrame = {
    val spark = ctx.spark
    val keys = spark.table(keysView).select("match_id")
    val m = spark.table(ctx.t(Tables.Matches)).join(keys, "match_id")
      .select("match_id", "duration_s", "match_mode", "average_badge_team0", "average_badge_team1")
    val p = spark.table(ctx.t(Tables.MatchPlayers)).join(keys, "match_id")

    val base = p.join(m, "match_id")
      .withColumn("average_badge", when(col("team") === "Team0", col("average_badge_team0")).otherwise(col("average_badge_team1")))
    val withDims = Lookups.patchAsOf(spark, ctx, Lookups.heroAsOf(spark, ctx, base))

    withDims.select(
      col("match_id"), col("player_slot"), col("start_time"), Model.dateKey(col("start_time")).as("date_key"),
      col("patch_key"), col("hero_sk"), col("hero_id"), Pii.playerKey(col("account_id"), ctx.config.playerHashSalt).as("player_key"),
      col("team"), col("won"), col("match_mode"), col("average_badge"), Model.badgeBucket(col("average_badge")).as("badge_bucket"),
      col("duration_s"), col("kills"), col("deaths"), col("assists"), Model.kda(col("kills"), col("deaths"), col("assists")).as("kda"),
      col("net_worth").cast("bigint").as("net_worth"), Model.soulsPerMinute(col("net_worth"), col("duration_s")).as("souls_per_min"),
      col("last_hits"), col("denies"), (coalesce(col("abandon_match_time_s"), lit(0)) > 0).as("abandoned"),
      lit(ctx.runId).as("_run_id"), lit(loadedAt).as("_loaded_at"),
      col("final_stats.player_damage").as("player_damage"), col("final_stats.player_healing").as("player_healing"))
  }
}

/** Grain: one row per shop purchase. Ability unlocks share the item timeline and are filtered out here. */
object FactItemPurchaseJob extends MatchGrainFactJob {
  val name = "fct_item_purchase"
  val inputs = Seq(Tables.ItemPurchases, Tables.MatchPlayers, Tables.DimItem, Tables.DimHero, Tables.DimPatch)
  val outputs = Seq(Tables.FctItemPurchase)
  protected val target = Tables.FctItemPurchase

  protected def build(ctx: JobContext, keysView: String, loadedAt: Timestamp): DataFrame = {
    val spark = ctx.spark
    val keys = spark.table(keysView).select("match_id")
    val players = spark.table(ctx.t(Tables.MatchPlayers)).join(keys, "match_id")
      .select("match_id", "player_slot", "hero_id", "account_id", "won")
    val purchases = spark.table(ctx.t(Tables.ItemPurchases)).join(keys, "match_id")
      .join(players, Seq("match_id", "player_slot"))

    val priced = Lookups.itemAsOf(spark, ctx, Lookups.patchAsOf(spark, ctx, Lookups.heroAsOf(spark, ctx, purchases)))
      .where(col("item_type") === "upgrade" || col("item_sk") === Tables.UnknownSk)

    val cost = coalesce(col("cost_souls"), lit(0L))
    val sold = coalesce(col("sold_time_s"), lit(0)) > 0
    priced.select(
      col("match_id"), col("player_slot"), col("purchase_seq"), col("start_time"), Model.dateKey(col("start_time")).as("date_key"),
      col("patch_key"), col("hero_sk"), col("hero_id"), Pii.playerKey(col("account_id"), ctx.config.playerHashSalt).as("player_key"),
      col("item_sk"), col("item_id"), col("slot_type"), col("tier"), cost.as("cost_souls"),
      when(sold, floor(cost / 2)).otherwise(lit(0L)).cast("bigint").as("refund_souls"),
      (cost - when(sold, floor(cost / 2)).otherwise(lit(0L))).cast("bigint").as("net_cost_souls"),
      col("game_time_s"), floor(col("game_time_s") / 60).cast("int").as("purchase_minute"), sold.as("sold"), col("won"),
      lit(ctx.runId).as("_run_id"), lit(loadedAt).as("_loaded_at"))
  }
}

/**
 * Grain: one row per player per match. Soul economics:
 *   revenue  = souls earned by source (lane, neutrals, players, objectives, treasure, denies)
 *   costs    = souls spent on items by category, valued at the price in force at match time
 *   check    = souls earned - souls lost on death should equal end-of-match net worth
 */
object FactPlayerEconomyJob extends MatchGrainFactJob {
  val name = "fct_player_economy"
  val inputs = Seq(Tables.MatchPlayers, Tables.Matches, Tables.FctItemPurchase, Tables.DimHero, Tables.DimPatch)
  val outputs = Seq(Tables.FctPlayerEconomy)
  protected val target = Tables.FctPlayerEconomy

  protected def build(ctx: JobContext, keysView: String, loadedAt: Timestamp): DataFrame = {
    val spark = ctx.spark
    val keys = spark.table(keysView).select("match_id")
    val m = spark.table(ctx.t(Tables.Matches)).join(keys, "match_id")
      .select("match_id", "duration_s", "average_badge_team0", "average_badge_team1")
    val p = spark.table(ctx.t(Tables.MatchPlayers)).join(keys, "match_id").join(m, "match_id")
    val spend = spark.table(ctx.t(Tables.FctItemPurchase)).join(keys, "match_id")
      .groupBy("match_id", "player_slot")
      .agg(
        sum(when(col("slot_type") === "weapon", col("net_cost_souls")).otherwise(0L)).as("spend_weapon"),
        sum(when(col("slot_type") === "vitality", col("net_cost_souls")).otherwise(0L)).as("spend_vitality"),
        sum(when(col("slot_type") === "spirit", col("net_cost_souls")).otherwise(0L)).as("spend_spirit"),
        sum(col("net_cost_souls")).as("spend_total"),
        count(lit(1)).cast("int").as("items_bought"))

    def fs(c: String) = coalesce(col(s"final_stats.$c"), lit(0L))
    val earned = fs("gold_player") + fs("gold_player_orbs") + fs("gold_lane_creep") + fs("gold_lane_creep_orbs") +
      fs("gold_neutral_creep") + fs("gold_neutral_creep_orbs") + fs("gold_boss") + fs("gold_boss_orb") + fs("gold_treasure") + fs("gold_denied")

    val base = Lookups.patchAsOf(spark, ctx, Lookups.heroAsOf(spark, ctx, p))
      .join(spend, Seq("match_id", "player_slot"), "left")
      .withColumn("average_badge", when(col("team") === "Team0", col("average_badge_team0")).otherwise(col("average_badge_team1")))
    val nw = col("net_worth").cast("bigint")
    val spendTotal = coalesce(col("spend_total"), lit(0L))

    base.select(
      col("match_id"), col("player_slot"), col("start_time"), Model.dateKey(col("start_time")).as("date_key"),
      col("patch_key"), col("hero_sk"), col("hero_id"), Pii.playerKey(col("account_id"), ctx.config.playerHashSalt).as("player_key"),
      col("won"), Model.badgeBucket(col("average_badge")).as("badge_bucket"), col("duration_s"), nw.as("net_worth"),
      (fs("gold_player") + fs("gold_player_orbs")).as("souls_players"),
      (fs("gold_lane_creep") + fs("gold_lane_creep_orbs")).as("souls_lane"),
      (fs("gold_neutral_creep") + fs("gold_neutral_creep_orbs")).as("souls_neutral"),
      (fs("gold_boss") + fs("gold_boss_orb")).as("souls_objectives"),
      fs("gold_treasure").as("souls_treasure"), fs("gold_denied").as("souls_denied"),
      earned.as("souls_earned"), fs("gold_death_loss").as("souls_lost_on_death"),
      coalesce(col("spend_weapon"), lit(0L)).as("spend_weapon"), coalesce(col("spend_vitality"), lit(0L)).as("spend_vitality"),
      coalesce(col("spend_spirit"), lit(0L)).as("spend_spirit"), spendTotal.as("spend_total"),
      coalesce(col("items_bought"), lit(0)).as("items_bought"),
      Model.soulsPerMinute(nw, col("duration_s")).as("souls_per_min"),
      when(nw > 0, round(spendTotal / nw, 4)).as("spend_ratio"),
      (earned - fs("gold_death_loss") - fs("net_worth")).as("reconciliation_gap"),
      lit(ctx.runId).as("_run_id"), lit(loadedAt).as("_loaded_at"))
  }
}
