package com.ahmedmasood.deadlock.gold

import java.sql.{Date, Timestamp}
import java.time.Instant

import com.ahmedmasood.deadlock.contracts.{Contract, ContractValidator}
import com.ahmedmasood.deadlock.core._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * Builds an SCD2 dimension from a snapshot source (the API returns current state only).
 *
 * Incremental: applies, in date order, every bronze snapshot newer than the last one applied.
 * Date-range (backfill / repair): rebuilds the whole dimension from every snapshot in bronze.
 * Because surrogate keys are deterministic, a rebuild reproduces identical keys.
 */
abstract class SnapshotScd2Job(sourceName: String, bronzeTable: String, spec: Scd2Spec) extends Job {
  val inputs: Seq[String] = Seq(bronzeTable)
  val outputs: Seq[String] = Seq(spec.table)

  /** Maps the contract-parsed `record` struct to natural key + attribute columns. */
  protected def project(records: DataFrame): DataFrame

  def run(ctx: JobContext): JobResult = {
    val spark = ctx.spark
    import spark.implicits._
    val target = ctx.t(spec.table)
    val contract = Contract.load(ctx.config, ctx.config.source(sourceName).contract)

    val rebuild = ctx.window.isInstanceOf[RunWindow.DateRange]
    if (rebuild) spark.sql(s"DELETE FROM $target WHERE true")
    val lastApplied: Option[String] = if (rebuild) None else OpsStore.getWatermark(ctx, name, bronzeTable).flatMap(_.value)

    val bronze = spark.table(ctx.t(bronzeTable))
    val pending = bronze.select("snapshot_date").distinct()
      .where(lastApplied.map(d => col("snapshot_date") > lit(Date.valueOf(d))).getOrElse(lit(true)))
      .as[Date].collect().sortBy(_.getTime)

    var (newKeys, changed, unchanged, invalid) = (0L, 0L, 0L, 0L)
    pending.foreach { d =>
      val snap = bronze.where(col("snapshot_date") === lit(d))
      val (valid, bad) = ContractValidator.parseAndValidate(snap, contract)
      invalid += bad.count()
      // Several pulls on the same day: keep the latest per key.
      val latest = valid
        .withColumn("_rn", row_number().over(Window.partitionBy(col("record_key")).orderBy(col("ingested_at").desc)))
        .where(col("_rn") === 1)
      val incoming = project(latest).withColumn("effective_from", to_timestamp(lit(s"$d 00:00:00")))
      val plan = Scd2.plan(spec, spark.table(target), incoming, ctx.runId)
      Scd2.apply(spark, spec, target, plan)
      newKeys += plan.newKeys; changed += plan.changedKeys; unchanged += plan.unchanged
      Log.info("scd2_snapshot_applied", "table" -> spec.table, "snapshot" -> d, "new" -> plan.newKeys,
        "changed" -> plan.changedKeys, "unchanged" -> plan.unchanged, "stale" -> plan.staleSkipped)
    }
    pending.lastOption.foreach { d =>
      ctx.watermarks.stage(Watermark(name, bronzeTable, None, None, Some(d.toString)))
    }
    JobResult(Map("snapshots_applied" -> pending.length.toLong, "new_keys" -> newKeys, "new_versions" -> changed,
      "unchanged" -> unchanged, "invalid_records" -> invalid))
  }
}

object DimHeroJob extends SnapshotScd2Job("heroes", Tables.RawHeroes, Scd2Spec(
  table = Tables.DimHero, naturalKey = "hero_id", surrogateKey = "hero_sk",
  tracked = Seq("hero_name", "hero_type", "complexity", "player_selectable", "disabled", "base_max_health"),
  attributes = Seq("class_name", "hero_name", "hero_type", "complexity", "player_selectable", "disabled", "base_max_health"))) {
  val name = "dim_hero"
  protected def project(r: DataFrame): DataFrame = r.select(
    col("record.id").as("hero_id"), col("record.class_name").as("class_name"), col("record.name").as("hero_name"),
    col("record.hero_type").as("hero_type"), col("record.complexity").as("complexity"),
    col("record.player_selectable").as("player_selectable"), col("record.disabled").as("disabled"),
    col("record.starting_stats.max_health.value").as("base_max_health"))
}

object DimItemJob extends SnapshotScd2Job("items", Tables.RawItems, Scd2Spec(
  table = Tables.DimItem, naturalKey = "item_id", surrogateKey = "item_sk",
  tracked = Seq("item_name", "item_type", "slot_type", "tier", "cost_souls", "shopable", "disabled"),
  attributes = Seq("class_name", "item_name", "item_type", "slot_type", "tier", "cost_souls", "shopable", "disabled"))) {
  val name = "dim_item"
  protected def project(r: DataFrame): DataFrame = r.select(
    col("record.id").as("item_id"), col("record.class_name").as("class_name"), col("record.name").as("item_name"),
    col("record.type").as("item_type"), col("record.item_slot_type").as("slot_type"), col("record.item_tier").as("tier"),
    col("record.cost").as("cost_souls"), col("record.shopable").as("shopable"), col("record.disabled").as("disabled"))
}

/**
 * Patch windows. Small and fully derivable from the feed, so it is rebuilt on every run
 * (the simplest correct option; SCD2 machinery would add nothing here).
 */
object DimPatchJob extends Job {
  val name = "dim_patch"
  val inputs = Seq(Tables.RawPatches)
  val outputs = Seq(Tables.DimPatch)

  def run(ctx: JobContext): JobResult = {
    val spark = ctx.spark
    val contract = Contract.load(ctx.config, ctx.config.source("patches").contract)
    val (valid, _) = ContractValidator.parseAndValidate(spark.table(ctx.t(Tables.RawPatches)), contract)
    val patches = valid
      .withColumn("_rn", row_number().over(Window.partitionBy(col("record.guid.text")).orderBy(col("ingested_at").desc)))
      .where(col("_rn") === 1)
      .select(
        xxhash64(col("record.guid.text")).as("patch_key"),
        col("record.guid.text").as("patch_guid"),
        col("record.title").as("title"),
        to_timestamp(col("record.pub_date")).as("released_at"),
        col("record.link").as("link"))
    // A few dozen rows at most: a single-partition window is the right tool (explicit to avoid the warning).
    val w = Window.partitionBy(lit(1)).orderBy("released_at")
    val dim = patches
      .withColumn("valid_from", col("released_at"))
      .withColumn("valid_to", coalesce(lead(col("released_at"), 1).over(w), to_timestamp(lit(Model.OpenEnd))))
      .withColumn("is_current", lead(col("released_at"), 1).over(w).isNull)
      .withColumn("_run_id", lit(ctx.runId))
      .select("patch_key", "patch_guid", "title", "released_at", "valid_from", "valid_to", "link", "is_current", "_run_id")
    dim.cache()
    val n = dim.count()
    dim.writeTo(ctx.t(Tables.DimPatch)).overwritePartitions() // unpartitioned: atomic full replace
    dim.unpersist()
    JobResult(Map("patches" -> n))
  }
}

/**
 * Player dimension, SCD Type 1 (latest values only). Aggregates are recomputed from the full
 * silver history of each affected player, so re-runs are idempotent (no += counters).
 */
object DimPlayerJob extends Job {
  val name = "dim_player"
  val inputs = Seq(Tables.MatchPlayers, Tables.Matches)
  val outputs = Seq(Tables.DimPlayer)

  def run(ctx: JobContext): JobResult = {
    val spark = ctx.spark
    val salt = ctx.config.playerHashSalt
    val window = GoldWindow.affectedMatches(ctx, name)
    window.keys.createOrReplaceTempView("dim_player_affected")

    val players = spark.table(ctx.t(Tables.MatchPlayers))
    val affectedPlayers = players.join(window.keys.select("match_id"), "match_id").select("account_id").distinct()
    val history = players.join(affectedPlayers, "account_id")
      .join(spark.table(ctx.t(Tables.Matches)).select("match_id", "average_badge_team0", "average_badge_team1"), "match_id")
      .withColumn("badge", when(col("team") === "Team0", col("average_badge_team0")).otherwise(col("average_badge_team1")))

    val heroCounts = history.groupBy("account_id", "hero_id").agg(count(lit(1)).as("n"))
      .groupBy("account_id").agg(max_by(col("hero_id"), col("n")).as("most_played_hero_id"))
    val profile = history.groupBy("account_id").agg(
      min("start_time").as("first_seen_at"), max("start_time").as("last_seen_at"),
      count(lit(1)).as("matches_played"), sum(when(col("won"), 1L).otherwise(0L)).as("wins"),
      max_by(col("badge"), col("start_time")).as("latest_badge"))
      .join(heroCounts, "account_id")
      .select(Pii.playerKey(col("account_id"), salt).as("player_key"), col("first_seen_at"), col("last_seen_at"),
        col("matches_played"), col("wins"), col("latest_badge"), col("most_played_hero_id"),
        lit(Timestamp.from(Instant.now())).as("_updated_at"), lit(ctx.runId).as("_run_id"))
      .where(col("player_key").isNotNull)

    profile.createOrReplaceTempView("dim_player_upsert")
    spark.sql(
      s"""MERGE INTO ${ctx.t(Tables.DimPlayer)} t
         |USING dim_player_upsert s ON t.player_key = s.player_key
         |WHEN MATCHED THEN UPDATE SET *
         |WHEN NOT MATCHED THEN INSERT *""".stripMargin)
    window.commit()
    JobResult(Map("players_upserted" -> spark.table("dim_player_upsert").count()))
  }
}
