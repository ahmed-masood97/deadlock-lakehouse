package com.ahmedmasood.deadlock

import java.time.LocalDate

import com.ahmedmasood.deadlock.core._
import com.ahmedmasood.deadlock.iceberg.IcebergOps
import com.ahmedmasood.deadlock.ingest.{BronzeIngestJob, SyntheticDeadlock}
import com.ahmedmasood.deadlock.orchestration.{Backfill, DailyPipeline, JobRegistry, Publisher}
import com.ahmedmasood.deadlock.quality.{Check, DataQualityException, Suite}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * End to end on a small synthetic season: the same code path as production, against a real
 * Iceberg catalog in a temp directory. Tests run in order and build on each other.
 */
class PipelineIntegrationSpec extends AnyFunSuite with BeforeAndAfterAll {
  private lazy val spark = SharedSpark.spark
  private lazy val cfg = SharedSpark.config
  private val season = LocalDate.parse("2026-10-01")
  private def count(sql: String): Long = spark.sql(sql).head().getLong(0)

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Days 1-21: covers patch 2 (item repricing, day 10) and patch 3 (rework + new hero, day 21).
    SyntheticDeadlock.generate(cfg, season, season, season.plusDays(20), matchesPerDay = 12)
  }

  test("daily run publishes a consistent gold layer") {
    DailyPipeline.run(SharedSpark.ctx(), fromApi = false)
    val players = count("SELECT count(*) FROM lake.silver.match_players")
    assert(players > 0)
    assert(count("SELECT count(*) FROM lake.gold.fct_player_match") == players)
    assert(count("SELECT count(*) FROM lake.silver.quarantine") > 0, "planted bad records must be quarantined")
    assert(count("SELECT count(*) FROM lake.ops.dq_results WHERE status = 'fail' AND severity = 'error'") == 0)
    assert(count("SELECT count(*) FROM lake.gold.mart_hero_patch_performance") > 0)
  }

  test("main is published and no WAP branch is left behind") {
    Tables.WapTables.foreach { t =>
      assert(IcebergOps.branches(spark, s"lake.$t") == Set("main"), t)
    }
  }

  test("re-running bronze ingestion loads nothing twice") {
    val before = count("SELECT count(*) FROM lake.bronze.raw_matches")
    val r = new BronzeIngestJob(cfg.source("matches")).run(SharedSpark.ctx())
    assert(r.metrics("files_loaded") == 0L)
    assert(count("SELECT count(*) FROM lake.bronze.raw_matches") == before)
  }

  test("an incremental run with no new data changes no facts") {
    val snapshot = IcebergOps.currentSnapshot(spark, "lake.gold.fct_player_match").map(_.id)
    DailyPipeline.run(SharedSpark.ctx(), fromApi = false)
    assert(IcebergOps.currentSnapshot(spark, "lake.gold.fct_player_match").map(_.id) == snapshot)
  }

  test("item purchases are valued at the price in force when the match was played") {
    val prices = spark.sql(
      """SELECT DISTINCT CASE WHEN start_time < TIMESTAMP '2026-10-10 00:00:00' THEN 'before' ELSE 'after' END AS period, cost_souls
        |FROM lake.gold.fct_item_purchase WHERE tier = 1 AND item_sk <> -1""".stripMargin).collect()
      .map(r => (r.getString(0), r.getLong(1))).toSet
    assert(prices == Set(("before", 500L), ("after", 800L)))
  }

  test("a failed audit keeps bad data off main") {
    val ctx = SharedSpark.ctx(RunWindow.DateRange(season, season))
    val before = IcebergOps.currentSnapshot(spark, "lake.gold.fct_player_match").map(_.id)
    val impossible = (c: JobContext) => Seq(Suite(Tables.FctPlayerMatch, "true", Seq(Check.RowCondition("impossible", "kills < 0"))))
    intercept[DataQualityException] {
      Publisher.run(ctx, s"wap_${ctx.runId}", Seq(JobRegistry.all(cfg).job("fct_player_match")), impossible)
    }
    assert(IcebergOps.currentSnapshot(spark, "lake.gold.fct_player_match").map(_.id) == before)
    assert(IcebergOps.branches(spark, "lake.gold.fct_player_match").contains(s"wap_${ctx.runId}"), "failed branch is kept for debugging")
  }

  test("a backfill from a dimension rebuilds everything downstream for the window") {
    val ctx = SharedSpark.ctx(RunWindow.DateRange(season.plusDays(20), season.plusDays(20)))
    assert(Backfill.plan(ctx, "gold.dim_hero").map(_.name) == Seq("dim_hero", "fct_player_match", "fct_item_purchase", "fct_player_economy", "marts"))
    val rowsBefore = count("SELECT count(*) FROM lake.gold.fct_player_match")
    Backfill.run(ctx, "gold.dim_hero", dryRun = false, publish = true)
    assert(count("SELECT count(*) FROM lake.gold.fct_player_match") == rowsBefore)
    assert(count(s"SELECT count(*) FROM lake.gold.fct_player_match WHERE _run_id = '${ctx.runId}'") > 0)
    assert(count("SELECT count(*) FROM lake.ops.backfill_diffs") > 0)
  }

  test("every job run and its snapshot lineage are recorded") {
    assert(count("SELECT count(*) FROM lake.ops.job_runs WHERE status = 'SUCCEEDED'") >= 15)
    assert(count("SELECT count(*) FROM lake.ops.lineage WHERE direction = 'output' AND snapshot_id IS NOT NULL") > 0)
  }
}
