package com.ahmedmasood.deadlock.orchestration

import java.io.File
import java.time.LocalDate

import scala.util.Try

import com.ahmedmasood.deadlock.core._
import com.ahmedmasood.deadlock.ingest.SyntheticDeadlock
import org.apache.spark.sql.SparkSession

/**
 * A scripted walk through the platform on synthetic data. Every step mirrors something that
 * happens in production; the console output is what you would show in a design review.
 */
object Demo {

  private val Season = LocalDate.parse("2026-10-01")

  def run(spark: SparkSession, cfg: PipelineConfig, matchesPerDay: Int = 250): Unit = {
    wipeLocal(cfg)
    def ctx(window: RunWindow = RunWindow.Incremental) = JobContext(spark, cfg, JobContext.newRunId("demo"), window)
    val fct = s"${cfg.catalog.name}.${Tables.FctPlayerMatch}"
    def q(title: String, sql: String, rows: Int = 30): Unit = {
      println(s"\n=== $title ===")
      spark.sql(sql.replace("$c", cfg.catalog.name)).show(rows, truncate = false)
    }

    banner("1. Days 1-20 land; first daily run (patches 1 and 2, item repricing on day 10)")
    SyntheticDeadlock.generate(cfg, Season, Season, Season.plusDays(19), matchesPerDay)
    DailyPipeline.run(ctx(), fromApi = false)

    banner("2. Day 21 lands: patch 3 releases Holliday, but the hero feed has not caught up yet")
    SyntheticDeadlock.generate(cfg, Season, Season.plusDays(20), Season.plusDays(20), matchesPerDay)
    DailyPipeline.run(ctx(), fromApi = false)
    q("Facts that reference a hero the dimension does not know yet (unknown member -1)",
      s"SELECT to_date(start_time) day, count(*) rows FROM $fct WHERE hero_sk = -1 GROUP BY 1 ORDER BY 1")

    banner("3. Days 22-30 land, including the hero snapshot with Holliday")
    SyntheticDeadlock.generate(cfg, Season, Season.plusDays(21), Season.plusDays(29), matchesPerDay)
    DailyPipeline.run(ctx(), fromApi = false)

    banner("4. Backfill day 21 from gold.dim_hero downstream: rebuilt in a branch, diffed, then cut over")
    Backfill.run(ctx(RunWindow.DateRange(Season.plusDays(20), Season.plusDays(20))), Tables.DimHero, dryRun = false, publish = true)
    q("Unknown hero members after the backfill", s"SELECT count(*) AS unknown_hero_rows FROM $fct WHERE hero_sk = -1")

    banner("5. What the platform recorded")
    q("SCD2: one item across the patch-2 repricing", "SELECT item_id, item_name, cost_souls, valid_from, valid_to, is_current FROM $c.gold.dim_item WHERE item_name = 'Extra Health' ORDER BY valid_from")
    q("SCD2: the Abrams rework in patch 3", "SELECT hero_id, hero_name, hero_type, base_max_health, valid_from, valid_to, is_current FROM $c.gold.dim_hero WHERE hero_id = 6 ORDER BY valid_from")
    q("Quarantined records by reason", "SELECT explode(reasons) reason, count(*) n FROM $c.silver.quarantine GROUP BY 1 ORDER BY n DESC")
    q("Schema drift detected", "SELECT source, field_path, change, sum(records_affected) records FROM $c.ops.schema_drift GROUP BY 1,2,3 ORDER BY 1,2")
    q("Data quality: failures and warnings across all runs", "SELECT table_name, check_name, severity, observed, expected, count(*) runs FROM $c.ops.dq_results WHERE status = 'fail' GROUP BY 1,2,3,4,5 ORDER BY 1,2")
    q("Job runs", "SELECT run_id, job, run_window, status, duration_ms, metrics FROM $c.ops.job_runs ORDER BY started_at DESC", 12)

    banner("6. Marts")
    q("Hero win rates by patch, all brackets combined (per-bracket rows carry 95% Wilson intervals)",
      """SELECT patch_title, hero_name, sum(matches) matches, round(sum(wins)/sum(matches),4) win_rate
        |FROM $c.gold.mart_hero_patch_performance GROUP BY 1,2 HAVING sum(matches) > 300 ORDER BY 1, win_rate DESC""".stripMargin, 15)
    q("Abrams before and after the patch-3 rework",
      """SELECT patch_title, badge_bucket, matches, win_rate, win_rate_ci_low, win_rate_ci_high
        |FROM $c.gold.mart_hero_patch_performance WHERE hero_id = 6 ORDER BY patch_title, badge_bucket""".stripMargin)
    q("Item unit economics: best win-rate lift per 1,000 souls (latest patch)",
      """SELECT item_name, slot_type, tier, cost_souls, purchases, buyer_win_rate, non_buyer_win_rate, win_rate_lift, lift_per_1k_souls
        |FROM $c.gold.mart_item_unit_economics
        |WHERE patch_key = (SELECT patch_key FROM $c.gold.dim_patch WHERE is_current) AND purchases >= 200
        |ORDER BY lift_per_1k_souls DESC""".stripMargin, 10)
    q("Soul economy: revenue mix and reconciliation by day",
      """SELECT event_date, sum(player_matches) player_matches, round(avg(avg_souls_per_min),1) spm,
        |round(avg(share_lane),3) lane, round(avg(share_neutral),3) neutral, round(avg(share_players),3) players,
        |round(avg(avg_spend_ratio),3) spend_ratio, max(reconciliation_gap_pct) gap_pct
        |FROM $c.gold.mart_economy_daily GROUP BY 1 ORDER BY 1""".stripMargin, 31)

    banner("7. Time travel: the fact table as it was before the backfill")
    val history = spark.sql(s"SELECT snapshot_id, committed_at, operation FROM $fct.snapshots ORDER BY committed_at").collect()
    println(s"$fct has ${history.length} snapshots; the first one is ${history.head.getLong(0)}")
    q("Row counts per snapshot of fct_player_match (last 5)",
      s"SELECT h.made_current_at, h.snapshot_id, s.operation, s.summary['total-records'] AS total_records FROM $fct.history h JOIN $fct.snapshots s USING (snapshot_id) ORDER BY h.made_current_at DESC LIMIT 5")
    println("\nDemo complete. Explore with: sql \"SELECT ... FROM lake.gold....\"")
  }

  private def banner(s: String): Unit = println(s"\n${"=" * 100}\n$s\n${"=" * 100}")

  /** Only ever deletes local, relative directories from the config (never s3:// or absolute paths). */
  private def wipeLocal(cfg: PipelineConfig): Unit =
    Seq(cfg.landingDir, cfg.catalog.warehouse).filter(p => p.startsWith("./") || p.startsWith("lakehouse")).foreach { p =>
      def rm(f: File): Unit = { Option(f.listFiles()).foreach(_.foreach(rm)); Try(f.delete()) }
      rm(new File(p))
    }
}
