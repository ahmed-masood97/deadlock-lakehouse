package com.ahmedmasood.deadlock.orchestration

import java.sql.Timestamp
import java.time.Instant

import com.ahmedmasood.deadlock.core._
import com.ahmedmasood.deadlock.ingest.{ApiExtractor, BronzeIngestJob}
import com.ahmedmasood.deadlock.quality.{DataQuality, DataQualityException, Expectations}
import org.apache.spark.sql.functions._

/** The daily incremental pipeline: land -> bronze -> silver -> gold (write-audit-publish). */
object DailyPipeline {

  def run(ctx: JobContext, fromApi: Boolean): Unit = {
    val graph = JobRegistry.all(ctx.config)
    MigrationRunner.migrate(ctx.spark, ctx.config)

    if (fromApi) ctx.config.sources.foreach(s => ApiExtractor.extract(ctx, s))
    JobRegistry.ingestJobs(ctx.config).foreach(j => OpsStore.execute(ctx, j))

    val silverJobs = graph.topologicalOrder().filter(j => !JobRegistry.isGold(j) && !j.isInstanceOf[BronzeIngestJob])
    silverJobs.foreach(j => OpsStore.execute(ctx, j))
    val silverBlocking = DataQuality.blocking(DataQuality.run(ctx, Expectations.silver(ctx)), "error")
    if (silverBlocking.nonEmpty) throw new DataQualityException(silverBlocking)

    val goldJobs = graph.topologicalOrder(JobRegistry.goldJobs)
    Publisher.run(ctx, s"wap_${ctx.runId}", goldJobs, Expectations.gold)
    Log.info("daily_pipeline_succeeded", "run_id" -> ctx.runId)
  }
}

/**
 * Backfills: recompute a date range for a job and everything downstream of it.
 *
 * 1. PLAN      impact analysis from the dependency graph (`--dry-run` stops here)
 * 2. REBUILD   silver jobs rewrite the window in place (idempotent); gold jobs write to a
 *              dedicated branch, so production readers are unaffected while it runs
 * 3. VALIDATE  the usual DQ suites, plus a main-vs-branch diff per table per day
 * 4. CUTOVER   fast-forward main to the branch (or leave it for review with --no-publish,
 *              then `publish --branch <name>` once someone has looked at the diff)
 */
object Backfill {

  def plan(ctx: JobContext, startAt: String): Seq[Job] = {
    val graph = JobRegistry.all(ctx.config)
    graph.downstreamClosure(graph.resolve(startAt)).filterNot(_.isInstanceOf[BronzeIngestJob])
  }

  def run(ctx: JobContext, startAt: String, dryRun: Boolean, publish: Boolean): Unit = {
    val window = ctx.window match {
      case r: RunWindow.DateRange => r
      case _                      => throw new IllegalArgumentException("A backfill needs --from and --to")
    }
    val jobs = plan(ctx, startAt)
    Log.info("backfill_plan", "start_at" -> startAt, "window" -> window.describe, "jobs" -> jobs.map(_.name).mkString(" -> "))
    println(s"\nBackfill plan for $startAt over ${window.describe}:\n" + jobs.zipWithIndex.map { case (j, i) =>
      f"  ${i + 1}%2d. ${j.name}%-22s writes ${j.outputs.mkString(", ")}"
    }.mkString("\n") + "\n")
    if (dryRun) return

    MigrationRunner.migrate(ctx.spark, ctx.config)
    val (gold, silver) = jobs.partition(JobRegistry.isGold)
    silver.foreach(j => OpsStore.execute(ctx, j))
    if (silver.nonEmpty) {
      val blocking = DataQuality.blocking(DataQuality.run(ctx, Expectations.silver(ctx)), "error")
      if (blocking.nonEmpty) throw new DataQualityException(blocking)
    }
    if (gold.isEmpty) return

    val branch = s"backfill_${ctx.runId}"
    Publisher.prepareBranches(ctx, branch)
    val results = Publisher.writeAndAudit(ctx, branch, gold, Expectations.gold)
    diffReport(ctx, branch, window)
    val blocking = DataQuality.blocking(results, ctx.config.quality.blockPublishOn)
    if (blocking.nonEmpty) throw new DataQualityException(blocking)
    if (publish) {
      Publisher.publish(ctx, branch)
      ctx.watermarks.drain().foreach(OpsStore.setWatermark(ctx, _))
      Log.info("backfill_published", "branch" -> branch)
    } else {
      println(s"Backfill audited and left on branch '$branch'. Review ops.backfill_diffs, then run: publish --branch $branch")
    }
  }

  /** Per-day row counts and a key measure, main vs backfill branch, for every fact table. */
  private def diffReport(ctx: JobContext, branch: String, window: RunWindow.DateRange): Unit = {
    val spark = ctx.spark
    import spark.implicits._
    // (table, label, aggregate expression): row counts plus measures a backfill is expected to change or keep.
    val measures = Seq(
      (Tables.FctPlayerMatch, "net_worth", "sum(net_worth)"),
      (Tables.FctPlayerMatch, "unknown_hero_rows", "sum(CASE WHEN hero_sk = -1 THEN 1 ELSE 0 END)"),
      (Tables.FctItemPurchase, "net_cost_souls", "sum(net_cost_souls)"),
      (Tables.FctItemPurchase, "unknown_item_rows", "sum(CASE WHEN item_sk = -1 THEN 1 ELSE 0 END)"),
      (Tables.FctPlayerEconomy, "souls_earned", "sum(souls_earned)"))
    val now = Timestamp.from(Instant.now())
    val rows = measures.flatMap { case (table, label, expr) =>
      def side(ref: String) = spark.sql(
        s"""SELECT to_date(start_time) AS event_date, count(*) AS n, CAST($expr AS DOUBLE) AS v
           |FROM ${ctx.t(table)} VERSION AS OF '$ref'
           |WHERE start_time >= ${window.fromTs} AND start_time < ${window.toTsExclusive}
           |GROUP BY to_date(start_time)""".stripMargin)
      val main = side("main").withColumnRenamed("n", "main_rows").withColumnRenamed("v", "main_value")
      val br = side(branch).withColumnRenamed("n", "branch_rows").withColumnRenamed("v", "branch_value")
      main.join(br, Seq("event_date"), "full_outer")
        .select(lit(branch).as("backfill_id"), lit(table).as("table_name"), $"event_date",
          coalesce($"main_rows", lit(0L)).as("main_rows"), coalesce($"branch_rows", lit(0L)).as("branch_rows"),
          lit(label).as("metric"), $"main_value", $"branch_value", lit(now).as("checked_at"))
        .collect().toSeq
    }
    if (rows.nonEmpty) {
      val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), rows.head.schema)
      df.writeTo(ctx.t(Tables.BackfillDiffs)).append()
      println("Backfill diff (main vs branch):")
      df.orderBy("table_name", "metric", "event_date")
        .select($"table_name", $"event_date", $"main_rows", $"branch_rows", $"metric", $"main_value", $"branch_value",
          ($"branch_value" - $"main_value").as("delta"))
        .show(200, truncate = false)
    }
  }
}
