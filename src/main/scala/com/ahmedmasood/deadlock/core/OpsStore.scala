package com.ahmedmasood.deadlock.core

import java.sql.Timestamp
import java.time.Instant

import scala.util.Try

import com.ahmedmasood.deadlock.iceberg.{IcebergOps, SnapshotRef}
import org.apache.spark.sql.SparkSession

final case class Watermark(job: String, sourceTable: String, snapshotId: Option[Long], snapshotTs: Option[Timestamp], value: Option[String])

private[core] final case class WatermarkRow(job: String, source_table: String, snapshot_id: Option[Long], snapshot_ts: Option[Timestamp],
                                            value: Option[String], updated_at: Timestamp, run_id: String)
private[core] final case class JobRunRow(run_id: String, job: String, run_window: String, status: String, started_at: Timestamp,
                                         finished_at: Timestamp, duration_ms: Long, metrics: Map[String, Long], error: Option[String])
private[core] final case class LineageRow(run_id: String, job: String, direction: String, table_name: String, ref: String,
                                          snapshot_id: Option[Long], recorded_at: Timestamp)

/**
 * The platform's own state: watermarks for incremental processing, an audit row per job run,
 * and snapshot-level lineage. All of it lives in Iceberg tables in the `ops` namespace, so it is
 * versioned, queryable with SQL and survives cluster restarts.
 */
object OpsStore {

  def getWatermark(ctx: JobContext, job: String, sourceTable: String): Option[Watermark] = {
    import ctx.spark.implicits._
    ctx.spark.table(ctx.t(Tables.Watermarks))
      .where($"job" === job && $"source_table" === sourceTable)
      .as[WatermarkRow]
      .collect()
      .headOption
      .map(r => Watermark(r.job, r.source_table, r.snapshot_id, r.snapshot_ts, r.value))
  }

  def setWatermark(ctx: JobContext, wm: Watermark): Unit = {
    import ctx.spark.implicits._
    val row = WatermarkRow(wm.job, wm.sourceTable, wm.snapshotId, wm.snapshotTs, wm.value, Timestamp.from(Instant.now()), ctx.runId)
    Seq(row).toDS().createOrReplaceTempView("wm_update")
    ctx.spark.sql(
      s"""MERGE INTO ${ctx.t(Tables.Watermarks)} t
         |USING wm_update s
         |ON t.job = s.job AND t.source_table = s.source_table
         |WHEN MATCHED THEN UPDATE SET *
         |WHEN NOT MATCHED THEN INSERT *""".stripMargin)
  }

  /** Executes a job with run auditing and snapshot-level lineage. Failures are recorded, then rethrown. */
  def execute(ctx: JobContext, job: Job): JobResult = {
    val spark = ctx.spark
    val started = Instant.now()
    val wapBranch = Try(spark.conf.get("spark.wap.branch")).toOption
    Log.info("job_started", "job" -> job.name, "run_id" -> ctx.runId, "window" -> ctx.window.describe,
      "wap_branch" -> wapBranch.getOrElse("-"))

    val inputSnapshots = job.inputs.map(t => (t, snapshotOf(spark, ctx.t(t), wapBranch)))
    // Cached DataFrames are job-scoped. A cache that outlives its job would be served to later
    // queries that read the same table and hide newer commits, so every job starts and ends clean.
    spark.catalog.clearCache()
    val outcome = try Try(job.run(ctx)) finally spark.catalog.clearCache()
    val finished = Instant.now()
    val outputSnapshots = job.outputs.map(t => (t, snapshotOf(spark, ctx.t(t), wapBranch)))

    val (status, error, result) = outcome match {
      case scala.util.Success(r) => ("SUCCEEDED", None, r)
      case scala.util.Failure(e) => ("FAILED", Some(s"${e.getClass.getSimpleName}: ${e.getMessage}".take(2000)), JobResult())
    }
    recordRun(spark, ctx, JobRunRow(ctx.runId, job.name, ctx.window.describe, status, Timestamp.from(started),
      Timestamp.from(finished), finished.toEpochMilli - started.toEpochMilli, result.metrics, error))
    recordLineage(spark, ctx, job.name, inputSnapshots.map { case (t, s) => ("input", t, s) } ++
      outputSnapshots.map { case (t, s) => ("output", t, s) }, wapBranch)

    outcome.get // rethrow on failure
    Log.info("job_finished", ("job" -> job.name) +: ("duration_ms" -> (finished.toEpochMilli - started.toEpochMilli)) +:
      result.metrics.toSeq.map { case (k, v) => k -> v }: _*)
    result
  }

  private def snapshotOf(spark: SparkSession, fq: String, branch: Option[String]): Option[SnapshotRef] =
    Try {
      branch.flatMap(b => IcebergOps.branchHead(spark, fq, b)).orElse(IcebergOps.currentSnapshot(spark, fq))
    }.toOption.flatten

  private def recordRun(spark: SparkSession, ctx: JobContext, row: JobRunRow): Unit = {
    import spark.implicits._
    Seq(row).toDS().writeTo(ctx.t(Tables.JobRuns)).append()
  }

  private def recordLineage(spark: SparkSession, ctx: JobContext, job: String,
                            refs: Seq[(String, String, Option[SnapshotRef])], branch: Option[String]): Unit = {
    import spark.implicits._
    val now = Timestamp.from(Instant.now())
    val rows = refs.map { case (dir, table, snap) =>
      LineageRow(ctx.runId, job, dir, table, branch.getOrElse("main"), snap.map(_.id), now)
    }
    if (rows.nonEmpty) rows.toDS().writeTo(ctx.t(Tables.Lineage)).append()
  }
}
