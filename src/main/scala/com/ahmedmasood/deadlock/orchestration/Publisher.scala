package com.ahmedmasood.deadlock.orchestration

import com.ahmedmasood.deadlock.core._
import com.ahmedmasood.deadlock.iceberg.IcebergOps
import com.ahmedmasood.deadlock.quality.{CheckResult, DataQuality, DataQualityException, Suite}

/**
 * Write-audit-publish for the gold layer, on Iceberg branches.
 *
 *  WRITE   every gold table gets a branch named after the run; with `spark.wap.branch` set,
 *          all gold writes land on that branch and main is untouched.
 *  AUDIT   data quality suites run against the branch (reads also resolve to the branch).
 *  PUBLISH if no blocking check failed, main is fast-forwarded to the branch, table by table,
 *          dimensions before facts before marts. Otherwise the branch is kept for debugging,
 *          main still serves yesterday's good data, and the run fails loudly.
 *
 * Iceberg commits are per table, so publish is atomic per table, not across tables. Ordering
 * (dims first) means a reader never sees a fact whose dimension row is not yet published.
 */
object Publisher {

  final case class Outcome(results: Seq[CheckResult], published: Boolean)

  def prepareBranches(ctx: JobContext, branch: String, tables: Seq[String] = Tables.WapTables): Unit =
    tables.foreach(t => IcebergOps.createBranch(ctx.spark, ctx.t(t), branch))

  def writeAndAudit(ctx: JobContext, branch: String, jobs: Seq[Job], suites: JobContext => Seq[Suite]): Seq[CheckResult] =
    IcebergOps.withWapBranch(ctx.spark, branch) {
      jobs.foreach(j => OpsStore.execute(ctx, j))
      DataQuality.run(ctx, suites(ctx), ref = branch)
    }

  /** Fast-forwards main to `branch` for each table, after checking none of them moved meanwhile. */
  def publish(ctx: JobContext, branch: String, tables: Seq[String] = Tables.WapTables): Unit = {
    val moved = tables.filter(t => IcebergOps.mainMovedSince(ctx.spark, ctx.t(t), branch))
    if (moved.nonEmpty)
      throw new IllegalStateException(
        s"main moved on ${moved.mkString(", ")} since branch '$branch' was created (concurrent writer). " +
          "Nothing was published. Re-run the job so it rebuilds on top of the new main.")
    tables.foreach { t =>
      IcebergOps.fastForward(ctx.spark, ctx.catalog, t, branch)
      IcebergOps.dropBranch(ctx.spark, ctx.t(t), branch)
    }
  }

  /** Full cycle used by the daily run and by backfills. */
  def run(ctx: JobContext, branch: String, jobs: Seq[Job], suites: JobContext => Seq[Suite], autoPublish: Boolean = true): Outcome = {
    prepareBranches(ctx, branch)
    val results = writeAndAudit(ctx, branch, jobs, suites)
    val blocking = DataQuality.blocking(results, ctx.config.quality.blockPublishOn)
    if (blocking.nonEmpty) {
      Log.error("wap_audit_failed", "branch" -> branch, "blocking_failures" -> blocking.size)
      throw new DataQualityException(blocking)
    }
    if (autoPublish) {
      publish(ctx, branch)
      ctx.watermarks.drain().foreach(OpsStore.setWatermark(ctx, _))
      Log.info("wap_published", "branch" -> branch, "tables" -> Tables.WapTables.size)
    } else Log.info("wap_audited_not_published", "branch" -> branch)
    Outcome(results, published = autoPublish)
  }
}
