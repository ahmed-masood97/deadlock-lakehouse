package com.ahmedmasood.deadlock.maintenance

import java.time.Instant
import java.time.temporal.ChronoUnit

import scala.util.Try

import com.ahmedmasood.deadlock.core._
import com.ahmedmasood.deadlock.iceberg.IcebergOps

/**
 * Weekly table hygiene. Streaming-ish daily appends and MERGEs leave many small files and a long
 * snapshot history; both make planning and scans slower and storage more expensive.
 *
 *  - compaction (bin-pack, or sort for tables with a write order so clustering is restored)
 *  - manifest rewrite (faster query planning)
 *  - snapshot expiry, keeping enough history for time travel and incremental readers
 *  - orphan file removal (files left by failed writes)
 *
 * Bronze snapshots are kept back to the oldest incremental watermark; expiring them would not
 * break the readers (they fall back to commit time) but would lose the cheap incremental path.
 */
object Maintenance {

  final case class Policy(compactSorted: Set[String], retainDays: Int, retainLast: Int, orphanDays: Int)

  val default = Policy(compactSorted = Set(Tables.FctPlayerMatch, Tables.FctItemPurchase), retainDays = 7, retainLast = 10, orphanDays = 3)

  def run(ctx: JobContext, policy: Policy = default): Seq[(String, String)] = {
    val spark = ctx.spark
    val tables = Seq(Tables.RawMatches, Tables.RawHeroes, Tables.RawItems, Tables.RawPatches,
      Tables.Matches, Tables.MatchPlayers, Tables.ItemPurchases) ++ Tables.WapTables
    val oldestWatermark = Try(spark.table(ctx.t(Tables.Watermarks)).where("snapshot_ts IS NOT NULL")
      .selectExpr("min(snapshot_ts)").head().getTimestamp(0)).toOption.flatMap(Option(_)).map(_.toInstant)

    tables.map { t =>
      val outcome = Try {
        val strategy = if (policy.compactSorted.contains(t)) "sort" else "binpack"
        val c = IcebergOps.compact(spark, ctx.catalog, t, strategy).head()
        IcebergOps.rewriteManifests(spark, ctx.catalog, t).collect()
        var cutoff = Instant.now().minus(policy.retainDays.toLong, ChronoUnit.DAYS)
        if (t.startsWith("bronze.")) oldestWatermark.foreach(w => if (w.isBefore(cutoff)) cutoff = w)
        val e = IcebergOps.expireSnapshots(spark, ctx.catalog, t, cutoff, policy.retainLast).head()
        IcebergOps.removeOrphans(spark, ctx.catalog, t, Instant.now().minus(policy.orphanDays.toLong, ChronoUnit.DAYS)).collect()
        s"$strategy: rewrote ${c.get(0)} files into ${c.get(1)}; expired ${e.get(0)} data files"
      }.recover { case ex => s"skipped: ${ex.getMessage.take(200)}" }.get
      Log.info("maintenance", "table" -> t, "result" -> outcome)
      t -> outcome
    }
  }
}
