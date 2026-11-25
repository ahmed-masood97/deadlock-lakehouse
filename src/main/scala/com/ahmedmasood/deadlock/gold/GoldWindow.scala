package com.ahmedmasood.deadlock.gold

import com.ahmedmasood.deadlock.core._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * The set of matches a gold job must (re)build in this run.
 *
 * Incremental: matches whose silver row changed since the job's high-water mark (`_updated_at`).
 * Silver is maintained with MERGE/DELETE, so snapshot-based incremental reads (append-only) do
 * not apply here; a high-water-mark column does. Late-arriving matches are picked up naturally
 * because they get a fresh `_updated_at` whatever their start_time.
 *
 * Date range: every match whose start_time falls in the window (backfill).
 *
 * @param keys   match_id + start_time of the affected matches (cached)
 * @param commit stages the new watermark; it is persisted only after the gold branch is published
 */
final case class GoldWindow(keys: DataFrame, count: Long, commit: () => Unit)

object GoldWindow {

  def affectedMatches(ctx: JobContext, jobName: String): GoldWindow = {
    val spark = ctx.spark
    val matches = spark.table(ctx.t(Tables.Matches))
    ctx.window match {
      case r: RunWindow.DateRange =>
        val keys = matches.where(s"start_time >= ${r.fromTs} AND start_time < ${r.toTsExclusive}").select("match_id", "start_time").cache()
        GoldWindow(keys, keys.count(), () => ())

      case RunWindow.Incremental =>
        val hwm = OpsStore.getWatermark(ctx, jobName, Tables.Matches).flatMap(_.value)
        val selected = hwm.fold(matches)(v => matches.where(col("_updated_at") > to_timestamp(lit(v)))).cache()
        val stats = selected.select(count(lit(1)), date_format(max(col("_updated_at")), "yyyy-MM-dd HH:mm:ss.SSSSSS")).head()
        val n = stats.getLong(0)
        val newHwm = Option(stats.getString(1))
        val keys = selected.select("match_id", "start_time").cache()
        GoldWindow(keys, n, () => newHwm.foreach(v => ctx.watermarks.stage(Watermark(jobName, Tables.Matches, None, None, Some(v)))))
    }
  }
}
