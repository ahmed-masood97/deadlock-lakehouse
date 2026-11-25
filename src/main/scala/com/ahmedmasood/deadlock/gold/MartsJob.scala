package com.ahmedmasood.deadlock.gold

import scala.io.Source

import com.ahmedmasood.deadlock.core._
import org.apache.spark.sql.functions._

/**
 * Business marts, written as plain SQL files (resources/sql/marts) and executed by a tiny
 * dbt-style runner: templated, version-controlled, reviewable by analysts.
 *
 * Only the partitions touched by this run are rebuilt (dynamic partition overwrite): the patches
 * and days whose facts were (re)written with this run id. An empty mart is rebuilt in full, which
 * is also how a newly added mart is initialised.
 */
object MartsJob extends Job {
  val name = "marts"
  val inputs = Seq(Tables.FctPlayerMatch, Tables.FctItemPurchase, Tables.FctPlayerEconomy, Tables.DimHero, Tables.DimItem, Tables.DimPatch)
  val outputs = Seq(Tables.MartHeroPatch, Tables.MartItemEconomy, Tables.MartEconomyDaily)

  private final case class Mart(table: String, sqlFile: String, partitionedBy: String)
  private val marts = Seq(
    Mart(Tables.MartHeroPatch, "mart_hero_patch_performance.sql", "patch"),
    Mart(Tables.MartItemEconomy, "mart_item_unit_economics.sql", "patch"),
    Mart(Tables.MartEconomyDaily, "mart_economy_daily.sql", "date"))

  def run(ctx: JobContext): JobResult = {
    val spark = ctx.spark
    import spark.implicits._
    val touchedPatches = spark.table(ctx.t(Tables.FctPlayerMatch)).where($"_run_id" === ctx.runId)
      .select("patch_key").distinct().as[Long].collect().sorted
    val touchedDates = spark.table(ctx.t(Tables.FctPlayerEconomy)).where($"_run_id" === ctx.runId)
      .select(date_format(to_date($"start_time"), "yyyy-MM-dd")).distinct().as[String].collect().sorted

    val metrics = marts.map { m =>
      val empty = spark.table(ctx.t(m.table)).isEmpty
      val filter = (m.partitionedBy, empty) match {
        case (_, true)                                 => "true"
        case ("patch", false) if touchedPatches.isEmpty => "false"
        case ("patch", false)                          => s"patch_key IN (${touchedPatches.mkString(", ")})"
        case (_, false) if touchedDates.isEmpty        => "false"
        case _ =>
          // Range predicate on the partition source column enables pruning; IN-list keeps it exact.
          val hi = java.time.LocalDate.parse(touchedDates.last).plusDays(1)
          s"start_time >= TIMESTAMP '${touchedDates.head} 00:00:00' AND start_time < TIMESTAMP '$hi 00:00:00' " +
            s"AND to_date(start_time) IN (${touchedDates.map(d => s"DATE '$d'").mkString(", ")})"
      }
      if (filter == "false") m.table -> 0L
      else {
        val sql = render(m.sqlFile, ctx, filter)
        val df = spark.sql(sql).cache()
        val n = df.count()
        df.writeTo(ctx.t(m.table)).overwritePartitions()
        df.unpersist()
        Log.info("mart_built", "mart" -> m.table, "rows" -> n, "full_rebuild" -> empty)
        m.table -> n
      }
    }
    JobResult(metrics.map { case (t, n) => s"rows_${t.split('.').last}" -> n }.toMap)
  }

  private[deadlock] def render(file: String, ctx: JobContext, filter: String): String = {
    val stream = getClass.getClassLoader.getResourceAsStream(s"sql/marts/$file")
    require(stream != null, s"Missing mart SQL $file")
    val src = Source.fromInputStream(stream, "UTF-8")
    val template = try src.mkString finally src.close()
    template
      .replace("${catalog}", ctx.catalog)
      .replace("${run_id}", ctx.runId)
      .replace("${patch_filter}", filter)
      .replace("${date_filter}", filter)
  }
}
