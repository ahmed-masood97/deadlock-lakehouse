package com.ahmedmasood.deadlock.core

import java.time.{Instant, LocalDate}
import java.util.UUID

import org.apache.spark.sql.SparkSession

/** Which slice of data a job processes. */
sealed trait RunWindow { def describe: String }

object RunWindow {

  /** Process whatever changed since this job's last successful run (watermark-driven). */
  case object Incremental extends RunWindow { val describe = "incremental" }

  /** Reprocess an explicit event-date range, inclusive. Used for backfills and repairs. */
  final case class DateRange(from: LocalDate, to: LocalDate) extends RunWindow {
    require(!to.isBefore(from), s"Invalid range $from..$to")
    val describe = s"range[$from..$to]"
    def fromTs: String = s"TIMESTAMP '$from 00:00:00'"
    def toTsExclusive: String = s"TIMESTAMP '${to.plusDays(1)} 00:00:00'"
  }
}

/** Everything a job needs at runtime. One context per pipeline run. */
final case class JobContext(
    spark: SparkSession,
    config: PipelineConfig,
    runId: String,
    window: RunWindow,
    startedAt: Instant = Instant.now(),
    watermarks: WatermarkBuffer = new WatermarkBuffer
) {
  def catalog: String = config.catalog.name
  def t(name: String): String = s"$catalog.$name" // fully qualified table name
}

object JobContext {
  def newRunId(prefix: String = "run"): String =
    s"${prefix}_${Instant.now().toString.replaceAll("[-:.TZ]", "").take(14)}_${UUID.randomUUID().toString.take(6)}"
}

/** Outcome of one job execution: row counts and anything else worth graphing. */
final case class JobResult(metrics: Map[String, Long] = Map.empty, notes: Seq[String] = Nil) {
  def ++(other: JobResult): JobResult = JobResult(metrics ++ other.metrics, notes ++ other.notes)
}

/**
 * A unit of work that reads some tables and writes others.
 *
 * Jobs declare their inputs and outputs. That declaration is the single source of truth
 * for lineage (ops.lineage), for dependency ordering (DependencyGraph) and for backfill
 * planning: "rebuild X and everything downstream of it" is computed, not hand-maintained.
 */
trait Job {
  def name: String
  def inputs: Seq[String]
  def outputs: Seq[String]
  def run(ctx: JobContext): JobResult
}

/** Minimal structured logging (one JSON line per event) so logs are queryable in any log store. */
object Log {
  private def esc(s: String) = s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n")

  def info(event: String, fields: (String, Any)*): Unit = emit("INFO", event, fields)
  def warn(event: String, fields: (String, Any)*): Unit = emit("WARN", event, fields)
  def error(event: String, fields: (String, Any)*): Unit = emit("ERROR", event, fields)

  private def emit(level: String, event: String, fields: Seq[(String, Any)]): Unit = {
    val body = fields.map { case (k, v) => s""""$k":"${esc(String.valueOf(v))}"""" }.mkString(",")
    val sep = if (body.isEmpty) "" else ","
    println(s"""{"ts":"${Instant.now()}","level":"$level","event":"${esc(event)}"$sep$body}""")
  }
}
