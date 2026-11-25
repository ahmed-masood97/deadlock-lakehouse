package com.ahmedmasood.deadlock.ingest

import com.ahmedmasood.deadlock.core.{Job, JobContext, JobResult, Log, Model, SourceConfig}
import org.apache.spark.sql.functions._

/**
 * Generic loader: landing NDJSON files -> bronze Iceberg table. One implementation for every
 * source; the per-source differences (key path, event-time path) come from configuration.
 *
 * Idempotent: files already present in bronze (by `source_file`) are skipped, so re-running a
 * failed or duplicated schedule never double-loads.
 */
final class BronzeIngestJob(source: SourceConfig) extends Job {
  val name: String = s"bronze_ingest_${source.name}"
  val inputs: Seq[String] = Nil
  val outputs: Seq[String] = Seq(source.bronzeTable)

  def run(ctx: JobContext): JobResult = {
    val spark = ctx.spark
    import spark.implicits._
    val target = ctx.t(source.bronzeTable)

    // Paths are compared without the URI scheme: file:/x, file:///x and /x are the same file.
    val landed = Landing.listFiles(ctx.config, source.name).map(_.getCanonicalPath)
    val alreadyLoaded = spark.table(target).select("source_file").distinct().as[String].collect().toSet
    val toLoad = landed.filterNot(alreadyLoaded.contains)
    if (toLoad.isEmpty) {
      Log.info("bronze_nothing_to_load", "source" -> source.name)
      return JobResult(Map("files_loaded" -> 0L, "records_loaded" -> 0L))
    }

    val eventTs = source.eventTime match {
      case Some(path) => Model.eventTime(get_json_object($"value", path))
      case None       => lit(null).cast("timestamp")
    }
    val df = spark.read.text(toLoad: _*)
      .where(length(trim($"value")) > 0)
      .select(
        lit(ctx.runId).as("ingest_run_id"),
        current_timestamp().as("ingested_at"),
        lit(source.name).as("source"),
        regexp_replace($"_metadata.file_path", "^file:/+", "/").as("source_file"),
        get_json_object($"value", source.recordKey).as("record_key"),
        eventTs.as("event_ts"),
        to_date(regexp_extract($"_metadata.file_path", "snapshot_date=(\\d{4}-\\d{2}-\\d{2})", 1)).as("snapshot_date"),
        $"value".as("payload"),
        sha2($"value", 256).as("payload_sha256")
      )
      .cache()

    val records = df.count()
    df.writeTo(target).append()
    df.unpersist()
    Log.info("bronze_loaded", "source" -> source.name, "files" -> toLoad.size, "records" -> records)
    JobResult(Map("files_loaded" -> toLoad.size.toLong, "records_loaded" -> records))
  }
}
