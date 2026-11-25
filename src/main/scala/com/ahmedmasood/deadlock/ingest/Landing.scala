package com.ahmedmasood.deadlock.ingest

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, StandardCopyOption}
import java.time.{Instant, LocalDate}

import scala.collection.JavaConverters._

import com.ahmedmasood.deadlock.core.{JobContext, Log, OpsStore, PipelineConfig, SourceConfig, Watermark}
import com.fasterxml.jackson.databind.ObjectMapper

/**
 * The landing zone: immutable NDJSON files exactly as fetched, one directory per source.
 *
 *   landing/matches/batch=<run_id>/part-00001.ndjson         (paged sources)
 *   landing/heroes/snapshot_date=2026-11-20/part-00001.ndjson (snapshot sources)
 *
 * Landing first and loading second means a failed load can always be replayed from the files,
 * and the bronze loader is the same code whether the files came from the API or the generator.
 */
object Landing {

  def sourceDir(cfg: PipelineConfig, source: String): File = new File(cfg.landingDir, source)

  /** Atomic write: write to a temp file, then rename, so readers never see half a file. */
  def writeLines(dir: File, fileName: String, lines: Iterator[String]): File = {
    dir.mkdirs()
    val target = new File(dir, fileName)
    val tmp = new File(dir, s".$fileName.tmp")
    val w = Files.newBufferedWriter(tmp.toPath, StandardCharsets.UTF_8)
    try lines.foreach { l => w.write(l); w.write('\n') } finally w.close()
    Files.move(tmp.toPath, target.toPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
    target
  }

  def listFiles(cfg: PipelineConfig, source: String): Seq[File] = {
    val root = sourceDir(cfg, source)
    if (!root.exists()) Nil
    else Files.walk(root.toPath).iterator().asScala.map(_.toFile.getCanonicalFile)
      .filter(f => f.isFile && f.getName.endsWith(".ndjson")).toSeq.sortBy(_.getPath)
  }
}

/** Pulls data from the live API into the landing zone. */
object ApiExtractor {

  private val json = new ObjectMapper()

  def extract(ctx: JobContext, source: SourceConfig, client: DeadlockApiClient = new DeadlockApiClient()): Seq[File] =
    source.kind match {
      case "paged_ndjson"  => extractPaged(ctx, source, client)
      case "json_snapshot" => extractSnapshot(ctx, source, client)
      case other           => throw new IllegalArgumentException(s"Unsupported source kind '$other'")
    }

  /**
   * Watermarked pagination: ask for records with key > last seen key, ordered by key, page by
   * page. The watermark only advances after a page is safely landed, so a crash mid-run resumes
   * from the last good page and never skips data. Duplicates on retry are harmless: silver dedups.
   */
  private def extractPaged(ctx: JobContext, source: SourceConfig, client: DeadlockApiClient): Seq[File] = {
    val api = source.api
    val wmParam = api.watermarkParam.getOrElse(throw new IllegalArgumentException(s"${source.name}: paged source needs watermark_param"))
    val pageSize = api.pageSize.getOrElse(1000)
    val jobName = s"extract_${source.name}"
    val wmKey = s"api:${api.endpoint}"
    val batchDir = new File(Landing.sourceDir(ctx.config, source.name), s"batch=${ctx.runId}")

    var lastKey: Long = OpsStore.getWatermark(ctx, jobName, wmKey).flatMap(_.value).map(_.toLong).getOrElse(0L)
    val files = Seq.newBuilder[File]
    var page = 1
    var done = false
    while (!done && page <= api.maxPages.getOrElse(10)) {
      val body = client.get(api, Map(wmParam -> (lastKey + 1).toString, "limit" -> pageSize.toString))
      val lines = body.split("\n").map(_.trim).filter(_.nonEmpty)
      if (lines.isEmpty) done = true
      else {
        files += Landing.writeLines(batchDir, f"part-$page%05d.ndjson", lines.iterator)
        val maxKey = lines.map(l => json.readTree(l).path("match_id").asLong(0L)).max
        lastKey = math.max(lastKey, maxKey)
        OpsStore.setWatermark(ctx, Watermark(jobName, wmKey, None, None, Some(lastKey.toString)))
        Log.info("api_page_landed", "source" -> source.name, "page" -> page, "records" -> lines.length, "max_key" -> maxKey)
        done = lines.length < pageSize
        page += 1
      }
    }
    files.result()
  }

  /** Snapshot sources return the full current state; we land it as NDJSON under today's date. */
  private def extractSnapshot(ctx: JobContext, source: SourceConfig, client: DeadlockApiClient): Seq[File] = {
    val body = client.get(source.api)
    val root = json.readTree(body)
    require(root.isArray, s"${source.name}: expected a JSON array from ${source.api.endpoint}")
    val lines = root.elements().asScala.map(n => json.writeValueAsString(n))
    val dir = new File(Landing.sourceDir(ctx.config, source.name), s"snapshot_date=${LocalDate.now()}")
    val f = Landing.writeLines(dir, s"part-${Instant.now().toEpochMilli}.ndjson", lines)
    Log.info("api_snapshot_landed", "source" -> source.name, "file" -> f.getPath)
    Seq(f)
  }
}
