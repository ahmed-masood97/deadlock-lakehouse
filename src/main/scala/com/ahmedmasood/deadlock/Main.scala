package com.ahmedmasood.deadlock

import java.time.LocalDate
import java.util.TimeZone

import com.ahmedmasood.deadlock.core._
import com.ahmedmasood.deadlock.ingest.{ApiExtractor, SyntheticDeadlock}
import com.ahmedmasood.deadlock.maintenance.Maintenance
import com.ahmedmasood.deadlock.orchestration._
import com.ahmedmasood.deadlock.perf.SkewLab

/**
 * Command line entry point (also what Airflow's spark-submit tasks call).
 *
 *   migrate                                    apply pending DDL migrations
 *   generate --from D --to D [--season-start D] [--matches-per-day N]
 *   extract  [--source NAME]                   pull from api.deadlock-api.com into landing
 *   daily    [--api]                           incremental run: landing -> bronze -> silver -> gold (WAP)
 *   backfill --from D --to D --start-at JOB|TABLE [--dry-run] [--no-publish]
 *   publish  --branch NAME                     cut over an audited backfill branch
 *   maintenance                                compaction, snapshot expiry, orphan cleanup
 *   lineage                                    print the job/table graph as Mermaid
 *   skew-lab                                   join-skew experiment (perf talking point)
 *   sql "SELECT ..."                           run a query against the lakehouse
 *   demo                                       end-to-end scenario on synthetic data
 *
 * Global: --config conf/pipeline.yaml
 */
object Main {

  def main(argv: Array[String]): Unit = {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC")) // belt and braces: Spark session is UTC too
    val (command, opts) = parse(argv)
    val cfg = PipelineConfig.load(opts.getOrElse("config", "conf/pipeline.yaml"))

    command match {
      case "generate" =>
        val from = LocalDate.parse(opts("from"))
        SyntheticDeadlock.generate(cfg, LocalDate.parse(opts.getOrElse("season-start", opts("from"))), from,
          LocalDate.parse(opts("to")), opts.getOrElse("matches-per-day", "300").toInt)

      case "lineage" => println(JobRegistry.all(cfg).toMermaid)

      case other =>
        val spark = SparkSessionFactory.build(cfg, s"deadlock-lakehouse:$other")
        try {
          val window = (opts.get("from"), opts.get("to")) match {
            case (Some(f), Some(t)) => RunWindow.DateRange(LocalDate.parse(f), LocalDate.parse(t))
            case _                  => RunWindow.Incremental
          }
          val ctx = JobContext(spark, cfg, JobContext.newRunId(other.replace('-', '_')), window)
          other match {
            case "migrate" => MigrationRunner.migrate(spark, cfg)
            case "extract" =>
              MigrationRunner.migrate(spark, cfg)
              val sources = opts.get("source").fold(cfg.sources)(n => Seq(cfg.source(n)))
              sources.foreach(s => ApiExtractor.extract(ctx, s))
            case "daily" => DailyPipeline.run(ctx, fromApi = opts.contains("api"))
            case "backfill" =>
              Backfill.run(ctx, opts.getOrElse("start-at", throw new IllegalArgumentException("--start-at is required")),
                dryRun = opts.contains("dry-run"), publish = !opts.contains("no-publish"))
            case "publish" => Publisher.publish(ctx, opts("branch"))
            case "maintenance" => Maintenance.run(ctx).foreach { case (t, r) => println(f"$t%-40s $r") }
            case "skew-lab" => SkewLab.run(spark).foreach(println)
            case "sql" => spark.sql(opts("query")).show(100, truncate = false)
            case "demo" => Demo.run(spark, cfg, opts.getOrElse("matches-per-day", "250").toInt)
            case unknown => throw new IllegalArgumentException(s"Unknown command '$unknown'")
          }
        } finally spark.stop()
    }
  }

  /** `cmd --key value --flag "positional"`; a bare value after `sql` becomes `query`. */
  private[deadlock] def parse(argv: Array[String]): (String, Map[String, String]) = {
    require(argv.nonEmpty, "usage: <command> [--options]; see Main.scala")
    val cmd = argv.head
    var opts = Map.empty[String, String]
    var i = 1
    while (i < argv.length) {
      val a = argv(i)
      if (a.startsWith("--")) {
        val key = a.drop(2)
        if (i + 1 < argv.length && !argv(i + 1).startsWith("--")) { opts += key -> argv(i + 1); i += 2 }
        else { opts += key -> "true"; i += 1 }
      } else { opts += "query" -> a; i += 1 }
    }
    (cmd, opts)
  }
}
