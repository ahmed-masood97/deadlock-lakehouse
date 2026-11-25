package com.ahmedmasood.deadlock.orchestration

import java.security.MessageDigest
import java.sql.Timestamp
import java.time.Instant

import scala.io.Source

import com.ahmedmasood.deadlock.core.{Log, PipelineConfig, Tables}
import org.apache.spark.sql.SparkSession

final case class Migration(version: Int, description: String, sql: String) {
  lazy val checksum: String =
    MessageDigest.getInstance("SHA-256").digest(sql.getBytes("UTF-8")).map("%02x".format(_)).mkString.take(16)
}

/**
 * Versioned, forward-only DDL migrations (Flyway semantics) for the lakehouse.
 *
 *  - Files live in resources/sql/migrations as V<NNN>__<description>.sql and run in order.
 *  - Applied versions and checksums are recorded in ops.schema_migrations.
 *  - Editing an already-applied migration is an error: schema history must be reproducible,
 *    so changes go in a new migration instead.
 */
object MigrationRunner {

  private val Pattern = """V(\d{3})__(.+)\.sql""".r

  /** Migrations bundled with the build, listed in resources/sql/migrations/INDEX. */
  def bundled(): Seq[Migration] = {
    val index = resource("sql/migrations/INDEX").split("\n").map(_.trim).filter(_.nonEmpty)
    index.toSeq.map {
      case f @ Pattern(v, desc) => Migration(v.toInt, desc.replace('_', ' '), resource(s"sql/migrations/$f"))
      case other                => throw new IllegalStateException(s"Bad migration file name: $other")
    }.sortBy(_.version)
  }

  def migrate(spark: SparkSession, cfg: PipelineConfig, migrations: Seq[Migration] = bundled()): Seq[Int] = {
    val cat = cfg.catalog.name
    val history = s"$cat.${Tables.SchemaMigrations}"
    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $cat.ops")
    spark.sql(
      s"""CREATE TABLE IF NOT EXISTS $history (
         |  version INT, description STRING, checksum STRING, applied_at TIMESTAMP
         |) USING iceberg""".stripMargin)

    val applied: Map[Int, String] = spark.table(history).collect()
      .map(r => r.getAs[Int]("version") -> r.getAs[String]("checksum")).toMap

    migrations.filter(m => applied.contains(m.version)).foreach { m =>
      if (applied(m.version) != m.checksum)
        throw new IllegalStateException(
          s"Migration V${m.version} was modified after it was applied (checksum ${applied(m.version)} -> ${m.checksum}). " +
            "Add a new migration instead of editing history.")
    }

    val pending = migrations.filterNot(m => applied.contains(m.version))
    pending.foreach { m =>
      Log.info("migration_applying", "version" -> m.version, "description" -> m.description)
      statements(m.sql.replace("${catalog}", cat)).foreach(s => spark.sql(s))
      import spark.implicits._
      Seq((m.version, m.description, m.checksum, Timestamp.from(Instant.now())))
        .toDF("version", "description", "checksum", "applied_at")
        .writeTo(history).append()
    }
    if (pending.isEmpty) Log.info("migrations_up_to_date", "latest" -> migrations.map(_.version).max)
    pending.map(_.version)
  }

  /** Splits a script on semicolons that end a line, ignoring `--` comment lines. */
  private[deadlock] def statements(sql: String): Seq[String] =
    sql.split("\n").filterNot(_.trim.startsWith("--")).mkString("\n")
      .split(";\\s*(\n|$)").map(_.trim).filter(_.nonEmpty).toSeq

  private def resource(path: String): String = {
    val stream = Option(getClass.getClassLoader.getResourceAsStream(path))
      .getOrElse(throw new IllegalStateException(s"Missing resource $path"))
    val src = Source.fromInputStream(stream, "UTF-8")
    try src.mkString finally src.close()
  }
}
