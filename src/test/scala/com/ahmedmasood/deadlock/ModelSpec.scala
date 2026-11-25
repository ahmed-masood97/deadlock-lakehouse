package com.ahmedmasood.deadlock

import com.ahmedmasood.deadlock.core.{Model, Pii}
import com.ahmedmasood.deadlock.orchestration.MigrationRunner
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite

class ModelSpec extends AnyFunSuite {
  private lazy val spark = SharedSpark.spark

  test("badge buckets follow the in-game rank tiers") {
    import spark.implicits._
    val buckets = Seq(Some(11), Some(46), Some(51), Some(76), Some(96), Some(116), None).toDF("b")
      .select(Model.badgeBucket(col("b"))).as[String].collect().toSeq
    assert(buckets.map(_.take(5)) == Seq("1_low", "1_low", "2_mid", "2_mid", "3_hig", "4_top", "unkno"))
  }

  test("KDA never divides by zero and souls per minute uses match length") {
    val r = spark.range(1).select(Model.kda(lit(5), lit(0), lit(3)), Model.soulsPerMinute(lit(30000), lit(1800))).head()
    assert(r.getDouble(0) == 8.0 && r.getDouble(1) == 1000.0)
  }

  test("player keys are stable, salted, and never expose the raw id") {
    import spark.implicits._
    val keys = Seq(Some(12345L), Some(12345L), Some(0L), None).toDF("id").select(Pii.playerKey(col("id"), "salt")).as[Option[String]].collect()
    assert(keys(0) == keys(1) && keys(0).get.length == 64 && !keys(0).get.contains("12345"))
    assert(keys(2).isEmpty && keys(3).isEmpty)
    val other = spark.range(1).select(Pii.playerKey(lit(12345L), "other-salt")).as[String].head()
    assert(other != keys(0).get)
  }

  test("event times parse from the API format, ISO-8601 and unix seconds alike") {
    import spark.implicits._
    val parsed = Seq("2026-10-01 12:00:00", "2026-10-01T12:00:00Z", "1790856000", "not a time").toDF("s")
      .select(date_format(Model.eventTime(col("s")), "yyyy-MM-dd HH:mm:ss")).as[Option[String]].collect().toSeq
    assert(parsed == Seq(Some("2026-10-01 12:00:00"), Some("2026-10-01 12:00:00"), Some("2026-10-01 12:00:00"), None))
  }

  test("migrations are bundled in order and split into statements") {
    val ms = MigrationRunner.bundled()
    assert(ms.map(_.version) == ms.map(_.version).sorted && ms.head.version == 1)
    val stmts = MigrationRunner.statements("-- comment\nCREATE TABLE a (x INT);\n\nALTER TABLE a ADD COLUMNS (y INT);\n")
    assert(stmts == Seq("CREATE TABLE a (x INT)", "ALTER TABLE a ADD COLUMNS (y INT)"))
  }
}
