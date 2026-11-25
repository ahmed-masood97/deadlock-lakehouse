package com.ahmedmasood.deadlock

import java.sql.Timestamp

import com.ahmedmasood.deadlock.gold.{Scd2, Scd2Spec}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class Scd2Spec extends AnyFunSuite with BeforeAndAfterAll {
  private lazy val spark = SharedSpark.spark
  private val table = "lake.test_scd2.dim_item"
  private val spec = Scd2Spec(table, "item_id", "item_sk", tracked = Seq("item_name", "cost_souls"), attributes = Seq("item_name", "cost_souls"))

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.test_scd2")
    spark.sql(s"DROP TABLE IF EXISTS $table")
    spark.sql(
      s"""CREATE TABLE $table (item_sk BIGINT, item_id BIGINT, item_name STRING, cost_souls BIGINT, row_hash STRING,
         |valid_from TIMESTAMP, valid_to TIMESTAMP, is_current BOOLEAN, _run_id STRING) USING iceberg""".stripMargin)
  }

  private def snapshot(day: String, rows: (Long, String, Long)*): DataFrame = {
    import spark.implicits._
    rows.toDF("item_id", "item_name", "cost_souls").withColumn("effective_from", lit(Timestamp.valueOf(s"$day 00:00:00")))
  }

  private def applySnapshot(df: DataFrame, run: String) = {
    val plan = Scd2.plan(spec, spark.table(table), df, run)
    Scd2.apply(spark, spec, table, plan)
    plan
  }

  private def history(id: Long) =
    spark.table(table).where(col("item_id") === id).orderBy("valid_from")
      .select("cost_souls", "valid_from", "valid_to", "is_current").collect().map(r => (r.getLong(0), r.getTimestamp(1).toString, r.getTimestamp(2).toString, r.getBoolean(3))).toSeq

  test("first snapshot inserts every key, valid from the beginning of time") {
    val p = applySnapshot(snapshot("2026-10-01", (1L, "Extra Health", 500L), (2L, "Basic Magazine", 500L)), "r1")
    assert(p.newKeys == 2 && p.changedKeys == 0)
    assert(history(1) == Seq((500L, "1900-01-01 00:00:00.0", "9999-12-31 00:00:00.0", true)))
  }

  test("an unchanged snapshot is a no-op") {
    val before = spark.table(table).count()
    val p = applySnapshot(snapshot("2026-10-05", (1L, "Extra Health", 500L), (2L, "Basic Magazine", 500L)), "r2")
    assert(p.unchanged == 2 && p.newKeys == 0 && p.changedKeys == 0)
    assert(spark.table(table).count() == before)
  }

  test("a price change closes the old version and opens a new one, contiguously") {
    val p = applySnapshot(snapshot("2026-10-10", (1L, "Extra Health", 800L), (2L, "Basic Magazine", 500L)), "r3")
    assert(p.changedKeys == 1 && p.unchanged == 1)
    assert(history(1) == Seq(
      (500L, "1900-01-01 00:00:00.0", "2026-10-10 00:00:00.0", false),
      (800L, "2026-10-10 00:00:00.0", "9999-12-31 00:00:00.0", true)))
    assert(spark.table(table).where("is_current").groupBy("item_id").count().where("count <> 1").count() == 0)
  }

  test("an out-of-order (older) snapshot is ignored") {
    val p = applySnapshot(snapshot("2026-10-03", (1L, "Extra Health", 999L)), "r4")
    assert(p.staleSkipped == 1 && p.changedKeys == 0)
    assert(history(1).last._1 == 800L)
  }

  test("surrogate keys are deterministic, so a rebuild reproduces them") {
    val keys = spark.table(table).select("item_sk").collect().map(_.getLong(0)).toSet
    spark.sql(s"DELETE FROM $table WHERE true")
    applySnapshot(snapshot("2026-10-01", (1L, "Extra Health", 500L), (2L, "Basic Magazine", 500L)), "rebuild")
    applySnapshot(snapshot("2026-10-10", (1L, "Extra Health", 800L), (2L, "Basic Magazine", 500L)), "rebuild")
    assert(spark.table(table).select("item_sk").collect().map(_.getLong(0)).toSet == keys)
  }

  test("point-in-time lookup returns the price in force at match time") {
    import spark.implicits._
    val facts = Seq(("before", Timestamp.valueOf("2026-10-09 23:59:59")), ("after", Timestamp.valueOf("2026-10-10 00:00:00")))
      .toDF("label", "start_time").withColumn("item_id", lit(1L))
    val d = spark.table(table)
    val priced = facts.join(d, facts("item_id") === d("item_id") && facts("start_time") >= d("valid_from") && facts("start_time") < d("valid_to"))
      .select("label", "cost_souls").as[(String, Long)].collect().toMap
    assert(priced == Map("before" -> 500L, "after" -> 800L))
  }
}
