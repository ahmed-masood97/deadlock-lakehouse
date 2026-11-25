package com.ahmedmasood.deadlock

import com.ahmedmasood.deadlock.quality.Check._
import com.ahmedmasood.deadlock.quality.{DataQuality, Severity}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class DataQualitySpec extends AnyFunSuite with BeforeAndAfterAll {
  private lazy val spark = SharedSpark.spark

  override def beforeAll(): Unit = {
    super.beforeAll()
    import spark.implicits._
    Seq((1L, 1, -1L, 10L), (1L, 2, 7L, 20L), (2L, 1, 7L, 30L), (2L, 1, 8L, 40L))
      .toDF("match_id", "player_slot", "hero_sk", "net_worth").createOrReplaceTempView("dq_fact")
    Seq(7L).toDF("hero_sk").createOrReplaceTempView("dq_dim")
  }

  test("uniqueness catches a broken grain") {
    val r = Unique(Seq("match_id", "player_slot")).evaluate(spark, "dq_fact", "true")
    assert(!r.passed && r.observed == "1")
  }

  test("checks can be scoped to the rows a run touched") {
    assert(Unique(Seq("match_id", "player_slot")).evaluate(spark, "dq_fact", "match_id = 1").passed)
  }

  test("referential integrity ignores the unknown member but flags orphans") {
    val r = References("hero_sk", "dq_dim", "hero_sk").evaluate(spark, "dq_fact", "true")
    assert(!r.passed && r.observed == "1") // hero_sk 8 is an orphan; -1 is allowed
  }

  test("ratio checks report the observed share") {
    val r = MaxRatio("unknown_hero", "hero_sk = -1", 0.2).evaluate(spark, "dq_fact", "true")
    assert(!r.passed && r.observed.startsWith("0.25000"))
  }

  test("reconciliation honours its tolerance") {
    val exact = Reconcile("sum", "SELECT sum(net_worth) FROM dq_fact", "SELECT 100").evaluate(spark, "dq_fact", "true")
    val close = Reconcile("sum", "SELECT sum(net_worth) FROM dq_fact", "SELECT 101", tolerance = 0.02).evaluate(spark, "dq_fact", "true")
    assert(exact.passed && close.passed)
    assert(!Reconcile("sum", "SELECT sum(net_worth) FROM dq_fact", "SELECT 90").evaluate(spark, "dq_fact", "true").passed)
  }

  test("only error-severity failures block publish by default") {
    val results = Seq(
      RowCondition("positive", "net_worth > 0").evaluate(spark, "dq_fact", "true"),
      MaxRatio("unknown", "hero_sk = -1", 0.1, Severity.Warn).evaluate(spark, "dq_fact", "true"),
      RowCondition("big", "net_worth > 15").evaluate(spark, "dq_fact", "true"))
    assert(DataQuality.blocking(results, "error").map(_.check) == Seq("condition(big)"))
    assert(DataQuality.blocking(results, "warn").size == 2)
  }
}
