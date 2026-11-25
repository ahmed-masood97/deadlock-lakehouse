package com.ahmedmasood.deadlock.perf

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * A reproducible experiment on join skew, the most common Spark performance problem in this
 * domain: starter items (and popular heroes) appear in almost every match, so a join keyed on
 * item_id sends a huge share of rows to one task while the others sit idle.
 *
 * Three runs of the same join, measured with a SparkListener:
 *   1. sort-merge join, AQE off           -> one straggler task dominates the stage
 *   2. AQE skew-join handling on          -> Spark splits the hot partition at runtime
 *   3. manual salting                     -> spread the hot key across N sub-keys by hand
 * (The real pipeline avoids the problem entirely by broadcasting the small dimension tables.)
 */
object SkewLab {

  final case class Measurement(label: String, wallMs: Long, tasks: Int, maxTaskMs: Long, medianTaskMs: Long, rows: Long) {
    def skewRatio: Double = if (medianTaskMs == 0) maxTaskMs.toDouble else maxTaskMs.toDouble / medianTaskMs
    override def toString: String =
      f"$label%-34s wall=${wallMs}%6d ms  tasks=$tasks%4d  max_task=${maxTaskMs}%6d ms  median_task=${medianTaskMs}%5d ms  max/median=${skewRatio}%6.1f  rows=$rows"
  }

  private class TaskTimes extends SparkListener {
    val durations = ArrayBuffer[Long]()
    override def onTaskEnd(e: SparkListenerTaskEnd): Unit = synchronized { if (e.taskInfo != null) durations += e.taskInfo.duration }
    def reset(): Unit = synchronized(durations.clear())
    def snapshot: Seq[Long] = synchronized(durations.toList)
  }

  def run(spark: SparkSession, purchases: Long = 4000000L, hotShare: Double = 0.6, saltBuckets: Int = 16): Seq[Measurement] = {
    val listener = new TaskTimes
    spark.sparkContext.addSparkListener(listener)
    val saved = Seq("spark.sql.adaptive.enabled", "spark.sql.autoBroadcastJoinThreshold", "spark.sql.adaptive.skewJoin.enabled",
      "spark.sql.shuffle.partitions", "spark.sql.adaptive.coalescePartitions.enabled",
      "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "spark.sql.adaptive.advisoryPartitionSizeInBytes")
      .map(k => k -> spark.conf.getOption(k))

    // Fact: item purchases, `hotShare` of them on item 1 (think "Extra Health").
    val facts = spark.range(purchases).select(
      when(rand(7) < hotShare, lit(1L)).otherwise((rand(11) * 50000).cast("long") + 2).as("item_id"),
      (rand(13) * 6400).cast("long").as("cost"))
    // Per-item stats, big enough that we pretend it cannot be broadcast.
    val itemStats = spark.range(1, 50003).select(col("id").as("item_id"), (rand(17) * 100).as("popularity"),
      concat(lit("item_"), col("id")).as("label"))

    def measure(label: String)(df: => DataFrame): Measurement = {
      listener.reset()
      val t0 = System.nanoTime()
      val rows = df.agg(count(lit(1)), sum("cost")).head().getLong(0)
      val wall = (System.nanoTime() - t0) / 1000000
      Thread.sleep(300) // let the listener bus drain
      val d = listener.snapshot.sorted
      Measurement(label, wall, d.size, if (d.isEmpty) 0 else d.last, if (d.isEmpty) 0 else d(d.size / 2), rows)
    }

    try {
      spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
      spark.conf.set("spark.sql.shuffle.partitions", "32")

      spark.conf.set("spark.sql.adaptive.enabled", "false")
      val baseline = measure("1. sort-merge join, AQE off")(facts.join(itemStats, "item_id"))

      spark.conf.set("spark.sql.adaptive.enabled", "true")
      spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "false")
      spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
      spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "1MB")
      spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "1MB")
      val aqe = measure("2. AQE skew join")(facts.join(itemStats, "item_id"))

      spark.conf.set("spark.sql.adaptive.enabled", "false")
      val salted = measure(s"3. salted join ($saltBuckets buckets)") {
        val saltedFacts = facts.withColumn("salt", (rand(19) * saltBuckets).cast("int"))
        val replicated = itemStats.withColumn("salt", explode(sequence(lit(0), lit(saltBuckets - 1))))
        saltedFacts.join(replicated, Seq("item_id", "salt"))
      }
      Seq(baseline, aqe, salted)
    } finally {
      saved.foreach { case (k, v) => v.fold(spark.conf.unset(k))(spark.conf.set(k, _)) }
      spark.sparkContext.removeSparkListener(listener)
    }
  }
}
