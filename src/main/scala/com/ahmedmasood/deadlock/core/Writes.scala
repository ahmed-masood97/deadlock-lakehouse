package com.ahmedmasood.deadlock.core

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.{DataFrame, SparkSession}

/** Shared write patterns. */
object Writes {

  /**
   * Replace the rows of the matches listed in temp view `keysView` (column `match_id`):
   * delete them, then append the new version. `prune` is a literal predicate on the partition
   * column so the delete only touches the partitions involved.
   *
   * Inside a write-audit-publish branch the two commits are invisible to readers until publish,
   * so consumers never observe the gap between delete and insert.
   */
  def replaceMatches(spark: SparkSession, table: String, keysView: String, prune: String, rows: DataFrame): Unit = {
    spark.sql(s"DELETE FROM $table WHERE $prune AND match_id IN (SELECT match_id FROM $keysView)")
    rows.writeTo(table).append()
  }

  /** Inclusive start_time bounds of a keys view as a pruning predicate, formatted in the session time zone. */
  def startTimeBounds(spark: SparkSession, keysView: String): String = {
    val b = spark.sql(
      s"""SELECT date_format(min(start_time), 'yyyy-MM-dd HH:mm:ss') lo,
         |       date_format(max(start_time), 'yyyy-MM-dd HH:mm:ss') hi FROM $keysView""".stripMargin).head()
    s"start_time >= TIMESTAMP '${b.getString(0)}' AND start_time <= TIMESTAMP '${b.getString(1)}'"
  }
}

/**
 * Watermarks for jobs that write inside a write-audit-publish branch. They are only committed
 * after the branch is published; if the audit fails, the next run starts from the same place.
 */
final class WatermarkBuffer {
  private val staged = ArrayBuffer[Watermark]()
  def stage(w: Watermark): Unit = synchronized { staged += w }
  def drain(): Seq[Watermark] = synchronized { val out = staged.toList; staged.clear(); out }
}
