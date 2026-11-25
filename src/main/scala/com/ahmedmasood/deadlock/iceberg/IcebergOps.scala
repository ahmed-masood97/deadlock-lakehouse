package com.ahmedmasood.deadlock.iceberg

import java.time.Instant

import scala.collection.JavaConverters._
import scala.util.Try

import com.ahmedmasood.deadlock.core.Log
import org.apache.iceberg.Table
import org.apache.iceberg.spark.Spark3Util
import org.apache.spark.sql.{DataFrame, SparkSession}

/** A committed table version: what incremental readers and lineage records point at. */
final case class SnapshotRef(id: Long, committedAt: Instant)

/** Thin, well-named wrappers around the Iceberg features the platform depends on. */
object IcebergOps {

  def load(spark: SparkSession, fqTable: String): Table = {
    val t = Spark3Util.loadIcebergTable(spark, fqTable)
    t.refresh()
    t
  }

  def exists(spark: SparkSession, fqTable: String): Boolean = spark.catalog.tableExists(fqTable)

  def currentSnapshot(spark: SparkSession, fqTable: String): Option[SnapshotRef] =
    Option(load(spark, fqTable).currentSnapshot()).map(s => SnapshotRef(s.snapshotId(), Instant.ofEpochMilli(s.timestampMillis())))

  def branchHead(spark: SparkSession, fqTable: String, branch: String): Option[SnapshotRef] = {
    val t = load(spark, fqTable)
    Option(t.refs().get(branch)).map { ref =>
      val s = t.snapshot(ref.snapshotId())
      SnapshotRef(s.snapshotId(), Instant.ofEpochMilli(s.timestampMillis()))
    }
  }

  def snapshotExists(spark: SparkSession, fqTable: String, snapshotId: Long): Boolean =
    load(spark, fqTable).snapshot(snapshotId) != null

  def branches(spark: SparkSession, fqTable: String): Set[String] =
    load(spark, fqTable).refs().asScala.collect { case (name, ref) if ref.isBranch => name }.toSet

  /**
   * Rows appended to an append-only table after `fromExclusive` up to and including `toInclusive`.
   * This is Iceberg's native incremental read: it reads only the data files added by those
   * snapshots, so the cost is proportional to new data, not to table size.
   */
  def readAppendsBetween(spark: SparkSession, fqTable: String, fromExclusive: Option[Long], toInclusive: Long): DataFrame =
    fromExclusive match {
      case Some(from) if from == toInclusive => spark.table(fqTable).limit(0)
      case Some(from) =>
        spark.read
          .format("iceberg")
          .option("start-snapshot-id", from.toString)
          .option("end-snapshot-id", toInclusive.toString)
          .load(fqTable)
      case None =>
        spark.read.format("iceberg").option("snapshot-id", toInclusive.toString).load(fqTable)
    }

  // ---------------------------------------------------------------------------------------------
  // Branching: the building block for write-audit-publish and for safe backfills.
  // ---------------------------------------------------------------------------------------------

  def createBranch(spark: SparkSession, fqTable: String, branch: String): Unit = {
    if (!branches(spark, fqTable).contains(branch)) {
      spark.sql(s"ALTER TABLE $fqTable CREATE BRANCH `$branch`")
      Log.info("branch_created", "table" -> fqTable, "branch" -> branch)
    }
  }

  def dropBranch(spark: SparkSession, fqTable: String, branch: String): Unit =
    if (branches(spark, fqTable).contains(branch)) spark.sql(s"ALTER TABLE $fqTable DROP BRANCH IF EXISTS `$branch`")

  /**
   * Publish: move `main` to the head of `branch`. Only legal when main has not moved since the
   * branch was created (main is an ancestor of the branch). If someone else committed to main
   * in the meantime this fails loudly instead of silently discarding their commit.
   */
  def fastForward(spark: SparkSession, catalog: String, table: String, branch: String): Unit = {
    spark.sql(s"CALL $catalog.system.fast_forward(table => '$table', branch => 'main', to => '$branch')").collect()
    Log.info("branch_published", "table" -> table, "branch" -> branch)
  }

  /** Has main moved since the branch was cut? (i.e. would a fast-forward be rejected?) */
  def mainMovedSince(spark: SparkSession, fqTable: String, branch: String): Boolean = {
    val t = load(spark, fqTable)
    (Option(t.refs().get("main")), Option(t.refs().get(branch))) match {
      case (Some(main), Some(br)) =>
        // main is fine if it is an ancestor of (or equal to) the branch head
        !org.apache.iceberg.util.SnapshotUtil.isAncestorOf(t, br.snapshotId(), main.snapshotId())
      case _ => false
    }
  }

  /** Run `body` with all writes to WAP-enabled tables (and reads of tables that have the branch) on `branch`. */
  def withWapBranch[T](spark: SparkSession, branch: String)(body: => T): T = {
    val previous = Try(spark.conf.get("spark.wap.branch")).toOption
    spark.conf.set("spark.wap.branch", branch)
    try body
    finally previous match {
      case Some(p) => spark.conf.set("spark.wap.branch", p)
      case None    => spark.conf.unset("spark.wap.branch")
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Table maintenance (small files, snapshot expiry, orphans, manifests).
  // ---------------------------------------------------------------------------------------------

  def compact(spark: SparkSession, catalog: String, table: String, strategy: String = "binpack"): DataFrame =
    spark.sql(
      s"""CALL $catalog.system.rewrite_data_files(
         |  table => '$table', strategy => '$strategy',
         |  options => map('min-input-files', '2', 'target-file-size-bytes', '134217728'))""".stripMargin)

  def expireSnapshots(spark: SparkSession, catalog: String, table: String, olderThan: Instant, retainLast: Int): DataFrame =
    spark.sql(
      s"""CALL $catalog.system.expire_snapshots(
         |  table => '$table', older_than => TIMESTAMP '${olderThan.toString.replace("T", " ").stripSuffix("Z")}',
         |  retain_last => $retainLast)""".stripMargin)

  def rewriteManifests(spark: SparkSession, catalog: String, table: String): DataFrame =
    spark.sql(s"CALL $catalog.system.rewrite_manifests(table => '$table')")

  def removeOrphans(spark: SparkSession, catalog: String, table: String, olderThan: Instant): DataFrame =
    spark.sql(
      s"""CALL $catalog.system.remove_orphan_files(
         |  table => '$table', older_than => TIMESTAMP '${olderThan.toString.replace("T", " ").stripSuffix("Z")}')""".stripMargin)
}
