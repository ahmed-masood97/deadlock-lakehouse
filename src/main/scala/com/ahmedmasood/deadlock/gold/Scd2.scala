package com.ahmedmasood.deadlock.gold

import com.ahmedmasood.deadlock.core.Model
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Describes an SCD Type 2 dimension.
 *
 * @param naturalKey  business key from the source (hero_id, item_id)
 * @param surrogateKey column holding the surrogate key
 * @param tracked     attributes whose change opens a new version
 * @param attributes  every business column stored on the dimension (tracked or not)
 */
final case class Scd2Spec(table: String, naturalKey: String, surrogateKey: String, tracked: Seq[String], attributes: Seq[String]) {
  require(tracked.forall(attributes.contains), "tracked columns must be attributes")
  def allColumns: Seq[String] = Seq(surrogateKey, naturalKey) ++ attributes ++ Seq("row_hash", "valid_from", "valid_to", "is_current", "_run_id")
}

/** What applying one snapshot will do. Pure DataFrame logic, unit-testable without Iceberg. */
final case class Scd2Plan(staged: DataFrame, newKeys: Long, changedKeys: Long, unchanged: Long, staleSkipped: Long)

/**
 * Type 2 slowly changing dimension maintenance, applied snapshot by snapshot.
 *
 * Design choices worth calling out:
 *  - Change detection by a hash of the tracked attributes (one comparison, NULL-safe).
 *  - Surrogate key = xxhash64(natural key, valid_from): deterministic, so rebuilding a dimension
 *    from bronze reproduces the same keys and existing facts stay joinable.
 *  - The first version of a key is valid from the beginning of time. When a fact arrives before
 *    its dimension row (late-arriving dimension) it is assigned the unknown member, and a re-run
 *    or backfill resolves it once the dimension row exists.
 *  - Open-ended versions use a far-future valid_to, so point-in-time joins are a plain
 *    `ts >= valid_from AND ts < valid_to` with no NULL handling.
 *  - A snapshot older than the current version is ignored (out-of-order delivery guard).
 *  - One MERGE per snapshot: expiring the old version and inserting the new one is atomic.
 */
object Scd2 {

  def rowHash(cols: Seq[String]): Column =
    sha2(concat_ws("||", cols.map(c => coalesce(col(c).cast("string"), lit("<null>"))): _*), 256)

  /**
   * @param current  the dimension table (all versions)
   * @param incoming one snapshot: natural key + attributes + `effective_from` timestamp
   */
  def plan(spec: Scd2Spec, current: DataFrame, incoming: DataFrame, runId: String): Scd2Plan = {
    val key = spec.naturalKey
    val in = incoming.withColumn("row_hash", rowHash(spec.tracked))
    val cur = current.where(col("is_current"))
      .select(col(key).as("_cur_key"), col("row_hash").as("_cur_hash"), col("valid_from").as("_cur_from"))

    val joined = in.join(cur, in(key) === cur("_cur_key"), "left").cache()
    val isNew = col("_cur_key").isNull
    val isStale = !isNew && col("effective_from") <= col("_cur_from")
    val isChanged = !isNew && !isStale && col("_cur_hash") =!= col("row_hash")

    def version(validFrom: Column): Seq[Column] =
      Seq(xxhash64(col(key), validFrom).as(spec.surrogateKey), col(key)) ++ spec.attributes.map(col) ++
        Seq(col("row_hash"), validFrom.as("valid_from"), to_timestamp(lit(Model.OpenEnd)).as("valid_to"),
          lit(true).as("is_current"), lit(runId).as("_run_id"))

    // New keys: one insert, valid since the beginning of time.
    val inserts = joined.where(isNew)
      .select((lit(null).cast(incoming.schema(key).dataType).as("merge_key") +: version(to_timestamp(lit(Model.BeginningOfTime)))): _*)
    // Changed keys: one row that expires the current version (matched on merge_key) ...
    val expires = joined.where(isChanged)
      .select((col(key).as("merge_key") +: version(col("effective_from"))): _*)
    // ... and one row that inserts the new version (merge_key NULL never matches).
    val newVersions = joined.where(isChanged)
      .select((lit(null).cast(incoming.schema(key).dataType).as("merge_key") +: version(col("effective_from"))): _*)

    val counts = joined.select(
      sum(when(isNew, 1).otherwise(0)).as("n"), sum(when(isChanged, 1).otherwise(0)).as("c"),
      sum(when(isStale, 1).otherwise(0)).as("s"), count(lit(1)).as("t")).head()
    val (n, c, s, t) = (counts.getLong(0), counts.getLong(1), counts.getLong(2), counts.getLong(3))
    // Materialise the staged rows before the MERGE changes the table they were derived from.
    val staged = inserts.unionByName(expires).unionByName(newVersions).localCheckpoint(eager = true)
    joined.unpersist()
    Scd2Plan(staged, n, c, t - n - c - s, s)
  }

  /** Applies a plan with a single atomic MERGE. */
  def apply(spark: SparkSession, spec: Scd2Spec, fqTable: String, plan: Scd2Plan): Unit = {
    if (plan.newKeys + plan.changedKeys == 0) return
    plan.staged.createOrReplaceTempView("scd2_staged")
    val cols = spec.allColumns
    spark.sql(
      s"""MERGE INTO $fqTable t
         |USING scd2_staged s
         |ON t.${spec.naturalKey} = s.merge_key AND t.is_current = true
         |WHEN MATCHED THEN UPDATE SET t.valid_to = s.valid_from, t.is_current = false, t._run_id = s._run_id
         |WHEN NOT MATCHED THEN INSERT (${cols.mkString(", ")}) VALUES (${cols.map("s." + _).mkString(", ")})""".stripMargin)
  }
}
