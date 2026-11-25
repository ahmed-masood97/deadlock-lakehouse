package com.ahmedmasood.deadlock.core

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

/** Common dimensional-model expressions, defined once so every fact uses the same definitions. */
object Model {
  val OpenEnd = "9999-12-31 00:00:00"
  val BeginningOfTime = "1900-01-01 00:00:00"

  def dateKey(ts: Column): Column = date_format(ts, "yyyyMMdd").cast("int")

  /**
   * Event time from a producer string: "yyyy-MM-dd HH:mm:ss" (what the API sends), ISO-8601, or
   * unix seconds. Tolerating all three means a producer format change degrades to nothing
   * instead of NULL timestamps (which the not-null checks would still catch).
   */
  def eventTime(raw: Column): Column =
    coalesce(
      to_timestamp(raw, "yyyy-MM-dd HH:mm:ss"),
      when(raw.rlike("^[0-9]{9,10}$"), timestamp_seconds(raw.cast("long"))),
      to_timestamp(raw))

  /** Deadlock badge = tier * 10 + subtier (tier 1 Initiate ... tier 11 Eternus). */
  def badgeBucket(badge: Column): Column = {
    val tier = floor(badge / 10)
    when(badge.isNull, lit("unknown"))
      .when(tier <= 4, lit("1_low (Initiate-Arcanist)"))
      .when(tier <= 7, lit("2_mid (Ritualist-Archon)"))
      .when(tier <= 9, lit("3_high (Oracle-Phantom)"))
      .otherwise(lit("4_top (Ascendant-Eternus)"))
  }

  def soulsPerMinute(netWorth: Column, durationS: Column): Column =
    when(durationS > 0, round(netWorth / (durationS / 60.0), 2))

  def kda(k: Column, d: Column, a: Column): Column = round((k + a) / greatest(d, lit(1)), 3)
}

/** Governance helpers for personal data. */
object Pii {

  /** Salted SHA-256 of a Steam account id. Stable for joins, not reversible without the salt. */
  def playerKey(accountId: Column, salt: String): Column =
    when(accountId.isNull || accountId === 0, lit(null).cast("string"))
      .otherwise(sha2(concat(lit(salt), lit(":"), accountId.cast("string")), 256))
}
