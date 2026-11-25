package com.ahmedmasood.deadlock.core

import org.apache.spark.sql.SparkSession

/** Builds the SparkSession with the Iceberg catalog and the tuning every job relies on. */
object SparkSessionFactory {

  def build(cfg: PipelineConfig, appName: String, master: Option[String] = sys.env.get("SPARK_MASTER")): SparkSession = {
    val c = cfg.catalog
    val catalogPrefix = s"spark.sql.catalog.${c.name}"

    val builder = SparkSession
      .builder()
      .appName(appName)
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config(catalogPrefix, "org.apache.iceberg.spark.SparkCatalog")
      .config(s"$catalogPrefix.type", c.catalogType)
      .config(s"$catalogPrefix.warehouse", c.warehouse)
      .config("spark.sql.defaultCatalog", c.name)
      // Adaptive Query Execution: coalesces small shuffle partitions, switches to broadcast joins
      // at runtime and splits skewed partitions (popular heroes/items produce real skew).
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .config("spark.sql.adaptive.skewJoin.enabled", "true")
      .config("spark.sql.shuffle.partitions", cfg.spark.shufflePartitions.toString)
      .config("spark.sql.autoBroadcastJoinThreshold", (32L * 1024 * 1024).toString)
      // Everything is UTC end to end; no local-time surprises in partition boundaries.
      .config("spark.sql.session.timeZone", "UTC")
      // Overwrites replace only the partitions present in the new data.
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
      .config("spark.ui.showConsoleProgress", "false")

    c.uri.foreach(u => builder.config(s"$catalogPrefix.uri", u))
    master.orElse(if (sys.props.contains("spark.master")) None else Some("local[*]")).foreach(builder.master)

    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(sys.env.getOrElse("SPARK_LOG_LEVEL", "WARN"))
    spark
  }
}
