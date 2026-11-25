package com.ahmedmasood.deadlock

import java.nio.file.Files

import com.ahmedmasood.deadlock.core._
import org.apache.spark.sql.SparkSession

/** One local SparkSession + Iceberg warehouse per test JVM (suites run serially). */
object SharedSpark {
  lazy val tmpRoot: java.io.File = {
    val d = Files.createTempDirectory("deadlock-lakehouse-test").toFile
    d.deleteOnExit()
    d
  }

  lazy val config: PipelineConfig = {
    val base = PipelineConfig.load("conf/pipeline.yaml")
    base.copy(
      catalog = base.catalog.copy(warehouse = new java.io.File(tmpRoot, "warehouse").getAbsolutePath),
      landingDir = new java.io.File(tmpRoot, "landing").getAbsolutePath,
      contractsDir = new java.io.File("contracts").getAbsolutePath,
      spark = SparkTuning(shufflePartitions = 2))
  }

  lazy val spark: SparkSession = {
    java.util.TimeZone.setDefault(java.util.TimeZone.getTimeZone("UTC"))
    SparkSessionFactory.build(config, "deadlock-lakehouse-tests", master = Some("local[2]"))
  }

  def ctx(window: RunWindow = RunWindow.Incremental): JobContext =
    JobContext(spark, config, JobContext.newRunId("test"), window)
}
