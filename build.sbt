// Deadlock Lakehouse — Scala / Spark / Iceberg data platform for the game Deadlock.
//
// Versions are pinned to the most common production combination in 2026:
// Spark 3.5 (Scala 2.12) + Apache Iceberg 1.10. Spark is "provided" because a
// cluster (EMR, Dataproc, Kubernetes, Databricks) supplies it at runtime.

ThisBuild / organization := "com.ahmedmasood"
ThisBuild / version      := "0.1.0"
ThisBuild / scalaVersion := "2.12.18"

val sparkVersion   = "3.5.10"
val icebergVersion = "1.10.2"

lazy val root = (project in file("."))
  .settings(
    name := "deadlock-lakehouse",
    libraryDependencies ++= Seq(
      "org.apache.spark"  %% "spark-sql"                      % sparkVersion % Provided,
      "org.apache.iceberg" % "iceberg-spark-runtime-3.5_2.12" % icebergVersion,
      // Jackson YAML + Scala module ship with Spark; declared for clarity, provided at runtime.
      "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % "2.15.2" % Provided,
      "com.fasterxml.jackson.module"    %% "jackson-module-scala"    % "2.15.2" % Provided,
      "org.scalatest" %% "scalatest" % "3.2.19" % Test
    ),
    scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-Xlint:-unused,_"),

    // Tests share one local SparkSession per suite; run suites serially in a forked JVM.
    Test / fork := true,
    Test / parallelExecution := false,
    Test / javaOptions ++= Seq(
      "-Xmx2g",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
    ),

    // `sbt run` should include provided deps so the pipeline runs locally.
    Compile / run := Defaults.runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner).evaluated,
    Compile / run / fork := true,
    Compile / run / javaOptions ++= (Test / javaOptions).value,
    Compile / mainClass := Some("com.ahmedmasood.deadlock.Main"),

    // Fat jar for spark-submit (Spark itself excluded because it is Provided).
    assembly / assemblyJarName := "deadlock-lakehouse.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _*) => MergeStrategy.concat
      case PathList("META-INF", _*)             => MergeStrategy.discard
      case "reference.conf"                     => MergeStrategy.concat
      case _                                    => MergeStrategy.first
    }
  )
