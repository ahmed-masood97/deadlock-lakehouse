package com.ahmedmasood.deadlock.orchestration

import com.ahmedmasood.deadlock.core.{Job, PipelineConfig, Tables}
import com.ahmedmasood.deadlock.gold._
import com.ahmedmasood.deadlock.ingest.BronzeIngestJob
import com.ahmedmasood.deadlock.silver.SilverMatchesJob

/**
 * The job DAG, derived from what each job declares it reads and writes.
 *
 * Job B depends on job A when B reads a table A writes. From that one fact we get:
 *  - execution order (topological sort) for the daily run
 *  - "everything downstream of X" for backfills and migrations (impact analysis)
 *  - a lineage diagram that cannot drift from the code
 */
final class DependencyGraph(val jobs: Seq[Job]) {
  require(jobs.map(_.name).distinct.size == jobs.size, "job names must be unique")

  private val producers: Map[String, Job] = {
    val pairs = jobs.flatMap(j => j.outputs.map(_ -> j))
    val dup = pairs.groupBy(_._1).collect { case (t, ps) if ps.size > 1 => t }
    require(dup.isEmpty, s"tables written by more than one job: ${dup.mkString(", ")}")
    pairs.toMap
  }

  /** Upstream jobs of `job` (jobs producing a table it reads). */
  def upstream(job: Job): Seq[Job] = job.inputs.flatMap(producers.get).distinct.filterNot(_ == job)

  def downstream(job: Job): Seq[Job] = jobs.filter(j => j != job && upstream(j).contains(job))

  def job(name: String): Job = jobs.find(_.name == name).getOrElse(throw new IllegalArgumentException(
    s"Unknown job '$name'. Known: ${jobs.map(_.name).mkString(", ")}"))

  /** Resolve a job name or a table name (the job producing it). */
  def resolve(jobOrTable: String): Job =
    jobs.find(_.name == jobOrTable).orElse(producers.get(jobOrTable)).getOrElse(job(jobOrTable))

  /** Kahn's algorithm; ties broken by declaration order so runs are deterministic. Fails on cycles. */
  def topologicalOrder(subset: Seq[Job] = jobs): Seq[Job] = {
    val set = subset.toSet
    var remaining = subset
    val ordered = Seq.newBuilder[Job]
    var done = Set.empty[Job]
    while (remaining.nonEmpty) {
      val ready = remaining.filter(j => upstream(j).filter(set.contains).forall(done.contains))
      require(ready.nonEmpty, s"dependency cycle among: ${remaining.map(_.name).mkString(", ")}")
      ordered ++= ready
      done ++= ready
      remaining = remaining.filterNot(ready.contains)
    }
    ordered.result()
  }

  /** `start` and every job transitively downstream of it, in execution order. */
  def downstreamClosure(start: Job): Seq[Job] = {
    def visit(j: Job, seen: Set[Job]): Set[Job] =
      downstream(j).foldLeft(seen + j)((acc, d) => if (acc.contains(d)) acc else visit(d, acc))
    topologicalOrder(jobs.filter(visit(start, Set.empty).contains))
  }

  /** Mermaid flowchart of tables and jobs, for the README and for reviews. */
  def toMermaid: String = {
    def id(s: String) = s.replaceAll("[^A-Za-z0-9_]", "_")
    val lines = Seq.newBuilder[String]
    lines += "flowchart LR"
    jobs.foreach { j =>
      lines += s"""  ${id("job_" + j.name)}{{"${j.name}"}}"""
      j.inputs.foreach(t => lines += s"""  ${id(t)}[("$t")] --> ${id("job_" + j.name)}""")
      j.outputs.foreach(t => lines += s"""  ${id("job_" + j.name)} --> ${id(t)}[("$t")]""")
    }
    lines.result().distinct.mkString("\n")
  }
}

object JobRegistry {

  def ingestJobs(cfg: PipelineConfig): Seq[Job] = cfg.sources.map(s => new BronzeIngestJob(s))

  /** Jobs that write to gold (and therefore run inside the write-audit-publish branch). */
  def goldJobs: Seq[Job] = Seq(DimHeroJob, DimItemJob, DimPatchJob, DimPlayerJob,
    FactPlayerMatchJob, FactItemPurchaseJob, FactPlayerEconomyJob, MartsJob)

  def all(cfg: PipelineConfig): DependencyGraph =
    new DependencyGraph(ingestJobs(cfg) ++ Seq(SilverMatchesJob) ++ goldJobs)

  def isGold(job: Job): Boolean = job.outputs.forall(t => Tables.WapTables.contains(t))
}
