package com.ahmedmasood.deadlock

import com.ahmedmasood.deadlock.core.{Job, JobContext, JobResult}
import com.ahmedmasood.deadlock.orchestration.{DependencyGraph, JobRegistry}
import org.scalatest.funsuite.AnyFunSuite

class DependencyGraphSpec extends AnyFunSuite {
  private lazy val graph = JobRegistry.all(SharedSpark.config)
  private def names(js: Seq[Job]) = js.map(_.name)

  test("topological order puts every job after the jobs it reads from") {
    val order = names(graph.topologicalOrder())
    graph.jobs.foreach { j =>
      graph.upstream(j).foreach(u => assert(order.indexOf(u.name) < order.indexOf(j.name), s"${u.name} must run before ${j.name}"))
    }
  }

  test("impact analysis: changing dim_item affects purchases, economy and marts, not player-match facts") {
    val impacted = names(graph.downstreamClosure(graph.job("dim_item")))
    assert(impacted == Seq("dim_item", "fct_item_purchase", "fct_player_economy", "marts"))
  }

  test("jobs can be resolved by the table they produce") {
    assert(graph.resolve("gold.dim_hero").name == "dim_hero")
    assert(graph.resolve("silver.match_players").name == "silver_matches")
  }

  test("a dependency cycle is rejected") {
    def job(n: String, in: Seq[String], out: Seq[String]): Job = new Job {
      val name = n; val inputs = in; val outputs = out
      def run(ctx: JobContext) = JobResult()
    }
    val cyclic = new DependencyGraph(Seq(job("a", Seq("t2"), Seq("t1")), job("b", Seq("t1"), Seq("t2"))))
    val e = intercept[IllegalArgumentException](cyclic.topologicalOrder())
    assert(e.getMessage.contains("cycle"))
  }

  test("two jobs may not write the same table") {
    def job(n: String): Job = new Job {
      val name = n; val inputs = Nil; val outputs = Seq("t")
      def run(ctx: JobContext) = JobResult()
    }
    intercept[IllegalArgumentException](new DependencyGraph(Seq(job("a"), job("b"))))
  }

  test("the lineage diagram is generated from the code") {
    val mermaid = graph.toMermaid
    assert(mermaid.startsWith("flowchart LR"))
    assert(mermaid.contains("gold.fct_item_purchase"))
  }
}
