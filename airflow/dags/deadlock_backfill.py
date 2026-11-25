"""
Manually triggered backfill / repair.

Trigger with a config such as:
    {"start_at": "gold.dim_item", "from": "2026-10-01", "to": "2026-10-31", "publish": false}

Flow:
    plan (dry run, logged for the reviewer)
      -> rebuild in a dedicated Iceberg branch + DQ + main-vs-branch diff (ops.backfill_diffs)
      -> [publish=false] stop: a human reviews the diff, then triggers again with publish=true,
         or runs `publish --branch backfill_<run_id>`
Long ranges are split into monthly chunks so each chunk is a small, independently retryable unit.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import DAG, Param, task

JAR = "{{ var.value.get('deadlock_jar', 's3://my-artifacts/deadlock-lakehouse/deadlock-lakehouse.jar') }}"
CONFIG = "{{ var.value.get('deadlock_config', 'conf/pipeline.yaml') }}"

with DAG(
    dag_id="deadlock_lakehouse_backfill",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "start_at": Param("gold.fct_player_match", type="string", description="Job or table to rebuild from (downstream included)"),
        "from": Param("2026-10-01", type="string", format="date"),
        "to": Param("2026-10-31", type="string", format="date"),
        "publish": Param(False, type="boolean", description="Cut over automatically if the audit passes"),
    },
    tags=["deadlock", "backfill"],
) as dag:

    @task
    def monthly_chunks(**context) -> list[list[str]]:
        p = context["params"]
        start, end = date.fromisoformat(p["from"]), date.fromisoformat(p["to"])
        chunks, cur = [], start
        while cur <= end:
            nxt = (cur.replace(day=1) + timedelta(days=32)).replace(day=1)
            chunk_end = min(end, nxt - timedelta(days=1))
            chunk = ["backfill", "--start-at", p["start_at"], "--from", cur.isoformat(), "--to", chunk_end.isoformat(), "--config", CONFIG]
            if not p["publish"]:
                chunk.append("--no-publish")
            chunks.append(chunk)
            cur = chunk_end + timedelta(days=1)
        return chunks

    plan = SparkSubmitOperator(
        task_id="plan",
        application=JAR,
        java_class="com.ahmedmasood.deadlock.Main",
        application_args=["backfill", "--start-at", "{{ params.start_at }}", "--from", "{{ params.from }}",
                          "--to", "{{ params.to }}", "--dry-run", "--config", CONFIG],
        conn_id="spark_default",
    )

    # One mapped task per month, run sequentially (max_active_tis_per_dag=1): chunks never race on main.
    rebuild = SparkSubmitOperator.partial(
        task_id="rebuild_chunk",
        application=JAR,
        java_class="com.ahmedmasood.deadlock.Main",
        conn_id="spark_default",
        max_active_tis_per_dag=1,
        retries=1,
    ).expand(application_args=monthly_chunks())

    plan >> rebuild
