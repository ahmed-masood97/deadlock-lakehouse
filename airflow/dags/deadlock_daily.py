"""
Daily Deadlock lakehouse pipeline.

One Spark application per stage keeps failures isolated and retries cheap: a failed gold audit
re-runs only the gold stage, not the API pulls. All stages are idempotent, so a retry or a
manual "clear" is always safe.

    extract (per source, parallel) -> daily (bronze, silver, gold write-audit-publish) -> freshness check
    maintenance runs weekly on Sundays after the daily run.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLCheckOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.sdk import DAG, TriggerRule

# Resolved at run time through Jinja (no metadata-DB access while the scheduler parses this file).
JAR = "{{ var.value.get('deadlock_jar', 's3://my-artifacts/deadlock-lakehouse/deadlock-lakehouse.jar') }}"
CONFIG = "{{ var.value.get('deadlock_config', 'conf/pipeline.yaml') }}"
SOURCES = ["matches", "heroes", "items", "patches"]

SPARK_CONF = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.shuffle.partitions": "400",
    "spark.dynamicAllocation.enabled": "true",
    "spark.dynamicAllocation.maxExecutors": "40",
}


def notify_on_call(context):
    """Page the owner. Wire this to Slack / PagerDuty in a real deployment."""
    ti = context["task_instance"]
    print(f"[ALERT] {ti.dag_id}.{ti.task_id} failed for {context['ds']}. Runbook: docs/runbook.md")


def spark_task(task_id: str, *args: str, **kwargs) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=task_id,
        application=JAR,
        java_class="com.ahmedmasood.deadlock.Main",
        application_args=[*args, "--config", CONFIG],
        conf=SPARK_CONF,
        conn_id="spark_default",
        name=f"deadlock-{task_id}",
        **kwargs,
    )


default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "on_failure_callback": notify_on_call,
    # Data freshness (not task duration) is the real SLA: see the gold_freshness check below.
}

with DAG(
    dag_id="deadlock_lakehouse_daily",
    description="Deadlock match data: API -> bronze -> silver -> gold (WAP) -> marts",
    start_date=datetime(2026, 10, 1),
    schedule="0 6 * * *",          # after the community API's nightly aggregation
    catchup=False,                  # history is rebuilt with the backfill DAG, not catchup
    max_active_runs=1,              # one writer at a time: WAP publish requires main not to move
    default_args=default_args,
    tags=["deadlock", "lakehouse", "iceberg"],
) as dag:

    start = EmptyOperator(task_id="start")

    # Extraction is network-bound and rate-limited; one task per source so a slow source
    # does not block the others and each retries independently.
    extracts = [spark_task(f"extract_{s}", "extract", "--source", s, pool="deadlock_api") for s in SOURCES]

    daily = spark_task("daily_bronze_silver_gold", "daily", execution_timeout=timedelta(hours=2))

    # Independent freshness check on the published gold layer (outside the Spark job on purpose).
    freshness = SQLCheckOperator(
        task_id="gold_freshness",
        conn_id="lakehouse_trino",
        sql="""
            SELECT max(_loaded_at) > current_timestamp - INTERVAL '26' HOUR
            FROM lake.gold.fct_player_match
        """,
    )

    # Compaction and snapshot expiry once a week, after a successful daily run.
    is_sunday = ShortCircuitOperator(
        task_id="weekly_gate",
        python_callable=lambda ds, **_: datetime.fromisoformat(ds).weekday() == 6,
        ignore_downstream_trigger_rules=True,
    )
    maintenance = spark_task("maintenance", "maintenance", trigger_rule=TriggerRule.ALL_SUCCESS)

    start >> extracts >> daily >> freshness >> is_sunday >> maintenance
