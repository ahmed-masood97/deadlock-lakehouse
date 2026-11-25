"""DAG integrity: every DAG imports, and the task graph has the shape the runbook describes."""
from pathlib import Path

import pytest
from airflow.models.dagbag import DagBag

DAG_DIR = Path(__file__).resolve().parents[1] / "dags"


@pytest.fixture(scope="module")
def bag() -> DagBag:
    return DagBag(dag_folder=str(DAG_DIR))


def test_no_import_errors(bag):
    assert bag.import_errors == {}


def test_daily_extracts_fan_in_to_one_spark_run(bag):
    dag = bag.dags["deadlock_lakehouse_daily"]
    daily = dag.get_task("daily_bronze_silver_gold")
    assert {t.task_id for t in daily.upstream_list} == {f"extract_{s}" for s in ["matches", "heroes", "items", "patches"]}
    assert dag.max_active_runs == 1, "WAP publish needs a single writer"


def test_backfill_is_manual_and_serial(bag):
    dag = bag.dags["deadlock_lakehouse_backfill"]
    assert dag.schedule is None or str(dag.timetable.summary) in ("None", "")
    assert dag.max_active_runs == 1
