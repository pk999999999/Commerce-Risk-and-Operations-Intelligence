"""Rebuild derived facts and search from replayable raw Parquet."""
from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="daily_reconciliation",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["commerce", "quality"],
) as dag:
    reconcile = BashOperator(
        task_id="replay_and_publish",
        bash_command="python /opt/project/jobs/reconcile.py",
        execution_timeout=None,
    )
