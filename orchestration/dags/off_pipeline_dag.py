"""Daily Airflow DAG: extract -> bronze -> silver -> gold.

Each task calls the same CLI that is used locally, so the pipeline logic stays
in the Python package and Airflow only handles scheduling, ordering and retries.
Requires the package to be installed in the Airflow worker environment.
"""

from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

CLI = "python -m off_pipeline --steps"

with DAG(
    dag_id="off_lakehouse_pipeline",
    start_date=datetime(2026, 10, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,  # two runs must never MERGE into Silver at the same time
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["open-food-facts", "lakehouse"],
) as dag:
    extract = BashOperator(task_id="extract", bash_command=f"{CLI} extract --max-pages 20")
    bronze = BashOperator(task_id="bronze", bash_command=f"{CLI} bronze")
    silver = BashOperator(task_id="silver", bash_command=f"{CLI} silver")
    gold = BashOperator(task_id="gold", bash_command=f"{CLI} gold")

    extract >> bronze >> silver >> gold
