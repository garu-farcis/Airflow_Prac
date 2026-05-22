from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="dynamic_tasks",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    for i in range(5):
        BashOperator(
            task_id=f"task_{i}",
            bash_command=f"echo Task {i}"
        )