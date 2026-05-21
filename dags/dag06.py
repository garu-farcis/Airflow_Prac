from airflow import DAG
from airflow.providers.standard.operators.python import BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime

def choose_path():
    return "task_a"

with DAG(
    dag_id="branching_example",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    branch = BranchPythonOperator(
        task_id="branch_task",
        python_callable=choose_path
    )

    task_a = EmptyOperator(task_id="task_a")
    task_b = EmptyOperator(task_id="task_b")

    branch >> [task_a, task_b]