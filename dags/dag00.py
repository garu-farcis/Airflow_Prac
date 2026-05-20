from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="my_first_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",          # or None for manual only
    catchup=False,
) as dag:

    task1 = BashOperator(
        task_id="print_hello",
        bash_command="echo 'Hello from Airflow on Mac! 🚀'",
    )

    task1