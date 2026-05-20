from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="list_files",
    start_date=datetime.now(),
    schedule="@daily",
    catchup=False
)as dag:
    task=BashOperator(
        task_id="listing_files",
        bash_command="ls -l"
    )