from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

def fail_task():
    raise Exception("Task failed!")

with DAG(
    dag_id="retry_example",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=1)
    }
) as dag:

    task = PythonOperator(
        task_id="failing_task",
        python_callable=fail_task
    )