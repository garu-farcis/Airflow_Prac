from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

def push_data(ti):
    ti.xcom_push(key="message", value="Hello XCom")

def pull_data(ti):
    value = ti.xcom_pull(task_ids="push_task", key="message")
    print(value)

with DAG(
    dag_id="xcom_example",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    push = PythonOperator(
        task_id="push_task",
        python_callable=push_data
    )

    pull = PythonOperator(
        task_id="pull_task",
        python_callable=pull_data
    )

    push >> pull
