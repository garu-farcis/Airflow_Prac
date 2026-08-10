"""Implement error handling:
    - A PythonOperator that deliberately fails if any order has quantity > 15
    - Use on_failure_callback to send a custom log message / print
    - Also configure retries=2 and retry_delay=timedelta(minutes=1)"""

from datetime import datetime,timedelta

import pandas as pd


from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator,ShortCircuitOperator,get_current_context
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import TaskGroup
def checks():
    df=pd.read_csv("/Users/prse/PycharmProjects/Airflow_Prac/data/sales_data.csv")
    my_check=(df["quantity"]>15).sum()
    if my_check:
        raise ValueError("task unsuccessful")
    else:
        print("task successful")
def failure_callback(context):
    task =context["task instance"]
    print(f"task failed is {task.task_id}")
    print(f"dag id is {task.dag_id}")

with DAG(
    dag_id="implement_error_handling",
    start_date=datetime(2026,7,8),
    schedule="* * * * *",
    catchup=False,
    tags=["sales"],
)as dag:
    check_failure=PythonOperator(
        task_id="check_failure",
        python_callable=checks,
        on_failure_callback=failure_callback,
        retries=2,
        retry_delay=timedelta(minutes=1)
    )