"""Create a DAG that demonstrates pool usage:
    - Define a pool with 2 slots
    - Create 4 parallel tasks that each process one region (North, South, East, West)
    - Observe that only 2 run at the same time."""

from datetime import datetime
import pandas as pd

from airflow.sdk import DAG, task
from airflow.providers.standard.operators.python import PythonOperator,get_current_context

from datetime import datetime
import time

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


def north():
    print("Task 1 started")
    time.sleep(30)
    print("Task 1 finished")


def south():
    print("Task 2 started")
    time.sleep(30)
    print("Task 2 finished")


def east():
    print("Task 3 started")
    time.sleep(30)
    print("Task 3 finished")


def west():
    print("Task 4 started")
    time.sleep(30)
    print("Task 4 finished")


with DAG(
    dag_id="pool_demo",
    start_date=datetime(2026, 8, 10),
    schedule=None,
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id="task_1",
        python_callable=north,
        pool="database_pool",
    )

    t2 = PythonOperator(
        task_id="task_2",
        python_callable=south,
        pool="database_pool",
    )

    t3 = PythonOperator(
        task_id="task_3",
        python_callable=east,
        pool="database_pool",
    )

    t4 = PythonOperator(
        task_id="task_4",
        python_callable=west,
        pool="database_pool",
    )

    [t1, t2, t3, t4]