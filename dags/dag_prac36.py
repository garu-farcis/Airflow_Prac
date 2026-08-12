"""Your CSV load task occasionally times out reading the file. Configure execution_timeout,
   retries, retry_exponential_backoff, and a max_retry_delay for that task, and
   explain the difference between execution_timeout and dagrun_timeout.
"""
from airflow.sdk import task,DAG,TaskGroup
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator, get_current_context, \
    ShortCircuitOperator
import pandas as pd
import datetime
from datetime import timedelta


file_path="/Users/prse/PycharmProjects/Airflow_Prac/data/orders_sample.csv"
def timeout_check():
    df=pd.read_csv(file_path)
    print(df)
with DAG(
    dag_id="timeout_check",
    start_date=datetime.date(2026,8,10),
    schedule="0 9 * * *",
    catchup=False,
    tags=["orders"],
)as dag:
    time_check=PythonOperator(
        task_id="timeout_check",
        python_callable=timeout_check,
        execution_timeout=timedelta(seconds=60),
        retries=3,
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(seconds=60),
    )