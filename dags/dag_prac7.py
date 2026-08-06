"""Use the ShortCircuitOperator to skip the rest of the DAG if there are zero
   "Cancelled" orders in the file. Otherwise continue to a "process_cancellations" task."""

from datetime import datetime

import pandas as pd

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator,ShortCircuitOperator
from airflow.sdk import TaskGroup

def check_order():
    df=pd.read_csv("/Users/prse/PycharmProjects/Airflow_Prac/data/sales_data.csv")
    check=(df["status"]=="Cancelled").sum()
    return check>0
def process_cancellations():
    print("process has been cancelled")

with DAG(
    dag_id="check_order_cancellation",
    start_date=datetime(2026,7,6),
    schedule="* * * * *",
    catchup=False,
    tags=["sales"],
)as dag:
    checking_order=ShortCircuitOperator(
        task_id="check_order",
        python_callable=check_order,
    )
    cancel_process=PythonOperator(
        task_id="cancel_order",
        python_callable=process_cancellations,
    )

    checking_order>>cancel_process

