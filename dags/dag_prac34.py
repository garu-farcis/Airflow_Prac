"""Explain and implement how you'd backfill this DAG for the full date range present
   in order_date (Jan–Jul 2025) if it were scheduled daily, without manually
   triggering 200+ runs. Cover catchup, schedule_interval, and start_date interactions."""
from airflow.sdk import task,DAG,TaskGroup
from airflow.providers.standard.operators.python import BranchPythonOperator,PythonOperator,get_current_context
import pandas as pd
import datetime

file_path="/Users/prse/PycharmProjects/Airflow_Prac/data/orders_sample.csv"

@DAG(
    dag_id="backup_fill",
    start_date=datetime.date(2025,1,1),
    schedule="0 9 * * *",
    catchup=True,
    tags=["orders"],
)
def my_dag():
    @task
    def back_fill_check(**context):
        df=pd.read_csv(file_path)
        df["order_date"]=pd.to_datetime(df["order_date"]).dt.date
        run_date=context["logical_date"].date
        orders_for_day = df[df["order_date"] == run_date]
        return orders_for_day
    task1=back_fill_check()
my_dag()