"""Use BranchPythonOperator to route the pipeline down two paths depending on
   whether today's order count is above or below a threshold (e.g. 50 orders/day) —
   one path triggers a "high volume alert" task, the other proceeds normally.
   Explain how trigger_rule affects the downstream join task."""

from airflow.sdk import task,DAG,TaskGroup
from airflow.providers.standard.operators.python import BranchPythonOperator,PythonOperator,get_current_context
import pandas as pd
import datetime

file_path="/Users/prse/PycharmProjects/Airflow_Prac/data/orders_sample.csv"

def choice():
    df=pd.read_csv(file_path)
    # order_by_day=df.groupby("order_date")["quantity"].value_counts()
    today=datetime.datetime.today().date()
    df["order_date"]=pd.to_datetime(df["order_date"]).dt.date
    order_by_day=df.loc[df["order_date"]==today,"quantity"].sum()
    if order_by_day>3:
        return "high_volume_alert"
    else:
        return "normal_task"
def high_volume_alert():
    print("vol below threshold")

def normal_task():
    print("normally proceeding")

with DAG(
    dag_id="branch_check",
    start_date=datetime.datetime(2026,8,19),
    schedule="0 9 * * 4",
    catchup=False,
    tags=["orders"],
)as dag:
    choice_data=BranchPythonOperator(
        task_id="choice_data",
        python_callable=choice,
    )
    hv_alert=PythonOperator(
        task_id="hv_alert",
        python_callable=high_volume_alert,
    )
    n_task=PythonOperator(
        task_id="n_task",
        python_callable=normal_task,
    )

    choice_data>>[hv_alert,n_task]