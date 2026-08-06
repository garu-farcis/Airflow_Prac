"""Build a BranchPythonOperator that checks the total number of "Pending" orders.
   - If Pending orders > 2 → go to task "send_alert"
   - Else → go to task "generate_report"
   Implement both downstream tasks (they can just print a message)."""

from airflow import DAG
from  airflow.providers.standard.operators.python import BranchPythonOperator,get_current_context,PythonOperator
from datetime import datetime
import pandas as pd

def check_pending():
    df=pd.read_csv("/Users/prse/PycharmProjects/Airflow_Prac/data/sales_data.csv")
    pending_orders = (df["status"] == "Pending").sum()
    if pending_orders>2:
        return "send_alert"
    else:
        return "generate_report"

def send_alert():
    print("Too many pending orders! Sending alert...")


def generate_report():
    print(" Pending orders are within limit. Generating report...")

with DAG(
    dag_id="check_pending_orders",
    start_date=datetime(2026,8,6),
    schedule="* * * * *",
    tags=["sales"],
    catchup=False,
)as dag:
    check_pending_orders=BranchPythonOperator(
        task_id="check_pending",
        python_callable=check_pending,
    )
    send_alerts=PythonOperator(
        task_id="send_alert",
        python_callable=send_alert,
    )
    generate_reports=PythonOperator(
        task_id="generate_report",
        python_callable=generate_report,
    )

    check_pending_orders>> [send_alerts,generate_reports]
