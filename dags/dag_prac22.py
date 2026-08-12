"""Use ShortCircuitOperator:
   - Check if there are any tickets with status = "Escalated"
   - If zero Escalated tickets → skip the rest of the DAG
   - If at least one exists → continue to a "handle_escalations" task"""
from datetime import datetime
import pandas as pd

from airflow.sdk import DAG, task,TaskGroup
from airflow.providers.standard.operators.python import PythonOperator, get_current_context, BranchPythonOperator, \
    ShortCircuitOperator

file_path="/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets-Table 1.csv"
path="/Users/prse/PycharmProjects/Airflow_Prac/data/"

def check_status():
    df=pd.read_csv(file_path)
    esc_ticks=df[df["status"]=="Escalated"].value_counts().sum()
    return esc_ticks>0

def handle_escalation():
    # data=context["ti"].xcom_pull(task_ids="check_status")
    print("escalations has been handled")

with DAG(
    dag_id="short_ops",
    start_date=datetime(2026,9,10),
    schedule="* * * * * ",
    catchup=False,
    tags=["support"],
)as dag:
    check_ops=ShortCircuitOperator(
        task_id="check_status",
        python_callable=check_status,
    )
    handle_ops=PythonOperator(
        task_id="han_ops",
        python_callable=handle_escalation,
    )

    check_ops>>handle_ops