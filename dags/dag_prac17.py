"""Use BranchPythonOperator:
   - Count how many tickets have priority = "Critical".
   - If Critical tickets >= 5 → branch to task "notify_manager"
   - Else → branch to task "generate_daily_summary"
   Implement both downstream tasks (they can just print a message)."""

from datetime import datetime
import pandas as pd

from airflow.sdk import DAG, task
from airflow.providers.standard.operators.python import PythonOperator,get_current_context,BranchPythonOperator

file_path="/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets-Table 1.csv"

def check_status():
    df=pd.read_csv(file_path)
    ticket_count=(df["priority"]=="Critical").sum()
    if ticket_count>=5:
        return "notify_manager"
    else:
        return "generate_daily_summary"

def notify_manager():
    print("tickets counts are more than 5")

def generate_daily_summary():
    print("tickets counts are less than 5")

with DAG(
    dag_id="check_ticket_status",
    start_date=datetime(2026,8,3),
    schedule="0 0 * * *",
    catchup=False,
    tags=["support"],
)as dag:
    check_stats=BranchPythonOperator(
        task_id="check_ticket",
        python_callable=check_status,

    )
    print_info=PythonOperator(
        task_id="notify_manager",
        python_callable=notify_manager,
    )
    gen_sum=PythonOperator(
        task_id="generate_daily_summary",
        python_callable=generate_daily_summary,
    )

    check_stats>>[print_info,gen_sum]