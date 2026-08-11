"""Create a DAG that runs every day at 07:30.
   - Task 1 (PythonOperator): Read the "support_tickets" sheet.
     Calculate how many tickets have status = "Open" or "Escalated".
     Push the count to XCom.
   - Task 2: Pull the value from XCom and log/print it.
   Bonus: Add a FileSensor that checks the Excel file exists before starting."""

from datetime import datetime
import pandas as pd

from airflow.sdk import DAG, task
from airflow.providers.standard.operators.python import PythonOperator,get_current_context

file_path="/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets-Table 1.csv"
@task
def read():
    df=pd.read_csv(file_path)
    calc_open=(df["status"]=="Open").sum()
    calc_esc=(df["status"]=="Escalated").sum()
    total_count=calc_esc+calc_open
    return total_count
@task
def res_print(result):
    print(f"the total count is {result}")

with DAG(
    dag_id="read_support_tickets",
    start_date=datetime(2026,7,8),
    schedule="30 7 * * *",
    catchup=False,
    tags=["support"],
)as dag:
    read_data=read()
    print_res=res_print(read_data)
