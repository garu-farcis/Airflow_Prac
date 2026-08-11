"""Create a TaskGroup named "ticket_quality_checks".
   Inside the group perform these checks:
   - No null values in ticket_id, customer_id, status
   - priority must be one of: Low, Medium, High, Critical
   - created_date must not be in the future
   If any check fails, the whole TaskGroup (and DAG) should fail."""

from datetime import datetime
import pandas as pd

from airflow.sdk import DAG, task,TaskGroup
from airflow.providers.standard.operators.python import PythonOperator,get_current_context,BranchPythonOperator

file_path="/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets-Table 1.csv"
def check1():
    df=pd.read_csv(file_path)
    check_df=df[["ticket_id","customer_id","status"]].isnull().any()
    if check_df:
        raise ValueError("too many nulls")

def check2():
    df=pd.read_csv(file_path)
    p_list=['Low', 'Medium', 'High', 'Critical']
    if not df["priority"].isin(p_list):
        raise ValueError("incorrect priority")
def check3():
    df=pd.read_csv(file_path)
    df["created_date"] = pd.to_datetime(df["created_date"])
    if not df["created_date"]<=datetime.now().any():

        raise ValueError("incorrect created date")

with DAG(
    dag_id="ticket_quality_checks",
    start_date=datetime(2026,9,10),
    schedule="0 0 * * *",
    catchup=False,
    tags=["support"],
)as dag:
    with TaskGroup(group_id="ticket_quality_checks") as ticket_quality_checks:
        check11=PythonOperator(
            task_id="check_1",
            python_callable=check1,
        )
        check12 = PythonOperator(
            task_id="check_2",
            python_callable=check2,
        )
        check13 = PythonOperator(
            task_id="check_3",
            python_callable=check3,
        )

        [check11,check12,check13]