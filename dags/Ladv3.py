"""Intermediate – Multiple XCom returns
   Create a @task that returns multiple values using:
       return {"open_count": ..., "escalated_count": ..., "critical_count": ...}
   Then create three separate downstream @task functions that each receive
   only one of those values and print it.
   (Practice multiple_outputs=True or dictionary unpacking)"""

from  airflow import DAG
from airflow.sdk import TaskGroup,dag,task
from airflow.providers.standard.operators.python import PythonOperator
from datetime import timedelta,datetime
import pandas as pd


file_path="/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets-Table 1.csv"

@dag(
    dag_id="Multiple_XCom_returns",
    start_date=datetime(2026,8,9),
    schedule="@weekly",
    catchup=False,
    tags=["taskflow"],
)
def my_dag():
    @task(multiple_outputs=True)

    def extract_val():
        df=pd.read_csv(file_path)
        open_count=(df['status']=='Open').sum()
        escalated_count=(df['status']=='Escalated').sum()
        critical_count=(df['status']=='Critical').sum()
        return {
            "open_count":open_count,
            "escalated_count":escalated_count,
            "critical_count":critical_count
        }
    @task
    def open_c(open_count):
        print(f"open_count is {open_count}")
    @task
    def escalated_c(escalated_count):
        print(f"open_count is {escalated_count}")
    @task
    def critical_c(critical_count):
        print(f"open_count is {critical_count}")

    extract=extract_val()
    openc=open_c(open_count=extract["open_count"])
    esc=escalated_c(escalated_count=extract["escalated_count"])
    crit=critical_c(critical_count=extract["critical_count"])

my_dag()