"""Advanced – Reusable @task with parameters
   Write a reusable @task that accepts parameters:

   @task
   def filter_by_status(status: str, output_path: str):
       # reads Excel, filters by the given status, saves to output_path

   Then call it three times from the DAG with different statuses:
   - "Open"       → data/open.csv
   - "Escalated"  → data/escalated.csv
   - "Closed"     → data/closed.csv"""

from  airflow import DAG
from airflow.sdk import TaskGroup,dag,task
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import timedelta,datetime
import pandas as pd


file_path="/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets-Table 1.csv"
out_path= "/Users/prse/PycharmProjects/Airflow_Prac/data/output_path.csv"
@dag(
    dag_id="reusable_logic",
    start_date=datetime(2026,8,9),
    schedule="@daily",
    tags=['Taskflow'],
    catchup=False,
)
def my_dag():

    @task(multiple_outputs=True)
    def filter_by_status():
        df=pd.read_csv(file_path)
        stat_open=df[df['Status']=='Open']
        col_open=stat_open.to_csv("/Users/prse/PycharmProjects/Airflow_Prac/data/open.csv",index_label=True)
        stat_close=df[df['Status']=='Closed']
        col_closed=stat_close.to_csv("/Users/prse/PycharmProjects/Airflow_Prac/data/closed.csv",index_label=True)
        stat_esc=df[df['Status']=='Escalated']
        col_escalated=stat_esc.to_csv("/Users/prse/PycharmProjects/Airflow_Prac/data/escalated.csv",index_label=True)
        return {
            "col_o":"/Users/prse/PycharmProjects/Airflow_Prac/data/open.csv",
            "col_c":"/Users/prse/PycharmProjects/Airflow_Prac/data/closed.csv",
            "col_esc":"/Users/prse/PycharmProjects/Airflow_Prac/data/escalated.csv"
        }
    @task
    def open(file_paths:str)->str:
        df=pd.read_csv(file_paths)
        print(f"open status data is {df} and file_path is {file_paths}")

    @task
    def closed(file_paths: str) -> str:
        df = pd.read_csv(file_paths)
        print(f"closed status data is {df} and file_path is {file_paths}")

    @task
    def esc(file_paths: str) -> str:
        df = pd.read_csv(file_paths)
        print(f"esc status data is {df} and file_path is {file_paths}")

    filter_d=filter_by_status()
    op=open(filter_d["col_o"])
    cl=closed(filter_d["col_c"])
    esc_d=esc(filter_d["col_esc"])

    filter_d>>op>>cl>>esc_d

my_dag()
