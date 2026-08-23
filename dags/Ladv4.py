"""Intermediate – TaskFlow with classical operator
   Mix TaskFlow with a classic operator:
   - @task extract() reads the Excel and returns the file path as string
   - A classic BashOperator uses that path (via templating or XComArg)
     to print the file size (`ls -lh`)
   - Then another @task continues after the BashOperator"""


from  airflow import DAG
from airflow.sdk import TaskGroup,dag,task
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import timedelta,datetime
import pandas as pd


file_path="/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets-Table 1.csv"

@dag(
    dag_id="TaskFlow_with_classical_operator",
    start_date=datetime(2026,8,9),
    schedule="@weekly",
    catchup=False,
    tags=["taskflow"],
)
def my_dag():
    @task
    def extract():
        df=pd.read_csv(file_path)
        file_path1=str(file_path)
        return file_path1
    ex=extract()

    bash_task=BashOperator(
        task_id="bashing",
        bash_command=f"ls -lh {ex}",
    )
    @task
    def print(filepath):
        print(f"the file path is {filepath}")
    pr=print(ex)
    ex>>bash_task>>pr

my_dag()