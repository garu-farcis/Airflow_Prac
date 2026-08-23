"""Advanced – Conditional logic with TaskFlow
   Using only @task and @dag:
   - One task counts how many Critical tickets exist
   - If the count is greater than 3, call a task `send_alert()`
   - Otherwise call a task `generate_normal_report()`
   (You can use BranchPythonOperator still, or pure TaskFlow with
    short-circuit style / selective downstream calling)"""


from  airflow import DAG
from airflow.sdk import TaskGroup,dag,task
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import timedelta,datetime
import pandas as pd


file_path="/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets-Table 1.csv"
@dag(
    dag_id="condition_logic",
    start_date=datetime(2026,8,9),
    schedule="@daily",
    tags=['Taskflow'],
    catchup=False,
)
def my_dag():
    @task.branch()
    def critical_counts():
        df=pd.read_csv(file_path)
        counts=(df['status']=='Critical').sum()
        if counts>3:
            return "send_alert"
        else:
            return "generate_normal_report"
    @task
    def send_alert():
        print("critical cannot be more than 3")

    @task
    def generate_normal_report():
        print("generating report")

    cc=critical_counts()
    sa=send_alert()
    gm=generate_normal_report()

    cc>>[sa,gm]

my_dag()
