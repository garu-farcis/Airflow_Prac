"""Intermediate – Basic TaskGroup
   Create a TaskGroup named "data_quality".
   Inside it put three PythonOperators:
   - check_nulls          → fails if ticket_id or status has nulls
   - check_valid_priority → fails if priority not in [Low, Medium, High, Critical]
   - check_dates          → fails if created_date is in the future
   The whole group should fail if any of the three checks fail.
   After the group, add a task that prints “All quality checks passed”."""


from  airflow import DAG,task
from airflow.sdk import TaskGroup,dag
from airflow.providers.standard.operators.python import PythonOperator
from datetime import timedelta,datetime
file_path="/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets-Table 1.csv"
import pandas as pd

@dag(
    dag_id="my_taskgroup_dag",
    start_date=datetime(2026, 8, 1),
    schedule="0 9 * * *",
    catchup=False,
    tags=["orders"],
)
def my_dag():
    @task
    def check_null(ti=None):
        df=pd.read_csv(file_path)
        val_null=((df[df["ticket_id"].isnull]) or df[df["status"].isnull]).sum
        ti.xcom_push(key="null_check", value="passed")

    @task
    def check_valid_priority(ti=None):
        df = pd.read_csv(file_path)
        pri=['Low', 'Medium', 'High', 'Critical']
        val_pri=df[df["priority"].isnull].sum
        ti.xcom_push(key="priority_check", value="passed")

    @task
    def check_dates(ti=None):
        df = pd.read_csv(file_path)
        val_date=df[df["created_date"]>datetime.today()].sum
        ti.xcom_push(key="date_check", value="passed")

    @task
    def quality_passed():
        print("All quality checks passed")

    with TaskGroup(group_id="data_quality"):
        null_check = check_null()

        priority_check = check_valid_priority()

        date_check = check_dates()

    [
        null_check,
        priority_check,
        date_check,
    ] >> quality_passed()


my_dag()
