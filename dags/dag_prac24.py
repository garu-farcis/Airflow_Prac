"""Write a PythonSensor callable that returns True only when:
    - The file support_tickets.xlsx exists
    - AND there are zero tickets with status == "Escalated"
    (Useful for waiting until all escalations are cleared)"""
import os.path
from datetime import datetime
import pandas as pd

from airflow.sdk import DAG, task,TaskGroup,Variable
from airflow.providers.standard.operators.python import PythonOperator, get_current_context, BranchPythonOperator, ShortCircuitOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk.exceptions import AirflowSkipException

file_path="/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets.xlsx"
path="/Users/prse/PycharmProjects/Airflow_Prac/data/low_satisfaction.csv"

def check():
    df=pd.read_csv(file_path)
    ticks_count=df[df["status"]=="Escalated"].value_counts()
    return ticks_count==0

def check1():
    ti=get_current_context()["ti"]
    data=ti.xcom_pull(task_ids="check_df")
    patf_for_file="/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets.xlsx"
    if not os.path.exists(patf_for_file)==True:
        return True
with DAG(
    dag_id="pythonsensor_test",
    start_date=datetime(2027,9,10),
    schedule="* * * * *",
    catchup=False,
    tags=["support"],
)as dag:
    check_data=PythonSensor(
        task_id="check_df",
        python_callable=check,
        poke_interval=60,
        retries=3,
    )

    check2=PythonOperator(
        task_id="check_2",
        python_callable=check1,
    )

    check_data>>check2



