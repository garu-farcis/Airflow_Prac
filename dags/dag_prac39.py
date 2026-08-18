"""Implement a FileSensor + processing pattern:

   - Sensor waits for the file data/new_tickets.xlsx (timeout 10 min, poke every 30s)
   - Downstream PythonOperator reads both sheets ("support_tickets" and "new_tickets")
   - Appends the new tickets to the historical data
   - Writes the combined result to data/all_tickets_combined.csv
   - Pushes total row count to XCom"""

from airflow.sdk import task,DAG,TaskGroup
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator, get_current_context, \
    ShortCircuitOperator
import pandas as pd
import datetime
from airflow.providers.standard.sensors.filesystem import FileSensor
from datetime import timedelta

file_path="/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets-Table 1.csv"
file_path1="/Users/prse/PycharmProjects/Airflow_Prac/data/new_tickets-Table 1.csv"
out_path="/Users/prse/PycharmProjects/Airflow_Prac/data/all_tickets_combined.csv"

def extract(**context):
    df=pd.read_csv(file_path)
    df1=pd.read_csv(file_path1)
    context["ti"].xcom_push(key="data_f",value=df.to_dict("data_frame1"))
    context["ti"].xcom_push(key="data_f1",value=df1.to_dict("data_frame2"))
    return {"data_f","data_f1"}

def transform(**context):
    ti=get_current_context()["ti"]
    df=ti.xcom_pull(task_ids="extract",key="data_f")
    df1=ti.xcom_pull(task_ids="extract",key="data_f1")
    new_df=pd.concat([df,df1],ignore_index=True)
    new_df.to_csv(out_path,index=False)
    row_count=len(new_df)
    context["ti"].xcom_push(key="row_count",value=row_count)

def load():
    ti = get_current_context()["ti"]
    row_count=ti.xcom_pull(task_ids="transform",key="row_count")
    print(f"the row count is {row_count}")

with DAG(
    dag_id="etl_test_pipeline",
    start_date=datetime.datetime(2026,10,19),
    schedule="0 9 * * 4",
    catchup=False,
    tags=["support"],
)as dag:
    wait_d=FileSensor(
        task_id="wait_for_files",
        filepath="/Users/prse/PycharmProjects/Airflow_Prac/data/new_tickets-Table 1.csv",
        poke_interval=30,
        timeout=timedelta(minutes=10),
    )
    extract_d=PythonOperator(
        task_id="extract",
        python_callable=extract,
    )
    transform_d=PythonOperator(
        task_id="transform",
        python_callable=transform,
    )
    load_d=PythonOperator(
        task_id="load",
        python_callable=load,
    )
    wait_d>>extract_d>>transform_d>>load_d