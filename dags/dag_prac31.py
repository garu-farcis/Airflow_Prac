"""Design a DAG with three tasks: extract (read CSV), transform (dedupe exact
   duplicate rows, cast quantity to int with a default of 1 for nulls), and load
   (write to a "cleaned" CSV). Explain how you'd set retries and retry_delay on each
   task and why they might differ between tasks."""

from airflow.sdk import task,DAG,TaskGroup
from airflow.providers.standard.operators.python import BranchPythonOperator,PythonOperator,get_current_context
import pandas as pd
import datetime

file_path="/Users/prse/PycharmProjects/Airflow_Prac/data/orders_sample.csv"
out_path="/Users/prse/PycharmProjects/Airflow_Prac/data/three_tasks.csv"

def extract():
    df=pd.read_csv(file_path)
    return df
def transform():
    ti=get_current_context()["ti"]
    data=ti.xcom_pull(task_ids="extract")
    df=pd.DataFrame(data)
    df.drop_duplicates()
    dedupe=df["quantity"].fillna(1).astype(int)
    return dedupe

def load():
    ti = get_current_context()["ti"]
    data = ti.xcom_pull(task_ids="transform")
    df=pd.DataFrame(data)
    df.to_csv(out_path,index=False)

with DAG(
    dag_id="not_null_check",
    start_date=datetime.datetime(2026,9,10),
    schedule="0 8 * * 5",
    catchup=False,
    tags=["orders"],
)as dag:
    extract_data=PythonOperator(
        task_id="extract",
        python_callable=extract,
        retries=3,
        retry_delay=3,
    )
    transform_data=PythonOperator(
        task_id="transform",
        python_callable=transform,
        retries=3,
        retry_delay=3,
    )
    load_data=PythonOperator(
        task_id="load_data",
        python_callable=load,
    )
    extract_data>>transform_data>>load_data