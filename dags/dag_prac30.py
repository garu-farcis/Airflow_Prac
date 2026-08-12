"""Write a DAG that reads orders_sample.csv daily via a PythonOperator, validates that
   order_date and unit_price are not null, and routes bad rows to a separate
   quarantine CSV. Use XComs to pass the row counts (valid vs quarantined) between tasks"""

from airflow.sdk import task,DAG,TaskGroup
from airflow.providers.standard.operators.python import BranchPythonOperator,PythonOperator,get_current_context
import pandas as pd
import datetime

file_path="/Users/prse/PycharmProjects/Airflow_Prac/data/orders_sample.csv"
out_path="/Users/prse/PycharmProjects/Airflow_Prac/data/bad_route.csv"

def extract(**context):
    df=pd.read_csv(file_path)
    mask=(df["order_date"].notnull() and df["unit_price"].notnull())
    new_df=df[mask]
    bad_route=df[~mask].copy()
    bad_route.to_csv(out_path, index=False)
    ti = context["ti"]
    ti.xcom_push(key="valid_count", value=len(new_df))
    ti.xcom_push(key="quarantined_count", value=len(bad_route))
    return {
        "valid_rows": len(new_df),
        "quarantined_rows": len(bad_route),
    }

def load():
    ti=get_current_context()["ti"]
    data_count=ti.xcom_pull(task_ids="extract_data",key="valid_count")
    q_count=ti.xcom_pull(task_ids="extract_data",key="quarantined_count")
    print(f"Valid rows written   : {data_count}")
    print(f"Quarantined rows     : {q_count}")

with DAG(
    dag_id="not_null_check",
    start_date=datetime.datetime(2026,9,10),
    schedule="0 8 * * 5",
    catchup=False,
    tags=["orders"],
)as dag:
    extract_data=PythonOperator(
        task_id="extract_data",
        python_callable=extract,
    )
    load_data=PythonOperator(
        task_id="load_data",
        python_callable=load,
    )
    extract_data>>load_data