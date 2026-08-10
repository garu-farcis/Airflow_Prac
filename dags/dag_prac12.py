"""Build a multi-step ETL pipeline:
    extract (read CSV) → clean (remove Cancelled + Pending) →
    enrich (add a column total_amount = quantity * unit_price) →
    load (write to parquet or another CSV)
    Use @task decorator (TaskFlow API) style."""

from datetime import datetime,timedelta

import pandas as pd


from airflow.sdk import DAG,task
from airflow.providers.standard.operators.python import PythonOperator,ShortCircuitOperator,get_current_context
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import TaskGroup
from pandas.core.common import maybe_make_list

OUTPUT_FILE = "/Users/prse/PycharmProjects/Airflow_Prac/data/filtered_data.csv"
@task
def extract():
    df=pd.read_csv("/Users/prse/PycharmProjects/Airflow_Prac/data/sales_data.csv")
    return df
@task

def clean(df):
    # df=pd.read_csv("/Users/prse/PycharmProjects/Airflow_Prac/data/sales_data.csv")
    df = df[
        ~df["status"].isin(["Cancelled", "Pending"])
    ]

    return df
    # datafr=pd.display(df[df["status"]=="Completed"])
    # filt_data=df.mask(cond=df[df["status"]=="Completed"])
    # return filt_data
@task

def enrich(df):
    df=pd.read_csv("/Users/prse/PycharmProjects/Airflow_Prac/data/sales_data.csv")
    df["total_amount"]=df["quantity"] * df["unit_price"]
@task

def load(df):
    df=pd.read_csv("/Users/prse/PycharmProjects/Airflow_Prac/data/sales_data.csv")
    df.to_csv(OUTPUT_FILE,index=False)

with DAG(
    dag_id="etl_check",
    start_date=datetime(2026,8,10),
    schedule="* * * * *",
    catchup=False,
    tags=["sales"],
)as dag:
    etl_pipeline=extract()
    clean(extract)
    load(clean)