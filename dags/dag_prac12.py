"""Build a multi-step ETL pipeline:
    extract (read CSV) → clean (remove Cancelled + Pending) →
    enrich (add a column total_amount = quantity * unit_price) →
    load (write to parquet or another CSV)
    Use @task decorator (TaskFlow API) style."""

# from datetime import datetime,timedelta
#
# import pandas as pd
#
#
# from airflow.sdk import DAG,task
# from airflow.providers.standard.operators.python import PythonOperator,ShortCircuitOperator,get_current_context
# from airflow.providers.standard.sensors.filesystem import FileSensor
# from airflow.sdk import TaskGroup
# from pandas.core.common import maybe_make_list
# addr="/Users/prse/PycharmProjects/Airflow_Prac/data/sales_data.csv"
# OUTPUT_FILE = "/Users/prse/PycharmProjects/Airflow_Prac/data/test_result_data.csv"
# @task
# def extract():
#     df=pd.read_csv("/Users/prse/PycharmProjects/Airflow_Prac/data/sales_data.csv")
#     return df
# @task
#
# def clean(input_path: str) -> str:
#     df=pd.read_csv(input_path)
#     df = df[
#         ~df["status"].isin(["Cancelled", "Pending"])
#     ]
#
#     return df
#     # datafr=pd.display(df[df["status"]=="Completed"])
#     # filt_data=df.mask(cond=df[df["status"]=="Completed"])
#     # return filt_data
# @task
#
# def enrich(input_path: str) -> str:
#     df=pd.read_csv(input_path)
#     df["total_amount"]=df["quantity"] * df["unit_price"]
#     return df
# @task
#
# def load(input_path: str) -> None:
#     df=pd.read_csv(input_path)
#     df.to_csv(OUTPUT_FILE,index=False)
#
# with DAG(
#     dag_id="etl_check",
#     start_date=datetime(2026,8,10),
#     schedule="* * * * *",
#     catchup=False,
#     tags=["sales"],
# )as dag:
#     etl_pipeline=extract()
#     part2=clean(etl_pipeline)
#     part3=enrich(part2)
#     part4=load(part3)


from datetime import datetime
from pathlib import Path
import pandas as pd

from airflow.sdk import DAG, task

DATA_DIR = Path("/Users/prse/PycharmProjects/Airflow_Prac/data")
INPUT_FILE = DATA_DIR / "sales_data.csv"
CLEANED_FILE = DATA_DIR / "cleaned_data.csv"
ENRICHED_FILE = DATA_DIR / "enriched_data.csv"
OUTPUT_FILE = DATA_DIR / "test_result_data.csv"


@task
def extract() -> str:
    # Source file already exists on disk – just return its path
    return str(INPUT_FILE)


@task
def clean(input_path: str) -> str:
    df = pd.read_csv(input_path)
    df = df[~df["status"].isin(["Cancelled", "Pending"])]
    df.to_csv(CLEANED_FILE, index=False)
    return str(CLEANED_FILE)          # ← return path, not DataFrame


@task
def enrich(input_path: str) -> str:
    df = pd.read_csv(input_path)
    df = df.copy()
    df["total_amount"] = df["quantity"] * df["unit_price"]
    df.to_csv(ENRICHED_FILE, index=False)
    return str(ENRICHED_FILE)         # ← return path


@task
def load(input_path: str) -> None:
    df = pd.read_csv(input_path)
    df.to_csv(OUTPUT_FILE, index=False)
    # Or: df.to_parquet(OUTPUT_FILE.with_suffix(".parquet"), index=False)


with DAG(
    dag_id="etl_check",
    start_date=datetime(2026, 8, 10),
    schedule="* * * * *",
    catchup=False,
    tags=["sales"],
) as dag:
    raw_path = extract()
    cleaned_path = clean(raw_path)
    enriched_path = enrich(cleaned_path)
    load(enriched_path)