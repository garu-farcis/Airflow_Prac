"""Write a complete classic DAG (not TaskFlow) that does the following:

    extract → transform → load

    - extract: reads the Excel and pushes the file path via XCom
    - transform: pulls the path, filters status in ["Resolved", "Closed"],
                 adds column "resolution_hours"
    - load: saves the transformed data as data/resolved_tickets.parquet
            (or .csv if parquet is not available"""

from datetime import datetime
import pandas as pd

from airflow.sdk import DAG, task,TaskGroup,Variable
from airflow.providers.standard.operators.python import PythonOperator, get_current_context, BranchPythonOperator,ShortCircuitOperator
from airflow.sdk.exceptions import AirflowSkipException

file_path="/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets-Table 1.csv"
out_path="/Users/prse/PycharmProjects/Airflow_Prac/data/resolved_tickets.parquet"
def extract():
    df=pd.read_csv(file_path)
    return df.to_dict("data")

def transform():
    ti=get_current_context()["ti"]
    my_data=ti.xcom_pull(task_ids="extract_data")
    df=pd.DataFrame(my_data)
    filtered_data=df[df["status"].isin(["Resolved", "Closed"])].sum()
    df["created_date"] = pd.to_datetime(df["created_date"])
    df["resolved_date"] = pd.to_datetime(df["resolved_date"])
    df["resolution_hours"]=df["resolved_date"]-df["created_date"]
    return df.to_dict("my_data")

def load():
    ti = get_current_context()["ti"]
    my_data = ti.xcom_pull(task_ids="transform_data")
    df=pd.DataFrame(my_data)
    df.to_parquet(out_path,index=False)

with DAG(
    dag_id="ETL_whole",
    start_date=datetime(2026,9,10),
    schedule="* * * * *",
    catchup=False,
    tags=["support"],
)as dag:
    extract_data=PythonOperator(
        task_id="extract_data",
        python_callable=extract,
    )
    transform_data=PythonOperator(
        task_id="transform_data",
        python_callable=transform,
    )
    load_data=PythonOperator(
        task_id="load_data",
        python_callable=load,
    )

    extract_data>>transform_data>>load_data
