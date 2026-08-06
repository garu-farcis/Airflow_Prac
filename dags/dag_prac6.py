"""Create a DAG with the following dependency pattern:
   extract → [transform_electronics, transform_furniture, transform_stationery] → load
   Use TaskGroups or classic dependencies.
   Each transform task should only process its own category."""

from datetime import datetime

import pandas as pd

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import TaskGroup

def extract():
    print("file has been extracted")

def transform(category):
    df=pd.read_csv("/Users/prse/PycharmProjects/Airflow_Prac/data/sales_data.csv")
    data=(df["category"]==category)
    print(data)

def load():
    print("data has been loaded")

with DAG(
    dag_id="ETL_tasks",
    start_date=datetime.now(),
    schedule="* * * * *",
    catchup=False,
    tags=["etl"],
)as dag:
    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=extract,
    )
    with TaskGroup(group_id="ETL_group_tasks") as etl_grouptasks:

       electronics=PythonOperator(
           task_id="transform_electronics",
           python_callable=transform,
           op_args=["Electronics"]
       )

       furniture = PythonOperator(
       task_id="transform_furniture",
       python_callable=transform,
       op_args=["Furniture"],
        )

       stationery = PythonOperator(
           task_id="transform_stationery",
           python_callable=transform,
           op_args=["Stationery"],
       )
    load_task = PythonOperator(
       task_id="load",
       python_callable=load,
       )

    extract_data>>etl_grouptasks>>load_task