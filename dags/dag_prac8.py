"""Build a DAG that uses Airflow Variables:
   - Store the path of the input file as an Airflow Variable
   - Store a threshold value (e.g. min_revenue = 5000)
   - In a PythonOperator, read the Variable and only generate a success email
     (or print) if total completed revenue exceeds the threshold."""

from datetime import datetime

import pandas as pd

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator,ShortCircuitOperator
from airflow.sdk import TaskGroup
from airflow.sdk import Variable

def read_variable():
    file_path=Variable.get("INPUT_PATH")
    df=pd.read_csv(file_path)
    # its a string so we use int
    threshold=int(Variable.get("min_revenue"))
    orders_worth=df[df["status"]=="Completed"]
    total_revenue=(orders_worth["quantity"]*orders_worth["unit_price"]).sum()
    if total_revenue>threshold:
        print("total completed revenue exceeds the threshold")
    else:
        print("revenue not reached")
with DAG(
    dag_id="read_variable",
    start_date=datetime(2026,7,6),
    schedule="* * * * *",
    catchup=False,
    tags=["sales"],
)as dag:
    variable_check=PythonOperator(
        task_id="variable_check",
        python_callable=read_variable,
    )