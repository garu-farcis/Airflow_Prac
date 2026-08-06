"""Create a DAG that runs daily at 06:00 UTC.
   - Task 1: Use PythonOperator to read sales_data.csv and calculate total revenue
     (quantity * unit_price) only for rows where status == "Completed".
   - Push the result to XCom.
   - Task 2: Pull the value from XCom and print it.
   Bonus: Add a sensor that waits until the file exists before starting."""

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.operators.python import get_current_context
import pandas as pd

def cal_revenue():
    df =pd.read_csv("/Users/prse/PycharmProjects/Airflow_Prac/data/sales_data.csv")
    completed=df[df["status"]=="Completed"]
    total=(completed["quantity"]*completed["unit_price"]).sum()
    return total

def print_val():
    ti=get_current_context()["ti"]
    revenue=ti.xcom_pull(task_ids="calc_task")
    print(f"total revenue is {revenue}")

with DAG(
    dag_id="sales_revenue",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule="0 6 * * *",
    tags=["sales"],
)as dag:
    wait_for_file=FileSensor(
        task_id="wait_for_files",
        filepath="/Users/prse/PycharmProjects/Airflow_Prac/data/sales_data.csv",
        poke_interval=30,
        timeout=300,
    )

    calc_revenue=PythonOperator(
        task_id="calc_task",
        python_callable=cal_revenue,
    )

    print_task=PythonOperator(
        task_id="print_task",
        python_callable= print_val,
    )

    wait_for_file>>calc_revenue>>print_task
