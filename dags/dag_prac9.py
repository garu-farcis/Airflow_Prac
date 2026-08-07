"""Create a sensor-based DAG:
   - FileSensor that waits for new_orders.csv to appear
   - Once available, use a PythonOperator to append the new rows to sales_data.csv
   - Then trigger a downstream calculation of daily revenue."""

from datetime import datetime

import pandas as pd

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator,ShortCircuitOperator,get_current_context
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import TaskGroup

def wait_to_check():
    df=pd.read_csv("/Users/prse/PycharmProjects/Airflow_Prac/data/sales_data.csv")
    new_row=[1016,'C009','Laptop Stand','Electronics',2,45.00,'2024-01-19','West','Completed']
    columns = [
        "order_id",
        "customer_id",
        "product",
        "category",
        "quantity",
        "unit_price",
        "order_date",
        "region",
        "status"
    ]
    n_df=pd.DataFrame([new_row],
    columns=columns)
    new_df=pd.concat([df, n_df],
    ignore_index=True)
    new_df.to_csv(
        "/Users/prse/PycharmProjects/Airflow_Prac/data/sales_data.csv",
        index=False
    )

def calc():
    df = pd.read_csv("/Users/prse/PycharmProjects/Airflow_Prac/data/sales_data.csv")
    mask=(df["status"]=="Completed")
    total_rev=(mask["quantity"]*mask["unit_price"]).sum()
    return total_rev

with DAG(
    dag_id="sensor_DAG",
    schedule="* * * * *",
    start_date=datetime(2026,8,7),
    catchup=False,
    tags=["sales"],
)as dag:
    wait_for_file=FileSensor(
        task_id="wait_data",
        filepath="/Users/prse/PycharmProjects/Airflow_Prac/data/sales_data.csv",
        poke_interval=30,
        timeout=300,
    )
    read_file=PythonOperator(
        task_id="read_data",
        python_callable=wait_to_check,
    )
    calc_rev=PythonOperator(
        task_id="calculate_rev",
        python_callable=calc,
    )

    wait_for_file>>read_file>>calc_rev