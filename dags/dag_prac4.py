"""Write a custom PythonOperator that:
   - Reads sales_data.csv
   - Groups by region and calculates:
       • total_quantity
       • total_revenue
       • average_order_value
   - Writes the result to /opt/airflow/data/region_summary.csv
   Schedule it to run every Monday at 07:00."""

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator,get_current_context
from datetime import datetime
import pandas as pd

def read_data():
    df=pd.read_csv("/Users/prse/PycharmProjects/Airflow_Prac/data/sales_data.csv")
    df["revenue"]=df["quanity"]*df["unit_price"]
    summary=df.groupby(df["region"]).agg(total_quantity=("quantity", "sum"),
            total_revenue=("revenue", "sum"),
            average_order_value=("revenue", "mean"),).reset_index()
    summary.to_csv("/Users/prse/PycharmProjects/Airflow_Prac/data/sales_data.csv",index=False)

with DAG(
    dag_id="read_data_tasks",
    schedule="0 7 * * 1",
    start_date=datetime.now(),
    catchup=False,
    tags=["sales"],
) as dag:
    data_tasks=PythonOperator(
        task_id="read_data",
        python_callable=read_data,
    )

