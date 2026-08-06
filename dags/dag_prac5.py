"""Implement a dynamic DAG that creates one task per unique "category" found in the CSV.
   Each task should filter the data for that category and write a separate file:
   /opt/airflow/data/category_{category_name}.csv"""

from datetime import datetime

import pandas as pd

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

INPUT_FILE = "/Users/prse/PycharmProjects/Airflow_Prac/data/sales_data.csv"
def write_category(category):
    df = pd.read_csv(INPUT_FILE)
    filtered = df[df["category"] == category]
    output = f"/Users/prse/PycharmProjects/Airflow_Prac/data_{category}.csv"
    filtered.to_csv(output, index=False)
    print(f"Wrote {output}")
df = pd.read_csv(INPUT_FILE)
categories = df["category"].unique()

with DAG(
    dag_id="dynamic_category_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    for category in categories:
        PythonOperator(
            task_id=f"write_{category}",
            python_callable=write_category,
            op_args=[category],
        )