"""Dynamic Task Mapping:
   - Extract the unique list of "category" values from the Excel file.
   - Use .expand() so that one mapped task is created per category.
   - Each mapped task filters the data for that category and saves
     a separate CSV: data/category_{category_name}.csv"""


from datetime import datetime
import pandas as pd

from airflow.sdk import DAG, task,TaskGroup
from airflow.providers.standard.operators.python import PythonOperator,get_current_context,BranchPythonOperator

from dags.dag_prac5 import category

file_path="/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets-Table 1.csv"

@task
def extract():
    df=pd.read_csv(file_path)
    cat_vals=df["category"].unique().tolist()
    return cat_vals
@task
def filter(categories):
    df=pd.read_csv(file_path)
    data=df[df["category"]==categories]
    filtered_path=f"/Users/prse/PycharmProjects/Airflow_Prac/data/{categories}.csv"
    data.to_csv(filtered_path,index=False)

with DAG(
    dag_id="dynamic_task_mapping",
    start_date=datetime(2026,9,10),
    schedule="* * * * *",
    catchup=False,
    tags=["support"],

)as dag:
    extract_data=extract()
    filter_data=filter.expand(category=extract_data)
