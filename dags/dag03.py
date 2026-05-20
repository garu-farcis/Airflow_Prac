from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

def extract():
    print("extracting")
def transform():
    print("transforming")
def load():
    print("loading")

with DAG(
    dag_id="etl_pipeline",
    start_date=datetime.now(),
    schedule="@daily",
    catchup=False
)as dag:
    t1=PythonOperator(task_id="extract",python_callable = extract)
    t2=PythonOperator(task_id="transform",python_callable=transform)
    t3=PythonOperator(task_id="load",python_callable=load)

    t1>>t2>>t3