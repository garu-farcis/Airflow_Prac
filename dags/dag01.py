from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime

with DAG(
    dag_id="dependency_dag",
    start_date=datetime.today(),
    schedule="@weekly",
    catchup=False
) as dag:
    start=EmptyOperator(task_id="start")
    process=EmptyOperator(task_id="process")
    end=EmptyOperator(task_id="stop")

    start>>process>>end