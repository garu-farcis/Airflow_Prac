"""Intermediate – Basic @dag + @task
   Write a complete DAG using only TaskFlow API that:
   - Uses @dag decorator with schedule="@daily", catchup=False
   - Has three @task functions:
        extract()      → reads support_tickets sheet and returns DataFrame
        filter_open()  → keeps only status == "Open"
        save()         → saves the filtered data to data/open_tickets.csv
   - Properly chains them: extract → filter_open → save"""

from  airflow import DAG,task
from airflow.sdk import TaskGroup,dag
from airflow.providers.standard.operators.python import PythonOperator
from datetime import timedelta,datetime


file_path="/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets-Table 1.csv"
out_path="/Users/prse/PycharmProjects/Airflow_Prac/data/open_tickets.csv"
import pandas as pd
@dag(
    dag_id="etl_taskflow_api",
    start_date=datetime(2027,9,8),
    schedule='@daily',
    catchup=False,
    tags=["taskflow"],
)
def my_dag():
    @task
    def extract(ti=None):
        df=pd.read_csv(file_path)
        ti.xcom_push(key='my_dict',value= df.to_dict("values"))

    @task
    def filter_open(ti=None):
        df=ti.xcom_pull(task_ids="extract",key='my_dict')
        filtered=df[df['status']=='Open'].sum
        ti.xcom_push(key='filtered',value=filtered)
    @task
    def save(ti=None):
        df=ti.xcom_pull(task_ids="filt_open",key='filtered')
        df.to_csv(out_path,index_label=False)


    extract=extract()
    filt_open=filter_open()
    save=save()


