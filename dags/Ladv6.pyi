"""Advanced – TaskFlow + FileSensor style waiting
   Create a DAG where:
   - A @task checks if data/new_tickets.xlsx exists
     (raise AirflowSkipException or return False if not)
   - Only if it exists, a downstream @task appends the new tickets
     to the main data and saves a combined file
   Practice controlling flow with skip / early return inside @task"""

from  airflow import DAG
from airflow.sdk import TaskGroup,dag,task
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from datetime import timedelta,datetime
import pandas as pd
from airflow.sdk.exceptions import AirflowSkipException
import os
file_path="/Users/prse/PycharmProjects/Airflow_Prac/data/new_tickets.csv"
file_path1="/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets-Table 1.csv"
out_path=file_path1="/Users/prse/PycharmProjects/Airflow_Prac/data/out_path.csv"

@dag(
    dag_id="filesensor_logic",
    start_date=datetime(2026,8,9),
    schedule="@daily",
    tags=['Taskflow'],
    catchup=False,
)
def my_dag():

    @task.sensor(
        poke_interval=30,
        timeout=10,
        retries=2,
        mode="reschedule",
    )
    def read():
        df=pd.read_csv(file_path)
        if os.path.exists(file_path):
            print("new_tickets file exists ")
        else:
            raise AirflowSkipException
    @task
    def append():
        df_newtickets=df=pd.read_csv(file_path)
        df_supp_tickets=pd.read_csv(file_path1)
        app_data=pd.concat([df_newtickets,df_supp_tickets])
        app_data.to_csv(out_path,index=False)
        return app_data

    read_d=read()
    app_data=append()

    read_d>>app_data

my_dag()