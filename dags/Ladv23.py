"""Dataset-Aware Scheduling + Producer/Consumer DAGs (Advanced)
   Create two DAGs:

   DAG A (Producer):
   - Reads support_tickets.xlsx
   - Filters new/updated tickets
   - Writes a clean CSV to data/clean_tickets.csv
   - Updates an Airflow Dataset (e.g. Dataset("file://data/clean_tickets.csv"))

   DAG B (Consumer):
   - Is scheduled only by the Dataset (no timetable)
   - When triggered, reads the clean CSV
   - Performs aggregations by agent and region
   - Writes results to SQLite using SqliteHook"""

from airflow.sdk import dag, task,Asset
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow import Dataset
import pandas as pd
import datetime
from datetime import timedelta


ticket_data=Asset("/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets.xlsx")
@dag(
    dag_id='Dataset_Aware_Scheduling',
    start_date=datetime.datetime(2026,8,10),
    schedule="@daily",
    catchup=False,
    tags=['hooks'],
)
def my_dag1():
    @task
    def read():
        file_path = "/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets.xlsx"
        df=pd.read_excel(file_path)
        return file_path

    @task(outlets=[ticket_data])
    def filt_data(file_path):
        df=pd.read_excel(file_path)
        filter_data=df[df['status']=='Open']
        out_path = "/Users/prse/PycharmProjects/Airflow_Prac/data/out_tickets.xlsx"
        filter_data.to_excel(out_path,index=False)
        return out_path


    r=read()
    f=filt_data(r)

    update_asset = Asset(r)
    r>>f
my_dag1()

@dag(
    dag_id='Producer/Consumer_DAG',
    start_date=datetime.datetime(2026,9,10),
    schedule=[],
    catchup=False,
    tags=['hooks'],
)
def my_dag2():
    @task
    def read():
        file_path = "/Users/prse/PycharmProjects/Airflow_Prac/data/out_tickets.xlsx"
        df=pd.read_excel(file_path)
        hook=SqliteHook(sqlite_conn_id='mysqlite')
        hook.run("""
        create table if not exists  ticket_summary
        (
                agent_id INTEGER,
                region VARCHAR(30),
                ticket_count INTEGER
        )
        """)
        return file_path
    @task
    def agg_data(file_path):
        df = pd.read_excel(file_path)
        data_agg = ( df.groupby(["agent_id", "region"]) .size() .reset_index(name="ticket_count") )
        return {'data_agg':data_agg.to_dict("records")}
    @task
    def write_res(data):
        data_agg=pd.DataFrame(data['data_agg'])
        hook=SqliteHook(sqlite_conn_id='mysqlite')
        rows=list(data_agg[['agent_id','region','ticket_count']].itertuples(index=False,name=None))
        hook.insert_rows(
            table='ticket_summary',
            rows=rows,
            target_fields=[
                'agent_id',
                'region',
                'ticket_count'
            ],replace=True
            )

    r=read()
    x=agg_data(r)
    wr=write_res(x)
    r>>x>>wr

my_dag2()


