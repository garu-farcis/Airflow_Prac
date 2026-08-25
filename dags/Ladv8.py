"""Advanced – Full TaskFlow pipeline with error handling
   Build this complete pipeline using only @dag and @task:

   extract → clean → enrich → load → notify

   - extract: read Excel
   - clean: drop rows with null ticket_id or status
   - enrich: add column "is_critical" (True/False)
   - load: save as data/cleaned_tickets.parquet (or csv)
   - notify: print how many critical tickets were found

   Add retries=2 on the `clean` task and an on_failure_callback
   on the `load` task."""
from  airflow import DAG
from airflow.sdk import TaskGroup,dag,task
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import timedelta,datetime
import pandas as pd


file_path="/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets-Table 1.csv"
out_path= "/Users/prse/PycharmProjects/Airflow_Prac/data/cleaned_tickets.csv"


def load_failure_callback(context):
    print("LOAD TASK FAILED!")
@dag(
    dag_id="reusable_logic",
    start_date=datetime(2026,8,9),
    schedule="@daily",
    tags=['Taskflow'],
    catchup=False,
)
def my_dag():
    @task
    def extract():
        df=pd.read_csv(file_path)
        return df
    @task
    def clean(df):
        new_df=df.dropna(subset=["ticket_id", "status"])
        print(new_df)
        return new_df
    @task
    def enrich(df):
        new_df=(df['Status']=='Critical')
        df['is_critical']=new_df
        print(df)
        crit_ticks=(df['Status']=='Critical').sum()
        return crit_ticks

    @task(on_failure_callback=load_failure_callback)
    def load(df):
        df.to_csv(out_path,index_label=True)

    @task
    def notify(crit_tks):
        print(f"{crit_tks} critical tickets were found")

    ex=extract()
    cl=clean(ex)
    en=enrich(cl)
    ld=load(ex)
    nt=notify(en)

    ex>>cl>>en>>ld>>nt

my_dag()