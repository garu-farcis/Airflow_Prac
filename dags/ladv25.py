"""Cross-DAG Dependency + TriggerDagRunOperator + Conf Passing (Advanced)
   Create two DAGs:

   DAG 1 – "ticket_ingestion":
   - Reads support_tickets.xlsx + new_tickets sheet
   - Appends new tickets and calculates daily metrics
   - At the end uses TriggerDagRunOperator to trigger DAG 2
   - Passes conf containing: {"open_count": N, "escalated_count": M}

   DAG 2 – "ticket_alerting":
   - Is triggered by DAG 1 (can also have its own schedule)
   - Reads the conf passed from DAG 1
   - If escalated_count ≥ 3 → runs an “alert” path
   - Otherwise runs a “normal summary” path
   - Uses BranchPythonOperator based on the received conf"""
from airflow.sdk import dag, task,Asset
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow import Dataset
import pandas as pd
import datetime
from datetime import timedelta


file_path="/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets-Table 1.csv"
file_path_new_ticks="/Users/prse/PycharmProjects/Airflow_Prac/data/new_tickets-Table 1.csv"

@dag(
    dag_id='ticket_ingestion',
    start_date=datetime.datetime(2026,9,10),
    schedule="@daily",
    catchup=False,
    tags=['hooks'],
)
def my_dag():
    @task
    def read():
        df=pd.read_csv(file_path)
        df1=pd.read_csv(file_path_new_ticks)
        new_df=pd.concat([df,df1])
        out_path = "/Users/prse/PycharmProjects/Airflow_Prac/data/ticket_consolidate.csv"
        new_df.to_csv(out_path,index=False)
        return out_path
    @task
    def daily_metrics(f_path):
        df=pd.read_csv(f_path)
        open_count=(df['status']=='Open').sum()
        esc_count=(df['status']=='Escalated').sum()
        return {
            'open_count':open_count,
            'esc_count':esc_count
        }
    re=read()
    dm=daily_metrics(re)

    my_trigger = TriggerDagRunOperator(
        task_id='trigger_dag_run',
        trigger_dag_id='ticket_alerting',
        conf={
            'open_count': '{{ti.xcom_pull(task_ids="daily_metrics")["open_count"]}}',
            'esc_count': '{{ti.xcom_pull(task_ids="daily_metrics")["esc_count"]}}'
        },
        reset_dag_run=True,
        wait_for_completion=False,
    )
    re>>dm>>my_trigger

@dag(
    dag_id='ticket_alerting',
    start_date=datetime.datetime(2026, 9, 10),
    schedule="@daily",
    catchup=False,
    tags=['hooks'],
)

def my_dag1():

    @task.branch
    def check(**context):
        conf = context["dag_run"].conf or {}
        open_count = conf.get("open_count", 0)
        escalated_count = conf.get("escalated_count", 0)
        open_c=int(open_count)
        esc_c=int(escalated_count)
        if open_c>3:
            return 'send_alert'
        else:
            return 'summary'
    @task
    def send_alert():
        raise ValueError('esc count cannot be more than 3')

    @task
    def summary(**context):
        conf = context["dag_run"].conf or {}
        open_count = conf.get("open_count", 0)
        escalated_count = conf.get("escalated_count", 0)
        open_c = int(open_count)
        esc_c = int(escalated_count)
        print(f'the open cpunt is {open_c}')
        print(f'esc count is {esc_c}')

    ch=check()
    sa=send_alert()
    summ=summary()
    ch>>[sa,summ]

my_dag1()