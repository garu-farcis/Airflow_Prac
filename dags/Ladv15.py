"""Multiple Hooks in one DAG (File + DB)
   Create a DAG that:
   - Uses a custom or simple file check (or FileSensor)
   - Then uses SqliteHook to delete old records from ticket_summary
     where status IN ('Resolved', 'Closed')
   - Then re-inserts the latest Open/Escalated tickets from the Excel file.
   Practice proper connection cleanup (hook.get_conn() + closing)."""

from airflow.sdk import dag,task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
import pandas as pd
import datetime
import os

fil_path="/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets.xlsx"

@dag(
    dag_id="Multiple_Hooks",
    start_date=datetime.datetime(2026,8,8),
    schedule="@monthly",
    catchup=False,
    tags=['hooks'],
)
def my_dag():
    @task.sensor(
        task_id="checking_file",retries=3,poke_interval=30,mode="reschedule",
    )
    def check_stats():
        if not os.path.exists(fil_path):
            return False
        hook=SqliteHook(sqlite_conn_id='mysqlite')
        hook.get_conn()
        hook.run("""delete * from ticket_summary  where status in ('Resolved', 'Closed')""")
        df=pd.read_excel(fil_path)
        latest=df[df['status'].isin(['Open','Escalated'])]
        rows=latest.values.tolist()
        hook.insert_rows(table='ticket_summary',rows=rows,target_fields=["ticket_id","status","region","priority"],replace=True)
        return True

my_dag()

