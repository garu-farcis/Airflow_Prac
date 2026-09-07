"""BashOperator + SqliteHook combination
   - BashOperator: Convert support_tickets.xlsx to CSV using a python one-liner
     or any CLI tool and save it as data/tickets.csv
   - Downstream PythonOperator: Use SqliteHook to load that CSV into a
     SQLite table called tickets_csv_load (use pandas + hook or executemany)."""


from airflow.sdk import dag,task
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
import pandas as pd
import datetime
from datetime import timedelta

fil_path="/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets.xlsx"

def load_csv():
    hook=SqliteHook(sqlite_conn_id="mysqlite")
    hook.run("""
        CREATE TABLE IF NOT EXISTS tickets_csv_load (
            ticket_id INTEGER,
            status VARCHAR(30),
            region VARCHAR(30),
            priority VARCHAR(30)
        )
    """)
    df=pd.read_csv("/Users/prse/PycharmProjects/Airflow_Prac/data/tikcets.csv")
    rows=df.values.tolist()
    hook.insert_rows(table='tickets_csv_load',rows=rows,target_fields=[
        "ticket_id",
        "status",
        "region",
        "priority"
    ],replace=True)

with DAG(
    dag_id="BashOperator_SqliteHook_combination",
    start_date=datetime.datetime(2026,8,7),
    schedule="@daily",
    catchup=False,
    tags=['hooks'],
) as dag:

    convert_file=BashOperator(
        task_id="convert_file",
        bash_command="""libreoffice --headless \
  --convert-to csv \
  --outdir /Users/prse/PycharmProjects/Airflow_Prac/data \
  /Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets.xlsx """,

    )
    loadcsv=PythonOperator(
        task_id="load_csv",
        python_callable=load_csv,
    )

    convert_file>>loadcsv
