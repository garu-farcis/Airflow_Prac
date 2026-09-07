"""Use SqliteHook to connect to your local airflow.db (or a new practice.db).
   - Create a table called ticket_summary (ticket_id, status, region, priority)
   - Write a PythonOperator that reads support_tickets.xlsx and inserts
     only the Open + Escalated tickets into that table using the hook.
   - Add a second task that runs a SELECT COUNT(*) via the same hook
     and pushes the result to XCom."""

from  airflow import DAG
from airflow.sdk import TaskGroup,dag,task
from airflow.providers.standard.operators.python import PythonOperator,get_current_context
from airflow.providers.standard.operators.bash import BashOperator
from datetime import timedelta,datetime
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

file_path="/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets.xlsx"

def create_table():
    hook=SqliteHook(sqlite_conn_id='my_sqlite')
    conten=hook.run("create table if not exists ticket_summary ("""
        "ticket_id int primary key,"
        "status varchar(30),"
        "region varchar(30),"
        "priority varchar(40))"""
    )
    out_path = "/Users/prse/PycharmProjects/Airflow_Prac/data/tick_summ.csv"
    with open(out_path,'w') as f:
        f.write(conten)
    # new_df.to_csv(out_path,index_label=False)
    return out_path


def read_table():
    ti=get_current_context()['ti']
    df = pd.read_excel(file_path)
    my_data=df['status'].isin(['Open','Escalated'])
    final_d=df[my_data]
    hook=SqliteHook(sqlite_conn_id='my_sqlite')
    for _, row in final_d.iterrows():
        hook.run(
            """
            INSERT OR REPLACE INTO ticket_summary
            (ticket_id, status, region, priority)
            VALUES (?, ?, ?, ?)
            """,
            parameters=(
                row["ticket_id"],
                row["status"],
                row["region"],
                row["priority"],
            ),
        )

def task_coll():
    ti = get_current_context()['ti']
    hook=SqliteHook(sqlite_conn_id='my_sqlite')
    cont=hook.get_first("""select count(*) from ticket_summary""")
    count=cont[0]
    ti.xcom_push(key='entire_summary',value=count)
    return count

with DAG (
        dag_id="sqlite_hook",
        start_date=datetime(2027, 8, 9),
        schedule="@daily",
        catchup=False,
        tags=['hooks']
) as dag:
        create=PythonOperator(
         task_id='create_table',
         python_callable=create_table,
        )
        read=PythonOperator(
        task_id='read_table',
        python_callable=read_table,
        )

        task=PythonOperator(
            task_id='task_coll',
            python_callable=task_coll,
        )

        create>>read>>task


