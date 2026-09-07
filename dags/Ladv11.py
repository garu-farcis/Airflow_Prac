"""SqliteHook + TaskFlow API
   Rewrite question 1 using only @dag and @task.
   - One @task uses SqliteHook to create the table (if not exists)
   - Another @task reads the Excel and bulk-inserts the rows
   - A final @task queries the number of Critical tickets from the database
     and prints it.
     Create a table called ticket_summary (ticket_id, status, region, priority)
   - Write a PythonOperator that reads support_tickets.xlsx and inserts
     only the Open + Escalated tickets into that table using the hook.
   - Add a second task that runs a SELECT COUNT(*) via the same hook
"""

from airflow.sdk import dag, task
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

import pandas as pd
import datetime
from datetime import timedelta


file_path = "/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets.xlsx"


@dag(
    dag_id="SqliteHook_TaskFlow_API",
    start_date=datetime.datetime(2026, 8, 7),
    schedule="@daily",
    catchup=False,
    tags=["hooks"],
)
def my_dag():

    @task
    def create_table():

        hook = SqliteHook(sqlite_conn_id="mysqlite")

        hook.run("""
            CREATE TABLE IF NOT EXISTS ticket_summary
            (
                ticket_id INTEGER PRIMARY KEY,
                status VARCHAR(20),
                region VARCHAR(30),
                priority VARCHAR(30)
            )
        """)


    @task
    def read_table():

        df = pd.read_excel(file_path)

        final_df = df[
            df["status"].isin(["Open", "Escalated"])
        ]

        hook = SqliteHook(
            sqlite_conn_id="mysqlite"
        )

        rows = list(
            final_df[
                ["ticket_id", "status", "region", "priority"]
            ].itertuples(
                index=False,
                name=None
            )
        )

        hook.insert_rows(
            table="ticket_summary",
            rows=rows,
            target_fields=[
                "ticket_id",
                "status",
                "region",
                "priority",
            ],
            replace=True,
        )


    @task
    def count_stats():

        hook = SqliteHook(
            sqlite_conn_id="mysqlite"
        )

        count = hook.get_first(
            """
            SELECT COUNT(*)
            FROM ticket_summary
            WHERE priority = 'Critical'
            """
        )

        res = count[0]

        print(res)

        return res


    cre = create_table()

    red = read_table()

    co_s = count_stats()


    cre >> red >> co_s


my_dag()

