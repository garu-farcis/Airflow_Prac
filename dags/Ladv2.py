"""Intermediate – Basic @dag + @task
   Write a complete DAG using only TaskFlow API that:
   - Uses @dag decorator with schedule="@daily", catchup=False
   - Has three @task functions:
        extract()      → reads support_tickets sheet and returns DataFrame
        filter_open()  → keeps only status == "Open"
        save()         → saves the filtered data to data/open_tickets.csv
   - Properly chains them: extract → filter_open → save"""

from airflow.sdk import dag, task
from datetime import datetime
import pandas as pd


file_path = "/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets-Table 1.csv"
out_path = "/Users/prse/PycharmProjects/Airflow_Prac/data/open_tickets.csv"


@dag(
    dag_id="etl_taskflow_api",
    start_date=datetime(2027, 9, 8),
    schedule="@daily",
    catchup=False,
    tags=["taskflow"],
)
def my_dag():

    @task
    def extract():
        df = pd.read_csv(file_path)
        return df

    @task
    def filter_open(df):
        filtered = df[df["status"] == "Open"]
        return filtered

    @task
    def save(df):
        df.to_csv(out_path, index=False)

    # TaskFlow automatically creates the dependency:
    # extract → filter_open → save
    data = extract()
    open_tickets = filter_open(data)
    save(open_tickets)


my_dag()

