"""Custom Hook (Intermediate → Advanced)
   Create a custom hook called `ExcelTicketHook` that inherits from `BaseHook`.

   Requirements:
   - __init__ receives `file_path` (default to your data/support_tickets.xlsx)
   - Method `get_tickets(sheet_name="support_tickets", status=None, priority=None)`
     → returns a filtered pandas DataFrame
   - Method `get_open_escalated_count()` → returns integer count
   - Method `save_summary(df, output_path)` → saves DataFrame to CSV/Parquet

   Then write a DAG that uses this custom hook inside two @task / PythonOperator:
   - One task gets only Critical + Escalated tickets
   - Another task calculates and saves a region-wise summary"""

from airflow.sdk import dag,task,BaseHook
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
import pandas as pd
import datetime
import os
fil_path="/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets.xlsx"
out_p="/Users/prse/PycharmProjects/Airflow_Prac/data/region_wise.csv"

class ExcelTicketHook(BaseHook):

    def __init__(self,file_path):
        self.file_path=file_path

    def get_tickets(self,sheet_name="support_tickets", status=None, priority=None):
        f_path=self.file_path
        df=pd.read_excel(f_path,sheet_name=sheet_name)
        if status is not None:
            df = df[df["status"] == status]
        if priority is not None:
            df = df[df["priority"] == priority]
        return df
    def get_open_escalated_count(self,sheet_name="support_tickets"):
        f_path = self.file_path
        df = pd.read_excel(f_path, sheet_name=sheet_name)
        filt_data=df[df['status'].isin(['open','escalated'])]
        count=len(filt_data)
        return count
    def save_summary(self,df, output_path):
        df.to_csv(out_p,index=False)

@dag(
    dag_id="ExcelTicketHook",
    start_date=datetime.datetime(2026,9,10),
    schedule="@monthly",
    catchup=False,
    tags=['hooks'],
)
def my_dag():
    @task
    def check_table():
        hook=ExcelTicketHook(fil_path)
        data = hook.get_tickets( status="Escalated", priority="Critical", )
        return data.to_dict(orient='records')
    @task
    def summary():
        my_object = ExcelTicketHook(fil_path)
        data=my_object.get_tickets()
        region_wise = (data.groupby("region").agg(ticket_count=("ticket_id", "count")).reset_index())
        my_object.save_summary(region_wise,out_p)
        return region_wise.to_dict(orient="records")


    ct=check_table()
    summ=summary()

    ct>>summ
my_dag()


