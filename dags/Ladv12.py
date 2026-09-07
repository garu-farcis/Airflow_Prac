""". Custom Hook + PythonOperator
   Create a simple custom hook called ExcelHook that:
   - Takes a file path in its __init__
   - Has a method get_dataframe(sheet_name="support_tickets")
   Then use this custom hook inside a PythonOperator to:
   - Read the data
   - Filter status == "Escalated"
   - Save the result as data/escalated_only.csv"""

from airflow.sdk import dag,task
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
import pandas as pd
import datetime
from datetime import timedelta

fil_path="/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets.xlsx"

class excel_hook:
    def __init__(self,file_path):
        self.file_path=file_path

    def get_dataframe(self,sheet_name="support_tickets"):
        f_path=self.file_path
        df=pd.read_excel(f_path,sheet_name=sheet_name)
        return df

@dag(
    dag_id="Custom Hook_PythonOperator",
    start_date=datetime(2026,8,7),
    schedule="@daily",
    catchup=False,
    tags=['hooks'],
)
def my_dag():
    @task

    def read_data():
        my_hook=excel_hook(fil_path)
        my_df=my_hook.get_dataframe()
        filter_data=my_df[my_df['status']=='Escalated']
        o_p="/Users/prse/PycharmProjects/Airflow_Prac/data/escalated_only.csv"
        filter_data.to_csv(o_p,index=False)
my_dag()
