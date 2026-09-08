"""Custom Sensor (Advanced)
   Create a custom sensor `NewCriticalTicketSensor` that inherits from `BaseSensorOperator`.

   Behaviour:
   - It should poke the support_tickets.xlsx (or a new_tickets file)
   - Return True only when there is at least one ticket that is:
        priority == "Critical" AND status in ["Open", "Escalated"]
   - Support poke_interval and timeout
   - Optionally accept a `min_count` parameter (default=1)

   Then build a small DAG:"""

from airflow.sdk import dag,task,BaseSensorOperator
from airflow import DAG
import datetime
import pandas as pd
import os

fil_path="/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets.xlsx"

class NewCriticalTicketSensor(BaseSensorOperator):
    def __init__(self,file_path,priority=None,status=None,min_count=1,**kwargs):
        super().__init__(**kwargs)
        self.file_path=file_path
        self.priority=priority
        self.status=status
        self.min_count=min_count


    def poke(self,context):
        if os.path.exists(self.file_path):
            df = pd.read_excel(self.file_path)
            if self.priority is not None:
                df=df[df['priority']==self.priority]
            if self.status is not None:
                df=df[df['status']].isin(self.status)
            if len(df)>=self.min_count:
                return True
        return False

@dag(
    dag_id="Custom_Sensor",
    start_date=datetime.datetime(2026,9,10),
    schedule="@monthly",
    catchup=False,
    tags=['hooks'],
)
def my_dag():

    custome_sen=NewCriticalTicketSensor(
        task_id="custom_sensor_check",
        file_path=fil_path,
        status=["Open", "Escalated"],
        priority="Critical",
        poke_interval=30,
        timeout=30,

    )

my_dag()