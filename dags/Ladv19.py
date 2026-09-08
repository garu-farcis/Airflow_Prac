"""Custom Trigger + Deferrable Operator (Advanced)
   Create a custom trigger `TicketCountTrigger` (inherits from `BaseEventTrigger` or `BaseTrigger`)
   and a corresponding deferrable operator.

   Goal:
   - The operator should defer until the number of "Escalated" tickets
     in the Excel file reaches a threshold (e.g. >= 3)
   - Use the trigger to asynchronously check the condition
   - Once the condition is met, the operator resumes and pushes the current
     escalated count to XCom

   Bonus: Make the threshold configurable via the operator parameters."""
from airflow import DAG
from airflow.triggers.base import BaseEventTrigger
import datetime
from airflow.sdk import BaseOperator
import pandas as pd
from airflow.triggers.base import TriggerEvent
import asyncio

file_p="/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets.xlsx"

class TicketCountTrigger(BaseEventTrigger):
    def __init__(self,file_path,threshold=3,**kwargs):
        super().__init__(**kwargs)
        self.file_path=file_path
        self.threshold=threshold

    async def run(self):
        while True:

            f_path=self.file_path
            df=pd.read_excel(f_path)
            count_ele=df[df['status']=='Escalated'].sum()
            if count_ele>=self.threshold:
                yield TriggerEvent({
                    "status":"success",
                    "count":int(count_ele)
                }

                )
                return
            await asyncio.sleep(30)

class TicketCountOperator(BaseOperator):
    def __init__(self,file_path,threshold=3,**kwargs):
        super().__init__(**kwargs)
        self.file_path = file_path
        self.threshold = threshold

    def execute(self,context):
        self.defer(trigger=TicketCountTrigger(
            file_path=self.file_path,
            threshold=self.threshold,
        ),method_name='execute_complete')
    def execute_complete(self,context,event=None):
        count=event['count']
        print(f"Escalated tickets reached: {count}")
        context["ti"].xcom_push(
            key="element_count",
            value=count
        )

with DAG(
    dag_id="Custom_Trigger",
    start_date=datetime.datetime(2026,9,10),
    schedule="@monthly",
    catchup=False,
    tags=['hooks'],
) as dag:
    my_trigger = TicketCountOperator(
        task_id="ticket_trigger",
        file_path=file_p,
        threshold=3,
    )

