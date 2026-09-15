"""Deferrable Operator + Custom Trigger (Advanced)
   Create a deferrable operator `WaitForCriticalTicketsOperator` that:
   - Defers execution using a custom Trigger
   - The trigger periodically checks support_tickets.xlsx
   - Resumes only when the number of tickets with
     priority="Critical" AND status in ["Open", "Escalated"] reaches a
     configurable threshold (default ≥ 3)
   - After resuming, pushes the matching ticket_ids to XCom
   - Supports poke_interval and timeout

   Then use this operator in a DAG followed by a task that processes those tickets."""

from airflow import DAG
from airflow.triggers.base import BaseEventTrigger
import datetime
from airflow.sdk import BaseOperator
import pandas as pd
from airflow.triggers.base import TriggerEvent
import asyncio


file_p="/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets.xlsx"

class WaitForCriticalTicketsTrigger(BaseEventTrigger):
    def __init__(self,file_path,status,priority,threshold=3,**kwargs):
        super().__init__(**kwargs)
        self.file_path=file_path
        self.status=status
        self.priority=priority
        self.threshold=threshold

    async def run(self):
        while True:
            file_paths=self.file_path
            df=pd.read_excel(file_paths)
            mask= (df['priority']==self.priority) &  (df['status'].isin(self.status))
            my_count=mask.sum()
            if my_count>=self.threshold:
                yield TriggerEvent(
                    {
                        'status':'success',
                        'count_ticket_ids':df.loc[mask, 'ticket_id'].tolist()
                    }
                )
                return
            await asyncio.sleep(20)

class WaitForCriticalTicketsOperator(BaseOperator):
    def __init__(self,file_path,threshold=3,**kwargs):
        super().__init__(**kwargs)
        self.file_path=file_path
        self.threshold=threshold

    def execute(self,context):
        self.defer(trigger=WaitForCriticalTicketsTrigger
            (file_path=self.file_path,
             threshold=self.threshold,
             status=["Open", "Escalated"],
             priority="Critical",
             ),
                   method_name='execute_complete',
                     )



    def execute_complete(self,context,event=None):
        count=event['count_ticket_ids']
        print(f'ticket ids are {count}')
        context['ti'].xcom_push(key='ticket_ids',value=count)


with DAG(
    dag_id="WaitForCriticalTicketsTrigger",
    start_date=datetime.datetime(2026,9,10),
    schedule="@monthly",
    catchup=False,
    tags=['hooks'],
) as dag:
    my_trigger = WaitForCriticalTicketsOperator(
        task_id="ticket_trigger",
        file_path=file_p,
        threshold=3,
    )

