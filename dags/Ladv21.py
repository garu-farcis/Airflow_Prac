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
from airflow.triggers.base import BaseTrigger
import datetime
from airflow.sdk import BaseOperator,dag,task
import pandas as pd
from airflow.triggers.base import TriggerEvent
import asyncio


file_p="/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets.xlsx"

class WaitForCriticalTicketsTrigger(BaseTrigger):
    def __init__(self,file_path,status,priority,threshold=3, poke_interval=30,
        timeout=None,**kwargs):
        super().__init__(**kwargs)
        self.file_path=file_path
        self.status=status
        self.priority=priority
        self.threshold=threshold
        self.poke_interval=poke_interval
        self.timeout=timeout

    async def run(self):
        while True:
            f_p=self.file_path
            df=pd.read_excel(f_p)
            mask=(df['priority']==self.priority) & (df['status'].isin(self.status))
            count=mask.sum()
            if count>=self.threshold:
                yield TriggerEvent(
                    {
                        'task_status':'success',
                        'no_ticket_ids': df.loc[mask,'ticket_id'].tolist()
                    }

                )
                return
            await asyncio.sleep(30)

class WaitForCriticalTicketsOperator(BaseOperator):
    def __init__(self,file_path,threshold=3,**kwargs):
        super().__init__(**kwargs)
        self.file_path=file_path
        self.threshold=threshold

    def execute(self,context):
        self.defer(trigger=WaitForCriticalTicketsTrigger(
            file_path=self.file_path,
            threshold=3,
            status=["Open", "Escalated"],
            priority='Critical',
            timeout= 10,
            poke_interval=30,
        ),method_name= 'execute_complete')

    def execute_complete(self,context,event=None):
        tick_id=event['no_ticket_ids']
        print(f"the no of ticket ids present are {tick_id}")
        context['ti'].xcom_push(key='no_ticket_ids',value=tick_id)

@dag(
    dag_id='WaitForCriticalTickets',
    start_date=datetime.datetime(2026,9,10),
    schedule="@monthly",
    catchup=False,
    tags=['hooks'],
)
def my_dag():
    my_operator = WaitForCriticalTicketsOperator(
        task_id='critical_ops',
        threshold=3,
        file_path=file_p,
        poke_interval=30,
        timeout=30,
    )

    @task
    def process_tickets(**context):
        ticket_ids = context["ti"].xcom_pull(
            task_ids="critical_ops",
            key="ticket_ids"
        )

        for ticket_id in ticket_ids:
            print(f"Processing ticket: {ticket_id}")


    process_task = process_tickets

    my_operator>>process_task


my_dag()