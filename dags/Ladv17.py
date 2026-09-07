"""Custom Operator (Advanced)
   Create a custom operator `TicketFilterOperator` that inherits from `BaseOperator`.

   It should accept these parameters:
   - file_path
   - status_filter (list)
   - priority_filter (list, optional)
   - output_path

   The operator must:
   - Read the Excel file
   - Apply the filters
   - Save the result to output_path
   - Push the number of rows written to XCom (key="row_count")

   Use this custom operator in a DAG with at least two instances:
   - One filtering status=["Open", "Escalated"]
   - One filtering priority=["Critical"] + status=["Open"]"""

from airflow.sdk import dag,task,BaseOperator
from airflow.providers.standard.operators.python import get_current_context
import datetime
import pandas as pd

fil_path="/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets.xlsx"
out_p="/Users/prse/PycharmProjects/Airflow_Prac/data/cust_operator.csv"


class TicketFilterOperator(BaseOperator):
    def __init__(self,file_path,status_filter,output_path,priority_filter=None,**kwargs):
        super.__init__(**kwargs)
        self.file_path=file_path
        self.status_filter=status_filter
        self.priority_filter=priority_filter
        self.output_path=output_path

    def apply_filter(self,context):
        f_path=self.file_path
        o_p=self.output_path
        df=pd.read_excel(f_path)
        df=df[df['status'].isin(self.status_filter)]
        if self.priority_filter is not None:
            df[df['priority'].isin(self.priority_filter)]

        count=len(df)
        df.to_csv(o_p,index=False)
        context['ti'].xcom_push(key='row_count',value=count)
        return count

@dag(
    dag_id="ticket_filter_operator",
    start_date=datetime.datetime(2026, 8, 7),
    schedule="@daily",
    catchup=False,
    tags=["custom_operator"],
)
def my_dag():

    open_escalated = TicketFilterOperator(
        task_id="open_escalated",
        file_path=fil_path,
        status_filter=["Open", "Escalated"],
        output_path=out_p,
    )

    critical_open = TicketFilterOperator(
        task_id="critical_open",
        file_path=fil_path,
        status_filter=["Open"],
        priority_filter=["Critical"],
        output_path=out_p,
    )


my_dag()
