"""Write an on_failure_callback function that:
   - Receives the context
   - Prints the DAG id, task id, and execution date
   - Also prints how many Critical tickets existed at the time of failure
     (read the Excel file inside the callback)"""

from datetime import datetime
import pandas as pd

from airflow.sdk import DAG, task,TaskGroup,Variable
from airflow.providers.standard.operators.python import PythonOperator, get_current_context, BranchPythonOperator,ShortCircuitOperator
from airflow.sdk.exceptions import AirflowSkipException

file_path="/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets-Table 1.csv"

def extract():
    df=pd.read_csv(file_path)
    return df.to_dict("data")

def failure_callback(context):
    dag_id = context["dag"].dag_id

    task_id = context["task"].task_id
    execution_date = context["logical_date"]
    df = pd.read_csv(file_path)
    critical_tickets = (df["priority"] == "Critical").sum()
    print(f"DAG ID: {dag_id}")
    print(f"Task ID: {task_id}")
    print(f"Execution date: {execution_date}")
    print(f"Critical tickets at time of failure: {critical_tickets}")


with DAG(
    dag_id="on_call_failure",
    start_date=datetime(2026,9,10),
    schedule="* * * * *",
    catchup=False,
    tags=["support"],
)as dag:
    task2 = PythonOperator(
        task_id="extract_data",
        python_callable=extract,
    )
    task = PythonOperator(
        task_id="print_details",
        python_callable=failure_callback(context=extract()),
        on_failure_callback=True,
    )
    task2>>task