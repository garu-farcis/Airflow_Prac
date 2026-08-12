"""Build this dependency structure:
   extract → [process_open, process_in_progress, process_resolved] → combine
   - extract reads the Excel file and pushes the full DataFrame (or path) via XCom
   - Each process_* task filters by status and writes its own file
   - combine task merges the three files into one final report"""

from datetime import datetime
import pandas as pd

from airflow.sdk import DAG, task,TaskGroup
from airflow.providers.standard.operators.python import PythonOperator,get_current_context,BranchPythonOperator


file_path="/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets-Table 1.csv"
path="/Users/prse/PycharmProjects/Airflow_Prac/data/"

def extract():
    df=pd.read_csv(file_path)
    return df.to_dict("records")

def process_task(status):
    ti=get_current_context()["ti"]
    data=ti.xcom_pull(task_ids="extract_data")
    my_df=pd.DataFrame(data)
    status_data=my_df[my_df[("status")==status]]
    # filtered_data=my_df.groupby("status")["ticket_id"]
    out_path=f"/Users/prse/PycharmProjects/Airflow_Prac/data/{status}.csv"
    status_data.to_csv(out_path,index=False)


def process_open():
    process_task("Open")
def process_in_progress():
    process_task("In Progress")
def process_resolved():
    process_task("Resolved")

def combine_task():
    open_df = pd.read_csv(f"{path}/Open.csv")
    in_progress_df = pd.read_csv(f"{path}/In Progress.csv")
    resolved_df = pd.read_csv(f"{path}/Resolved.csv")
    final_df = pd.concat([open_df, in_progress_df, resolved_df, ],ignore_index=True, )
    print(final_df)


with DAG(
    dag_id="task_merger",
    start_date=datetime(2026,9,10),
    schedule="* * * * *",
    catchup=False,
    tags=["support"],
)as dag:
    extract_data=PythonOperator(
        task_id="extract",
        python_callable=extract,
    )
    proc_task=PythonOperator(
        task_id="process_task",
        python_callable=process_task,

    )
    process_open_task = PythonOperator(
        task_id="process_open",
        python_callable=process_open,
     )
    process_in_progress_task = PythonOperator(
        task_id="process_in_progress",
        python_callable=process_in_progress,
    )
    process_resolved_task = PythonOperator(
        task_id="process_resolved",
        python_callable=process_resolved,
    )
    com_task=PythonOperator(
        task_id="combine_task",
        python_callable=combine_task,
    )
    extract_data >> [ proc_task,process_open_task, process_in_progress_task, process_resolved_task, ] >> com_task
