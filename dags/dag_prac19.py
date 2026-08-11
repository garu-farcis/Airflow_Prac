"""Write a PythonOperator that:
   - Reads the support_tickets sheet
   - Groups by "region" and calculates:
       • total_tickets
       • open_tickets
       • avg_satisfaction_score (only for Resolved/Closed)
   - Writes the result to data/region_ticket_summary.csv
   Schedule it every Monday at 08:00."""

from datetime import datetime
import pandas as pd

from airflow.sdk import DAG, task,TaskGroup
from airflow.providers.standard.operators.python import PythonOperator,get_current_context,BranchPythonOperator

file_path="/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets-Table 1.csv"
out_path="/Users/prse/PycharmProjects/Airflow_Prac/data/region_ticket_summary.csv"

def read_ticket():
    df=pd.read_csv(file_path)
    summary=df.groupby("region").agg(
        total_tickets=("ticket_id","count"),
        open_tickets=("status",lambda x:(x=="Open").sum()),
        avg_satisfaction_score=("satisfaction_score", "mean"),
    ).reset_index()

    avg_mask=df["status"].isin(["Resolved","Closed"])

    avg_score=avg_mask.groupby("region")["satisfaction_score"].mean().rename("avg_satisfaction_score")


def write_res():
    ti=get_current_context()["ti"]
    data=ti.xcom_pull(task_ids="read_ticket")
    summary=pd.DataFrame(data)
    summary.to_csv(out_path,index=False)

with DAG(
    dag_id="support_ticket_check_test",
    start_date=datetime(2026,8,10),
    schedule="0 8 * * 1",
    catchup=False,
    tags=["support"],

)as dag:
    reac_task=PythonOperator(
        task_id="read_ticket",
        python_callable=read_ticket,
    )
    write_task=PythonOperator(
        task_id="write_task",
        python_callable=write_res()
    )

    reac_task>>write_task