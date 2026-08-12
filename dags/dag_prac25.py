""". Coding challenge – Top Agents report:

    Write a PythonOperator that:
    - Groups by agent_id
    - Calculates for each agent:
        • total_tickets
        • resolved_or_closed
        • escalation_rate = escalated / total_tickets
    - Sorts by escalation_rate descending
    - Saves only the top 5 agents to data/top_escalating_agents.csv"""

from datetime import datetime
import pandas as pd

from airflow.sdk import DAG, task,TaskGroup,Variable
from airflow.providers.standard.operators.python import PythonOperator, get_current_context, BranchPythonOperator,ShortCircuitOperator
from airflow.sdk.exceptions import AirflowSkipException

file_path="/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets-Table 1.csv"
@task
def parser():
    df=pd.read_csv(file_path)
    escalated =df[df["status"]=="Escalated"]
    parse_data=df.groupby("agent_id").agg(
        total_tickets=("ticket_id","count"),
        ticks_over=("status",lambda x:x.isin(["Resolved", "Closed"])).sum(),
        escalated=("status", lambda x: (x == "Escalated").sum()
                   ))


    parse_data["escalation_rate"] = parse_data["escalated"]/parse_data["total_tickets"]
    return parse_data

with DAG(
    dag_id="parsing_data",
    start_date=datetime(2027,10,19),
    schedule="* * * * *",
    catchup=False,
    tags=["support"],
)as dag:
    parsing_data=parser()

