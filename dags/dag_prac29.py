"""Write a dynamic task mapping example:

   @task
   def get_regions():
       # return list of unique regions from the Excel file

   @task
   def process_region(region: str):
       # filter tickets for that region
       # calculate: total tickets, open tickets, avg satisfaction
       # save to data/region_{region}.csv
       # return a dict with the metrics

   regions = get_regions()
   results = process_region.expand(region=regions)"""

from datetime import datetime
import pandas as pd

from airflow.sdk import DAG, task,TaskGroup,Variable
from airflow.providers.standard.operators.python import PythonOperator, get_current_context, BranchPythonOperator,ShortCircuitOperator
from airflow.sdk.exceptions import AirflowSkipException

file_path="/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets-Table 1.csv"


@task
def get_regions():
    df=pd.read_csv(file_path)
    unique_regions=df["region"].dropna().unique().tolist()
    return unique_regions
# return list of unique regions from the Excel file

@task
def process_region(region: str):
    # ti=get_current_context()["ti"]
    df=pd.read_csv(file_path)
    regions=df[df["region"]==region]
    regions = get_regions()
    total_tickets = len(regions)
    open_tickets = (regions["status"] == "Open").sum()
    avg_satisfaction = regions["satisfaction_score"].mean()
    # df["region"]=regions
    # tickts_by_reg=df.groupby("region")["ticket_id"].agg(
    #     total_tickets=("ticket_id","count"),
    #     open_tickets=("status",lambda x:(x=="Open")),
    #     avg_sat=("satisfaction_score","mean")
    # ).reset_index(name="region")
    return { "region": region, "total_tickets": total_tickets, "open_tickets": open_tickets, "avg_satisfaction": avg_satisfaction, }

# filter tickets for that region
# calculate: total tickets, open tickets, avg satisfaction
# save to data/region_{region}.csv
# return a dict with the metrics

with DAG(
    dag_id="dynamic_magic",
    start_date=datetime(2026,9,10),
    schedule="* * * * *",
    catchup=False,
    tags=["support"],
)as dag:
    extract_region=get_regions()
    results = process_region.expand(region=extract_region)
