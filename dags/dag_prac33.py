"""Build a DAG using the TaskFlow API (@dag, @task decorators) that computes daily
   revenue (quantity * unit_price) per region from orders_sample.csv and writes one
   output file per region using dynamic task mapping (.expand())."""

from airflow.sdk import DAG,TaskGroup,task
from airflow.providers.standard.operators.python import BranchPythonOperator,PythonOperator,get_current_context
import pandas as pd
import datetime

file_path="/Users/prse/PycharmProjects/Airflow_Prac/data/orders_sample.csv"
out_path="/Users/prse/PycharmProjects/Airflow_Prac/data/"



@DAG(
    dag_id="my_taskflow_dag",
    start_date=datetime.date(2026, 8, 1),
    schedule="0 9 * * *",
    catchup=False,
    tags=["orders"],
)
def my_dag():
    @task
    def calc_rev():
        df = pd.read_csv(file_path)
        df["total_revenue"] = df["quantity"] * df["unit_price"]
        rev_by_region = df.groupby("region")["total_revenue"].sum().reset_index()
        region=df["region"].unique().tolist()
        return {
            "rev_by_region":rev_by_region.to_dicts("records"),
            "region":region
        }
    @task
    def get_regions(rev_by_region):
        return [row["region"] for row in rev_by_region]

    @task
    def write_task(region, rev_by_region):
        df = pd.DataFrame(rev_by_region)
        df = df[df["region"] == region]
        df.to_csv(f"{out_path}/{region}.csv", index=False)

    calc=calc_rev()
    regions=get_regions(rev_by_region=calc)
    write_t=write_task.expand(region=regions)
my_dag()