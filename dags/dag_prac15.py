"""Advanced: Dynamic task mapping
    - Read the unique list of regions from the CSV
    - Use .expand() (or dynamic task mapping) so that one mapped task is created
      per region.
    - Each mapped task should calculate the total revenue for its region and
      return it via XCom.
    - A final task collects all results and prints a summary."""

from datetime import datetime
import pandas as pd

from airflow.sdk import DAG, task
from airflow.providers.standard.operators.python import PythonOperator,get_current_context

file_path="/Users/prse/PycharmProjects/Airflow_Prac/data/test_result_data.csv"
@task
def region_list():
    df=pd.read_csv(file_path)
    region=df["region"].unique().tolist()
    # region_list.expand(regions=region)
    # return region_list(region)
    return region
@task
def calc_rev(regions):
    ti=get_current_context()["ti"]
    # region=ti.xcom_pull(task_ids="region_lists")
    df=pd.read_csv(file_path)
    region_df = df[
        df["region"] == regions
        ]
    region_df["total_rev"]=region_df["quantity"]*region_df["unit_price"]
    total_rev=region_df["total_rev"].sum()
    # calc_rev.expand(regions=region,total_revenue=df["total_rev"])
    # return calc_rev(region,df["total_rev"])
    return {
        "region": regions,
        "total_rev": total_rev,
    }

@task
def summary(results):
    for r in results:
        print(r["region"],r["total_rev"])

with DAG(
    dag_id="dynamic_region_revenue",
    start_date=datetime(2026, 8, 10),
    schedule="@daily",
    catchup=False,
    tags=["sales", "dynamic-mapping"],
) as dag:

    regions=region_list()
    rev_cal=calc_rev.expand(regions=regions)
    summarys=summary(rev_cal)