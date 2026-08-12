"""One task in your DAG aggregates revenue by channel, and a second task prints a
   summary. The print task should only run if the aggregation succeeded AND it's a
   Monday. Implement this using a mix of ShortCircuitOperator and Jinja templated
   fields."""

from airflow.sdk import task,DAG,TaskGroup
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator, get_current_context, \
    ShortCircuitOperator
import pandas as pd
import datetime

file_path="/Users/prse/PycharmProjects/Airflow_Prac/data/orders_sample.csv"


def agg(**context):
    df = pd.read_csv(file_path)
    df["total_revenue"] = df["quantity"] * df["unit_price"]
    agg_by_channel = df.groupby("channel")["total_revenue"].agg(
        total_revenue=("total_revenue", "sum"),
        total_orders=("order_id", "nunique"),
        total_quantity=("quantity", "sum"),
    ).reset_index()
    # df["order_date"] = pd.to_datetime(df["order_date"]).dt.day
    # run_date = context["logical_date"].day
    context["ti"].xcom_push(key="agg_by_channel", value=agg_by_channel.to_dict(orient="records"))
    return agg_by_channel.to_dict(orient="records")

def is_monday(**context):
    logical_date = context["logical_date"]
    return logical_date.strftime("%A") == "Monday"

def print_summary(**context):
    agg = context["ti"].xcom_pull(task_ids="agg_data", key="agg_by_channel")
    print(f"Monday revenue summary by channel:\n{agg}")

with DAG(
        dag_id="revenue_by_channel_monday_summary",
        start_date=datetime(2024, 1, 1),
        schedule="@daily",
        catchup=False,
        tags=["example"],
) as dag:
    agg_data=PythonOperator(
        task_id="agg_data",
        python_callable=agg,
    )
    only_on_monday = ShortCircuitOperator(
        task_id="only_on_monday",
        python_callable=is_monday,
    )
    summ_data = PythonOperator(
        task_id="summ_data",
        python_callable=print_summary,
    )

    agg_data>>only_on_monday>>summ_data