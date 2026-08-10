"""Schedule a DAG that runs only on weekdays (Mon–Fri).
    Inside it, use a PythonOperator to calculate the top 3 products by revenue
    and push the result as a dictionary to XCom.
    A second task should format that dictionary nicely and print it."""

from datetime import datetime
import pandas as pd

from airflow.sdk import DAG, task
from airflow.providers.standard.operators.python import PythonOperator,get_current_context

file_path="/Users/prse/PycharmProjects/Airflow_Prac/data/test_result_data.csv"

def calc():
    df =pd.read_csv(file_path)
    # sorted_df=df.sort_values(by="total_amount",ascending=False)
    # top_prods=sorted_df["product"].head(3)
    # prod_dict=dict(top_prods)
    # return prod_dict
    product_revenue = (
        df.groupby("product")["total_amount"]
        .sum()
        .sort_values(ascending=False)
        .head(3)
    )

    top_products = product_revenue.to_dict()

    print(f"Top 3 products: {top_products}")

    return top_products

def format_dict():
    ti=get_current_context()["ti"]
    item_dict=ti.xcom_pull(task_ids="calc_rev")
    for k,v in item_dict.items():
        print(f"the dict of items is {k}:{v}")

with DAG(
    dag_id="max_prod_check",
    start_date=datetime(2026,9,6),
    schedule="0 0 * * 1-5",
    catchup=False,
    tags=["sales"],
)as dag:

    calc_topprod=PythonOperator(
        task_id="calc_rev",
        python_callable=calc,
    )
    dict_form=PythonOperator(
        task_id="format_dicts",
        python_callable=format_dict,
    )
    calc_topprod>>dict_form

