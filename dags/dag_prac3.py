"""Create a TaskGroup called "data_quality_checks".
   Inside the group:
   - Check that no null values exist in critical columns (order_id, customer_id, quantity)
   - Check that unit_price > 0
   - Check that order_date is not in the future
   If any check fails, the TaskGroup should fail the DAG."""

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator,get_current_context
from datetime import datetime
import pandas as pd
#
# def data_quality_checks():
#     df=pd.read_csv("/Users/prse/PycharmProjects/Airflow_Prac/data/sales_data.csv")
#     check1=(df["unit_price"]<=0).sum()
#     if check1:
#         raise ValueError("unit price must be more than 0")
#     else:
#         print("unit price more than 0")
#     df["order_date"]=pd.to_datetime(df["order_date"])
#     check2=(df["order_date"]>datetime.now()).sum()
#     if check2:
#         raise ValueError("order date incorrect")
#     else:
#         print("time contrainst test is passed")
#     critical = ["order_id", "customer_id", "quantity"]
#
#     null_count = df[critical].isna().sum().sum()
#     if critical:
#         raise ValueError("null values exist")
#     else:
#         print("no null values ")

# from datetime import datetime
#
# import pandas as pd
#
# from airflow import DAG
# from airflow.operators.python import PythonOperator
from airflow.sdk import TaskGroup


CSV_PATH = "/Users/prse/PycharmProjects/Airflow_Prac/data/sales_data.csv"


def check_null_values():
    df = pd.read_csv(CSV_PATH)

    critical_cols = ["order_id", "customer_id", "quantity"]

    null_count = df[critical_cols].isna().sum().sum()

    if null_count > 0:
        raise ValueError(f"Found {null_count} null values in critical columns.")

    print("No null values found.")


def check_unit_price():
    df = pd.read_csv(CSV_PATH)

    invalid_rows = (df["unit_price"] <= 0).sum()

    if invalid_rows > 0:
        raise ValueError(f"{invalid_rows} rows have invalid unit_price.")

    print("All unit prices are valid.")


def check_order_date():
    df = pd.read_csv(CSV_PATH)

    df["order_date"] = pd.to_datetime(df["order_date"])

    future_orders = (df["order_date"] > pd.Timestamp.now()).sum()

    if future_orders > 0:
        raise ValueError(f"{future_orders} future order dates found.")

    print(" No future dates found.")


with DAG(
    dag_id="data_quality_checks_dag",
    start_date=datetime(2028, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["quality"],
) as dag:

    with TaskGroup(group_id="data_quality_checks") as quality_group:

        check_nulls = PythonOperator(
            task_id="check_null_values",
            python_callable=check_null_values,
        )

        check_price = PythonOperator(
            task_id="check_unit_price",
            python_callable=check_unit_price,
        )

        check_dates = PythonOperator(
            task_id="check_order_date",
            python_callable=check_order_date,
        )

    # Run all checks inside the TaskGroup
    [check_nulls, check_price, check_dates]