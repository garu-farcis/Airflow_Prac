"""Create a DAG that uses XCom to pass a list of customer_ids who have
    more than one order.
    Downstream task should filter the original CSV and write only those customers’
    orders to a new file."""

from datetime import datetime,timedelta

import pandas as pd


from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator,ShortCircuitOperator,get_current_context
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import TaskGroup
from pandas.core.common import maybe_make_list

OUTPUT_FILE = "/Users/prse/PycharmProjects/Airflow_Prac/data/filtered_data.csv"
def xcom_checks():
    df=pd.read_csv("/Users/prse/PycharmProjects/Airflow_Prac/data/sales_data.csv")
    order_counts=(df["customer_id"]).value_counts()
    my_list=order_counts[order_counts>1].index.to_list()
    return my_list
def task_second():
    ti=get_current_context()["ti"]
    cust_id=ti.xcom_pull(task_ids="xcom_checks")
    df=pd.read_csv("/Users/prse/PycharmProjects/Airflow_Prac/data/sales_data.csv")
    filt_df=df[df["customer_id"].isin(cust_id)]
    filt_df.to_csv(OUTPUT_FILE,index=False)

with DAG(
    dag_id="pass_xcom",
    start_date=datetime(2026,8,9),
    schedule="* * * * *",
    catchup=False,
    tags=["sales"],
)as dag:
    first_task=PythonOperator(
        task_id="xcom_checks",
        python_callable=xcom_checks,
    )
    second_task=PythonOperator(
        task_id="xcom_pull",
        python_callable=task_second,
    )

    first_task>>second_task