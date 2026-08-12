"""Write a DAG that uses Airflow Variables:

    - Variable "min_satisfaction" = 3
    - Task reads the variable
    - Filters tickets where satisfaction_score < min_satisfaction
      (and score is not null)
    - Saves those low-score tickets to data/low_satisfaction.csv
    - If the filtered DataFrame is empty, raise an AirflowSkipException"""
from datetime import datetime
import pandas as pd

from airflow.sdk import DAG, task,TaskGroup,Variable
from airflow.providers.standard.operators.python import PythonOperator, get_current_context, BranchPythonOperator, \
    ShortCircuitOperator
from airflow.sdk.exceptions import AirflowSkipException

file_path="/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets-Table 1.csv"
path="/Users/prse/PycharmProjects/Airflow_Prac/data/low_satisfaction.csv"

def var_set():
    df=pd.read_csv(file_path)
    Variable.set("min_satisfaction",3)
    val= Variable.get("min_satisfaction")

def filtre_tickets():
    ti=get_current_context()["ti"]
    data=ti.xcom_pull(task_ids="var_setting")
    df=pd.DataFrame(data)
    val= Variable.get("min_satisfaction")
    filtre_tickets=df[df["satisfaction_score"].notna() & (df["satisfaction_score"] <  val)]
    filtre_tickets.to_csv(path,index=False)
    if filtre_tickets.empty:
        raise AirflowSkipException

with DAG(
    dag_id="airflow_variables_sales",
    start_date=datetime(2027,10,9),
    schedule="* * * * *",
    catchup=False,
    tags=["support"],
)as dag:
    var_set=PythonOperator(
        task_id="var_setting",
        python_callable=var_set,
    )

    filt_ticks=PythonOperator(
        task_id="filter_ticktes",
        python_callable=filtre_tickets,
    )
    var_set>>filt_ticks