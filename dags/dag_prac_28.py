"""Create a TaskGroup called "priority_checks" that contains three tasks:
   - check_high_priority   → count of High priority tickets
   - check_critical        → count of Critical priority tickets
   - check_low_priority    → count of Low priority tickets
   Each task pushes its count to XCom.
   After the TaskGroup, a final task pulls all three values and prints them."""


from datetime import datetime
import pandas as pd

from airflow.sdk import DAG, task,TaskGroup,Variable
from airflow.providers.standard.operators.python import PythonOperator, get_current_context, BranchPythonOperator,ShortCircuitOperator
from airflow.sdk.exceptions import AirflowSkipException

file_path="/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets-Table 1.csv"
def check_high_priority():
    df=pd.read_csv(file_path)
    h_prio=df[df["priority"]=="High"].value_counts().sum()
    return h_prio
def check_critical():
    df = pd.read_csv(file_path)
    c_prio = df[df["priority"] == "Critical"].value_counts().sum()
    return c_prio
def check_low_priority():
    df = pd.read_csv(file_path)
    l_prio = df[df["priority"] == "Low"].value_counts().sum()
    return l_prio
def final_task():
    # context=get_current_context()
    # print(f"high priority is {context["high_priority"].h_prio}")
    # print(f"high priority is {context["critical_priority"].c_prio}")
    # print(f"high priority is {context["low_priority"].l_prio}")
    ti=get_current_context()["ti"]
    h_count=ti.xcom_pull(task_ids="priority_checks.high_priority")
    critical_count = ti.xcom_pull(task_ids="priority_checks.critical_priority")
    low_count = ti.xcom_pull(task_ids="priority_checks.low_priority")
    print(f"High priority tickets: {h_count}")
    print(f"Critical priority tickets: {critical_count}")
    print(f"Low priority tickets: {low_count}")

with DAG(
    dag_id="taskgroup_test",
    start_date=datetime(2026,8,9),
    schedule="* * * * *",
    catchup=False,
    tags=["support"],
)as dag:
    with TaskGroup(group_id="priority_checks") as priority_checks:
        check_high=PythonOperator(
            task_id="high_priority",
            python_callable=check_high_priority,
        )
        check_mid=PythonOperator(
            task_id="critical_priority",
            python_callable=check_critical,
        )
        check_low=PythonOperator(
            task_id="low_priority",
            python_callable=check_low_priority,
        )
    final_check=PythonOperator(
        task_id="print_details",
        python_callable=final_task,
    )
    priority_checks>>final_check