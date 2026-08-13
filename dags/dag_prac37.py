"""Refactor a single monolithic PythonOperator that does extract+transform+load into
   three separate tasks that share data via XCom. Discuss the size/serialization
   limits of XCom and when you'd switch to an external storage handoff (e.g. writing
   intermediate CSVs to disk or S3) instead."""

from airflow.sdk import task,DAG,TaskGroup
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator, get_current_context, \
    ShortCircuitOperator
import pandas as pd
import datetime
from datetime import timedelta


file_path="/Users/prse/PycharmProjects/Airflow_Prac/data/orders_sample.csv"

def etl():
