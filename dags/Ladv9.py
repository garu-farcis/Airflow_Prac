"""Advanced – Full TaskFlow pipeline with error handling
   Build this complete pipeline using only @dag and @task:

   extract → clean → enrich → load → notify

   - extract: read Excel
   - clean: drop rows with null ticket_id or status
   - enrich: add column "is_critical" (True/False)
   - load: save as data/cleaned_tickets.parquet (or csv)
   - notify: print how many critical tickets were found

   Add retries=2 on the `clean` task and an on_failure_callback
   on the `load` task."""

from  airflow import DAG
from airflow.sdk import TaskGroup,dag,task
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import timedelta,datetime
import pandas as pd

from dags.dag_prac10 import failure_callback

file_path="/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets-Table 1.csv"
out_path= "/Users/prse/PycharmProjects/Airflow_Prac/data/cleaned_tickets.csv"

@dag(
    dag_id="etl_full_taskflow_ppeline",
    start_date=datetime(2026,8,9),
    schedule="@daily",
    tags=['Taskflow'],
    catchup=False,
)
def my_dag():
    @task
    def extract():
        df=pd.read_csv(file_path)
        file_path1=file_path
        return file_path1
    @task(retries=2)
    def clean(file_p:str)->str:
        df=pd.read_csv(file_p)
        clean=(pd.dropna(df[df['ticket_id'].isna()])) and (pd.dropna(df[df['status'].isna()]))
        clean.to_csv(out_path,index=False)
        return out_path
    @task
    def enrich(file_p: str) -> str:
        """Add is_critical column (True when status == 'Critical')."""
        df = pd.read_csv(file_p)
        df["is_critical"] = df["status"].str.lower() == "critical"
        df.to_csv(file_p, index=False)  # overwrite intermediate
        print(f"Enriched: {df['is_critical'].sum()} critical tickets")
        return file_p

    @task(on_failure_callback=failure_callback)
    def load(file_p: str) -> str:
        """Save final cleaned data as parquet (or csv)."""
        df = pd.read_csv(file_p)
        # Prefer parquet if the environment has pyarrow/fastparquet
        try:
            df.to_parquet(out_path, index=False)
            final_path = out_path
        except Exception:
            final_path = out_path.replace(".parquet", ".csv")
            df.to_csv(final_path, index=False)
        print(f"Loaded data to {final_path}")
        return final_path

    @task
    def notify(file_p: str):
        """Print how many critical tickets were found."""
        df = pd.read_csv(file_p) if file_p.endswith(".csv") else pd.read_parquet(file_p)
        critical_count = int(df["is_critical"].sum())
        print(f"Found {critical_count} critical tickets")

    # Pipeline
    extracted = extract()
    cleaned = clean(extracted)
    enriched = enrich(cleaned)
    loaded = load(enriched)
    notify(loaded)


my_dag()
