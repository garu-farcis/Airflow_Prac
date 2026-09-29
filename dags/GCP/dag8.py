"""Batch Mutation Performance Challenge
   You receive 500k rows that need to be loaded into Spanner daily.
   Write a Composer task that:
   - Splits the DataFrame into optimal batch sizes
   - Uses database.batch() with insert_or_update
   - Implements exponential backoff on Aborted/DeadlineExceeded errors
   - Reports total successful mutations and failed batche"""

from datetime import datetime,timedelta
import time
from airflow.models import Variable
from airflow.sdk import dag,task
from airflow.providers.google.cloud.hooks.spanner import SpannerHook
from google.cloud import spanner
from airflow.providers.google.cloud.operators.spanner import SpannerDeployInstanceOperator
import pandas as pd
from google.api_core.exceptions import Aborted, DeadlineExceeded

GCP_CONN_ID='google_cloud_default'
INSTANCE_ID='{{var.value.gcp_instance_id}}'
PROJECT_ID='{{var.value.gcp_project_id}}'
DATABASE_ID='{{var.value.gcp_database_id}}'
DEFAULT_ARGS={
    "owner":"DAAS",
    "email":"seshfg@xya.com",
    "email_on_failure":True,
    "retries":1,
    "max_retries":5,
    "retry_delay":timedelta(minutes=1),
    "retry_exponential_backoff":True
}

@dag(
    dag_id='incremental_spanner_etl',
    start_date=datetime(2026,9,10),
    schedule="@daily",
    default_args=DEFAULT_ARGS,
    catchup=False,
    tags=['spanner'],
)
def myspanner_dag():

    spanner_ins=SpannerDeployInstanceOperator(
        task_id='spanner_instance',
        gcp_conn_id=GCP_CONN_ID,
        configuration_name='regional-us-central1',
        node_count=1,

    )

    @task(task_id='read_and_split')
    def read_split_update():
        # read
        hook=SpannerHook(gcp_conn_id=GCP_CONN_ID)
        client=hook.get_client()
        instance=client.instance(instance_id=INSTANCE_ID)
        database=instance.database(database_id=DATABASE_ID)
        query="""select * from customer_info"""
        watermark_var = "customer_scd_watermark"
        with database.snapshot() as snapshot:
            results=snapshot.execute_sql(query)
            rows=list(results)
            cols=[field.name for field in results.field]
            # df=pd.DataFrame(rows,columns=cols)
            batch_size=1000
            batches=[]
            # split
            for each_batch in range(0,len(rows),batch_size):
                batches.append(rows[each_batch:each_batch+batch_size])

        # update
        col_names=['cust_id', 'cust_name', 'total_orders', 'total_amount', 'last_updated']
        failed_batches = 0
        max_retries = 5
        base_delay = 1
        count=0
        for b in batches:
            success=False
            for attempt in range(max_retries):
                try:
                    with database.batch() as batch:
                        batch.insert_or_update(
                            table='customer_info',
                            columns=col_names,
                            values=b
                        )
                        count+=len(b)
                        print("batch succesfull")
                        success=True
                    print(
                        f"Batch successful: "
                        f"{len(b)} mutations"

                    )
                    break
                except (Aborted, DeadlineExceeded) as exc:

                    if attempt == max_retries - 1:
                        print(
                            f"Batch failed after "
                            f"{max_retries} attempts: {exc}"
                        )
                        break

                    delay = base_delay * (2 ** attempt)

                    print(
                        f"Batch failed "
                        f"(attempt {attempt + 1}). "
                        f"Retrying in {delay} seconds..."
                    )

                    time.sleep(delay)

            if not success:
                failed_batches += 1


    r=read_split_update()

    spanner_ins>>r
myspanner_dag()