"""CD Type 2 Implementation in Spanner
   Create a pipeline that maintains Slowly Changing Dimension Type 2:
   - Source system sends daily customer snapshots
   - Detect changes (name, address, status)
   - Close previous version (set end_date + is_current = false)
   - Insert new version with commit timestamp
   - Keep history queryable by effective date"""
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
    @task
    def scd_implement():
        hook = SpannerHook(gcp_conn_id=GCP_CONN_ID)
        client = hook.get_client()
        instance = client.instance(instance_id=INSTANCE_ID)
        database = instance.database(database_id=DATABASE_ID)
        watermark_var = "customer_scd_watermark"
        present_watermark=Variable.get(watermark_var)
        query = """select name, address, status,last_updated from customer_info where last_updated>@present_watermark"""
        params={'present_watermark':present_watermark}
        param_type={'present_watermark':spanner.COMMIT_TIMESTAMP}
        with database.snapshot() as snapshot:
            result=snapshot.execute_sql(query,params=params,param_types=param_type)
            rows=list(result)
            cols=[field.name for field in result.field]
            df=pd.DataFrame(rows,columns=cols)
        col_names=['cust_id','name','address','status','last_updated']
        values=[]
        for _,rows in df.iterrows():
            values.append(
                str(rows['cust_id']),
                str(rows['name']),
                str(rows['address']),
                str(rows['status']),
                spanner.COMMIT_TIMESTAMP

            )
        with database.batch() as batch:
            batch.insert_or_update(
                table='customer_info',
                columns=col_names,
                values=values
            )
        Variable.set(watermark_var,spanner.COMMIT_TIMESTAMP)

    scd=scd_implement()
    spanner_ins >> scd
myspanner_dag()