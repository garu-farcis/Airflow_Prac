"""Incremental Load using Commit Timestamp
   Design a DAG that performs incremental upserts:
   - Reads only records from Spanner where last_updated > previous high-watermark
   - Stores the high-watermark in an Airflow Variable or Spanner control table
   - Merges new/changed records back into the same table
   - Guarantees exactly-once semantics on retries"""

from datetime import datetime,timedelta

from airflow.models import Variable
from airflow.sdk import dag,task
from airflow.providers.google.cloud.hooks.spanner import SpannerHook
from google.cloud import spanner
from airflow.providers.google.cloud.operators.spanner import SpannerDeployInstanceOperator
import pandas as pd

GCP_CONN_ID='google_cloud_default'
INSTANCE_ID='{{var.value.gcp_instance_id}}'
PROJECT_ID='{{var.value.gcp_project_id}}'
DATABASE_ID='{{var.value.gcp_database_id}}'
DEFAULT_ARGS={
    "owner":"DAAS",
    "email":"seshfg@xya.com",
    "email_on_failure":True,
    "retries":1,
    "retry_delay":timedelta(minutes=1)
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
    spanner_instance=SpannerDeployInstanceOperator(
        task_id='spanner_inst',
        gcp_conn_id=GCP_CONN_ID,
        default_args=DEFAULT_ARGS,
        configuration_name="regional-us-central1",
        node_count=1,
    )
    @task(task_id='read_data')
    def read_data(**context):
        hook=SpannerHook(gcp_conn_id=GCP_CONN_ID)
        project_id=hook.project_id
        client=hook.get_client(project_id=project_id)
        instance=client.instance(instance_id=INSTANCE_ID)
        database=instance.database(database_id=DATABASE_ID)
        from airflow.models import Variable
        previous_watermark=Variable.get(watermark_var,default_var="1970-01-01T00:00:00+00:00")
        query="""select * from cust_info where last_updated>@previous_watermark"""
        params={'previous_watermark':previous_watermark}
        param_types={"previous_watermark":spanner.COMMIT_TIMESTAMP}
        with database.snapshot() as snapshot:
            results=snapshot.execute_sql(query)
            rows = list(results)
            cols = [field.name for field in results.field]
            df=pd.DataFrame(rows,columns=cols)
            new_high_watermark=df['last_updated'].max()
        return {"new_df":df.to_dict("records"),
                "new_watermark": new_high_watermark.isoformat()}

    @task(task_id="merge_data")
    def merge_data(data_dict,**context):
        new_df=pd.DataFrame(data_dict['new_df'])
        new_watermark = data_dict["new_watermark"]
        hook = SpannerHook(gcp_conn_id=GCP_CONN_ID)
        client = hook.get_client(project_id=PROJECT_ID)
        instance = client.instance(instance_id=INSTANCE_ID)
        database = instance.database( database_id=DATABASE_ID)
        columns_to_save = ['cust_id', 'cust_name', 'total_orders', 'total_amount', 'last_updated']
        values = []
        for _, row in new_df.iterrows():
            values.append(
                (
                    int(row["cust_id"]),
                    str(row["cust_name"]),
                    int(row["total_orders"]),
                    float(row["total_amount"]),
                    row["last_updated"],
                )
            )
        def write_transaction(trans):
            trans.insert_or_update(
                table='cust_info',
                columns=columns_to_save,
                values=values
            )
        database.run_in_transaction(write_transaction)
        Variable.set(watermark_var,new_watermark)


    rd=read_data()
    md=merge_data(rd)

    spanner_instance>>rd>>md
myspanner_dag()


