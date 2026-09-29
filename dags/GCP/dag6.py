"""Full Refresh ETL with Commit Timestamp
   Build a TaskFlow DAG that:
   - Extracts customer metrics from a source (mock BigQuery or GCS CSV)
   - Transforms the data in pandas (calculate average order value, tier)
   - Writes the result to Spanner using insert_or_update + "spanner.commit_timestamp()"
   - Handles empty source data gracefully
   - Logs the number of rows upserted"""


from datetime import datetime,timedelta
from airflow.sdk import dag,task
from airflow.providers.google.cloud.operators.spanner import SpannerHook,SpannerDeployInstanceOperator,SpannerQueryDatabaseInstanceOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import pandas as pd

GCP_CONN_ID='google_cloud_default'
INSTANCE_ID='{{var.value.gcp_instance_id}}'
PROJECT_ID='{{var.value.gcp_project_id}}'
DATABASE_ID='{{var.value.gcp_database_id}}'

DEFAULT_ARGS={
    "owner":"DAAS",
    "email":"seshfg@xya.com",
    "email_on_failure":True,
    "retries":2,
    "retry_delay":timedelta(minutes=1)
}

@dag(
    dag_id='refresh_spanner_etl',
    start_date=datetime(2026,9,10),
    schedule="@daily",
    default_args=DEFAULT_ARGS,
    catchup=False,
    tags=['spanner'],
)
def myspanner_dag():
    gcs_data=GCSToBigQueryOperator(
        task_id='extract_data',
        gcp_conn_id=GCP_CONN_ID,
        bucket='{{var.value.gcs_data_bucket}}',
        source_objects=['data/2026/sales_*.csv'],
        source_format='CSV',
        destination_project_dataset_table ='my_project.my_dataset.cust_info',
        skip_leading_rows=1,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
    )
    @task(task_id='transform_data')
    def transform():
        hook=BigQueryHook(gcp_conn_id=GCP_CONN_ID)
        client=hook.get_client(project_id=PROJECT_ID)
        query=[""" select * from `{PROJECT_ID}.my_dataset.cust_info`"""]
        df=client.query(query).to_dataframe()
        if df.empty:
            print("No records found")
            return


        # project_id=hook.project_id

        # instance=client.instance(instance_id=INSTANCE_ID)
        # database=instance.database(database_id=DATABASE_ID)


