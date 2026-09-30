"""Data Quality Gate before Write
   Build a DAG that:
   - Extracts data from Spanner
   - Runs data quality checks (null rates, referential integrity, value ranges)
   - Writes good records to a clean table
   - Writes bad records + rejection reasons to an error table in Spanner
   - Fails the DAG only if critical checks fail (using Airflow branching)"""

from datetime import datetime, timedelta

from airflow.models import Variable
from airflow.sdk import dag, task
from airflow.providers.google.cloud.hooks.spanner import SpannerHook
from airflow.providers.google.cloud.operators.spanner import SpannerDeployInstanceOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud import spanner
from google.cloud.spanner_v1 import param_types


GCP_CONN_ID = 'google_cloud_default'
INSTANCE_ID = '{{var.value.gcp_instance_id}}'
PROJECT_ID = '{{var.value.gcp_project_id}}'
DATABASE_ID = '{{var.value.gcp_database_id}}'

DEFAULT_ARGS = {
    "owner": "DAAS",
    "email": "seshfg@xya.com",
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True
}


@dag(
    dag_id='incremental_spanner_etl',
    start_date=datetime(2026, 9, 10),
    schedule="@daily",
    default_args=DEFAULT_ARGS,
    catchup=False,
    tags=['spanner'],
)
def myspanner_dag():

    spanner_ins = SpannerDeployInstanceOperator(
        task_id='spanner_instance',
        gcp_conn_id=GCP_CONN_ID,
        configuration_name='regional-us-central1',
        node_count=1,
    )
    get_data=GCSToBigQueryOperator(
        task_id='getting_data_from_gcs',
        gcp_conn_id=GCP_CONN_ID,
        source_objects='{{var.value.gcs_path_id}}',
        source_format='CSV',
        compression=None,
        destination_project_dataset_table='{{var.value.destination_path}}',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_EMPTY'
    )

    @task(task_id='read_data')
