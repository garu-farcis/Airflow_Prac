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
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from airflow.providers.google.cloud.operators.spanner import SpannerDeployInstanceOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud import spanner

from dags.GCP.clean_table import clean_table
from dags.GCP.error_table import error_table

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
        bucket='{{var.value.gcs_data_bucket}}',
        gcp_conn_id=GCP_CONN_ID,
        source_objects='{{var.value.gcs_path_id}}',
        source_format='CSV',
        compression=None,
        destination_project_dataset_table='{{var.value.destination_path}}',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_EMPTY'
    )

    @task(task_id='data_quality_gate')
    def data_quality_gate():
        hook=BigQueryHook(gcp_conn_id=GCP_CONN_ID)
        data=hook.get_client(project_id=PROJECT_ID)
        query=f"select * from `{PROJECT_ID}.my_dataset.customer_metrics` "
        df=data.query(query).to_dataframe()
        # checks
        clean_values=[]
        error_values=[]
        # check null status
        mask_null=df['status'].isnull()
        count_null=mask_null.sum()
        for _,rows in df[mask_null].iterrows():
            error_values.append(
                {
                    "cust_id": int(rows["cust_id"]),
                    "error_type": "NULL_VALUE",
                    "error_message": "status cannot be NULL",
                    "error_column": "status",
                    "error_value": None
                }
            )
        mask_na=df['cust_spending']<0
        count_na=mask_na.sum()
        for _,rows in df[mask_na].iterrows():
            error_values.append(
                {
                    "cust_id":int(rows["cust_id"]),
                    "error_type": "Negative_VALUE",
                    "error_message": "cust_spending cannot be negative",
                    "error_column": "cust_spending",
                    "error_value": str(rows["total_spent"])
                }
            )
        if count_na>0 or count_null>0:
            raise ValueError("status cant be null and spending cant be negative")

        mask_clean_vals=~(mask_na | mask_null)
        for _,row in df[mask_clean_vals].iterrows():
            clean_values.append({
               "cust_id": int(row["cust_id"]),
                "cust_name":str(row["cust_name"]),
                "total_orders":int(row["total_orders"]),
                "total_spent": float(row["total_spent"]),
            })

        # updating clean values to clean_table
        for clean in clean_values:
            clean_table(
                database=DATABASE_ID,
                pipeline_name="customer_scd2",
                run_id="airflow_run_id",
                source_file="gs://bucket/customer.csv",
                cust_id=clean["cust_id"],
                cust_name=clean["cust_name"],
                total_orders=clean["total_orders"],
                total_spent=clean["total_spent"],
            )
        # updating error values to error_table
        for error in error_values:
            error_table(
                database=DATABASE_ID,
                pipeline_name="customer_scd2",
                run_id="airflow_run_id",
                source_file="gs://bucket/customer.csv",
                cust_id=error["cust_id"],
                error_type=error["error_type"],
                error_message=error["error_message"],
                error_column=error["error_column"],
                error_value=error["error_value"],
            )
        print("data quality gate passed")
    dc=data_quality_gate()
    spanner_ins>>get_data>>dc
myspanner_dag()