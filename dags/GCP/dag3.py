from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.spanner import SpannerDeployInstanceOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator

# GCP Configuration Constants
GCP_CONN_ID = "google_cloud_default"
PROJECT_ID = "my-gcp-project"
LOCATION = "us-central1"

SPANNER_INSTANCE_ID = "production-spanner-instance"
SPANNER_CONFIG = "regional-us-central1"

BASELINE_NODE_COUNT = 1
PEAK_NODE_COUNT = 3

DATAFLOW_TEMPLATE_GCS_PATH = "gs://my-bucket/templates/gcs_to_spanner_flex_template.json"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="spanner_dataflow_scale_etl",
    default_args=default_args,
    schedule_interval="0 2 * * *",  # Runs daily at 02:00 AM
    catchup=False,
    tags=["spanner", "dataflow", "scaling", "etl"],
) as dag:

    # Step 1: Scale Up Spanner Nodes for High Ingestion Capacity
    scale_up_spanner = SpannerDeployInstanceOperator(
        task_id="scale_up_spanner",
        instance_id=SPANNER_INSTANCE_ID,
        configuration_name=SPANNER_CONFIG,
        node_count=PEAK_NODE_COUNT,
        project_id=PROJECT_ID,
        gcp_conn_id=GCP_CONN_ID,
    )

    # Step 2: Trigger Dataflow ETL Job (Flex Template)
    run_dataflow_etl = DataflowStartFlexTemplateOperator(
        task_id="run_dataflow_etl",
        project_id=PROJECT_ID,
        location=LOCATION,
        body={
            "launchParameter": {
                "jobName": f"gcs-to-spanner-etl-{datetime.now().strftime('%Y%m%d%H%M%S')}",
                "containerSpecGcsPath": DATAFLOW_TEMPLATE_GCS_PATH,
                "parameters": {
                    "inputGcsPattern": "gs://my-bucket/raw_data/*.json",
                    "instanceId": SPANNER_INSTANCE_ID,
                    "databaseId": "analytics_db",
                    "table": "Orders",
                },
                "environment": {
                    "tempLocation": "gs://my-bucket/temp",
                    "stagingLocation": "gs://my-bucket/staging",
                },
            }
        },
        gcp_conn_id=GCP_CONN_ID,
        wait_for_completion=True,  # Block until the Dataflow job completes
    )

    # Step 3: Scale Down Spanner Nodes back to Baseline
    scale_down_spanner = SpannerDeployInstanceOperator(
        task_id="scale_down_spanner",
        instance_id=SPANNER_INSTANCE_ID,
        configuration_name=SPANNER_CONFIG,
        node_count=BASELINE_NODE_COUNT,
        project_id=PROJECT_ID,
        gcp_conn_id=GCP_CONN_ID,
        trigger_rule="all_done",  # Ensures scale down runs even if Dataflow fails
    )

    # Task Dependency Chain
    scale_up_spanner >> run_dataflow_etl >> scale_down_spanner