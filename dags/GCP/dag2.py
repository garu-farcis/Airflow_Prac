from datetime import datetime, timedelta
import json
from airflow import DAG
from airflow.sdk import task
from airflow.providers.google.cloud.hooks.spanner import SpannerHook
from airflow.providers.google.cloud.operators.spanner import SpannerDeployDatabaseInstanceOperator

# Define pipeline constants
GCP_CONN_ID = "google_cloud_default"
INSTANCE_ID = "production-spanner-instance"
DATABASE_ID = "analytics_db"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="spanner_etl_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "spanner", "gcp"],
) as dag:

    # Step 1: Ensure Target Table Schema Exists
    create_schema_task = SpannerDeployDatabaseInstanceOperator(
        task_id="create_schema",
        gcp_conn_id=GCP_CONN_ID,
        instance_id=INSTANCE_ID,
        database_id=DATABASE_ID,
        ddl_statements=[
            """
            CREATE TABLE IF NOT EXISTS CustomerMetrics (
                CustomerId STRING(36) NOT NULL,
                TotalOrders INT64 NOT NULL,
                TotalSpent FLOAT64 NOT NULL,
                LastUpdated TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)
            ) PRIMARY KEY (CustomerId)
            """
        ],
    )

    # Step 2: Extract Task
    @task
    def extract_raw_data() -> str:
        """Simulates extracting raw order records from upstream sources."""
        raw_orders = [
            {"customer_id": "cust-001", "amount": 120.50, "status": "COMPLETED"},
            {"customer_id": "cust-002", "amount": 45.00, "status": "COMPLETED"},
            {"customer_id": "cust-001", "amount": 80.00, "status": "COMPLETED"},
            {"customer_id": "cust-003", "amount": 210.00, "status": "CANCELLED"},
        ]
        return json.dumps(raw_orders)

    # Step 3: Transform Task
    @task
    def transform_data(raw_data_str: str) -> list[dict]:
        """Aggregates valid customer order totals."""
        orders = json.loads(raw_data_str)
        aggregated = {}

        for order in orders:
            if order["status"] != "COMPLETED":
                continue

            cid = order["customer_id"]
            if cid not in aggregated:
                aggregated[cid] = {"total_orders": 0, "total_spent": 0.0}

            aggregated[cid]["total_orders"] += 1
            aggregated[cid]["total_spent"] += order["amount"]

        transformed_records = [
            {
                "CustomerId": cid,
                "TotalOrders": metrics["total_orders"],
                "TotalSpent": metrics["total_spent"],
            }
            for cid, metrics in aggregated.items()
        ]
        return transformed_records

    # Step 4: Load Task (Writing to Cloud Spanner)
    @task
    def load_to_spanner(records: list[dict]):
        """Upserts transformed records into Google Cloud Spanner."""
        if not records:
            return

        hook = SpannerHook(gcp_conn_id=GCP_CONN_ID)
        client = hook.get_client()
        instance = client.instance(INSTANCE_ID)
        database = instance.database(DATABASE_ID)

        columns = ["CustomerId", "TotalOrders", "TotalSpent"]
        values = [
            [r["CustomerId"], r["TotalOrders"], r["TotalSpent"]]
            for r in records
        ]

        # Perform an insert_or_update mutation batch in Spanner
        with database.batch() as batch:
            batch.insert_or_update(
                table="CustomerMetrics",
                columns=columns,
                values=values,
            )

    # Define execution flow
    raw_data = extract_raw_data()
    transformed = transform_data(raw_data)

    create_schema_task >> raw_data
    transformed >> load_to_spanner(transformed)