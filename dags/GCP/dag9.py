"""CD Type 2 Implementation in Spanner
   Create a pipeline that maintains Slowly Changing Dimension Type 2:
   - Source system sends daily customer snapshots
   - Detect changes (name, address, status)
   - Close previous version (set end_date + is_current = false)
   - Insert new version with commit timestamp
   - Keep history queryable by effective date"""

from datetime import datetime, timedelta

from airflow.models import Variable
from airflow.sdk import dag, task
from airflow.providers.google.cloud.hooks.spanner import SpannerHook
from airflow.providers.google.cloud.operators.spanner import SpannerDeployInstanceOperator

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

    @task
    def scd_implement():

        hook = SpannerHook(gcp_conn_id=GCP_CONN_ID)

        client = hook.get_client()
        instance = client.instance(instance_id=INSTANCE_ID)
        database = instance.database(database_id=DATABASE_ID)

        watermark_var = "customer_scd_watermark"

        previous_watermark = Variable.get(
            watermark_var,
            default_var="1970-01-01T00:00:00+00:00"
        )
        query = """
            SELECT
                cust_id,
                name,
                address,
                status,
                last_updated
            FROM customer_snapshot
            WHERE last_updated > @previous_watermark
        """

        params = {
            "previous_watermark": previous_watermark
        }

        param_types_dict = {
            "previous_watermark": param_types.TIMESTAMP
        }

        with database.snapshot() as snapshot:

            result = snapshot.execute_sql(
                query,
                params=params,
                param_types=param_types_dict
            )

            rows = list(result)

        if not rows:
            print("No new customer records found")
            return

        changed_count = 0
        new_count = 0

        for row in rows:

            cust_id = row[0]
            new_name = row[1]
            new_address = row[2]
            new_status = row[3]
            source_last_updated = row[4]

            def process_customer(transaction):

                current_query = """
                    SELECT
                        name,
                        address,
                        status
                    FROM customer_info
                    WHERE cust_id = @cust_id
                      AND is_current = TRUE
                """

                current_rows = list(
                    transaction.execute_sql(
                        current_query,
                        params={
                            "cust_id": cust_id
                        },
                        param_types={
                            "cust_id": param_types.INT64
                        }
                    )
                )


                if not current_rows:

                    transaction.insert(
                        table="customer_info",
                        columns=[
                            "cust_id",
                            "name",
                            "address",
                            "status",
                            "last_updated",
                            "effective_date",
                            "end_date",
                            "is_current"
                        ],
                        values=[[
                            cust_id,
                            new_name,
                            new_address,
                            new_status,
                            source_last_updated,
                            spanner.COMMIT_TIMESTAMP,
                            None,
                            True
                        ]]
                    )

                    return "new"


                old_name = current_rows[0][0]
                old_address = current_rows[0][1]
                old_status = current_rows[0][2]

                # Detect change
                changed = (
                    old_name != new_name
                    or old_address != new_address
                    or old_status != new_status
                )

                # No change
                if not changed:
                    return "unchanged"


                transaction.execute_update(
                    """
                    UPDATE customer_info
                    SET
                        end_date = PENDING_COMMIT_TIMESTAMP(),
                        is_current = FALSE
                    WHERE cust_id = @cust_id
                      AND is_current = TRUE
                    """,
                    params={
                        "cust_id": cust_id
                    },
                    param_types={
                        "cust_id": param_types.INT64
                    }
                )


                transaction.insert(
                    table="customer_info",
                    columns=[
                        "cust_id",
                        "name",
                        "address",
                        "status",
                        "last_updated",
                        "effective_date",
                        "end_date",
                        "is_current"
                    ],
                    values=[[
                        cust_id,
                        new_name,
                        new_address,
                        new_status,
                        source_last_updated,
                        spanner.COMMIT_TIMESTAMP,
                        None,
                        True
                    ]]
                )

                return "changed"

            result = database.run_in_transaction(process_customer)

            if result == "new":
                new_count += 1

            elif result == "changed":
                changed_count += 1

        new_watermark = max(row[4] for row in rows)

        Variable.set(
            watermark_var,
            new_watermark.isoformat()
        )

        print(f"New customers: {new_count}")
        print(f"Changed customers: {changed_count}")
        print(f"New watermark: {new_watermark}")

    scd = scd_implement()

    spanner_ins >> scd


myspanner_dag()