from uuid import uuid4
from google.cloud import spanner

def clean_table(database,pipeline_name,run_id,source_file,cust_id,):
    with database.batch() as batch:
        batch.insert_or_update(
            table='customer_metrics',
            columns=[
                "error_id",
                "pipeline_name",
                "run_id",
                "source_file",
                "cust_id",
                "cust_name",
                "total_orders",
                "total_spent",

        ],
            values=[
                [
                    str(uuid4()),
                    pipeline_name,
                    run_id,
                    source_file,
                    cust_id,
                    spanner.COMMIT_TIMESTAMP,
                ]
            ]
        )
