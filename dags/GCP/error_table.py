from uuid import uuid4
from google.cloud import spanner

def error_table(database,pipeline_name,run_id,source_file,error_type,error_message,error_column,error_value):
    with database.batch() as batch:
        batch.insert_or_update(
            table='customer_metrics',
            columns=[
                "error_id",
                "pipeline_name",
                "run_id",
                "source_file",
                "cust_id",
                "error_type",
                "error_message",
                "error_column",
                "error_value",
                "error_timestamp",
        ],
            values=[
                [
                    str(uuid4()),
                    pipeline_name,
                    run_id,
                    source_file,
                    error_type,
                    error_message,
                    error_column,
                    error_value,
                    spanner.COMMIT_TIMESTAMP,
                ]
            ]
        )
