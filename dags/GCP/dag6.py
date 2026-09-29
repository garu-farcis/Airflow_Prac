"""Full Refresh ETL with Commit Timestamp
   Build a TaskFlow DAG that:
   - Extracts customer metrics from a source (mock BigQuery or GCS CSV)
   - Transforms the data in pandas (calculate average order value, tier)
   - Writes the result to Spanner using insert_or_update + "spanner.commit_timestamp()"
   - Handles empty source data gracefully
   - Logs the number of rows upserted"""


from datetime import datetime,timedelta
from airflow.sdk import dag,task,Variable
from airflow.providers.google.cloud.hooks.spanner import SpannerHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
import pandas as pd
from google.cloud import spanner

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
    def transform(**context):
        hook=BigQueryHook(gcp_conn_id=GCP_CONN_ID)
        projet_id=context['params'].get('project_id')
        client=hook.get_client(project_id=projet_id)
        query = f"select * from `{PROJECT_ID}.my_dataset.cust_info`"
        df=client.query(query).to_dataframe()
        if df.empty:
            print("No records found")
            return
        df['tier']=df['total_amount'].fillna(0).apply(lambda x:'premium' if x>20000 else 'standard')
        df['avg_val']=df['total_amount']/df['total_orders']
        return df.to_dict("records")

    @task(task_id='write_data')
    def write_data(transaction,data_dict):
        project_id=Variable.get('project_id')
        instance_id = Variable.get("gcp_instance_id")
        database_id = Variable.get("gcp_database_id")
        spanner_hook = SpannerHook(
            gcp_conn_id=GCP_CONN_ID
        )

        client = spanner_hook.get_client(
            project_id=project_id
        )

        instance = client.instance(
            instance_id=instance_id
        )

        database = instance.database(
            database_id=database_id
        )
        df=pd.DataFrame(data_dict)
        cols=["cust_id",
                "cust_name",
                "total_orders",
                "total_spent",
                "last_updated","tier",'avg_val']
        values=[]
        for _,row in df.iterrows():
            values.append((
                        int(row["cust_id"]),
                        str(row["cust_name"]),
                        int(row["total_orders"]),
                        float(row["total_spent"]),
                        spanner.COMMIT_TIMESTAMP,
                        str(row['tier']),
                        float(row['avg_val'])
                    ))

            def write_in_transaction(transaction):
                transaction.execute_update(
                    "DELETE FROM cust_info WHERE TRUE"
                )
                transaction.insert_or_update(
                    table="cust_info",
                    columns=cols,
                    values=values,
                )

            database.run_in_transaction(
                write_in_transaction
            )

            no_of_rows_upserts=len(values)
            print(no_of_rows_upserts)

    tt=transform()
    ww=write_data(tt)

    gcs_data>>tt

myspanner_dag()