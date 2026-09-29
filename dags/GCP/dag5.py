"""1. Schema Bootstrap & Idempotent Deployment
   Write a Composer DAG that:
   - Creates a Spanner instance (if missing) using SpannerDeployInstanceOperator
   - Creates a database and applies a multi-table schema (customers, orders, order_items)
     with proper interleaving and a commit-timestamp column
   - Uses SpannerUpdateDatabaseInstanceOperator so the DAG is safe to re-run
   - Includes a check that the schema was applied correctly"""

from airflow.sdk import DAG,dag,task
from airflow.providers.google.cloud.operators.spanner import SpannerDeployInstanceOperator,SpannerUpdateDatabaseInstanceOperator,SpannerDeployDatabaseInstanceOperator,SpannerHook
import pandas as pd
from datetime import datetime,timedelta

PROJECT_ID='{{var.value.gcp_project_id}}'
GCP_CONN_ID='google_cloud_default'
INSTANCE_ID='spanner_experiment_instance'
DATABASE_ID='practice_db'

DEFAULT_ARGS={
    "owner":"DAAS",
    "email_on_failure":True,
    "email":"xyz@xyz.com",
    "retries":2,
    "retry_delay":timedelta(seconds=30),
    "retry_exponential_backoff":True
}

@dag(
    dag_id='spanner_practice_bootstrap',
    start_date=datetime(2027,9,10),
    schedule="@daily",
    default_args=DEFAULT_ARGS,
    catchup=False,
    tags=['spanner'],
)
def my_spanner_dag():
    deploy_instance=SpannerDeployInstanceOperator(
        task_id='deploy_spanner_instance',
        gcp_conn_id=GCP_CONN_ID,
        instance_id=INSTANCE_ID,
        project_id=PROJECT_ID,
        config="regional-us-central1",
        node_count=1,
    )

    create_schema=SpannerDeployDatabaseInstanceOperator(
        task_id='deploy_spanner_instance_create_schema',
        gcp_conn_id=GCP_CONN_ID,
        database_id=DATABASE_ID,
        instance_id=INSTANCE_ID,
        project_id=PROJECT_ID,
        ddl_statements=["""
        create table if not exists cust_info(
        cust_id int64 not null,
        cust_name string,
        cust_email string,
        last_updated timestamp not null options(allow_commit_timestamp=True)
        ) primary key(cust_id)
        """],
    )

    dag_update=SpannerUpdateDatabaseInstanceOperator(
        task_id='dag_safe_checks',
        gcp_conn_id=GCP_CONN_ID,
        project_id=PROJECT_ID,
        instance_id=INSTANCE_ID,
        database_id=DATABASE_ID,
        ddl_statements=["""
        create table if not exists order_info(
        cust_id int64 not null,
        order_id int64 not null,
        order_quantity int64,
        status string(40),
        updated timestamp not null options(allow_commit_timestamp=True)
        )primary key(cust_id,order_id)
        interleave in parent cust_info on delete cascade
        """,
                        """
                        create table if not exists order_items(
                        cust_id int64 not null,
                        order_id int64 not null,
                        item_id int64 not null,
                        item_name string,
                        item_price int64,
                        last_update timestamp not null options(allow_commit_timestamp=True)
                        )primary key(cust_id,order_id,item_id)
                        interleave in parent order_info on delete cascade
                        """],
    )

    @task(task_id='check_dag')
    def check_dag():
        hook=SpannerHook(gcp_conn_id=GCP_CONN_ID)
        project_id=hook.project_id
        client=hook.get_client()(project_id=project_id)
        instance=client.instance(instance_id=INSTANCE_ID)
        database=instance.database(database_id=DATABASE_ID)
        query="""select table_name from information_schema.tables where table_schema=''"""
        with database.snapshot() as snapshot:
            results=list(snapshot.execute_sql(query))
            # cols=[field.name for field in results.field]
            # rows=list(results)
            tables={row[0] for row in results}
            exp={'cust_info','order_info','order_items'}
            missed=exp-tables
            if missed:
                raise ValueError("schema does not match")
            print(f"Schema successfully verified! Existing tables: {tables}")
    verify_d=check_dag


    deploy_instance>>create_schema>>dag_update>>verify_d
my_spanner_dag()