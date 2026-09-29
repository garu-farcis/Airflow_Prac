from datetime import datetime,timedelta
from airflow.providers.google.cloud.operators.spanner import SpannerDeployInstanceOperator, SpannerHook,SpannerQueryDatabaseInstanceOperator, SpannerDeployDatabaseInstanceOperator
from airflow.sdk import task,dag,task_group,DAG
import pandas as pd

GCP_CONN_ID='google_cloud_default'
INSTANCE_ID='experiment_spanner_instance'
DATABASE_ID='analytics_db'

DEFAULT_ARGS={
    'owner':'DAAS',
    'email_on_failure':True,
    'email':'myname@xyz.com',
    'depends_on_past':False,
    'retries':2,
    'retry_delay':timedelta(minutes=3)
}
with DAG(
    dag_id='spanner_etl_pipeline',
    start_date=datetime(2027,10,9),
    schedule='@daily',
    default_args=DEFAULT_ARGS,
    catchup=False,
    tags=['spanner']
) as dag:
    create_schema=SpannerDeployDatabaseInstanceOperator(
        task_id='making_schema',
        gcp_conn_id=GCP_CONN_ID,
        instance_id=INSTANCE_ID,
        database_id=DATABASE_ID,
        ddl_statements=["""
        create table if not exists cust_metrics(
        cust_id int64 not null,
        cust_name string(30),
        total_orders int64,
        total_amount int64,
        last_updated timestamp not null options(allow_commit_timestamp=True)
        
        ) primary key(cust_id)"""],
    )

    @task
    def extract():
        hook=SpannerHook(gcp_conn_id=GCP_CONN_ID)
        project_id = hook.project_id
        client=hook.get_client(project_id=project_id)
        instance=client.instance(INSTANCE_ID)
        database=instance.database(DATABASE_ID)
        query='select * from cust_metrics'
        with database.snapshot() as snapshot:
            results=snapshot.execute_sql(query)
            rows=list(results)
            cols=[field.name for field in results.metadata.row_type.fields]
            df=pd.DataFrame(rows,columns=cols)
            df['avg_order_value'] = df['total_amount'] / df['total_orders']
            df['customer_tier'] = df['total_amount'].apply(
                lambda x: 'VIP' if x >= 10000 else 'Standard'
            )
            tier_summary = df.groupby('customer_tier').agg(
                total_revenue=('total_amount', 'sum'),
                customer_count=('cust_id', 'count')
            ).reset_index()

            print("Transformed DataFrame:\n", df)
            print("Tier Summary:\n", tier_summary)
            df['last_updated']="spanner.commit_timestamp()"
            columns_to_save = ['cust_id', 'cust_name', 'total_orders', 'total_amount', 'last_updated']
            df_to_save=df[columns_to_save]
            vals_to_save=df_to_save.values.to_list()
            with database.batch() as batch:
                batch.insert_or_update(
                    table='cust_metrics',
                    columns=columns_to_save,
                    values=vals_to_save
                )

    create_schema>>extract
