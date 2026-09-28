"""
Spanner ETL pipeline – Airflow 3.x (Task SDK) compatible
Works with apache-airflow-providers-google >= 22.x
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import pandas as pd
from airflow.sdk import dag, task, task_group, chain, TriggerRule
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.google.cloud.hooks.spanner import SpannerHook
from airflow.providers.google.cloud.operators.spanner import (
    SpannerQueryDatabaseInstanceOperator,
)
from google.cloud.spanner_v1 import param_types

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
GCP_CONN_ID = "google_cloud_default"
PROJECT_ID = "{{ var.value.gcp_project_id }}"
INSTANCE_ID = "{{ var.value.spanner_instance_id }}"
DATABASE_ID = "{{ var.value.spanner_database_id }}"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

@dag(
    dag_id="spanner_etl_airflow3",
    description="Extract → Transform → Load for Cloud Spanner (Airflow 3.x)",
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "spanner", "gcp"],
)
def spanner_etl():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    # -----------------------------------------------------------------------
    # EXTRACT
    # -----------------------------------------------------------------------
    @task(task_id="extract_orders")
    def extract_orders() -> list[dict[str, Any]]:
        """Pull incremental data from Spanner using SpannerHook + snapshot."""
        hook = SpannerHook(gcp_conn_id=GCP_CONN_ID)
        client = hook.get_conn(project_id=PROJECT_ID)
        database = client.instance(INSTANCE_ID).database(DATABASE_ID)

        sql = """
            SELECT
                order_id,
                customer_id,
                order_date,
                amount,
                status,
                updated_at
            FROM orders
            WHERE updated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
        """

        rows: list[dict[str, Any]] = []
        with database.snapshot() as snapshot:
            results = snapshot.execute_sql(sql)
            columns = [field.name for field in results.fields]
            for row in results:
                rows.append(dict(zip(columns, row)))

        print(f"Extracted {len(rows)} rows")
        return rows

    # -----------------------------------------------------------------------
    # TRANSFORM
    # -----------------------------------------------------------------------
    @task(task_id="transform_orders")
    def transform_orders(raw_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not raw_rows:
            return []

        df = pd.DataFrame(raw_rows)

        df = df.dropna(subset=["order_id", "customer_id"])
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
        df["amount_usd"] = df["amount"] * 1.08
        df["is_high_value"] = df["amount"] > 1000
        df["order_month"] = pd.to_datetime(df["order_date"]).dt.to_period("M").astype(str)

        df = (
            df.sort_values("updated_at")
            .drop_duplicates(subset=["order_id"], keep="last")
        )

        transformed = df.to_dict(orient="records")
        print(f"Transformed {len(transformed)} rows")
        return transformed

    # -----------------------------------------------------------------------
    # LOAD
    # -----------------------------------------------------------------------
    @task_group(group_id="load")
    def load_group(transformed_rows: list[dict[str, Any]]):

        @task(task_id="load_via_dml")
        def load_via_dml(rows: list[dict[str, Any]]) -> str:
            if not rows:
                return "nothing_to_load"

            hook = SpannerHook(gcp_conn_id=GCP_CONN_ID)
            client = hook.get_conn(project_id=PROJECT_ID)
            database = client.instance(INSTANCE_ID).database(DATABASE_ID)

            def dml_upsert(transaction):
                for r in rows:
                    transaction.execute_update(
                        """
                        INSERT OR UPDATE orders_staging (
                            order_id, customer_id, order_date, amount,
                            amount_usd, is_high_value, order_month, updated_at
                        ) VALUES (
                            @order_id, @customer_id, @order_date, @amount,
                            @amount_usd, @is_high_value, @order_month, @updated_at
                        )
                        """,
                        params={
                            "order_id": r["order_id"],
                            "customer_id": r["customer_id"],
                            "order_date": r["order_date"],
                            "amount": float(r["amount"]),
                            "amount_usd": float(r["amount_usd"]),
                            "is_high_value": bool(r["is_high_value"]),
                            "order_month": r["order_month"],
                            "updated_at": r["updated_at"],
                        },
                        param_types={
                            "order_id": param_types.STRING,
                            "customer_id": param_types.STRING,
                            "order_date": param_types.DATE,
                            "amount": param_types.FLOAT64,
                            "amount_usd": param_types.FLOAT64,
                            "is_high_value": param_types.BOOL,
                            "order_month": param_types.STRING,
                            "updated_at": param_types.TIMESTAMP,
                        },
                    )

            database.run_in_transaction(dml_upsert)
            return f"loaded_{len(rows)}_rows"

        # Optional: pure DML cleanup via the official operator
        cleanup = SpannerQueryDatabaseInstanceOperator(
            task_id="cleanup_old_rows",
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            database_id=DATABASE_ID,
            query=[
                "DELETE FROM orders_staging WHERE order_date < DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)"
            ],
            gcp_conn_id=GCP_CONN_ID,
        )

        loaded = load_via_dml(transformed_rows)
        loaded >> cleanup
        return loaded

    # -----------------------------------------------------------------------
    # DATA QUALITY
    # -----------------------------------------------------------------------
    @task(task_id="data_quality")
    def data_quality_check():
        hook = SpannerHook(gcp_conn_id=GCP_CONN_ID)
        client = hook.get_conn(project_id=PROJECT_ID)
        database = client.instance(INSTANCE_ID).database(DATABASE_ID)

        with database.snapshot() as snapshot:
            bad_amount = list(
                snapshot.execute_sql(
                    "SELECT COUNT(*) FROM orders_staging WHERE amount < 0"
                )
            )[0][0]
            null_ids = list(
                snapshot.execute_sql(
                    "SELECT COUNT(*) FROM orders_staging WHERE order_id IS NULL"
                )
            )[0][0]

        if bad_amount > 0 or null_ids > 0:
            raise ValueError(
                f"DQ failed: negative amounts={bad_amount}, null ids={null_ids}"
            )
        return "dq_passed"

    # -----------------------------------------------------------------------
    # Wiring
    # -----------------------------------------------------------------------
    extracted = extract_orders()
    transformed = transform_orders(extracted)
    loaded = load_group(transformed)
    dq = data_quality_check()

    chain(start, extracted, transformed, loaded, dq, end)

spanner_etl()