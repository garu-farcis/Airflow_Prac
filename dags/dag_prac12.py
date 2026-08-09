"""Build a multi-step ETL pipeline:
    extract (read CSV) → clean (remove Cancelled + Pending) →
    enrich (add a column total_amount = quantity * unit_price) →
    load (write to parquet or another CSV)
    Use @task decorator (TaskFlow API) style."""