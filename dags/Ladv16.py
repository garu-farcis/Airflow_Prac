"""Custom Hook (Intermediate → Advanced)
   Create a custom hook called `ExcelTicketHook` that inherits from `BaseHook`.

   Requirements:
   - __init__ receives `file_path` (default to your data/support_tickets.xlsx)
   - Method `get_tickets(sheet_name="support_tickets", status=None, priority=None)`
     → returns a filtered pandas DataFrame
   - Method `get_open_escalated_count()` → returns integer count
   - Method `save_summary(df, output_path)` → saves DataFrame to CSV/Parquet

   Then write a DAG that uses this custom hook inside two @task / PythonOperator:
   - One task gets only Critical + Escalated tickets
   - Another task calculates and saves a region-wise summary"""

