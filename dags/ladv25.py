"""Cross-DAG Dependency + TriggerDagRunOperator + Conf Passing (Advanced)
   Create two DAGs:

   DAG 1 – "ticket_ingestion":
   - Reads support_tickets.xlsx + new_tickets sheet
   - Appends new tickets and calculates daily metrics
   - At the end uses TriggerDagRunOperator to trigger DAG 2
   - Passes conf containing: {"execution_date": "...", "open_count": N, "escalated_count": M}

   DAG 2 – "ticket_alerting":
   - Is triggered by DAG 1 (can also have its own schedule)
   - Reads the conf passed from DAG 1
   - If escalated_count ≥ 3 → runs an “alert” path
   - Otherwise runs a “normal summary” path
   - Uses BranchPythonOperator based on the received conf"""