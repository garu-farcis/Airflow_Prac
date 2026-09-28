"""1. TaskFlow API + XCom + Filtering
   Write a complete DAG using the TaskFlow API (@dag and @task) that:

   - Reads a CSV/Excel file containing ticket data
     (columns: ticket_id, status, priority, region, created_date)
   - Filters only tickets where status is "Open" or "Escalated"
   - Calculates the count of Critical tickets among them
   - Pushes both the filtered DataFrame path (or count) and the critical count
   - Has a downstream task that prints:
     “Found X open/escalated tickets, out of which Y are Critical”

   Requirements:
   - Use @task
   - Proper dependency chaining
   - Do not push large DataFrames through XCom """