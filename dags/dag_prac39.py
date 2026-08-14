"""Implement a FileSensor + processing pattern:

   - Sensor waits for the file data/new_tickets.xlsx (timeout 10 min, poke every 30s)
   - Downstream PythonOperator reads both sheets ("support_tickets" and "new_tickets")
   - Appends the new tickets to the historical data
   - Writes the combined result to data/all_tickets_combined.csv
   - Pushes total row count to XCom"""