"""Custom Sensor (Advanced)
   Create a custom sensor `NewCriticalTicketSensor` that inherits from `BaseSensorOperator`.

   Behaviour:
   - It should poke the support_tickets.xlsx (or a new_tickets file)
   - Return True only when there is at least one ticket that is:
        priority == "Critical" AND status in ["Open", "Escalated"]
   - Support poke_interval and timeout
   - Optionally accept a `min_count` parameter (default=1)

   Then build a small DAG:"""
