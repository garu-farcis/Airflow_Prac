"""BashOperator – Basic file processing
   Create a DAG with a BashOperator that:
   - Uses bash commands (not Python) to:
     • Check if data/support_tickets.xlsx exists
     • Copy it to data/backup/support_tickets_YYYYMMDD.xlsx
       (use Airflow macros for the date)
     • Print the file size of the backup
   Hint: Use `cp`, `ls -lh`, and `{{ ds_nodash }}` or `{{ macros.ds_format(...) }}`."""