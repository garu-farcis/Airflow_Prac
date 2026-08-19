"""BashOperator – Basic file processing
   Create a DAG with a BashOperator that:
   - Uses bash commands (not Python) to:
     • Check if data/support_tickets.xlsx exists
     • Copy it to data/backup/support_tickets_YYYYMMDD.xlsx
       (use Airflow macros for the date)
     • Print the file size of the backup
   Hint: Use `cp`, `ls -lh`, and `{{ ds_nodash }}` or `{{ macros.ds_format(...) }}`."""

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime


with DAG(
    dag_id="support_tickets_backup",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["support"]
) as dag:

    backup_file = BashOperator(
        task_id="backup_support_tickets",
        bash_command="""
        if [ ! -f data/support_tickets.xlsx ]; then
            echo "File not found: data/support_tickets.xlsx"
            exit 1
        fi

        cp data/support_tickets.xlsx data/backup/support_tickets_{{ ds_nodash }}.xlsx

        ls -lh data/backup/support_tickets_{{ ds_nodash }}.xlsx
        """
    )