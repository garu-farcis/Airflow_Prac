# import datetime
# import os
# import pendulum
# import requests
#
# from airflow.sdk import dag, task
# from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook
#
#
# @dag(
#     dag_id="process_employees",
#     schedule="0 0 * * *",
#     start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
#     catchup=False,
#     dagrun_timeout=datetime.timedelta(minutes=60),
# )
# def ProcessEmployees():
#     # Create main table
#     create_employees_table = SQLExecuteQueryOperator(
#         task_id="create_employees_table",
#         conn_id="tutorial_pg_conn",
#         sql="""
#             CREATE TABLE IF NOT EXISTS employees (
#                 "Serial Number" NUMERIC PRIMARY KEY,
#                 "Company Name" TEXT,
#                 "Employee Markme" TEXT,
#                 "Description" TEXT,
#                 "Leave" INTEGER
#             );
#         """,
#     )
#
#     # Create temp staging table
#     create_employees_temp_table = SQLExecuteQueryOperator(
#         task_id="create_employees_temp_table",
#         conn_id="tutorial_pg_conn",
#         sql="""
#             DROP TABLE IF EXISTS employees_temp;
#             CREATE TABLE employees_temp (
#                 "Serial Number" NUMERIC PRIMARY KEY,
#                 "Company Name" TEXT,
#                 "Employee Markme" TEXT,
#                 "Description" TEXT,
#                 "Leave" INTEGER
#             );
#         """,
#     )
#
#     @task
#     def get_data():
#         """Download CSV and load into staging table"""
#         data_path = os.path.join(os.path.dirname(__file__), "files", "employees.csv")
#         os.makedirs(os.path.dirname(data_path), exist_ok=True)
#
#         url = "https://raw.githubusercontent.com/apache/airflow/main/airflow-core/docs/tutorial/pipeline_example.csv"
#
#         response = requests.get(url)
#         response.raise_for_status()
#
#         with open(data_path, "w", encoding="utf-8") as file:
#             file.write(response.text)
#
#         # Load CSV into Postgres using copy_expert
#         postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
#         conn = postgres_hook.get_conn()
#         cur = conn.cursor()
#
#         with open(data_path, "r", encoding="utf-8") as file:
#             cur.copy_expert(
#                 "COPY employees_temp FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
#                 file,
#             )
#         conn.commit()
#         cur.close()
#         conn.close()
#
#     @task
#     def merge_data():
#         """Clean and merge data into final table"""
#         query = """
#             INSERT INTO employees
#             SELECT *
#             FROM (
#                 SELECT DISTINCT *
#                 FROM employees_temp
#             ) t
#             ON CONFLICT ("Serial Number") DO UPDATE
#             SET
#                 "Employee Markme" = EXCLUDED."Employee Markme",
#                 "Description" = EXCLUDED."Description",
#                 "Leave" = EXCLUDED."Leave";
#         """
#
#         postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
#         conn = postgres_hook.get_conn()
#         cur = conn.cursor()
#         cur.execute(query)
#         conn.commit()
#         cur.close()
#         conn.close()
#
#     # Task dependencies
#     [create_employees_table, create_employees_temp_table] >> get_data() >> merge_data()
#
#
# # Instantiate the DAG
# dag = ProcessEmployees()


from airflow.sdk import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import pendulum
import os
import requests


@dag(
    dag_id="simple_pipeline",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
)
def simple_pipeline():
    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="tutorial_pg_conn",  # even though it's SQLite now
        sql="""
            CREATE TABLE IF NOT EXISTS employees (
                serial_number INTEGER PRIMARY KEY,
                company_name TEXT,
                employee_markme TEXT,
                description TEXT,
                leave_days INTEGER
            );
        """,
        dag=dag,
    )

    @task
    def download_and_insert():
        url = "https://raw.githubusercontent.com/apache/airflow/main/airflow-core/docs/tutorial/pipeline_example.csv"
        data_path = "/tmp/employees.csv"

        response = requests.get(url)
        with open(data_path, "w") as f:
            f.write(response.text)

        print("✅ Downloaded data successfully")

    create_table >> download_and_insert()


simple_pipeline()