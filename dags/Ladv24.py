"""Setup / Teardown Tasks + Complex Trigger Rules (Advanced)
   Design a DAG with the following structure:

   setup_task (creates a temporary working directory + copies the Excel file)
     ├── validation_group (TaskGroup with 2 parallel quality checks)
     ├── processing_group (TaskGroup that filters & enriches data)
     └── teardown_task (deletes the temporary directory)

   Requirements:
   - setup_task must run first
   - teardown_task must run with trigger_rule="all_done"
     (even if validation or processing fails)
   - processing_group should only run if all validations succeed
     (use trigger_rule or dependency carefully)
   - Use Airflow’s setup/teardown feature (or emulate it cleanly)"""


from airflow.sdk import dag, task, Asset, setup, teardown, task_group
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow import Dataset
import pandas as pd
import datetime
import os
from datetime import timedelta


file_path="/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets.csv"
@dag(
    dag_id='setup_teardown_task',
    start_date=datetime.datetime(2027,9,10),
    schedule="@daily",
    catchup=False,
    tags=['hooks'],
)
def my_dag():
    @setup
    def create_temp():
        temp_dir="/Users/prse/PycharmProjects/Airflow_Prac/data/"
        os.makedirs(temp_dir, exist_ok=True)
        temp_file = os.path.join(temp_dir, "support_tickets.csv")
        f_path=file_path
        df=pd.read_csv(f_path)
        df.to_csv(temp_file,index=False)
        return temp_dir

    cc = create_temp()
    @task_group
    def validation_group(file_path):
        @task
        def validate_sat_score(file_path):
            temp_file = os.path.join(file_path, "support_tickets.csv")
            df=pd.read_csv(temp_file)
            filter_data=df[~df['satisfaction_score'].isna()]
            if filter_data:
                print("test passed")
                return file_path
            return ValueError("Validation failed")
        @task
        def validate_sat_score1(file_path):

            df=pd.read_csv(file_path)
            filter_data=df[~df['resolved_date'].isna()]
            if filter_data:
                print("test passed")
                return file_path
            return ValueError("Validation failed")

        vs=validate_sat_score(file_path)
        vcc=validate_sat_score1(file_path)
        file_path >> [vs, vcc]
        return [vs, vcc]

    @task_group
    def processing_group(file_path):
        @task
        def processing(file_path):
            df=pd.read_csv(file_path)
            enrich_data=df[df['satisfaction_score'].isna()].fillna(0)
            return {'file_path':file_path,
                'enrich_data':enrich_data.to_dict("records")}
        @task
        def processed(file_path,data):
            my_data=pd.DataFrame(data['enrich_data'])
            filter_data=my_data[my_data['resolved_date'].isna()].fillna('NA')
            print(f"final filetered data is {filter_data}")
            return { 'file_path':file_path,
                'summary':filter_data.to_dict("records")}

        pp = processing(file_path)

        pr = processed(
            file_path=file_path,
            data=pp
        )
        pp>>pr
        return pr
    @teardown
    def final_task(file_path,data):
        my_data=pd.DataFrame(data['summary'])
        print(f"final filetered data is {my_data}")
        if os.path.exists(file_path):
            import shutil
            shutil.rmtree(file_path)
        print("file removed")

    vg=validation_group(cc)
    pg=processing_group(vg)
    vg >> pg
    ft = final_task(file_path=cc,data=pg)
    pg>>ft


my_dag()








