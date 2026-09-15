"""Dynamic Task Mapping + TaskGroup + Partial Mapping (Advanced)
   Build a DAG that:
   - Extracts unique combinations of (region, priority) from the Excel file
   - Uses dynamic task mapping to create one mapped task per combination
   - Groups the mapped tasks inside a TaskGroup called "region_priority_processing"
   - Each mapped task calculates metrics and writes
     data/region_{region}_priority_{priority}.csv
   - After the TaskGroup, a final task collects all results using XComArg
     and creates a single summary report
   - Limit concurrency of the mapped tasks using a pool"""

import datetime
from airflow.sdk import dag, task, task_group
import pandas as pd


file_p="/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets.xlsx"
@dag(
    dag_id='task_mapping_taskgroup',
    start_date=datetime.datetime(2026,8,19),
    schedule="@monthly",
    catchup=False,
    tags=['hooks'],
)
def my_dag():
        @task
        def extract():
            df=pd.read_excel(file_p)
            file_path=file_p
            data = df[['region', 'priority']].drop_duplicates()
            return {'file_path':file_path,
                    'my_data':data.to_dict("records")}

        @task_group
        def region_priority_processing(file_path,data):
            @task
            def calc_agg(file_path,data):
                df = pd.read_excel(file_path)

                region = data['region']
                priority = data['priority']

                my_data = df[
                    (df['region'] == region) &
                    (df['priority'] == priority)
                    ]
                data_agg = pd.DataFrame({
                    'total_tickets': [len(my_data)]
                })

                return {'data_agg':data_agg.to_dict("records"),
                        'region': region,
                        'priority':priority}
            @task
            def write_file(data):
                data_agg=pd.DataFrame(data['data_agg'])
                region=data['region']
                priority=data['priority']
                fil_path=f"data/region_{region}_priority_{priority}.csv"
                data_agg.to_csv(f"data/region_{region}_priority_{priority}.csv")
                return fil_path
            cal=calc_agg(file_path,data)
            wrt=write_file(cal)

            return wrt

        @task
        def final_task(file_path):
            all_data=[]
            for f in file_path:
                df=pd.read_csv(f)
                all_data.append(f)
                summary = pd.concat(all_data, ignore_index=True)
            print(f"the data is {summary}")

        extr=extract()
        task_gr=region_priority_processing.partial(file_path=extr['file_path']).expand(data=extr['my_data'])

        last=final_task(task_gr)

        extr>>task_gr>>last

my_dag()

