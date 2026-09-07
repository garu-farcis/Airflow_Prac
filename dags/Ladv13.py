"""HttpHook (or requests via Python) + sensor-style check
   Simulate an external API:
   - Write a PythonOperator / @task that uses HttpHook (or plain requests)
     to call a public API (e.g. https://httpbin.org/get or any simple endpoint).
   - Combine the API response with the count of Open tickets from the Excel.
   - Push a combined dictionary to XCom.
   (If you don’t want real HTTP, mock it with a local JSON file and still
    structure the code as if you were using HttpHook.)"""

from airflow.sdk import dag,task,PokeReturnValue
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.http.hooks.http import HttpHook
import pandas as pd
import datetime
from airflow.providers.standard.sensors.filesystem import FileSensor
from datetime import timedelta

fil_path="/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets.xlsx"
@dag(
    dag_id="HttpHook_(or_requests_via_Python)_+_sensor-style_check",
    start_date=datetime.datetime(2027,3,5),
    schedule="@daily",
    catchup=False,
    tags=['hooks'],
)
def my_dag():
    @task.sensor(task_id="check_conn",
        retries=3,
        poke_interval=30,)
    def make_conn():
        hook=HttpHook(
            method='GET',
            http_conn_id="https://httpbin.org/get"
        )
        response=hook.run(
            endpoint='users'
        )
        if response.status_code == 200:
            return PokeReturnValue(
                is_done=True,
                xcom_value=response.json()
            )

        return PokeReturnValue(
            is_done=False
        )

    @task
    def combine(response:str):
        response=response
        df=pd.read_excel(fil_path)
        filtered=df[df['status']=='Open']
        count=len(filtered)
        combined={
            "api response":response,
            "count":count
        }
        print(combined)

        return combined

    api_response = make_conn()

    result = combine(api_response)

    api_response >> result


my_dag()


