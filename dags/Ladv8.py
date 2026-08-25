from airflow.sdk import dag, task
from datetime import datetime
import pandas as pd


file_path = "/Users/prse/PycharmProjects/Airflow_Prac/data/support_tickets-Table 1.csv"

clean_path = "/Users/prse/PycharmProjects/Airflow_Prac/data/cleaned_step.csv"

out_path = "/Users/prse/PycharmProjects/Airflow_Prac/data/cleaned_tickets.csv"


def load_failure_callback(context):
    print("LOAD TASK FAILED!")


@dag(
    dag_id="full_taskflow_logic",
    start_date=datetime(2026, 8, 9),
    schedule="@daily",
    tags=["Taskflow"],
    catchup=False,
)
def my_dag():

    @task
    def extract():
        df = pd.read_csv(file_path)

        return file_path

    @task(retries=2)
    def clean(input_path):
        df = pd.read_csv(input_path)

        new_df = df.dropna(
            subset=["ticket_id", "status"]
        )

        print(new_df)

        new_df.to_csv(clean_path, index=False)

        return clean_path

    @task
    def enrich(input_path):
        df = pd.read_csv(input_path)

        df["is_critical"] = df["status"] == "Critical"

        print(df)

        crit_ticks = int(df["is_critical"].sum())

        df.to_csv(out_path, index=False)

        return out_path, crit_ticks

    @task(on_failure_callback=load_failure_callback)
    def load(data):
        input_path, crit_ticks = data

        df = pd.read_csv(input_path)

        df.to_csv(out_path, index=False)

        return crit_ticks

    @task
    def notify(crit_tks):
        print(f"{crit_tks} critical tickets were found")

    ex = extract()
    cl = clean(ex)
    en = enrich(cl)
    ld = load(en)
    nt = notify(ld)

    ex >> cl >> en >> ld >> nt


my_dag()