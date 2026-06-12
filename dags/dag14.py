import time
from airflow.sdk import dag,task

@task.kubernetes(
    image="python:3.9-slim-buster",
    name="k8s_test",
    namespace="default",
    in_cluster=False,
    config_file="/path/to/.kube/config",
)
def execute_in_k8s_pod():
    import time

    print("Hello from k8s pod")
    time.sleep(2)

@task.kubernetes(image="python:3.9-slim-buster", namespace="default", in_cluster=False)
def print_pattern():
    n = 5
    for i in range(n):
        # inner loop to handle number of columns
        # values changing acc. to outer loop
        for _ in range(i + 1):
            # printing stars
            print("* ", end="")

        # ending line after each row
        print("\r")

execute_in_k8s_pod_instance = execute_in_k8s_pod()
print_pattern_instance = print_pattern()

execute_in_k8s_pod_instance >> print_pattern_instance