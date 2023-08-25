from airflow import DAG
from airflow.decorators import task
from datetime import datetime

@task
def print_hello():
    print("hello!")
    return "hello!"

@task
def print_goodbye():
    print("goodbye!")
    return "govodbye!"

with DAG(
    dag_id = 'HelloWorld_v2',
    start_date = datetime(2023,8,15),
    catchup=False,
    tags=['example'],
    schedule = '0 2 * * *'
) as dag:

    # Assign the tasks to the DAG in order
    print_hello() >> print_goodbye()
