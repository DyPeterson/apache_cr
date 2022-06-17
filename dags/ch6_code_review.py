import random
from datetime import datetime
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

APPLES = ["pink lady", "jazz", "orange pippin", "granny smith", "red delicious", "gala", "honeycrisp", "mcintosh", "fuji"]

@task
def print_hello():
    """ Function that reads a specific file and prints it back"""
    with open('/dags/ch6_code_review.txt') as file:
        print(file.read())

def string_print(string:str):
    print(string)


@dag(
    schedule_interval='@once',
    start_date=datetime.utcnow(),
    catchup=False,
    default_view='graph',
    is_paused_upon_creation=True,
    tags=['code_review', 'dsa', 'ch6']
)
def ch6_code_review():
    """Placeholder"""
    t1 = BashOperator(
        task_id='echo_to_file',
        bash_command='echo "Dylan" > /dags/ch6_code_review.txt'
    )

    t2 = print_hello

    t3 = BashOperator(
        task_id='pick_3',
        bash_command='echo "Picking three random apples"'
    )

    apple_tasks = []
    for i in range(4):
        apple = random.choice(APPLES)
        task = PythonOperator(
            task_id= f'apple_{i}',
            python_callable= string_print(apple)
        )
        apple_tasks.append(task)
    
    t7 = EmptyOperator(
        task_id='end_transmission'
    )
    t1 >> t2 >> t3 >> apple_tasks >> t7