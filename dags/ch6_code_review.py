import random
from datetime import datetime
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

APPLES = ["pink lady", "jazz", "orange pippin", "granny smith", "red delicious", "gala", "honeycrisp", "mcintosh", "fuji"]

def print_hello():
    """ Function that reads a specific file and prints it back"""
    with open('/opt/airflow/dags/ch6_code_review.txt', 'r') as file:
        print(f"Hello {file.read()}! Welcome to the logs!")

def string_print(string):
    print(f"{string}")


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
        bash_command='echo "Dylan" > /opt/airflow/dags/ch6_code_review.txt'
    )

    t2 = PythonOperator(
        task_id="print_hello",
        python_callable=print_hello
    )

    t3 = BashOperator(
        task_id='pick_3',
        bash_command='echo "Picking three random apples"'
    )

    apple_tasks = []
    for i in range(3):
        apple = random.choice(APPLES)
        task = PythonOperator(
            task_id= f'apple_{i}',
            python_callable= string_print,
            op_kwargs= {'string': apple},
        )
        apple_tasks.append(task)
    
    t7 = EmptyOperator(task_id='end_transmission')

    t1 >> t2 >> t3 >> apple_tasks >> t7

dag = ch6_code_review()