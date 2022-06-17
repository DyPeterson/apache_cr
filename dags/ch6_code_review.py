import random
from datetime import datetime
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

APPLES = ["pink lady", "jazz", "orange pippin", "granny smith", "red delicious", "gala", "honeycrisp", "mcintosh", "fuji"]

@task
def print_hello():

    pass


@dag(
    schedule_interval='@once',
    start_date=datetime.utcnow(),
    catchup=False,
    default_view='graph',
    is_paused_upon_creation=True,
    tags=['code_review', 'dsa']
)
def ch6_code_review():
    """Placeholder"""
    t1 = BashOperator(
        task_id='echo_to_file',
        bash_command='echo "Dylan" > /dags/ch6_code_review.txt'
    )

    t3 = BashOperator(
        task_id=''
        bash_command='echo "Picking three random apples"'
    )
    pass