import random
from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

APPLES = ["pink lady", "jazz", "orange pippin", "granny smith", "red delicious", "gala", "honeycrisp", "mcintosh", "fuji"]

def print_hello():
    """Function that reads a specific file and prints it back"""
    # Opens the .txt file created by the BashOperator
    with open('/opt/airflow/dags/ch6_code_review.txt', 'r') as file:
        #Print the following fstring which calls on txt files contents
        print(f"Hello {file.read()}! Welcome to the logs!")

def print_pt2(string):
    """
    Renaming the print function so it can be a task.

    `print(value, ..., sep=' ', end='\n', file=sys.stdout, flush=False)`

    Prints the values to a stream, or to sys.stdout by default.
    Optional keyword arguments:
    - file: a file-like object (stream); defaults to the current sys.stdout.
    - sep: string inserted between values, default a space.
    - end: string appended after the last value, default a newline.
    - flush: whether to forcibly flush the stream.
    """
    # prints the inputted parameter
    print(f"{string}")

@dag(
    # Run this DAG only once, or when started manually
    schedule_interval='@once',
    # start date set to now
    start_date=datetime.utcnow(),
    # if start date was earlier, do not run tasks until it's caught up
    catchup=False,
    # when clicking the dag name in the GUI the first tab viewed is graph
    default_view='graph',
    # When created pause this dag
    is_paused_upon_creation=True,
    # Tags to help find this dag, doesn't actually help in this instance since there is only one DAG using this tag notation
    tags=['code_review', 'dsa', 'ch6']
)
def ch6_code_review():
    """
    DAG that does the following tasks:
    1. BashOperator to `echo "Dylan" > /opt/airflow/dags/ch6_code_review.txt`.
    1. PythonOperator to print the contents of the .txt file with a greeting to the console.
    1. 3 simultaneous PythonOperators that print a random value the list defined in APPLES. 
    1. EmptyOperator which does nothing but demonstrate the end of the DAG.
    """
    # task to run a bash command
    t1 = BashOperator(
        task_id='echo_to_file',
        bash_command='echo "Dylan" > /opt/airflow/dags/ch6_code_review.txt'
    )
    # defines a task to run our previously defined print_hello function
    t2 = PythonOperator(
        task_id="print_hello",
        python_callable=print_hello
    )
    # task to run a bash command
    t3 = BashOperator(
        task_id='pick_3',
        bash_command='echo "Picking three random apples"'
    )
    # empty list that will later be appended to
    apple_tasks = []
    for i in range(3):
        # setting apple variable to a random item from the APPLES list
        apple = random.choice(APPLES)
        # Defining our task as a PythonOperator
        task = PythonOperator(
            # setting our task id to the position it is at in the for loop, either 0,1,2
            task_id= f'apple_{i}',
            # setting the python function for this task to print_pt2
            python_callable= print_pt2,
            # giving our print_pt2 parameter apple
            op_kwargs= {'string': apple},
        )
        # append the task to apple_tasks
        apple_tasks.append(task)
    # Demonstrates the end of our dag with end_transmission tag
    t7 = EmptyOperator(task_id='end_transmission')
    # setting out task dependencies 
    t1 >> t2 >> t3 >> apple_tasks >> t7

dag = ch6_code_review()