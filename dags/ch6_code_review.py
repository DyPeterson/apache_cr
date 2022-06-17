import random
from datetime import datetime
from airflow import DAG
from airflow.decorators import dag, task


APPLES = ["pink lady", "jazz", "orange pippin", "granny smith", "red delicious", "gala", "honeycrisp", "mcintosh", "fuji"]

@dag(
    schedule_interval='@once',
    start_date=datetime.utcnow(),
    catchup=False,
    default_view='graph',
    is_paused_upon_creation=True,
    tags=['code_review', 'dsa']
)
def ch6_code_review():

    pass