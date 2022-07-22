from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import os

PROJECT_PATH = 'python3 /home/ubuntu/aws_task3/'
default_args = {"owner": "airflow"}


def notify(context):
    import os
    os.system(PROJECT_PATH + f"part3_sns/main.py --mes 'People card dag at {datetime.utcnow} info: {context}'")


with DAG(
    dag_id="people_cards_dag_not",
    default_args=default_args,
    schedule_interval='0 * * * *',
    start_date=days_ago(2),
    on_failure_callback=notify
) as dag:

    def create_db():
        os.system(PROJECT_PATH + 'part1_db/main.py --create')

    def gen_people_cards():
        os.system(PROJECT_PATH + 'part1_db/main.py --peoplecards 10')

    create_database = PythonOperator(
        task_id='create_db',
        python_callable=create_db,
    )

    generate_people = PythonOperator(
        task_id='gen_people',
        python_callable=gen_people_cards,
    )


    create_database >> generate_people