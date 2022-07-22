from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import os

PROJECT_PATH = 'python3 /home/ubuntu/aws_task3/'
default_args = {"owner": "airflow"}


def notify(context):
    os.system(PROJECT_PATH + f"part3_sns/main.py --mes 'People card dag at {datetime.utcnow} info: {context}'")


with DAG(
    dag_id="transactions_dag_not",
    default_args=default_args,
    schedule_interval='*/30 * * * *',
    start_date=days_ago(2),
    on_failure_callback=notify
) as dag:

    def create_db():
        os.system(PROJECT_PATH + 'part1_db/main.py --create')

    def gen_transactions():
        os.system(PROJECT_PATH + 'part1_db/main.py --transactions')

    create_database = PythonOperator(
        task_id='create_db',
        python_callable=create_db,
    )

    generate_transactions = PythonOperator(
        task_id='gen_transactions',
        python_callable=gen_transactions,
    )


    create_database >> generate_transactions