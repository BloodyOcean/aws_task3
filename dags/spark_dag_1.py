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
    dag_id="spark_dag_not",
    default_args=default_args,
    schedule_interval='20 * * * *',
    start_date=days_ago(2),
    on_failure_callback=notify
) as dag:

    def create_db():
        os.system(PROJECT_PATH + 'part1_db/main.py --create')

    def run_spark():
        os.system(PROJECT_PATH + 'part2_spark/main.py')

    create_database = PythonOperator(
        task_id='create_db',
        python_callable=create_db,
    )

    start_spark = PythonOperator(
        task_id='run_spark',
        python_callable=run_spark,
    )


    create_database >> start_spark