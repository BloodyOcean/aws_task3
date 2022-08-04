from datetime import datetime, timedelta
from re import sub
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
import os
import subprocess

PROJECT_PATH = 'python3 /home/ubuntu/aws_task3/'


def notify(context):
    os.system(PROJECT_PATH + f"part3_sns/main.py --mes 'Spark dag at {datetime.utcnow()} info: {context}'")


with DAG(
    catchup=False,
    dag_id="spark_dag_not",
    default_args={'on_failure_callback':notify},
    schedule_interval='20 * * * *',
    start_date=days_ago(2),
) as dag:

    def create_db():
        subprocess.run(['python3', '/home/ubuntu/aws_task3/part1_db/main.py', '--create'], check=True)

    def run_spark():
        subprocess.run(['python3', '/home/ubuntu/aws_task3/part2_spark/main.py'], check=True)

    create_database = PythonOperator(
        task_id='create_db',
        provide_context=True,
        python_callable=create_db,
    )

    start_spark = PythonOperator(
        task_id='run_spark',
        provide_context=True,
        python_callable=run_spark,
    )


    create_database >> start_spark