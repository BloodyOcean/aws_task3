import os
from datetime import datetime
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

PROJECT_PATH = 'python3 /home/ubuntu/aws_task3/'

default_args = {"owner": "airflow"}


@dag(
    'spark_dag',
    default_args=default_args,
    schedule_interval='20 * * * *',
    start_date=days_ago(2),
)
def my_dag_dag():
    
    @task()
    def create_db():
        import os
        os.system(PROJECT_PATH + 'part1_db/main.py --create')

    @task()
    def run_spark():
        import os
        os.system(PROJECT_PATH + 'part2_spark/main.py')

    @task(
        trigger_rule=TriggerRule.ALL_FAILED
    )
    def notify():
        import os
        os.system(PROJECT_PATH + 'part3_sns/main.py --mes Your spark DAG throwed some error.')

    create_db()
    run_spark()


mydag = my_dag_dag()