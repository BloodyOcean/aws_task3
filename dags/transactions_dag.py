import os
from datetime import datetime
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule


PROJECT_PATH = 'python3 /home/ubuntu/aws_task3/'

default_args = {"owner": "airflow"}


@dag(
    'transactions_dag',
    default_args=default_args,
    schedule_interval='*/30 * * * *',
    start_date=days_ago(2),
)
def my_dag_dag():
    
    @task()
    def create_db():
        import os
        os.system(PROJECT_PATH + 'part1_db/main.py --create')

    @task()
    def gen_transactions():
        import os
        os.system(PROJECT_PATH + 'part1_db/main.py --transactions')

    @task(
        trigger_rule=TriggerRule.ALL_FAILED
    )
    def notify():
        import os
        os.system(PROJECT_PATH + 'part3_sns/main.py --mes Your transactions DAG throwed some error.')


    create_db()
    gen_transactions()

mydag = my_dag_dag()