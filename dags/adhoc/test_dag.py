from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from dags.utils.notifiers.tg import TelegramNotifier
from dags.utils.cfg.configs import Config as cfg
import datetime


default_args = {'retries': 5,
                'email_on_failure': False,
                'email_on_success': False,
                'depends_on_past': False,
                'retry_delay': datetime.timedelta(minutes=5),
                'owner': 'alexchikov'}

with DAG(dag_id='test_dag',
         description='This is my test DAG',
         schedule='30 22 * * *',
         default_args=default_args,
         catchup=False,
         start_date=datetime.datetime(2024, 7, 15)) as dag:
    task1 = PythonOperator(task_id='python_func',
                           python_callable=print,
                           op_args=["Hello World"],
                           on_success_callback=TelegramNotifier('Yeeah!',
                                                                cfg.get('TOKEN'),
                                                                cfg.get('CHAT_ID')),
                           on_failure_callback=TelegramNotifier(':(',
                                                                cfg.get('TOKEN'),
                                                                cfg.get('CHAT_ID')))

    task2 = BashOperator(task_id='some_bash',
                         bash_command='echo 1',
                         on_success_callback=TelegramNotifier('Yeeyy, success :)',
                                                              cfg.get('TOKEN'),
                                                              cfg.get('CHAT_ID')))

    task1 >> task2

if __name__ == '__main__':
    dag.test()